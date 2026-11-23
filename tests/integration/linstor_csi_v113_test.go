//go:build integration

// SPDX-License-Identifier: Apache-2.0

/*
Copyright 2026 Cozystack contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package integration

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	lapiconsts "github.com/LINBIT/golinstor"
	lapi "github.com/LINBIT/golinstor/client"
	"github.com/LINBIT/golinstor/clonestatus"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/tests/integration/harness"
)

// The calls below are the ones linstor-csi v1.13 (pkg/client/linstor.go on
// master) makes, in its order and with its arguments, through the golinstor
// it pins (v0.65.4). Each helper names the driver function it stands for.
// The driver itself is not imported: it drags the CSI and Kubernetes client
// stacks in, and every request it makes goes through these golinstor calls.

const (
	v113RG       = "sc-v113"
	v113Pool     = "zfs-thin"
	v113SizeKiB  = 1024 * 1024
	v113Timeout  = 60 * time.Second
	v113KeyCSI   = "Aux/csi-provisioning-completed-by"
	v113TwoPrims = "DrbdOptions/Net/allow-two-primaries"
)

// nil404 is linstor-csi's: a 404 is success for a delete.
func nil404(err error) error {
	if errors.Is(err, lapi.NotFoundError) {
		return nil
	}

	return err
}

// TestLinstorCSIv113 drives the controller-side calls of linstor-csi v1.13
// against the real store, reconcilers and REST server: start-up, CreateVolume,
// topology, Attach and Detach of an RWX block volume, ControllerExpandVolume,
// CreateSnapshot, CreateVolume from a snapshot and from a volume,
// DeleteSnapshot and DeleteVolume, with and without snapshots left.
func TestLinstorCSIv113(t *testing.T) {
	stack := harness.StartStack(t)
	harness.SeedThreeNodeCluster(t, stack)

	c := harness.NewCSI(t, stack).Client
	ctx := context.Background()

	// cmd/linstor-csi: the driver refuses to start below REST 1.29.
	t.Run("EnsureMinimumApiVersion", func(t *testing.T) {
		version, err := c.Controller.GetVersion(ctx)
		if err != nil {
			t.Fatalf("GetVersion: %v", err)
		}

		var major, minor int
		if _, err := fmt.Sscanf(version.RestApiVersion, "%d.%d", &major, &minor); err != nil {
			t.Fatalf("parse %q: %v", version.RestApiVersion, err)
		}

		if major != 1 || minor < 29 {
			t.Fatalf("linstor-csi v1.13 refuses LINSTOR %s (REST API %s)", version.Version, version.RestApiVersion)
		}
	})

	t.Run("CreateVolume", func(t *testing.T) {
		v113CreateVolume(ctx, t, stack, c, "pvc-v113-a")

		// A concurrent CreateVolume loses every create race to the
		// first one; linstor-csi tells that from a failure by the code.
		err := c.ResourceGroups.Create(ctx, lapi.ResourceGroup{Name: v113RG})
		if !errors.Is(err, lapi.ApiCallRcErr(lapiconsts.FailExistsRscGrp)) {
			t.Errorf("second resource group create: %v, want FAIL_EXISTS_RSC_GRP", err)
		}

		err = c.ResourceDefinitions.Create(ctx, lapi.ResourceDefinitionCreate{
			ResourceDefinition: lapi.ResourceDefinition{Name: "pvc-v113-a", ResourceGroupName: v113RG},
		})
		if !errors.Is(err, lapi.ApiCallRcErr(lapiconsts.FailExistsRscDfn)) {
			t.Errorf("second resource definition create: %v, want FAIL_EXISTS_RSC_DFN", err)
		}

		zero := int32(0)

		err = c.ResourceDefinitions.CreateVolumeDefinition(ctx, "pvc-v113-a", lapi.VolumeDefinitionCreate{
			VolumeDefinition: lapi.VolumeDefinition{VolumeNumber: &zero, SizeKib: v113SizeKiB},
		})
		if !errors.Is(err, lapi.ApiCallRcErr(lapiconsts.FailExistsVlmDfn)) {
			t.Errorf("second volume definition create: %v, want FAIL_EXISTS_VLM_DFN", err)
		}
	})

	// HighLevelClient.GenericAccessibleTopologies: the nodes a volume is
	// deployed on, asked by name.
	t.Run("AccessibleTopologies", func(t *testing.T) {
		diskful := v113DiskfulNodes(ctx, t, c, "pvc-v113-a")

		nodes, err := c.Nodes.GetAll(ctx, &lapi.ListOpts{Node: diskful})
		if err != nil {
			t.Fatalf("Nodes.GetAll: %v", err)
		}

		got := make([]string, 0, len(nodes))
		for i := range nodes {
			got = append(got, nodes[i].Name)
		}

		slices.Sort(got)

		if !slices.Equal(got, diskful) {
			t.Errorf("nodes asked by name: got %v, want %v", got, diskful)
		}
	})

	// HighLevelClient.ReservedCapacity, which GetCapacity runs for a
	// StorageClass with overProvision: what the volumes in a pool have
	// promised, from their STORAGE layer data.
	t.Run("ReservedCapacity", func(t *testing.T) {
		cached := true

		pools, err := c.Nodes.GetStoragePoolView(ctx, &lapi.ListOpts{Cached: &cached, StoragePool: []string{v113Pool}})
		if err != nil || len(pools) == 0 {
			t.Fatalf("GetStoragePoolView: %d pools, %v", len(pools), err)
		}

		nodes := make([]string, 0, len(pools))
		for i := range pools {
			nodes = append(nodes, pools[i].NodeName)
		}

		// The satellites report the volumes right after the replicas
		// come up.
		reserved := func() int64 {
			reses, err := c.Resources.GetResourceView(ctx, &lapi.ListOpts{Node: nodes, StoragePool: []string{v113Pool}})
			if err != nil {
				t.Fatalf("GetResourceView: %v", err)
			}

			var sum int64

			for i := range reses {
				if reses[i].Name != "pvc-v113-a" {
					continue
				}

				for j := range reses[i].Volumes {
					layers := reses[i].Volumes[j].LayerDataList
					if len(layers) == 0 {
						continue
					}

					if sv, ok := layers[len(layers)-1].Data.(*lapi.StorageVolume); ok {
						sum += sv.UsableSizeKib
					}
				}
			}

			return sum
		}

		harness.Eventually(t, v113Timeout, func() bool { return reserved() >= 2*v113SizeKiB },
			"the two replicas of pvc-v113-a to reserve their size in "+v113Pool)
	})

	t.Run("AttachDetachRWXBlock", func(t *testing.T) {
		v113AttachDetach(ctx, t, stack, c, "pvc-v113-a")
	})

	// Linstor.Resize.
	t.Run("ControllerExpandVolume", func(t *testing.T) {
		vd, err := c.ResourceDefinitions.GetVolumeDefinition(ctx, "pvc-v113-a", 0)
		if err != nil {
			t.Fatalf("GetVolumeDefinition: %v", err)
		}

		want := vd.SizeKib * 2

		err = c.ResourceDefinitions.ModifyVolumeDefinition(ctx, "pvc-v113-a", 0, lapi.VolumeDefinitionModify{SizeKib: want})
		if err != nil {
			t.Fatalf("ModifyVolumeDefinition: %v", err)
		}

		vd, err = c.ResourceDefinitions.GetVolumeDefinition(ctx, "pvc-v113-a", 0)
		if err != nil || vd.SizeKib != want {
			t.Errorf("after expand: size %d (%v), want %d", vd.SizeKib, err, want)
		}
	})

	t.Run("CreateSnapshot", func(t *testing.T) {
		v113CreateSnapshot(ctx, t, c, "pvc-v113-a", "snap-v113-a")
	})

	t.Run("CreateVolumeFromSnapshot", func(t *testing.T) {
		v113VolFromSnap(ctx, t, c, "pvc-v113-a", "snap-v113-a", "pvc-v113-restored")
	})

	t.Run("CreateVolumeFromVolume", func(t *testing.T) {
		v113Clone(ctx, t, c, "pvc-v113-a", "pvc-v113-clone")
	})

	// DeleteSnapshot of a volume that is still there keeps its definition.
	t.Run("DeleteSnapshotOfALiveVolume", func(t *testing.T) {
		v113DeleteSnapshot(ctx, t, c, "pvc-v113-a", "snap-v113-a")

		if _, err := c.ResourceDefinitions.Get(ctx, "pvc-v113-a"); err != nil {
			t.Errorf("the live volume's definition: %v", err)
		}
	})

	// DeleteVolume of a volume with a snapshot keeps the definition for
	// the snapshot, hidden behind deleted volume definitions; the
	// snapshot's DeleteSnapshot then takes the definition along.
	t.Run("DeleteVolumeWithSnapshot", func(t *testing.T) {
		v113CreateSnapshot(ctx, t, c, "pvc-v113-restored", "snap-v113-b")
		v113DeleteVolume(ctx, t, c, "pvc-v113-restored")

		if _, err := c.ResourceDefinitions.Get(ctx, "pvc-v113-restored"); err != nil {
			t.Fatalf("the definition a snapshot keeps: %v", err)
		}

		vds, err := c.ResourceDefinitions.GetVolumeDefinitions(ctx, "pvc-v113-restored")
		if err != nil || len(vds) != 0 {
			t.Errorf("volume definitions of a deleted volume: %v (%v), want none", vds, err)
		}

		v113DeleteSnapshot(ctx, t, c, "pvc-v113-restored", "snap-v113-b")

		if _, err := c.ResourceDefinitions.Get(ctx, "pvc-v113-restored"); !errors.Is(err, lapi.NotFoundError) {
			t.Errorf("the definition after its last snapshot went: %v, want 404", err)
		}
	})

	t.Run("DeleteVolume", func(t *testing.T) {
		v113DeleteVolume(ctx, t, c, "pvc-v113-clone")

		if _, err := c.ResourceDefinitions.Get(ctx, "pvc-v113-clone"); !errors.Is(err, lapi.NotFoundError) {
			t.Errorf("the deleted volume's definition: %v, want 404", err)
		}

		// The external-provisioner retries a DeleteVolume that timed out.
		v113DeleteVolume(ctx, t, c, "pvc-v113-clone")

		v113DeleteVolume(ctx, t, c, "pvc-v113-a")

		// The last volume of the class is gone, and so is its group.
		if _, err := c.ResourceGroups.Get(ctx, v113RG); !errors.Is(err, lapi.NotFoundError) {
			t.Errorf("the unused resource group: %v, want 404", err)
		}
	})
}

// v113ReconcileResourceGroup is Linstor.reconcileResourceGroup.
func v113ReconcileResourceGroup(ctx context.Context, t *testing.T, c *lapi.Client) {
	t.Helper()

	_, err := c.ResourceGroups.Get(ctx, v113RG)
	if errors.Is(err, lapi.NotFoundError) {
		err = c.ResourceGroups.Create(ctx, lapi.ResourceGroup{
			Name:  v113RG,
			Props: map[string]string{},
		})
		if err != nil && !errors.Is(err, lapi.ApiCallRcErr(lapiconsts.FailExistsRscGrp)) {
			t.Fatalf("ResourceGroups.Create: %v", err)
		}
	} else if err != nil {
		t.Fatalf("ResourceGroups.Get: %v", err)
	}

	place := int32(2)

	err = c.ResourceGroups.Modify(ctx, v113RG, lapi.ResourceGroupModify{
		SelectFilter: lapi.AutoSelectFilter{PlaceCount: place, StoragePool: v113Pool},
	})
	if err != nil {
		t.Fatalf("ResourceGroups.Modify: %v", err)
	}

	if _, err := c.ResourceGroups.Get(ctx, v113RG); err != nil {
		t.Fatalf("ResourceGroups.Get: %v", err)
	}
}

// v113ReconcileResourceDefinition is Linstor.reconcileResourceDefinition.
func v113ReconcileResourceDefinition(ctx context.Context, t *testing.T, c *lapi.Client, rd string) {
	t.Helper()

	got, err := c.ResourceDefinitions.Get(ctx, rd)
	if errors.Is(err, lapi.NotFoundError) {
		err = c.ResourceDefinitions.Create(ctx, lapi.ResourceDefinitionCreate{
			ResourceDefinition: lapi.ResourceDefinition{Name: rd, ResourceGroupName: v113RG},
		})
		if err != nil && !errors.Is(err, lapi.ApiCallRcErr(lapiconsts.FailExistsRscDfn)) {
			t.Fatalf("ResourceDefinitions.Create(%s): %v", rd, err)
		}

		got, err = c.ResourceDefinitions.Get(ctx, rd)
	}

	if err != nil {
		t.Fatalf("ResourceDefinitions.Get(%s): %v", rd, err)
	}

	if slices.Contains(got.Flags, lapiconsts.FlagDelete) || got.ResourceGroupName != v113RG {
		t.Fatalf("definition %s: flags %v, group %q", rd, got.Flags, got.ResourceGroupName)
	}
}

// v113CreateVolume is Linstor.Create with the autoplace scheduler.
func v113CreateVolume(ctx context.Context, t *testing.T, stack *harness.Stack, c *lapi.Client, rd string) {
	t.Helper()

	v113ReconcileResourceGroup(ctx, t, c)
	v113ReconcileResourceDefinition(ctx, t, c, rd)

	// reconcileVolumeDefinition.
	_, err := c.ResourceDefinitions.GetVolumeDefinition(ctx, rd, 0)
	if !errors.Is(err, lapi.NotFoundError) {
		t.Fatalf("GetVolumeDefinition of a new volume: %v, want 404", err)
	}

	zero := int32(0)

	err = c.ResourceDefinitions.CreateVolumeDefinition(ctx, rd, lapi.VolumeDefinitionCreate{
		VolumeDefinition: lapi.VolumeDefinition{VolumeNumber: &zero, SizeKib: v113SizeKiB},
	})
	if err != nil {
		t.Fatalf("CreateVolumeDefinition: %v", err)
	}

	if _, err := c.ResourceDefinitions.GetVolumeDefinition(ctx, rd, 0); err != nil {
		t.Fatalf("GetVolumeDefinition: %v", err)
	}

	// reconcileResourcePlacement, autoplace scheduler.
	if err := c.Resources.Autoplace(ctx, rd, lapi.AutoPlaceRequest{}); err != nil {
		t.Fatalf("Autoplace: %v", err)
	}

	err = c.ResourceDefinitions.Modify(ctx, rd, lapi.GenericPropsModify{
		OverrideProps: map[string]string{v113KeyCSI: "linstor-csi"},
	})
	if err != nil {
		t.Fatalf("ResourceDefinitions.Modify: %v", err)
	}

	v113WaitUpToDate(ctx, t, stack, rd, 2)
}

// v113WaitUpToDate waits for want diskful replicas of rd to report UpToDate.
func v113WaitUpToDate(ctx context.Context, t *testing.T, stack *harness.Stack, rd string, want int) {
	t.Helper()

	harness.Eventually(t, v113Timeout, func() bool {
		var list blockstoriov1alpha1.ResourceList
		if err := stack.Env.Client.List(ctx, &list); err != nil {
			return false
		}

		upToDate := 0

		for i := range list.Items {
			res := &list.Items[i]
			if res.Spec.ResourceDefinitionName == rd && !slices.Contains(res.Spec.Flags, lapiconsts.FlagDiskless) &&
				res.Status.DrbdState == "UpToDate" {
				upToDate++
			}
		}

		return upToDate >= want
	}, fmt.Sprintf("%d UpToDate diskful replicas of %s", want, rd))
}

func v113DiskfulNodes(ctx context.Context, t *testing.T, c *lapi.Client, rd string) []string {
	t.Helper()

	reses, err := c.Resources.GetAll(ctx, rd)
	if err != nil {
		t.Fatalf("Resources.GetAll(%s): %v", rd, err)
	}

	var nodes []string

	for i := range reses {
		if !slices.Contains(reses[i].Flags, lapiconsts.FlagDiskless) {
			nodes = append(nodes, reses[i].NodeName)
		}
	}

	slices.Sort(nodes)

	return nodes
}

// v113AttachDetach is Linstor.Attach and Linstor.Detach for an RWX block
// volume on the node without a replica, the target of a live migration.
func v113AttachDetach(ctx context.Context, t *testing.T, stack *harness.Stack, c *lapi.Client, rd string) {
	t.Helper()

	diskful := v113DiskfulNodes(ctx, t, c, rd)

	var target string

	for _, node := range []string{harness.NodeWorker1, harness.NodeWorker2, harness.NodeWorker3} {
		if !slices.Contains(diskful, node) {
			target = node
		}
	}

	if target == "" {
		t.Fatalf("no node without a disk among %v", diskful)
	}

	// The target has no storage of its own, as in the live migration
	// test: blockstor gives an in-use diskless replica on a node with a
	// pool a disk, and a disk on the FILE pool would keep the volume
	// from being snapshotted later on.
	lmRemovePools(ctx, t, stack, target)

	err := c.Resources.MakeAvailable(ctx, rd, target, lapi.ResourceMakeAvailable{AutoManageDualPrimary: true})
	if err != nil {
		t.Fatalf("MakeAvailable(%s): %v", target, err)
	}

	// Attach answers the device path, read right after make-available,
	// and ControllerPublishVolume fails on an empty one.
	vol, err := c.Resources.GetVolume(ctx, rd, target, 0)
	if err != nil || vol.DevicePath == "" {
		t.Fatalf("GetVolume(%s): device path %q, %v", target, vol.DevicePath, err)
	}

	// The volume is still open on the node: the detach is refused with
	// FAIL_IN_USE and retried by the external-attacher.
	setInUse := func(using bool) {
		err := retryStatusPatch(ctx, stack, rd+"."+target, func(r *blockstoriov1alpha1.Resource) {
			r.Status.InUse = using
		})
		if err != nil {
			t.Fatalf("in_use=%v on %s: %v", using, target, err)
		}

		harness.Eventually(t, v113Timeout, func() bool {
			res, err := c.Resources.Get(ctx, rd, target)

			return err == nil && res.State != nil && res.State.InUse != nil && *res.State.InUse == using
		}, "the REST server to see in_use on "+target)
	}

	setInUse(true)

	err = c.Resources.UnmakeAvailable(ctx, rd, target)
	if !errors.Is(err, lapi.ApiCallRcErr(lapiconsts.FailInUse)) {
		t.Fatalf("UnmakeAvailable while in use: %v, want FAIL_IN_USE", err)
	}

	setInUse(false)

	if err := nil404(c.Resources.UnmakeAvailable(ctx, rd, target)); err != nil {
		t.Fatalf("UnmakeAvailable: %v", err)
	}

	// A second detach of the same node is a no-op.
	if err := nil404(c.Resources.UnmakeAvailable(ctx, rd, target)); err != nil {
		t.Fatalf("repeated UnmakeAvailable: %v", err)
	}

	got, err := c.ResourceDefinitions.Get(ctx, rd)
	if err != nil {
		t.Fatalf("ResourceDefinitions.Get: %v", err)
	}

	if _, ok := got.Props[v113TwoPrims]; ok {
		t.Errorf("allow-two-primaries left on the definition after the detach: %v", got.Props)
	}
}

// v113CreateSnapshot is Linstor.createInClusterSnapshots plus the readiness
// the CSI snapshotter polls for.
func v113CreateSnapshot(ctx context.Context, t *testing.T, c *lapi.Client, rd, snap string) {
	t.Helper()

	if err := c.Resources.CreateSnapshots(ctx, lapi.Snapshot{Name: snap, ResourceName: rd}); err != nil {
		t.Fatalf("CreateSnapshots(%s/%s): %v", rd, snap, err)
	}

	harness.Eventually(t, v113Timeout, func() bool {
		view, err := c.Resources.GetSnapshotView(ctx)
		if err != nil {
			return false
		}

		return slices.ContainsFunc(view, func(s lapi.Snapshot) bool {
			return s.Name == snap && s.ResourceName == rd && slices.Contains(s.Flags, lapiconsts.FlagSuccessful) &&
				len(s.Snapshots) > 0 && len(s.VolumeDefinitions) > 0
		})
	}, "snapshot "+rd+"/"+snap+" ready to use")
}

// v113VolFromSnap is Linstor.VolFromSnap for an in-cluster snapshot:
// reconcileSnapshotVolumeDefinitions, reconcileSnapshotResources,
// waitSnapshotResourcesRestored and reconcileInheritedProperties, each
// run twice, as a retried CreateVolume does.
func v113VolFromSnap(ctx context.Context, t *testing.T, c *lapi.Client, src, snap, target string) {
	t.Helper()

	v113ReconcileResourceGroup(ctx, t, c)
	v113ReconcileResourceDefinition(ctx, t, c, target)

	restoreVDs := func() {
		vdefs, err := c.ResourceDefinitions.GetVolumeDefinitions(ctx, target)
		if err != nil {
			t.Fatalf("GetVolumeDefinitions(%s): %v", target, err)
		}

		if len(vdefs) != 0 {
			return
		}

		err = c.Resources.RestoreVolumeDefinitionSnapshot(ctx, src, snap, lapi.SnapshotRestore{ToResource: target})
		if err != nil && !errors.Is(err, lapi.ApiCallRcErr(lapiconsts.FailExistsVlmDfn)) {
			t.Fatalf("RestoreVolumeDefinitionSnapshot: %v", err)
		}
	}

	restoreVDs()
	restoreVDs()

	// A restore that raced the first one is told by its code.
	err := c.Resources.RestoreVolumeDefinitionSnapshot(ctx, src, snap, lapi.SnapshotRestore{ToResource: target})
	if !errors.Is(err, lapi.ApiCallRcErr(lapiconsts.FailExistsVlmDfn)) {
		t.Errorf("a racing volume definition restore: %v, want FAIL_EXISTS_VLM_DFN", err)
	}

	restoreResources := func() {
		reses, err := c.Resources.GetAll(ctx, target)
		if err != nil {
			t.Fatalf("Resources.GetAll(%s): %v", target, err)
		}

		if len(reses) != 0 {
			return
		}

		lsnap, err := c.Resources.GetSnapshot(ctx, src, snap)
		if err != nil || len(lsnap.Nodes) == 0 {
			t.Fatalf("GetSnapshot(%s/%s): %+v, %v", src, snap, lsnap, err)
		}

		node, err := c.Nodes.Get(ctx, lsnap.Nodes[0])
		if err != nil || node.ConnectionStatus != "ONLINE" {
			t.Fatalf("Nodes.Get(%s): %+v, %v", lsnap.Nodes[0], node, err)
		}

		err = c.Resources.RestoreSnapshot(ctx, src, snap, lapi.SnapshotRestore{
			ToResource: target,
			Nodes:      []string{lsnap.Nodes[0]},
		})
		if err != nil && !errors.Is(err, lapi.ApiCallRcErr(lapiconsts.FailExistsRsc)) {
			t.Fatalf("RestoreSnapshot: %v", err)
		}
	}

	restoreResources()
	restoreResources()

	lsnap, err := c.Resources.GetSnapshot(ctx, src, snap)
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}

	// A resource restore that raced the first one: LINSTOR answers
	// FAIL_EXISTS_RSC, blockstor a repeat of the same restore as done.
	// linstor-csi goes on either way.
	err = c.Resources.RestoreSnapshot(ctx, src, snap, lapi.SnapshotRestore{
		ToResource: target,
		Nodes:      []string{lsnap.Nodes[0]},
	})
	if err != nil && !errors.Is(err, lapi.ApiCallRcErr(lapiconsts.FailExistsRsc)) {
		t.Errorf("a racing resource restore: %v, want success or FAIL_EXISTS_RSC", err)
	}

	// waitSnapshotResourcesRestored.
	harness.Eventually(t, v113Timeout, func() bool {
		reses, err := c.Resources.GetAll(ctx, target)

		return err == nil && len(reses) > 0 && !slices.ContainsFunc(reses, func(r lapi.Resource) bool {
			return slices.Contains(r.Flags, lapiconsts.FlagRestoreFromSnapshot)
		})
	}, "the restore of "+target)

	// reconcileResourcePlacement on a restored volume.
	if err := c.Resources.Autoplace(ctx, target, lapi.AutoPlaceRequest{}); err != nil &&
		!errors.Is(err, lapi.ApiCallRcErr(lapiconsts.FailNotEnoughNodes)) {
		t.Fatalf("Autoplace(%s): %v", target, err)
	}

	v113ReconcileInheritedProperties(ctx, t, c, target)
}

// v113ReconcileInheritedProperties is Linstor.reconcileInheritedProperties
// for a block volume.
func v113ReconcileInheritedProperties(ctx context.Context, t *testing.T, c *lapi.Client, rd string) {
	t.Helper()

	err := c.ResourceDefinitions.Modify(ctx, rd, lapi.GenericPropsModify{
		OverrideProps: map[string]string{v113KeyCSI: "linstor-csi"},
		DeleteProps: []string{
			"Aux/qos.linbit.com/read-bps", "Aux/qos.linbit.com/write-bps",
			"Aux/qos.linbit.com/read-iops", "Aux/qos.linbit.com/write-iops",
			"FileSystem/Type", "FileSystem/MkfsParams",
			v113TwoPrims,
		},
	})
	if err != nil {
		t.Fatalf("ResourceDefinitions.Modify(%s): %v", rd, err)
	}
}

// v113Clone is Linstor.Clone.
func v113Clone(ctx context.Context, t *testing.T, c *lapi.Client, src, target string) {
	t.Helper()

	v113ReconcileResourceGroup(ctx, t, c)

	_, err := c.ResourceDefinitions.CloneStatus(ctx, src, target)
	if !errors.Is(err, lapi.NotFoundError) {
		t.Fatalf("CloneStatus before the clone: %v, want 404", err)
	}

	_, err = c.ResourceDefinitions.Clone(ctx, src, lapi.ResourceDefinitionCloneRequest{
		Name:          target,
		LayerList:     nil,
		ResourceGroup: v113RG,
		GenericPropsModify: lapi.GenericPropsModify{
			OverrideProps: map[string]string{"Aux/csi-volume-annotations": "{}"},
			DeleteProps: []string{
				v113KeyCSI, v113TwoPrims,
				"Aux/qos.linbit.com/read-bps", "Aux/qos.linbit.com/write-bps",
				"Aux/qos.linbit.com/read-iops", "Aux/qos.linbit.com/write-iops",
				"FileSystem/Type", "FileSystem/MkfsParams",
			},
		},
	})
	if err != nil {
		t.Fatalf("Clone: %v", err)
	}

	harness.Eventually(t, v113Timeout, func() bool {
		status, err := c.ResourceDefinitions.CloneStatus(ctx, src, target)

		return err == nil && status.Status == clonestatus.Complete
	}, "the clone of "+src+" to complete")

	v113ReconcileInheritedProperties(ctx, t, c, target)
}

// v113DeleteSnapshot is Linstor.DeleteSnapshot for an in-cluster snapshot.
func v113DeleteSnapshot(ctx context.Context, t *testing.T, c *lapi.Client, rd, snap string) {
	t.Helper()

	rDef, err := c.ResourceDefinitions.Get(ctx, rd)
	if err != nil {
		if nil404(err) == nil {
			return
		}

		t.Fatalf("ResourceDefinitions.Get(%s): %v", rd, err)
	}

	err = c.Resources.DeleteSnapshot(ctx, rd, snap, &lapi.SnapshotDeleteOpts{DeleteEmptyResourceDefinition: true})
	if nil404(err) != nil {
		t.Fatalf("DeleteSnapshot(%s/%s): %v", rd, snap, err)
	}

	v113DeleteResourceGroupIfUnused(ctx, t, c, rDef.ResourceGroupName)
}

// v113DeleteVolume is Linstor.Delete for a standalone volume:
// deleteResource.
func v113DeleteVolume(ctx context.Context, t *testing.T, c *lapi.Client, rd string) {
	t.Helper()

	rDef, err := c.ResourceDefinitions.Get(ctx, rd)
	if err != nil {
		if nil404(err) == nil {
			return
		}

		t.Fatalf("ResourceDefinitions.Get(%s): %v", rd, err)
	}

	reses, err := c.Resources.GetAll(ctx, rd)
	if nil404(err) != nil {
		t.Fatalf("Resources.GetAll(%s): %v", rd, err)
	}

	for i := range reses {
		if reses[i].State != nil && reses[i].State.InUse != nil && *reses[i].State.InUse {
			t.Fatalf("%s is in use on %s", rd, reses[i].NodeName)
		}
	}

	err = c.ResourceDefinitions.Truncate(ctx, rd, &lapi.ResourceDefinitionTruncateOpts{
		DeleteEmptyResourceDefinition: true,
	})
	if nil404(err) != nil {
		t.Fatalf("Truncate(%s): %v", rd, err)
	}

	vds, err := c.ResourceDefinitions.GetVolumeDefinitions(ctx, rd)
	if nil404(err) != nil {
		t.Fatalf("GetVolumeDefinitions(%s): %v", rd, err)
	}

	slices.SortFunc(vds, func(a, b lapi.VolumeDefinition) int {
		return cmp.Compare(*b.VolumeNumber, *a.VolumeNumber)
	})

	for _, vd := range vds {
		err = c.ResourceDefinitions.DeleteVolumeDefinition(ctx, rd, int(*vd.VolumeNumber))
		if nil404(err) != nil {
			t.Fatalf("DeleteVolumeDefinition(%s/%d): %v", rd, *vd.VolumeNumber, err)
		}
	}

	v113DeleteResourceGroupIfUnused(ctx, t, c, rDef.ResourceGroupName)
}

// v113DeleteResourceGroupIfUnused is Linstor.deleteResourceGroupIfUnused.
func v113DeleteResourceGroupIfUnused(ctx context.Context, t *testing.T, c *lapi.Client, rg string) {
	t.Helper()

	err := c.ResourceGroups.Delete(ctx, rg)
	if errors.Is(err, lapi.ApiCallRcErr(lapiconsts.FailExistsRscDfn)) {
		return
	}

	if nil404(err) != nil {
		t.Fatalf("ResourceGroups.Delete(%s): %v", rg, err)
	}
}
