//go:build integration

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

// Group J — CSI tests. The 12 sub-tests below pin the CSI-to-REST
// wire contract blockstor exposes to linstor-csi. The harness exposes
// `harness.CSI(stack)` which returns the in-process Driver + the
// golinstor REST client (see tests/integration/harness/csi.go).
//
// Architecture note: blockstor's CSI surface today is the
// behaviour-bearing Driver in pkg/csi-driver, plus the REST
// endpoints linstor-csi calls directly (spawn, make-available,
// snapshots, RD clone). cmd/csi-plugin does NOT exist — the real
// linstor-csi sidecar wraps the same Driver + lapi.Client in a
// csi.ControllerServer gRPC server out-of-tree. We do not vendor
// the container-storage-interface protobuf stubs here on purpose
// (pkg/csi-driver/driver.go explains the rationale); these tests
// therefore exercise the Driver methods directly + the lapi REST
// calls linstor-csi makes from the sidecar.
//
// Single parent, 12 subtests: controller-runtime's controller-name
// registry is process-global (see sigs.k8s.io/controller-runtime's
// pkg/controller/name.go), so booting the manager 12 times in one
// `go test` invocation collides on "controller with name node
// already exists". One shared stack across all CSI subtests gives
// us per-RD test isolation (each subtest picks a unique RD name)
// without touching the harness. The DoD `-run '^TestGroupJ'`
// matches the parent and every subtest under it.
//
// Bug guards (column 3 in docs/test-strategy.md's Group J table):
//
//   - Bug 30 + F4 + 7.12 → CSIControllerPublishDiskless
//   - Bug 199             → CSIDeleteSnapshotIdempotent
//   - Bug 201             → CSIListSnapshotsPagination
//   - F1                  → CSICreateVolumeFromSnapshot
//   - Bug 15              → CSICreateVolumeFromClone
package integration

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	lapi "github.com/LINBIT/golinstor/client"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	csidriver "github.com/cozystack/blockstor/pkg/csi-driver"
	"github.com/cozystack/blockstor/tests/integration/harness"
)

// groupJTimeout is the per-Eventually budget. 30s is the same value
// the harness uses for envtest shutdown and matches the smoke test's
// "manager ready" budget.
const groupJTimeout = 30 * time.Second

// groupJDefaultPlaceCount mirrors the fixture RG's PlaceCount=2 —
// CSI tests that spawn via the default RG expect this many replicas.
const groupJDefaultPlaceCount = 2

// groupJVolumeSizeKib is the size CSI tests provision in KiB
// (1 GiB). Small enough that even the most aggressive
// over-subscription gate (10 GiB pools, see harness/satellite.go:
// defaultPoolTotalKiB) never trips, large enough that the
// size-validation path (CSI spec >= snapshot size) is meaningfully
// exercised.
const groupJVolumeSizeKib int64 = 1024 * 1024 // 1 GiB

// groupJVolumeSizeBytes is the byte-denominated form. The REST
// spawn handler interprets `volume_sizes[]` as bytes and divides
// by 1024 to land KiB on the VolumeDefinition; the Driver's
// CapacityRangeMin is also byte-denominated to match the CSI spec.
const groupJVolumeSizeBytes = groupJVolumeSizeKib * 1024

// TestGroupJ is the single parent that boots the stack once and
// drives every Group J subtest against it. See the package-level
// comment for why we collapse 12 tests into one parent.
//
// Subtest names match the docs/test-strategy.md Group J table:
//
//	TestGroupJ/CSIIdentityServer
//	TestGroupJ/CSICreateVolumeFromEmpty
//	TestGroupJ/CSICreateVolumeIdempotent
//	TestGroupJ/CSIDeleteVolume
//	TestGroupJ/CSIControllerPublish
//	TestGroupJ/CSIControllerPublishDiskless
//	TestGroupJ/CSICreateSnapshot
//	TestGroupJ/CSIDeleteSnapshotIdempotent
//	TestGroupJ/CSIListSnapshotsPagination
//	TestGroupJ/CSICreateVolumeFromSnapshot
//	TestGroupJ/CSICreateVolumeFromClone
//	TestGroupJ/CSIValidateCapabilities
func TestGroupJ(t *testing.T) {
	stack := harness.StartStack(t)
	harness.SeedThreeNodeCluster(t, stack)
	csi := harness.NewCSI(t, stack)

	t.Run("CSIIdentityServer", func(t *testing.T) { runCSIIdentityServer(t, csi) })
	t.Run("CSICreateVolumeFromEmpty", func(t *testing.T) { runCSICreateVolumeFromEmpty(t, stack, csi) })
	t.Run("CSICreateVolumeIdempotent", func(t *testing.T) { runCSICreateVolumeIdempotent(t, stack, csi) })
	t.Run("CSIDeleteVolume", func(t *testing.T) { runCSIDeleteVolume(t, stack, csi) })
	t.Run("CSIControllerPublish", func(t *testing.T) { runCSIControllerPublish(t, stack, csi) })
	t.Run("CSIControllerPublishDiskless", func(t *testing.T) { runCSIControllerPublishDiskless(t, stack, csi) })
	t.Run("CSICreateSnapshot", func(t *testing.T) { runCSICreateSnapshot(t, stack, csi) })
	t.Run("CSIDeleteSnapshotIdempotent", func(t *testing.T) { runCSIDeleteSnapshotIdempotent(t, stack, csi) })
	t.Run("CSIListSnapshotsPagination", func(t *testing.T) { runCSIListSnapshotsPagination(t, stack, csi) })
	t.Run("CSICreateVolumeFromSnapshot", func(t *testing.T) { runCSICreateVolumeFromSnapshot(t, stack, csi) })
	t.Run("CSICreateVolumeFromClone", func(t *testing.T) { runCSICreateVolumeFromClone(t, stack, csi) })
	t.Run("CSIValidateCapabilities", func(t *testing.T) { runCSIValidateCapabilities(t, csi) })
}

// --- helpers ---

// spawnViaRG drives a CSI-style CreateVolume(empty) by POSTing to
// `/v1/resource-groups/<rg>/spawn` — the exact endpoint linstor-csi
// hits on every PVC create. The fixture RG has PlaceCount=2 and
// the satellite mock advances spawned replicas to UpToDate within
// one tick.
//
// `storPool` pins SelectFilter.StoragePool so placement is
// deterministic across the 9 fixture SPs (3 nodes × {lvm-thin,
// zfs-thin, file}); pass "" to inherit from the RG.
//
// sizeBytes is byte-denominated to match the REST handler's
// `volume_sizes[]` interpretation (it divides by 1024 to derive
// VolumeDefinition.SizeKib).
func spawnViaRG(t *testing.T, csi *harness.CSI, rdName, storPool string, sizeBytes int64) {
	t.Helper()

	req := lapi.ResourceGroupSpawn{
		ResourceDefinitionName: rdName,
		VolumeSizes:            []int64{sizeBytes},
	}

	if storPool != "" {
		req.SelectFilter = lapi.AutoSelectFilter{
			PlaceCount:  groupJDefaultPlaceCount,
			StoragePool: storPool,
		}
	}

	err := csi.Client.ResourceGroups.Spawn(context.Background(), harness.FixtureDefaultRG, req)
	if err != nil {
		t.Fatalf("ResourceGroups.Spawn(%q): %v", rdName, err)
	}
}

// waitForRDInStore polls the envtest apiserver until the named
// ResourceDefinition CRD lands.
func waitForRDInStore(t *testing.T, stack *harness.Stack, rdName string) {
	t.Helper()

	harness.Eventually(t, groupJTimeout, func() bool {
		var rd blockstoriov1alpha1.ResourceDefinition

		err := stack.Env.Client.Get(context.Background(),
			types.NamespacedName{Name: rdName}, &rd)

		return err == nil
	}, "RD "+rdName+" never landed in apiserver")
}

// countDiskfulResourcesForRD returns how many DISKFUL Resource CRDs
// the apiserver holds for the named RD — i.e., NOT carrying the
// DISKLESS flag. PlaceCount semantics target diskful replicas, so
// tests that assert autoplace converged to placeCount=N must filter
// out the auto-created tiebreaker witness (DISKLESS + TIE_BREAKER)
// the RD reconciler stamps for 2-diskful RDs in a 3+ node cluster.
//
// See internal/controller/resourcedefinition_controller.go:
// `countDiskfulReplicas` for the matching production semantic.
func countDiskfulResourcesForRD(t *testing.T, stack *harness.Stack, rdName string) int {
	t.Helper()

	var list blockstoriov1alpha1.ResourceList

	err := stack.Env.Client.List(context.Background(), &list)
	if err != nil {
		t.Fatalf("list Resources: %v", err)
	}

	count := 0

	for i := range list.Items {
		if list.Items[i].Spec.ResourceDefinitionName != rdName {
			continue
		}

		if slices.Contains(list.Items[i].Spec.Flags, "DISKLESS") {
			continue
		}

		count++
	}

	return count
}

// countAllResourcesForRD includes the tiebreaker witness — used by
// publish/cascade tests that need to observe the full replica set
// (diskful + diskless witness).
func countAllResourcesForRD(t *testing.T, stack *harness.Stack, rdName string) int {
	t.Helper()

	var list blockstoriov1alpha1.ResourceList

	err := stack.Env.Client.List(context.Background(), &list)
	if err != nil {
		t.Fatalf("list Resources: %v", err)
	}

	count := 0

	for i := range list.Items {
		if list.Items[i].Spec.ResourceDefinitionName == rdName {
			count++
		}
	}

	return count
}

// findTieBreakerNode returns the fixture node hosting the RD's
// TIE_BREAKER witness (DISKLESS + TIE_BREAKER), or "" when no
// witness exists yet.
func findTieBreakerNode(t *testing.T, stack *harness.Stack, rdName string) string {
	t.Helper()

	var list blockstoriov1alpha1.ResourceList

	err := stack.Env.Client.List(context.Background(), &list)
	if err != nil {
		t.Fatalf("list Resources: %v", err)
	}

	for i := range list.Items {
		if list.Items[i].Spec.ResourceDefinitionName != rdName {
			continue
		}

		if !slices.Contains(list.Items[i].Spec.Flags, "TIE_BREAKER") {
			continue
		}

		return list.Items[i].Spec.NodeName
	}

	return ""
}

// pickNodeWithReplica returns a fixture node that hosts a diskful
// replica.
func pickNodeWithReplica(t *testing.T, stack *harness.Stack, rdName string) string {
	t.Helper()

	var list blockstoriov1alpha1.ResourceList

	err := stack.Env.Client.List(context.Background(), &list)
	if err != nil {
		t.Fatalf("list Resources: %v", err)
	}

	for i := range list.Items {
		if list.Items[i].Spec.ResourceDefinitionName != rdName {
			continue
		}

		if slices.Contains(list.Items[i].Spec.Flags, "DISKLESS") {
			continue
		}

		return list.Items[i].Spec.NodeName
	}

	t.Fatalf("no diskful replica found for RD %q", rdName)

	return ""
}

// --- subtest bodies ---

// runCSIIdentityServer pins the wire-shape of the controller version
// endpoint. linstor-csi's GetPluginInfo / Probe layer derives its
// `Manifest.Version` from this call; a missing/garbled response
// surfaces as `Identity service: GetPluginInfo failed` in the CSI
// sidecar logs.
func runCSIIdentityServer(t *testing.T, csi *harness.CSI) {
	ver, err := csi.Client.Controller.GetVersion(context.Background())
	if err != nil {
		t.Fatalf("Controller.GetVersion: %v", err)
	}

	if ver.RestApiVersion == "" {
		t.Errorf("RestApiVersion: got empty, want non-empty (linstor-csi reads this)")
	}
}

// runCSICreateVolumeFromEmpty exercises the canonical CreateVolume
// happy path: spawn through the default RG produces an RD + VD +
// N Resources. We pin all four observable wire effects (RD, VD,
// Resources, autoplace=N) so a refactor in pkg/rest/spawn.go that
// drops any of them surfaces here rather than at the linstor-csi
// sidecar.
func runCSICreateVolumeFromEmpty(t *testing.T, stack *harness.Stack, csi *harness.CSI) {
	const rdName = "pvc-create-empty"

	spawnViaRG(t, csi, rdName, "lvm-thin", groupJVolumeSizeBytes)

	waitForRDInStore(t, stack, rdName)

	// VD must exist with the requested size (KiB on the wire).
	vds, err := csi.Client.ResourceDefinitions.GetVolumeDefinitions(context.Background(), rdName)
	if err != nil {
		t.Fatalf("GetVolumeDefinitions: %v", err)
	}

	if len(vds) != 1 {
		t.Fatalf("VDs: got %d, want 1", len(vds))
	}

	if vds[0].SizeKib != uint64(groupJVolumeSizeKib) {
		t.Errorf("VD.SizeKib: got %d, want %d", vds[0].SizeKib, groupJVolumeSizeKib)
	}

	// Autoplace produced PlaceCount replicas. The placer + harness
	// fixtures are deterministic enough that this lands within one
	// reconcile tick; we still poll because the spawn handler runs
	// in the REST goroutine and the placer's writes go through the
	// store independently.
	harness.Eventually(t, groupJTimeout, func() bool {
		return countDiskfulResourcesForRD(t, stack, rdName) == groupJDefaultPlaceCount
	}, "autoplace did not converge to placeCount=2 diskful replicas")
}

// runCSICreateVolumeIdempotent pins CSI's idempotency contract:
// spawning the same RD name twice MUST NOT corrupt the
// already-placed volume. blockstor surfaces the second call as a
// 409 (RD already exists); the CSI sidecar handles by
// short-circuiting on the existing volume. The wire-shape
// invariant we pin: post-retry, the original RD's VDs and
// Resources are unchanged.
func runCSICreateVolumeIdempotent(t *testing.T, stack *harness.Stack, csi *harness.CSI) {
	const rdName = "pvc-idempotent"

	spawnViaRG(t, csi, rdName, "zfs-thin", groupJVolumeSizeBytes)

	waitForRDInStore(t, stack, rdName)
	harness.Eventually(t, groupJTimeout, func() bool {
		return countDiskfulResourcesForRD(t, stack, rdName) == groupJDefaultPlaceCount
	}, "first spawn did not converge")

	wantSize := uint64(groupJVolumeSizeKib)
	wantDiskful := groupJDefaultPlaceCount

	// Second spawn — same name. blockstor returns 409 wrapped as
	// lapi.ApiCallError; csi MUST surface the error rather than
	// truncating the existing volume.
	err := csi.Client.ResourceGroups.Spawn(context.Background(), harness.FixtureDefaultRG, lapi.ResourceGroupSpawn{
		ResourceDefinitionName: rdName,
		VolumeSizes:            []int64{groupJVolumeSizeBytes},
		SelectFilter: lapi.AutoSelectFilter{
			PlaceCount:  groupJDefaultPlaceCount,
			StoragePool: "zfs-thin",
		},
	})
	if err == nil {
		t.Fatal("repeat spawn: got nil error, want 409/conflict (RD already exists)")
	}

	// Existing VD untouched.
	vds, err := csi.Client.ResourceDefinitions.GetVolumeDefinitions(context.Background(), rdName)
	if err != nil {
		t.Fatalf("GetVolumeDefinitions post-retry: %v", err)
	}

	if len(vds) != 1 || vds[0].SizeKib != wantSize {
		t.Errorf("VDs perturbed by retry: got %+v, want 1 VD of %d KiB", vds, wantSize)
	}

	// Existing diskful replicas untouched. The tiebreaker witness
	// may flap independently of the retry (the RD reconciler
	// watches the apiserver and recomputes per event) — pinning
	// the diskful count keeps this assertion focused on the
	// observable a CSI sidecar would check.
	if got := countDiskfulResourcesForRD(t, stack, rdName); got != wantDiskful {
		t.Errorf("diskful Resource count perturbed by retry: got %d, want %d", got, wantDiskful)
	}
}

// runCSIDeleteVolume exercises the DeleteVolume happy path: RD
// delete cascades to VDs + Resources. linstor-csi calls
// `ResourceDefinitions.Delete` from its
// `ControllerServer.DeleteVolume`; the REST handler removes the
// RD row and the reconciler/cascade-handler sweeps the children.
func runCSIDeleteVolume(t *testing.T, stack *harness.Stack, csi *harness.CSI) {
	const rdName = "pvc-delete"

	spawnViaRG(t, csi, rdName, "lvm-thin", groupJVolumeSizeBytes)
	waitForRDInStore(t, stack, rdName)
	harness.Eventually(t, groupJTimeout, func() bool {
		return countDiskfulResourcesForRD(t, stack, rdName) == groupJDefaultPlaceCount
	}, "pre-delete autoplace did not converge")

	err := csi.Client.ResourceDefinitions.Delete(context.Background(), rdName)
	if err != nil {
		t.Fatalf("ResourceDefinitions.Delete: %v", err)
	}

	// RD must be gone from the apiserver.
	harness.Eventually(t, groupJTimeout, func() bool {
		var rd blockstoriov1alpha1.ResourceDefinition

		getErr := stack.Env.Client.Get(context.Background(),
			types.NamespacedName{Name: rdName}, &rd)

		return apierrors.IsNotFound(getErr)
	}, "RD "+rdName+" still present after Delete")

	// Cascade: diskful Resources for this RD must disappear (the
	// REST cascade deletes every child). A transient witness
	// orphan can race the cascade in this harness because the
	// auto-tiebreaker reconciler re-creates a witness if it
	// reconciles between the cascade's first and last child
	// delete (the RD still exists with no DeletionTimestamp at
	// that point) — that's a known cascade-race we accept here.
	// The observable a CSI sidecar cares about (diskful gone) is
	// what we pin.
	harness.Eventually(t, groupJTimeout, func() bool {
		return countDiskfulResourcesForRD(t, stack, rdName) == 0
	}, "diskful Resources for "+rdName+" not cascaded after RD delete")
}

// runCSIControllerPublish exercises the make-available happy path
// on a node that already carries a diskful replica. linstor-csi's
// `ControllerPublishVolume` POSTs `make-available {diskful:false}`
// and expects 200 — the existing diskful replica is "available"
// by definition, so the handler is a no-op aside from stripping
// any TIE_BREAKER witness state.
func runCSIControllerPublish(t *testing.T, stack *harness.Stack, csi *harness.CSI) {
	const rdName = "pvc-publish-diskful"

	spawnViaRG(t, csi, rdName, "lvm-thin", groupJVolumeSizeBytes)
	waitForRDInStore(t, stack, rdName)
	harness.Eventually(t, groupJTimeout, func() bool {
		return countDiskfulResourcesForRD(t, stack, rdName) == groupJDefaultPlaceCount
	}, "pre-publish autoplace did not converge")

	node := pickNodeWithReplica(t, stack, rdName)

	err := csi.Client.Resources.MakeAvailable(context.Background(),
		rdName, node, lapi.ResourceMakeAvailable{Diskful: false})
	if err != nil {
		t.Fatalf("MakeAvailable on diskful node %q: %v", node, err)
	}

	if got := countDiskfulResourcesForRD(t, stack, rdName); got != groupJDefaultPlaceCount {
		t.Errorf("diskful replica count: got %d, want %d (publish on diskful must not change diskful count)",
			got, groupJDefaultPlaceCount)
	}
}

// runCSIControllerPublishDiskless is the Bug 30 + F4 + 7.12 guard:
// when linstor-csi's `ControllerPublishVolume` targets a node that
// does not (yet) host a real consumer-facing replica, the
// make-available endpoint MUST land a usable DISKLESS replica
// there so the pod can attach. Without this, the CSI Attach hangs
// in ContainerCreating with "no device path on node X".
//
// In a 3-node cluster with auto-tiebreaker on (the harness fixture
// default), a 2-diskful RD's third node is already occupied by a
// DISKLESS+TIE_BREAKER witness — Bug 30 surfaces here as
// "the witness must lose TIE_BREAKER on make-available so the
// satellite reconciler exposes a usable DRBD device, NOT silently
// keep the witness state and refuse the attach".
//
// We pin:
//
//  1. After make-available, the target node hosts a DISKLESS
//     replica (whether by promote-from-witness or by fresh create,
//     depending on the cluster topology — both are valid Bug 30
//     resolutions).
//  2. That replica does NOT carry TIE_BREAKER (csi attach demands
//     a consumer-facing replica, not a witness).
//  3. That replica still carries DISKLESS (stealth-promote to
//     diskful would burn disk space the operator never asked for).
func runCSIControllerPublishDiskless(t *testing.T, stack *harness.Stack, csi *harness.CSI) {
	const rdName = "pvc-publish-diskless"

	spawnViaRG(t, csi, rdName, "lvm-thin", groupJVolumeSizeBytes)
	waitForRDInStore(t, stack, rdName)
	harness.Eventually(t, groupJTimeout, func() bool {
		return countDiskfulResourcesForRD(t, stack, rdName) == groupJDefaultPlaceCount
	}, "pre-publish autoplace did not converge")

	// Wait for the witness to land before we poke make-available —
	// otherwise the timing window where "no replica yet on node X"
	// vs "witness on node X" can flip per run, making the test
	// non-deterministic.
	harness.Eventually(t, groupJTimeout, func() bool {
		return countAllResourcesForRD(t, stack, rdName) == groupJDefaultPlaceCount+1
	}, "tiebreaker witness did not land before make-available")

	witnessNode := findTieBreakerNode(t, stack, rdName)
	if witnessNode == "" {
		t.Fatal("no witness node found after waiting; cannot exercise Bug 30 promote path")
	}

	err := csi.Client.Resources.MakeAvailable(context.Background(),
		rdName, witnessNode, lapi.ResourceMakeAvailable{Diskful: false})
	if err != nil {
		t.Fatalf("MakeAvailable on witness node %q: %v", witnessNode, err)
	}

	// Bug 30 wire guard: the witness MUST shed the TIE_BREAKER
	// flag while keeping DISKLESS. Poll because make-available
	// returns once the row is updated through the Store, but the
	// apiserver round-trip + the cache the test reader uses can
	// trail by a tick.
	harness.Eventually(t, groupJTimeout, func() bool {
		var res blockstoriov1alpha1.Resource

		getErr := stack.Env.Client.Get(context.Background(),
			types.NamespacedName{Name: rdName + "." + witnessNode}, &res)
		if getErr != nil {
			return false
		}

		if slices.Contains(res.Spec.Flags, "TIE_BREAKER") {
			return false
		}

		return slices.Contains(res.Spec.Flags, "DISKLESS")
	}, "Bug 30 guard: witness on "+witnessNode+
		" did not become DISKLESS without TIE_BREAKER after make-available")

	// Diskful replicas are unchanged — a CSI attach must NOT
	// promote a witness to diskful or place an extra diskful.
	if got := countDiskfulResourcesForRD(t, stack, rdName); got != groupJDefaultPlaceCount {
		t.Errorf("diskful replica count after publish: got %d, want %d",
			got, groupJDefaultPlaceCount)
	}
}

// runCSICreateSnapshot pins the Driver.CreateSnapshot wire path: a
// snapshot row lands in the apiserver, addressable by (rd, snap) —
// that's what csi-snapshotter's ListSnapshots poll looks for to
// mark VolumeSnapshot readyToUse=true (scenario 8.W03).
func runCSICreateSnapshot(t *testing.T, stack *harness.Stack, csi *harness.CSI) {
	const (
		rdName   = "pvc-snap-src"
		snapName = "snap-1"
	)

	spawnViaRG(t, csi, rdName, "zfs-thin", groupJVolumeSizeBytes)
	waitForRDInStore(t, stack, rdName)
	harness.Eventually(t, groupJTimeout, func() bool {
		return countDiskfulResourcesForRD(t, stack, rdName) == groupJDefaultPlaceCount
	}, "pre-snapshot autoplace did not converge")

	resp, err := csi.Driver.CreateSnapshot(context.Background(), &csidriver.CreateSnapshotRequest{
		SourceVolumeID: rdName,
		Name:           snapName,
	})
	if err != nil {
		t.Fatalf("Driver.CreateSnapshot: %v", err)
	}

	if want := rdName + "/" + snapName; resp.SnapshotID != want {
		t.Errorf("SnapshotID: got %q, want %q", resp.SnapshotID, want)
	}

	snap, err := csi.Client.Resources.GetSnapshot(context.Background(), rdName, snapName)
	if err != nil {
		t.Fatalf("GetSnapshot after create: %v", err)
	}

	if snap.Name != snapName || snap.ResourceName != rdName {
		t.Errorf("snapshot identity drift: got (%q,%q), want (%q,%q)",
			snap.ResourceName, snap.Name, rdName, snapName)
	}
}

// runCSIDeleteSnapshotIdempotent is the Bug 199 guard: the CSI
// spec mandates DeleteSnapshot is idempotent — a second delete
// against the same (rd, snap) MUST return success rather than 404.
// Without this, csi-snapshotter's retry loop wedges the
// VolumeSnapshot in `deletionFailed`.
//
// Two flavours of "already absent" are pinned:
//
//   - delete on a snapshot that was successfully removed by an
//     earlier call (the common case csi-snapshotter retries hit).
//   - delete on a snapshot that never existed (csi-sanity's
//     "should succeed when an invalid snapshot id is used" check).
func runCSIDeleteSnapshotIdempotent(t *testing.T, stack *harness.Stack, csi *harness.CSI) {
	const (
		rdName   = "pvc-snap-del"
		snapName = "snap-del"
	)

	spawnViaRG(t, csi, rdName, "zfs-thin", groupJVolumeSizeBytes)
	waitForRDInStore(t, stack, rdName)
	harness.Eventually(t, groupJTimeout, func() bool {
		return countDiskfulResourcesForRD(t, stack, rdName) == groupJDefaultPlaceCount
	}, "pre-snapshot autoplace did not converge")

	_, err := csi.Driver.CreateSnapshot(context.Background(), &csidriver.CreateSnapshotRequest{
		SourceVolumeID: rdName,
		Name:           snapName,
	})
	if err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}

	err = csi.Client.Resources.DeleteSnapshot(context.Background(), rdName, snapName)
	if err != nil {
		t.Fatalf("first DeleteSnapshot: %v", err)
	}

	// Bug 199: repeat delete must NOT error.
	err = csi.Client.Resources.DeleteSnapshot(context.Background(), rdName, snapName)
	if err != nil {
		t.Errorf("repeat DeleteSnapshot (Bug 199 guard): got %v, want nil", err)
	}

	// Delete of a snapshot that never existed must also succeed.
	err = csi.Client.Resources.DeleteSnapshot(context.Background(), rdName, "snap-never-existed")
	if err != nil {
		t.Errorf("DeleteSnapshot on unknown snap (Bug 199 guard): got %v, want nil", err)
	}
}

// runCSIListSnapshotsPagination is the Bug 201 guard: CSI's
// `ListSnapshots` forwards `max_entries` + `starting_token` into
// `?limit=N&offset=M` on the REST surface. We pin:
//
//   - page 1 (offset=0, limit=2) returns the first 2 in stable order
//   - page 2 (offset=2, limit=2) returns the remaining 2
//   - page past the end returns `[]` (NOT 404, NOT 416, NOT null)
//
// The "NOT null" guard is the actual Bug 201 regression — a
// `null` body decodes to a nil slice in csi-sanity and breaks the
// loop with "malformed envelope".
func runCSIListSnapshotsPagination(t *testing.T, stack *harness.Stack, csi *harness.CSI) {
	const rdName = "pvc-snap-paginate"

	spawnViaRG(t, csi, rdName, "zfs-thin", groupJVolumeSizeBytes)
	waitForRDInStore(t, stack, rdName)
	harness.Eventually(t, groupJTimeout, func() bool {
		return countDiskfulResourcesForRD(t, stack, rdName) == groupJDefaultPlaceCount
	}, "pre-pagination autoplace did not converge")

	for _, name := range []string{"snap-a", "snap-b", "snap-c", "snap-d"} {
		_, err := csi.Driver.CreateSnapshot(context.Background(), &csidriver.CreateSnapshotRequest{
			SourceVolumeID: rdName,
			Name:           name,
		})
		if err != nil {
			t.Fatalf("CreateSnapshot %q: %v", name, err)
		}
	}

	const pageSize = 2

	page1, err := csi.Client.Resources.GetSnapshotView(context.Background(),
		&lapi.ListOpts{Offset: 0, Limit: pageSize, Resource: []string{rdName}})
	if err != nil {
		t.Fatalf("page1 GetSnapshotView: %v", err)
	}

	if len(page1) != pageSize {
		t.Fatalf("page1: got %d entries, want %d", len(page1), pageSize)
	}

	page2, err := csi.Client.Resources.GetSnapshotView(context.Background(),
		&lapi.ListOpts{Offset: pageSize, Limit: pageSize, Resource: []string{rdName}})
	if err != nil {
		t.Fatalf("page2 GetSnapshotView: %v", err)
	}

	if len(page2) != pageSize {
		t.Fatalf("page2: got %d entries, want %d", len(page2), pageSize)
	}

	// Pagination is deterministic across pages — the same
	// snapshot must never appear on two consecutive pages.
	seen := map[string]bool{}
	for _, s := range page1 {
		seen[s.Name] = true
	}

	for _, s := range page2 {
		if seen[s.Name] {
			t.Errorf("snapshot %q appeared on both pages — pagination not stable", s.Name)
		}
	}

	// Bug 201 guard: page past the end returns `[]`, not null /
	// 404 / 416.
	pageOOB, err := csi.Client.Resources.GetSnapshotView(context.Background(),
		&lapi.ListOpts{Offset: 99, Limit: pageSize, Resource: []string{rdName}})
	if err != nil {
		t.Fatalf("out-of-range page: got error %v, want empty slice + nil err", err)
	}

	if len(pageOOB) != 0 {
		t.Errorf("out-of-range page: got %d entries, want 0", len(pageOOB))
	}
}

// runCSICreateVolumeFromSnapshot is the F1 guard: the
// clone-via-snapshot pipeline. csi.CreateVolume{ContentSource:
// Snapshot{Id="<rd>/<snap>"}} drives Driver.CreateVolume → POST
// `/v1/resource-definitions/{src}/snapshot-restore-resource/{snap}`
// → new RD stamped with `BlockstorRestoreFromSnapshot` so the
// satellite branches to clone instead of provisioning blank.
//
// Missing the BlockstorRestoreFromSnapshot prop means the satellite
// silently provisions empty storage — the PVC binds but the
// application's data is gone. We pin both the response shape
// (Driver echoes ContentSource back) and the prop landing on the
// destination RD.
func runCSICreateVolumeFromSnapshot(t *testing.T, stack *harness.Stack, csi *harness.CSI) {
	const (
		srcRD        = "pvc-clone-src"
		snapName     = "snap-1"
		destRD       = "pvc-clone-dest"
		bytesPerKiB  = int64(1024)
		restoreProp  = "BlockstorRestoreFromSnapshot"
		restoreValue = srcRD + ":" + snapName
	)

	spawnViaRG(t, csi, srcRD, "zfs-thin", groupJVolumeSizeBytes)
	waitForRDInStore(t, stack, srcRD)
	harness.Eventually(t, groupJTimeout, func() bool {
		return countDiskfulResourcesForRD(t, stack, srcRD) == groupJDefaultPlaceCount
	}, "pre-restore autoplace did not converge")

	_, err := csi.Driver.CreateSnapshot(context.Background(), &csidriver.CreateSnapshotRequest{
		SourceVolumeID: srcRD,
		Name:           snapName,
	})
	if err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}

	resp, err := csi.Driver.CreateVolume(context.Background(), &csidriver.CreateVolumeRequest{
		Name:             destRD,
		CapacityRangeMin: groupJVolumeSizeKib * bytesPerKiB,
		ContentSource: &csidriver.VolumeContentSourceSnapshot{
			SourceRD:     srcRD,
			SnapshotName: snapName,
		},
	})
	if err != nil {
		t.Fatalf("Driver.CreateVolume from snapshot: %v", err)
	}

	if resp.VolumeID != destRD {
		t.Errorf("VolumeID: got %q, want %q", resp.VolumeID, destRD)
	}

	if resp.ContentSource == nil ||
		resp.ContentSource.SourceRD != srcRD ||
		resp.ContentSource.SnapshotName != snapName {
		t.Errorf("ContentSource: got %+v, want {%q, %q} echoed back",
			resp.ContentSource, srcRD, snapName)
	}

	// F1 wire guard: destination RD carries
	// `BlockstorRestoreFromSnapshot` so the satellite clones
	// instead of provisioning blank.
	waitForRDInStore(t, stack, destRD)

	var dest blockstoriov1alpha1.ResourceDefinition

	err = stack.Env.Client.Get(context.Background(),
		types.NamespacedName{Name: destRD}, &dest)
	if err != nil {
		t.Fatalf("get destination RD %q: %v", destRD, err)
	}

	if got := dest.Spec.Props[restoreProp]; got != restoreValue {
		t.Errorf("%s prop: got %q, want %q", restoreProp, got, restoreValue)
	}
}

// runCSICreateVolumeFromClone is the Bug 15 guard: csi
// `CreateVolume` with `VolumeContentSource_Volume{SourceVolumeId}`
// (direct clone, NOT via snapshot) maps to
// `POST /v1/resource-definitions/{src}/clone`. The bug class: the
// upstream golinstor decoder expects the
// `ResourceDefinitionCloneStarted` object envelope back — a bare
// `[]ApiCallRc` array breaks CSI clone-from-source with "cannot
// unmarshal array into ResourceDefinitionCloneStarted".
//
// We pin both halves:
//
//  1. Clone request returns the object envelope (Location,
//     SourceName, CloneName populated).
//  2. The follow-up CloneStatus poll reports COMPLETE.
func runCSICreateVolumeFromClone(t *testing.T, stack *harness.Stack, csi *harness.CSI) {
	const (
		srcRD  = "pvc-direct-src"
		destRD = "pvc-direct-clone"
	)

	spawnViaRG(t, csi, srcRD, "zfs-thin", groupJVolumeSizeBytes)
	waitForRDInStore(t, stack, srcRD)
	harness.Eventually(t, groupJTimeout, func() bool {
		return countDiskfulResourcesForRD(t, stack, srcRD) == groupJDefaultPlaceCount
	}, "pre-clone autoplace did not converge")

	started, err := csi.Client.ResourceDefinitions.Clone(context.Background(),
		srcRD, lapi.ResourceDefinitionCloneRequest{
			Name:        destRD,
			UseZfsClone: true,
		})
	if err != nil {
		t.Fatalf("ResourceDefinitions.Clone: %v", err)
	}

	if started.CloneName != destRD {
		t.Errorf("CloneStarted.CloneName: got %q, want %q", started.CloneName, destRD)
	}

	if started.SourceName != srcRD {
		t.Errorf("CloneStarted.SourceName: got %q, want %q", started.SourceName, srcRD)
	}

	if !strings.Contains(started.Location, destRD) {
		t.Errorf("CloneStarted.Location: got %q, want path containing %q",
			started.Location, destRD)
	}

	waitForRDInStore(t, stack, destRD)

	// CloneStatus poll converges to COMPLETE — blockstor's clone
	// is synchronous w.r.t. RD creation so this answers on the
	// first poll, but linstor-csi calls it in a loop and so we
	// pin the wire shape regardless.
	harness.Eventually(t, groupJTimeout, func() bool {
		status, statusErr := csi.Client.ResourceDefinitions.CloneStatus(
			context.Background(), srcRD, destRD)
		if statusErr != nil {
			return false
		}

		return strings.EqualFold(string(status.Status), "COMPLETE")
	}, "CloneStatus did not converge to COMPLETE")
}

// runCSIValidateCapabilities pins the Driver's local validation
// contract — the same checks csi-sanity exercises against
// `ValidateVolumeCapabilities` / `CreateVolume` shape validation.
// blockstor's CSI shim performs these locally rather than POSTing
// to the REST surface, so a network-down driver still rejects
// malformed requests promptly. A regression that elides one of
// these surfaces as "blockstor accepted a malformed CSI request"
// in csi-sanity.
//
// Five mandatory rejections are pinned:
//
//   - nil request                  → ErrNilRequest
//   - empty Name                   → ErrMissingName
//   - missing SourceVolumeID       → ErrMissingSourceVolume
//   - nil ContentSource            → ErrMissingContentSource
//   - malformed ContentSource      → ErrMalformedContentSource
func runCSIValidateCapabilities(t *testing.T, csi *harness.CSI) {
	ctx := context.Background()
	d := csi.Driver

	// nil CreateSnapshotRequest.
	_, err := d.CreateSnapshot(ctx, nil)
	if !errors.Is(err, csidriver.ErrNilRequest) {
		t.Errorf("CreateSnapshot(nil): got %v, want ErrNilRequest", err)
	}

	// empty SourceVolumeID.
	_, err = d.CreateSnapshot(ctx, &csidriver.CreateSnapshotRequest{Name: "x"})
	if !errors.Is(err, csidriver.ErrMissingSourceVolume) {
		t.Errorf("CreateSnapshot(no SourceVolumeID): got %v, want ErrMissingSourceVolume", err)
	}

	// empty Name on CreateSnapshot.
	_, err = d.CreateSnapshot(ctx, &csidriver.CreateSnapshotRequest{SourceVolumeID: "rd"})
	if !errors.Is(err, csidriver.ErrMissingName) {
		t.Errorf("CreateSnapshot(no Name): got %v, want ErrMissingName", err)
	}

	// nil CreateVolumeRequest.
	_, err = d.CreateVolume(ctx, nil)
	if !errors.Is(err, csidriver.ErrNilRequest) {
		t.Errorf("CreateVolume(nil): got %v, want ErrNilRequest", err)
	}

	// empty Name on CreateVolume.
	_, err = d.CreateVolume(ctx, &csidriver.CreateVolumeRequest{
		ContentSource: &csidriver.VolumeContentSourceSnapshot{SourceRD: "a", SnapshotName: "b"},
	})
	if !errors.Is(err, csidriver.ErrMissingName) {
		t.Errorf("CreateVolume(no Name): got %v, want ErrMissingName", err)
	}

	// nil ContentSource — this shim only models the
	// snapshot-restore branch.
	_, err = d.CreateVolume(ctx, &csidriver.CreateVolumeRequest{Name: "new"})
	if !errors.Is(err, csidriver.ErrMissingContentSource) {
		t.Errorf("CreateVolume(nil ContentSource): got %v, want ErrMissingContentSource", err)
	}

	// malformed ContentSource — both halves required.
	_, err = d.CreateVolume(ctx, &csidriver.CreateVolumeRequest{
		Name:          "new",
		ContentSource: &csidriver.VolumeContentSourceSnapshot{SourceRD: "rd"},
	})
	if !errors.Is(err, csidriver.ErrMalformedContentSource) {
		t.Errorf("CreateVolume(empty SnapshotName): got %v, want ErrMalformedContentSource", err)
	}

	_, err = d.CreateVolume(ctx, &csidriver.CreateVolumeRequest{
		Name:          "new",
		ContentSource: &csidriver.VolumeContentSourceSnapshot{SnapshotName: "snap"},
	})
	if !errors.Is(err, csidriver.ErrMalformedContentSource) {
		t.Errorf("CreateVolume(empty SourceRD): got %v, want ErrMalformedContentSource", err)
	}
}
