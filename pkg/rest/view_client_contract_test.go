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

package rest

import (
	"context"
	"sync/atomic"
	"testing"

	lapi "github.com/LINBIT/golinstor/client"
	"github.com/LINBIT/golinstor/devicelayerkind"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// These tests read /v1/view/resources through golinstor, the client
// linstor-csi and linstor-scheduler-extender are built on, against the
// Kubernetes-backed store the apiserver runs in production, and then
// ask the question each of those clients asks of the answer.

const contractRD = "pvc-contract"

func contractNodeNames() []string { return []string{"node-a", "node-b", "node-c"} }

// seedThreeReplicaRD creates a one-volume RD with a diskful replica on
// each contract node and no satellite-reported state: the state a new
// volume is in between placement and the satellites building it.
func seedThreeReplicaRD(t *testing.T, st store.Store) {
	t.Helper()

	seedContractRD(t, st, map[string]string{"node-a": "data", "node-b": "data", "node-c": "data"})
}

// seedContractRD creates a one-volume RD with a diskful replica on each
// node of pools, in that node's pool; an empty pool leaves the
// replica's spec without one.
func seedContractRD(t *testing.T, st store.Store, pools map[string]string) {
	t.Helper()

	seedContractRDWith(t, st, apiv1.ResourceDefinition{Name: contractRD}, pools)
}

// seedContractRDWith is seedContractRD with the definition given.
func seedContractRDWith(t *testing.T, st store.Store, rd apiv1.ResourceDefinition, pools map[string]string) {
	t.Helper()

	ctx := t.Context()

	for _, n := range contractNodeNames() {
		if err := st.Nodes().Create(ctx, &apiv1.Node{Name: n}); err != nil {
			t.Fatalf("seed node %s: %v", n, err)
		}
	}

	if err := st.ResourceDefinitions().Create(ctx, &rd); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.VolumeDefinitions().Create(ctx, contractRD, &apiv1.VolumeDefinition{
		VolumeNumber: 0,
		SizeKib:      1024 * 1024,
	}); err != nil {
		t.Fatalf("seed VD: %v", err)
	}

	for _, n := range contractNodeNames() {
		pool, ok := pools[n]
		if !ok {
			continue
		}

		props := map[string]string{}
		if pool != "" {
			props["StorPoolName"] = pool
		}

		if err := st.Resources().Create(ctx, &apiv1.Resource{
			Name:     contractRD,
			NodeName: n,
			Props:    props,
		}); err != nil {
			t.Fatalf("seed resource on %s: %v", n, err)
		}
	}
}

func contractResourceView(t *testing.T, base string) []lapi.ResourceWithVolumes {
	t.Helper()

	view, err := newClient(t, base).Resources.GetResourceView(t.Context(), &lapi.ListOpts{
		Resource: []string{contractRD},
	})
	if err != nil {
		t.Fatalf("GetResourceView: %v", err)
	}

	return view
}

func drbdLayer(layer *lapi.ResourceLayer) *lapi.DrbdResource {
	if layer == nil {
		return nil
	}

	if layer.Type == devicelayerkind.Drbd {
		return layer.Drbd
	}

	for i := range layer.Children {
		if drbd := drbdLayer(&layer.Children[i]); drbd != nil {
			return drbd
		}
	}

	return nil
}

func hasDRBDLayer(layer *lapi.ResourceLayer) bool {
	if layer == nil {
		return false
	}

	if layer.Type == devicelayerkind.Drbd {
		return true
	}

	for i := range layer.Children {
		if hasDRBDLayer(&layer.Children[i]) {
			return true
		}
	}

	return false
}

// TestViewUnbuiltReplicasListEveryVolume: linstor-scheduler-extender's
// InspectVolume reads `volumes[0].state.disk_state` of every replica
// with a DRBD layer, relying on LINSTOR listing one volume per volume
// definition from the moment the replica exists. blockstor answered
// `volumes: []` until the satellite reported, and the extender
// panicked with index out of range on every filter call for a pod
// whose volume was still being built.
func TestViewUnbuiltReplicasListEveryVolume(t *testing.T) {
	t.Parallel()

	st := k8s.New(fake.NewClientBuilder().WithScheme(crScheme(t)).Build())
	seedThreeReplicaRD(t, st)

	base, stop := startServerWithStore(t, st)
	defer stop()

	view := contractResourceView(t, base)
	if len(view) != len(contractNodeNames()) {
		t.Fatalf("view: got %d replicas, want %d", len(view), len(contractNodeNames()))
	}

	for _, r := range view {
		if !hasDRBDLayer(r.LayerObject) {
			t.Fatalf("%s: no DRBD layer in %+v", r.NodeName, r.LayerObject)
		}

		if len(r.Volumes) != 1 {
			t.Fatalf("%s: got %d volumes, want 1 per volume definition (the extender indexes volumes[0])", r.NodeName, len(r.Volumes))
		}

		vol := r.Volumes[0]
		if vol.VolumeNumber != 0 || vol.StoragePoolName != "data" {
			t.Errorf("%s: volume = number %d pool %q, want 0 and the replica's pool", r.NodeName, vol.VolumeNumber, vol.StoragePoolName)
		}

		// Not built yet, so not UpToDate: the extender must skip
		// the node rather than offer it.
		if vol.State.DiskState != "" {
			t.Errorf("%s: disk_state = %q before any report, want empty", r.NodeName, vol.State.DiskState)
		}
	}
}

// reportHealthyReplicas writes onto every contract replica the Status
// the satellite observer records for a healthy, connected, UpToDate
// Secondary.
func reportHealthyReplicas(t *testing.T, cli ctrlclient.Client) {
	t.Helper()

	ctx := t.Context()
	score := int32(10102)
	mayPromote := true

	for _, n := range contractNodeNames() {
		var crd crdv1alpha1.Resource
		if err := cli.Get(ctx, ctrlclient.ObjectKey{Name: k8s.ResourceCRName(contractRD, n)}, &crd); err != nil {
			t.Fatalf("get replica on %s: %v", n, err)
		}

		crd.Status.Role = "Secondary"
		crd.Status.DrbdState = "UpToDate"
		crd.Status.PromotionScore = &score
		crd.Status.MayPromote = &mayPromote
		crd.Status.Volumes = []crdv1alpha1.ResourceVolumeStatus{{
			VolumeNumber: 0,
			StoragePool:  "data",
			DevicePath:   "/dev/drbd1000",
			DiskState:    "UpToDate",
		}}

		for _, peer := range contractNodeNames() {
			if peer != n {
				crd.Status.Connections = append(crd.Status.Connections, crdv1alpha1.ResourceConnectionStatus{
					PeerNodeName:     peer,
					Connected:        true,
					Message:          "Connected",
					ReplicationState: "Established",
				})
			}
		}

		if err := cli.Status().Update(ctx, &crd); err != nil {
			t.Fatalf("report replica on %s: %v", n, err)
		}
	}
}

// TestViewHealthyReplicasPassTheCSIVolumeCondition: linstor-csi's
// ControllerGetVolume and ListVolumes report a volume abnormal on
// every replica with no `state` or with a DRBD layer whose
// `promotion_score` is 0. LINSTOR publishes the kernel's promotion
// score there; blockstor never did, so every replica of every volume
// was reported as a node with issues on each health-monitor pass while
// the same view showed it UpToDate and connected.
func TestViewHealthyReplicasPassTheCSIVolumeCondition(t *testing.T) {
	t.Parallel()

	cli := fake.NewClientBuilder().WithScheme(crScheme(t)).
		WithStatusSubresource(&crdv1alpha1.Resource{}).Build()
	st := k8s.New(cli)
	seedThreeReplicaRD(t, st)
	reportHealthyReplicas(t, cli)

	base, stop := startServerWithStore(t, st)
	defer stop()

	view := contractResourceView(t, base)
	if len(view) != len(contractNodeNames()) {
		t.Fatalf("view: got %d replicas, want %d", len(view), len(contractNodeNames()))
	}

	for _, r := range view {
		if r.State == nil {
			t.Errorf("%s: no state, linstor-csi reports the node as having issues", r.NodeName)

			continue
		}

		drbd := drbdLayer(r.LayerObject)
		if drbd == nil {
			t.Fatalf("%s: no DRBD layer in %+v", r.NodeName, r.LayerObject)
		}

		if drbd.PromotionScore == 0 {
			t.Errorf("%s: layer_object.drbd.promotion_score = 0 on a healthy replica, linstor-csi reports the node as having issues", r.NodeName)
		}

		if !drbd.MayPromote {
			t.Errorf("%s: layer_object.drbd.may_promote = false, want the observed true", r.NodeName)
		}
	}
}

// extenderDataNodes runs the replica loop of
// linstor-scheduler-extender v0.3.6 InspectVolume
// (pkg/driver/linstor.go): it reads volumes[0] of every replica with a
// DRBD layer, unguarded, and keeps the UpToDate diskful nodes. A panic
// there is what the extender's HTTP server turns into an EOF for the
// scheduler, so it fails the test instead.
func extenderDataNodes(t *testing.T, view []lapi.ResourceWithVolumes) []string {
	t.Helper()

	var nodes []string

	for _, r := range view {
		func() {
			defer func() {
				if p := recover(); p != nil {
					t.Fatalf("extender InspectVolume panics on the replica on %s (volumes %+v): %v", r.NodeName, r.Volumes, p)
				}
			}()

			if hasDRBDLayer(r.LayerObject) && r.Volumes[0].State.DiskState != "UpToDate" {
				return
			}

			nodes = append(nodes, r.NodeName)
		}()
	}

	return nodes
}

// reportInconsistent records on the replica on node what the satellite
// observes for a built volume whose initial sync never finished.
func reportInconsistent(t *testing.T, cli ctrlclient.Client, node string) {
	t.Helper()

	var crd crdv1alpha1.Resource
	if err := cli.Get(t.Context(), ctrlclient.ObjectKey{Name: k8s.ResourceCRName(contractRD, node)}, &crd); err != nil {
		t.Fatalf("get replica on %s: %v", node, err)
	}

	crd.Status.Role = "Secondary"
	crd.Status.DrbdState = "Inconsistent"
	crd.Status.Volumes = []crdv1alpha1.ResourceVolumeStatus{{
		VolumeNumber: 0,
		StoragePool:  "data",
		DevicePath:   "/dev/drbd1000",
		DiskState:    "Inconsistent",
	}}

	if err := cli.Status().Update(t.Context(), &crd); err != nil {
		t.Fatalf("report replica on %s: %v", node, err)
	}
}

// TestViewPoollessReplicaListsItsVolume: a restore can leave a diskful
// replica with no pool on its spec while its siblings are built from
// their local clones. LINSTOR cannot hold such a replica: it creates
// every Volume with a resolved pool, a diskful neighbour's when no
// StorPoolName is set and that pool exists on the replica's node.
// blockstor listed it with no volumes, and
// linstor-scheduler-extender panicked with index out of range on every
// filter call for the pod of that volume.
func TestViewPoollessReplicaListsItsVolume(t *testing.T) {
	t.Parallel()

	cli := fake.NewClientBuilder().WithScheme(crScheme(t)).
		WithStatusSubresource(&crdv1alpha1.Resource{}).Build()
	st := k8s.New(cli)
	seedContractRD(t, st, map[string]string{"node-a": "data", "node-b": "data", "node-c": ""})
	seedPools(t, st, "data", contractNodeNames()...)
	reportInconsistent(t, cli, "node-a")
	reportInconsistent(t, cli, "node-b")

	base, stop := startServerWithStore(t, st)
	defer stop()

	view := contractResourceView(t, base)
	if len(view) != len(contractNodeNames()) {
		t.Fatalf("view: got %d replicas, want %d", len(view), len(contractNodeNames()))
	}

	if nodes := extenderDataNodes(t, view); len(nodes) != 0 {
		t.Errorf("extender data nodes = %v, want none while no replica is UpToDate", nodes)
	}

	for _, r := range view {
		if len(r.Volumes) != 1 {
			t.Fatalf("%s: got %d volumes, want 1 per volume definition", r.NodeName, len(r.Volumes))
		}

		if r.Volumes[0].StoragePoolName != "data" {
			t.Errorf("%s: storage_pool_name = %q, want the diskful neighbours' %q", r.NodeName, r.Volumes[0].StoragePoolName, "data")
		}
	}
}

// TestViewUnresolvablePoolStillListsTheVolume: with no pool anywhere to
// resolve, the replica still lists one volume per volume definition.
// The row then names no pool, which only the Python CLI's `n describe`
// trips over; an empty list panics the scheduler extender instead.
func TestViewUnresolvablePoolStillListsTheVolume(t *testing.T) {
	t.Parallel()

	st := k8s.New(fake.NewClientBuilder().WithScheme(crScheme(t)).Build())
	seedContractRD(t, st, map[string]string{"node-a": ""})

	base, stop := startServerWithStore(t, st)
	defer stop()

	view := contractResourceView(t, base)
	if len(view) != 1 {
		t.Fatalf("view: got %d replicas, want 1", len(view))
	}

	extenderDataNodes(t, view)

	if got := len(view[0].Volumes); got != 1 {
		t.Fatalf("got %d volumes, want 1 per volume definition", got)
	}
}

// seedPools creates a pool named pool of a backed kind on each node.
func seedPools(t *testing.T, st store.Store, pool string, nodes ...string) {
	t.Helper()

	for _, n := range nodes {
		if err := st.StoragePools().Create(t.Context(), &apiv1.StoragePool{
			StoragePoolName: pool,
			NodeName:        n,
			ProviderKind:    apiv1.StoragePoolKindZFSThin,
		}); err != nil {
			t.Fatalf("seed pool %s on %s: %v", pool, n, err)
		}
	}
}

func contractVolumePool(t *testing.T, view []lapi.ResourceWithVolumes, node string) string {
	t.Helper()

	for _, r := range view {
		if r.NodeName != node {
			continue
		}

		if len(r.Volumes) != 1 {
			t.Fatalf("%s: got %d volumes, want 1 per volume definition", node, len(r.Volumes))
		}

		return r.Volumes[0].StoragePoolName
	}

	t.Fatalf("no replica on %s in the view", node)

	return ""
}

// TestViewPoollessReplicaPoolResolvesLikeLINSTOR: the placeholder of a
// replica with no pool on its spec names the pool LINSTOR v1.33.1
// would have created its Volume in (CtrlStorPoolResolveHelper
// .resolveStorPool). A neighbour's pool counts only when it exists on
// the replica's own node, DfltStorPool comes after the neighbours, and
// a StorPoolName set on the definition is the only candidate. Before,
// the view took a sibling's pool unchecked and listed a pool the
// replica's node does not have.
func TestViewPoollessReplicaPoolResolvesLikeLINSTOR(t *testing.T) {
	t.Parallel()

	pools := map[string]string{"node-a": "data", "node-b": "data", "node-c": ""}

	cases := []struct {
		name string
		rd   apiv1.ResourceDefinition
		seed func(t *testing.T, st store.Store)
		want string
	}{{
		name: "neighbour pool present on the node",
		rd:   apiv1.ResourceDefinition{Name: contractRD},
		seed: func(t *testing.T, st store.Store) {
			t.Helper()
			seedPools(t, st, "data", contractNodeNames()...)
		},
		want: "data",
	}, {
		name: "neighbour pool diskless on the node",
		rd:   apiv1.ResourceDefinition{Name: contractRD},
		seed: func(t *testing.T, st store.Store) {
			t.Helper()
			seedPools(t, st, "data", "node-a", "node-b")

			if err := st.StoragePools().Create(t.Context(), &apiv1.StoragePool{
				StoragePoolName: "data",
				NodeName:        "node-c",
				ProviderKind:    apiv1.StoragePoolKindDiskless,
			}); err != nil {
				t.Fatalf("seed diskless pool: %v", err)
			}
		},
		want: "",
	}, {
		name: "neighbour pool missing on the node",
		rd:   apiv1.ResourceDefinition{Name: contractRD},
		seed: func(t *testing.T, st store.Store) {
			t.Helper()
			seedPools(t, st, "data", "node-a", "node-b")
		},
		want: "",
	}, {
		name: "DfltStorPool after the neighbours",
		rd:   apiv1.ResourceDefinition{Name: contractRD},
		seed: func(t *testing.T, st store.Store) {
			t.Helper()
			seedPools(t, st, "data", "node-a", "node-b")
			seedPools(t, st, defaultStorPoolName, "node-c")
		},
		want: defaultStorPoolName,
	}, {
		name: "definition prop wins over the neighbours",
		rd:   apiv1.ResourceDefinition{Name: contractRD, Props: map[string]string{storPoolPropKey: "fast"}},
		seed: func(t *testing.T, st store.Store) {
			t.Helper()
			seedPools(t, st, "data", contractNodeNames()...)
			seedPools(t, st, "fast", "node-c")
		},
		want: "fast",
	}, {
		name: "definition prop missing on the node is not replaced",
		rd:   apiv1.ResourceDefinition{Name: contractRD, Props: map[string]string{storPoolPropKey: "fast"}},
		seed: func(t *testing.T, st store.Store) {
			t.Helper()
			seedPools(t, st, "data", contractNodeNames()...)
		},
		want: "",
	}}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			st := k8s.New(fake.NewClientBuilder().WithScheme(crScheme(t)).Build())
			seedContractRDWith(t, st, tc.rd, pools)
			tc.seed(t, st)

			base, stop := startServerWithStore(t, st)
			defer stop()

			view := contractResourceView(t, base)
			extenderDataNodes(t, view)

			if got := contractVolumePool(t, view, "node-c"); got != tc.want {
				t.Errorf("node-c placeholder storage_pool_name = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestViewPoolLookupFailureKeepsTheView: resolving a placeholder's
// pool is a read made for display. A failed lookup used to fail the
// whole /v1/view/resources answer with a 500, so one definition broke
// every client listing the view; it now lists the volume without a
// pool.
func TestViewPoolLookupFailureKeepsTheView(t *testing.T) {
	t.Parallel()

	backend := k8s.New(fake.NewClientBuilder().WithScheme(crScheme(t)).Build())
	seedContractRD(t, backend, map[string]string{"node-a": ""})

	// The first read of the definition is the pool lookup's; the view's
	// own effective-props read after it goes through.
	st := faultyStore{backend, storeFaults{target: contractRD, rdGetPasses: budgetOf(0)}}

	base, stop := startServerWithStore(t, st)
	defer stop()

	view := contractResourceView(t, base)
	if got := contractVolumePool(t, view, "node-a"); got != "" {
		t.Errorf("storage_pool_name = %q, want empty after a failed lookup", got)
	}
}

// countingSiblingLists counts Resources().ListByDefinition calls.
type countingSiblingLists struct {
	store.Store

	lists *atomic.Int32
}

type countingReplicas struct {
	store.ResourceStore

	lists *atomic.Int32
}

func (s countingSiblingLists) Resources() store.ResourceStore {
	return countingReplicas{s.Store.Resources(), s.lists}
}

func (r countingReplicas) ListByDefinition(ctx context.Context, rdName string) ([]apiv1.Resource, error) {
	r.lists.Add(1)

	return r.ResourceStore.ListByDefinition(ctx, rdName) //nolint:wrapcheck // pass-through test double
}

// TestViewPoolLookupOnlyForAPlaceholder: a replica with no pool on its
// spec whose volumes the satellite has reported gets no placeholder,
// so the view must not pay for resolving one on every request.
func TestViewPoolLookupOnlyForAPlaceholder(t *testing.T) {
	t.Parallel()

	cli := fake.NewClientBuilder().WithScheme(crScheme(t)).
		WithStatusSubresource(&crdv1alpha1.Resource{}).Build()
	backend := k8s.New(cli)
	seedContractRD(t, backend, map[string]string{"node-a": ""})
	reportInconsistent(t, cli, "node-a")

	var lists atomic.Int32

	base, stop := startServerWithStore(t, countingSiblingLists{backend, &lists})
	defer stop()

	contractResourceView(t, base)

	if n := lists.Load(); n != 0 {
		t.Errorf("sibling replicas listed %d times for a replica with every volume reported, want 0", n)
	}
}
