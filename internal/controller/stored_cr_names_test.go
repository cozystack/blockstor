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

package controller

import (
	"context"
	"slices"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/naming"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// mixedCaseStand is a fake API server with a definition registered as
// Pvc-Mix and a node registered as Node-1 (offline), and one replica created
// through the store per entry of replicas, each a pair of the spellings the
// caller used for the definition and the node.
func mixedCaseStand(t *testing.T, funcs interceptor.Funcs, replicas ...[2]string) client.Client {
	t.Helper()

	rd := blockstoriov1alpha1.ResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: naming.Key("Pvc-Mix")}}
	k8s.SetOriginalName(&rd.ObjectMeta, "Pvc-Mix")

	node := blockstoriov1alpha1.Node{ObjectMeta: metav1.ObjectMeta{Name: naming.Key("Node-1")}}
	k8s.SetOriginalName(&node.ObjectMeta, "Node-1")
	node.Status.ConnectionStatus = blockstoriov1alpha1.NodeConnectionStatusOffline

	base := fake.NewClientBuilder().WithScheme(bug048Scheme(t)).WithObjects(&rd, &node).Build()

	for _, names := range replicas {
		if err := k8s.New(base).Resources().Create(context.Background(),
			&apiv1.Resource{Name: names[0], NodeName: names[1]}); err != nil {
			t.Fatalf("create %v through the store: %v", names, err)
		}
	}

	return interceptor.NewClient(base, funcs)
}

func storedReplicas(t *testing.T, cli client.Client) []blockstoriov1alpha1.Resource {
	t.Helper()

	var list blockstoriov1alpha1.ResourceList
	if err := cli.List(context.Background(), &list); err != nil {
		t.Fatalf("list replicas: %v", err)
	}

	return list.Items
}

// The controller does not take a replica of a mixed-case definition for an
// orphan: it finds the definition by the stored name. With the registered
// spelling in spec the Get answered NotFound and the replica was deleted as
// soon as the satellite had stamped its finalizer.
func TestOrphanCheckFindsTheMixedCaseDefinition(t *testing.T) {
	t.Parallel()

	cli := mixedCaseStand(t, interceptor.Funcs{}, [2]string{"PVC-MIX", "NODE-1"})
	res := storedReplicas(t, cli)[0]
	res.Finalizers = append(res.Finalizers, satelliteResourceFinalizer)

	r := &ResourceReconciler{Client: cli}

	orphaned, err := r.handleOrphan(context.Background(), &res)
	if err != nil {
		t.Fatalf("handleOrphan: %v", err)
	}

	if orphaned {
		t.Fatal("a replica of Pvc-Mix was taken for an orphan and deleted")
	}
}

// A replica written before the store stored the node's CR name, by
// `r c pvc-old node-1` over a node registered as Node-1, carries node-1. A
// replica created now as NODE-1 stores the CR name node-1, while an older
// replica written before that keeps Node-1. The port allocator has to see
// the older replica's port when it picks for the new one.
func TestTakenPortsSeeAReplicaCreatedInAnotherNodeSpelling(t *testing.T) {
	t.Parallel()

	cli := mixedCaseStand(t, interceptor.Funcs{}, [2]string{"pvc-new", "NODE-1"})
	ctx := context.Background()

	port := int32(7001)

	// As main wrote it before the store stored CR names: `r c pvc-old Node-1`
	// kept the registered spelling.
	if err := cli.Create(ctx, &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-old.node-1"},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "pvc-old", NodeName: "Node-1", DRBDPort: &port,
		},
	}); err != nil {
		t.Fatalf("seed the older replica: %v", err)
	}

	var self blockstoriov1alpha1.Resource

	for _, res := range storedReplicas(t, cli) {
		if res.Name != "pvc-old.node-1" {
			self = res
		}
	}

	taken, err := (&ResourceReconciler{Client: cli}).takenPortsOnNode(ctx, self.Spec.NodeName, self.Name)
	if err != nil {
		t.Fatalf("takenPortsOnNode: %v", err)
	}

	if !slices.Contains(taken, port) {
		t.Errorf("ports taken on %s = %v, want the %d of the older replica on Node-1", self.Spec.NodeName, taken, port)
	}
}

// The state projection finds the node of a replica created in a mixed-case
// node spelling, and a node event reaches the replica: an offline node
// turns its replicas Unknown. With the registered spelling in spec the
// Node Get answered NotFound and the replica was never projected.
func TestProjectionFollowsTheNodeOfAMixedCaseReplica(t *testing.T) {
	t.Parallel()

	projected := false

	cli := mixedCaseStand(t, interceptor.Funcs{
		SubResourcePatch: func(_ context.Context, _ client.Client, _ string, _ client.Object, _ client.Patch, _ ...client.SubResourcePatchOption) error {
			projected = true

			return nil
		},
	}, [2]string{"Pvc-Mix", "NODE-1"})

	res := storedReplicas(t, cli)[0]
	r := &ResourceStateProjectionReconciler{Client: cli}

	var node blockstoriov1alpha1.Node
	if err := cli.Get(context.Background(), types.NamespacedName{Name: "node-1"}, &node); err != nil {
		t.Fatalf("get the node: %v", err)
	}

	reqs := r.enqueueResourcesForNode(context.Background(), &node)
	if len(reqs) != 1 || reqs[0].Name != res.Name {
		t.Errorf("node event enqueued %v, want %s", reqs, res.Name)
	}

	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: res.Name}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if !projected {
		t.Error("the replica on the offline Node-1 was not projected Unknown")
	}
}

// sameDefinition tells two spellings of one definition from two definitions.
// The peer list, the node-id allocation, the sibling fan-out and the
// snapshot's sibling check use it for replicas written or adopted before
// the store kept CR names in spec, which may carry the display spelling.
func TestSameDefinitionFoldsCase(t *testing.T) {
	t.Parallel()

	if !sameDefinition("Pvc-Mix", "pvc-mix") {
		t.Error("Pvc-Mix and pvc-mix read as two definitions")
	}

	if sameDefinition("pvc-mix", "pvc-other") {
		t.Error("pvc-mix and pvc-other read as one definition")
	}
}

// The snapshot gate refuses a replica that is not UpToDate. The snapshot's
// node list carries the registered spelling, Node-1, while the replica stores
// the CR name node-1, and the gate has to match the two or it skips the
// replica and snapshots torn bytes.
func TestSnapshotGateSeesAReplicaUnderItsNodesCRName(t *testing.T) {
	t.Parallel()

	cli := mixedCaseStand(t, interceptor.Funcs{}, [2]string{"Pvc-Mix", "NODE-1"})
	ctx := context.Background()

	res := storedReplicas(t, cli)[0]
	res.Status.Volumes = []blockstoriov1alpha1.ResourceVolumeStatus{{VolumeNumber: 0, DiskState: "Outdated"}}

	if err := cli.Update(ctx, &res); err != nil {
		t.Fatalf("mark the replica Outdated: %v", err)
	}

	snap := &blockstoriov1alpha1.Snapshot{Spec: blockstoriov1alpha1.SnapshotSpec{
		ResourceDefinitionName: "pvc-mix", SnapshotName: "snap", Nodes: []string{"Node-1"},
	}}

	ok, node, err := (&SnapshotReconciler{Client: cli}).siblingReplicasUpToDate(ctx, snap)
	if err != nil {
		t.Fatalf("siblingReplicasUpToDate: %v", err)
	}

	if ok {
		t.Errorf("the gate passed an Outdated replica on Node-1 stored as %s", res.Spec.NodeName)
	}

	if node != res.Spec.NodeName {
		t.Errorf("offending node %q, want %q", node, res.Spec.NodeName)
	}
}

// The auto-diskful promotion picks a pool on the replica's node. The pool
// keeps the node spelling it was created with, the replica the CR name.
func TestFirstAvailablePoolMatchesTheReplicasCRNodeName(t *testing.T) {
	t.Parallel()

	cli := mixedCaseStand(t, interceptor.Funcs{})
	ctx := context.Background()
	st := k8s.New(cli)

	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		StoragePoolName: "pool", NodeName: "Node-1", ProviderKind: apiv1.StoragePoolKindLVMThin,
	}); err != nil {
		t.Fatalf("create the pool through the store: %v", err)
	}

	for name, find := range map[string]func(context.Context, string) (string, error){
		"resource":     (&ResourceReconciler{Client: cli, Store: st}).firstAvailablePool,
		"auto-diskful": (&AutoDiskfulReconciler{Client: cli, Store: st}).firstAvailablePool,
	} {
		got, err := find(ctx, naming.Key("Node-1"))
		if err != nil {
			t.Fatalf("%s firstAvailablePool: %v", name, err)
		}

		if got != "pool" {
			t.Errorf("%s firstAvailablePool(node-1) = %q, want the pool created on Node-1", name, got)
		}
	}
}

// Evacuating a node migrates every replica on it, the one stored under the
// node's CR name as well as one written before in the registered spelling.
// A definition without a group refuses the migration with an annotation,
// which shows the replica was reached.
func TestEvacuationReachesReplicasInEverySpellingOfTheNode(t *testing.T) {
	t.Parallel()

	for _, spelling := range []string{"NODE-1", "node-1"} {
		t.Run(spelling, func(t *testing.T) {
			t.Parallel()

			cli := mixedCaseStand(t, interceptor.Funcs{}, [2]string{"Pvc-Mix", spelling})
			ctx := context.Background()

			var node blockstoriov1alpha1.Node
			if err := cli.Get(ctx, types.NamespacedName{Name: "node-1"}, &node); err != nil {
				t.Fatalf("get the node: %v", err)
			}

			node.Spec.Flags = append(node.Spec.Flags, apiv1.NodeFlagEvicted)
			if err := cli.Update(ctx, &node); err != nil {
				t.Fatalf("evict the node: %v", err)
			}

			r := &NodeReconciler{Client: cli, Store: k8s.New(cli)}
			if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "node-1"}}); err != nil {
				t.Fatalf("Reconcile: %v", err)
			}

			var rd blockstoriov1alpha1.ResourceDefinition
			if err := cli.Get(ctx, types.NamespacedName{Name: "pvc-mix"}, &rd); err != nil {
				t.Fatalf("get the definition: %v", err)
			}

			if rd.Annotations[AnnotationMigrationBlocked] == "" {
				t.Errorf("the replica created as %s on Node-1 was not migrated", spelling)
			}
		})
	}
}

// A replica written before the store stored CR names keeps Node-1 in spec;
// a node event for node-1 still reaches it.
func TestProjectionReachesAReplicaStoredInTheRegisteredSpelling(t *testing.T) {
	t.Parallel()

	cli := mixedCaseStand(t, interceptor.Funcs{})
	ctx := context.Background()

	if err := cli.Create(ctx, &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-mix.node-1"},
		Spec:       blockstoriov1alpha1.ResourceSpec{ResourceDefinitionName: "Pvc-Mix", NodeName: "Node-1"},
	}); err != nil {
		t.Fatalf("seed the older replica: %v", err)
	}

	var node blockstoriov1alpha1.Node
	if err := cli.Get(ctx, types.NamespacedName{Name: "node-1"}, &node); err != nil {
		t.Fatalf("get the node: %v", err)
	}

	reqs := (&ResourceStateProjectionReconciler{Client: cli}).enqueueResourcesForNode(ctx, &node)
	if len(reqs) != 1 || reqs[0].Name != "pvc-mix.node-1" {
		t.Errorf("node event enqueued %v, want pvc-mix.node-1", reqs)
	}
}

// sameNode tells two spellings of one node from two nodes, the way the
// store addresses them. A case variant of a name outside RFC 1123 hashes to
// another CR name, so My_Node and my_node are two nodes.
func TestSameNodeFoldsCase(t *testing.T) {
	t.Parallel()

	for _, pair := range [][2]string{{"Node-1", "node-1"}, {"Node-1", naming.Key("Node-1")}, {"my_node", naming.Key("my_node")}} {
		if !sameNode(pair[0], pair[1]) {
			t.Errorf("%s and %s read as two nodes", pair[0], pair[1])
		}
	}

	for _, pair := range [][2]string{{"node-1", "node-2"}, {"My_Node", "my_node"}} {
		if sameNode(pair[0], pair[1]) {
			t.Errorf("%s and %s read as one node", pair[0], pair[1])
		}
	}
}
