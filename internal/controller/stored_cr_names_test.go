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
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// mixedCaseStand is a fake API server with a definition registered as
// Pvc-Mix and a node registered as Node-1 (offline), and one replica created
// through the store per entry of replicas, each a pair of the spellings the
// caller used for the definition and the node.
func mixedCaseStand(t *testing.T, funcs interceptor.Funcs, replicas ...[2]string) client.Client {
	t.Helper()

	rd := blockstoriov1alpha1.ResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: k8s.Name("Pvc-Mix")}}
	k8s.SetOriginalName(&rd.ObjectMeta, "Pvc-Mix")

	node := blockstoriov1alpha1.Node{ObjectMeta: metav1.ObjectMeta{Name: k8s.Name("Node-1")}}
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
// replica created now as NODE-1 stores the same name, so the port allocator
// sees the older replica's port when it picks for the new one. With the
// registered spelling Node-1 in spec the exact compare missed it.
func TestTakenPortsSeeAReplicaCreatedInAnotherNodeSpelling(t *testing.T) {
	t.Parallel()

	cli := mixedCaseStand(t, interceptor.Funcs{}, [2]string{"pvc-new", "NODE-1"})
	ctx := context.Background()

	port := int32(7001)

	if err := cli.Create(ctx, &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-old.node-1"},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "pvc-old", NodeName: "node-1", DRBDPort: &port,
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
		t.Errorf("ports taken on %s = %v, want the %d of the older replica on node-1", self.Spec.NodeName, taken, port)
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
