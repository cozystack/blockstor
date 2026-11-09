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
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/naming"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// The evacuation and LOST paths address a replica by its definition and
// node as the wire spells them, Pvc-Mix on Node-1, and must reach the CR
// the store wrote. The migration controller asks with the CR names from a
// replica's spec instead; for a node outside RFC 1123 the composite of
// those is not the hashed CR name, so that replica is found by its spec.
func TestEvacuationFindsTheReplicaUnderEverySpelling(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		node, created, askedRD, askedNode string
	}{
		"folded name":                          {"Node-1", "NODE-1", "Pvc-Mix", "Node-1"},
		"CR names":                             {"Node-1", "node-1", "pvc-mix", "node-1"},
		"CR names outside RFC 1123":            {"my_node", "my_node", "pvc-mix", naming.Key("my_node")},
		"registered spelling outside RFC 1123": {"my_node", "my_node", "Pvc-Mix", "my_node"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			cli := mixedCaseStand(t, interceptor.Funcs{})
			ctx := context.Background()

			if tc.node != "Node-1" {
				node := blockstoriov1alpha1.Node{ObjectMeta: metav1.ObjectMeta{Name: naming.Key(tc.node)}}
				k8s.SetOriginalName(&node.ObjectMeta, tc.node)

				if err := cli.Create(ctx, &node); err != nil {
					t.Fatalf("create node %s: %v", tc.node, err)
				}
			}

			if err := k8s.New(cli).Resources().Create(ctx, &apiv1.Resource{Name: "Pvc-Mix", NodeName: tc.created}); err != nil {
				t.Fatalf("create the replica: %v", err)
			}

			res := storedReplicas(t, cli)[0]
			res.Status.Volumes = []blockstoriov1alpha1.ResourceVolumeStatus{{VolumeNumber: 0, DiskState: "UpToDate"}}

			if err := cli.Update(ctx, &res); err != nil {
				t.Fatalf("mark the replica UpToDate: %v", err)
			}

			r := &NodeReconciler{Client: cli, Store: k8s.New(cli)}

			upToDate, err := r.replicaUpToDate(ctx, tc.askedRD, tc.askedNode)
			if err != nil || !upToDate {
				t.Errorf("replicaUpToDate(%s, %s) = %v, %v; want the UpToDate replica %s", tc.askedRD, tc.askedNode, upToDate, err, res.Name)
			}

			if err := r.pruneSource(ctx, tc.askedRD, tc.askedNode); err != nil {
				t.Fatalf("pruneSource: %v", err)
			}

			var left blockstoriov1alpha1.Resource

			err = cli.Get(ctx, types.NamespacedName{Name: res.Name}, &left)
			if err == nil && left.DeletionTimestamp.IsZero() {
				t.Errorf("pruneSource(%s, %s) left the replica %s in place", tc.askedRD, tc.askedNode, res.Name)
			}
		})
	}
}

// A replica written before the store stored CR names keeps Node-1 in spec;
// the projection still reads the node and marks the replica Unknown when
// the node is offline.
func TestProjectionProjectsAReplicaStoredInTheRegisteredSpelling(t *testing.T) {
	t.Parallel()

	projected := false

	cli := mixedCaseStand(t, interceptor.Funcs{
		SubResourcePatch: func(_ context.Context, _ client.Client, _ string, _ client.Object, _ client.Patch, _ ...client.SubResourcePatchOption) error {
			projected = true

			return nil
		},
	})
	ctx := context.Background()

	if err := cli.Create(ctx, &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-mix.node-1"},
		Spec:       blockstoriov1alpha1.ResourceSpec{ResourceDefinitionName: "Pvc-Mix", NodeName: "Node-1"},
	}); err != nil {
		t.Fatalf("seed the older replica: %v", err)
	}

	r := &ResourceStateProjectionReconciler{Client: cli}
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "pvc-mix.node-1"}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if !projected {
		t.Error("the replica stored as Node-1 on the offline node was not projected Unknown")
	}
}

// A satellite writes its snapshot entry under its own node name, node-1,
// while the snapshot's node list carries the registered spelling, Node-1.
// The phase gates see that entry, and the adopted backfill does not add a
// second one for the same node.
func TestSnapshotGatesMatchAnEntryInAnotherSpelling(t *testing.T) {
	t.Parallel()

	entries := []blockstoriov1alpha1.SnapshotPerNodeStatus{{NodeName: "node-1", Ready: true}}

	if !allNodesReady(entries, []string{"Node-1"}) {
		t.Error("allNodesReady missed node-1's entry for Node-1")
	}

	if !hasNodeStatusEntry(entries, "Node-1") {
		t.Error("hasNodeStatusEntry missed node-1's entry for Node-1")
	}

	if allNodesReady(entries, []string{"Node-2"}) {
		t.Error("allNodesReady took node-1's entry for Node-2")
	}
}

// A replica written before the store stored CR names keeps Pvc-Mix in spec;
// the orphan check still finds its definition and does not delete it.
func TestOrphanCheckFindsTheDefinitionOfAnOlderRow(t *testing.T) {
	t.Parallel()

	cli := mixedCaseStand(t, interceptor.Funcs{})
	ctx := context.Background()

	old := blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-mix.node-1", Finalizers: []string{satelliteResourceFinalizer}},
		Spec:       blockstoriov1alpha1.ResourceSpec{ResourceDefinitionName: "Pvc-Mix", NodeName: "Node-1"},
	}
	if err := cli.Create(ctx, &old); err != nil {
		t.Fatalf("seed the older replica: %v", err)
	}

	orphaned, err := (&ResourceReconciler{Client: cli}).handleOrphan(ctx, &old)
	if err != nil || orphaned {
		t.Fatalf("handleOrphan on the Pvc-Mix row = %v, %v; want it kept", orphaned, err)
	}
}

// The migration controller prunes the source replica by the CR names its
// destination's spec carries. For a node outside RFC 1123 their composite
// is not the hashed CR name the store wrote, so the source is found by its
// spec.
func TestMigrationPrunesASourceOnANodeOutsideRFC1123(t *testing.T) {
	t.Parallel()

	cli := mixedCaseStand(t, interceptor.Funcs{})
	ctx := context.Background()

	node := blockstoriov1alpha1.Node{ObjectMeta: metav1.ObjectMeta{Name: naming.Key("my_node")}}
	k8s.SetOriginalName(&node.ObjectMeta, "my_node")

	if err := cli.Create(ctx, &node); err != nil {
		t.Fatalf("create node: %v", err)
	}

	if err := k8s.New(cli).Resources().Create(ctx, &apiv1.Resource{Name: "Pvc-Mix", NodeName: "my_node"}); err != nil {
		t.Fatalf("create the replica: %v", err)
	}

	res := storedReplicas(t, cli)[0]

	dst := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-mix.elsewhere"},
		Spec:       blockstoriov1alpha1.ResourceSpec{ResourceDefinitionName: res.Spec.ResourceDefinitionName},
	}

	if err := (&ResourceMigrationReconciler{Client: cli}).pruneSrc(ctx, dst, res.Spec.NodeName); err != nil {
		t.Fatalf("pruneSrc: %v", err)
	}

	var left blockstoriov1alpha1.Resource

	err := cli.Get(ctx, types.NamespacedName{Name: res.Name}, &left)
	if err == nil && left.DeletionTimestamp.IsZero() {
		t.Errorf("pruneSrc(%s, %s) left the replica %s in place", res.Spec.ResourceDefinitionName, res.Spec.NodeName, res.Name)
	}
}

// pruneSrc never deletes the destination itself, whatever the marker that
// led to it says.
func TestPruneSrcLeavesTheDestinationItself(t *testing.T) {
	t.Parallel()

	cli := mixedCaseStand(t, interceptor.Funcs{})
	ctx := context.Background()

	if err := k8s.New(cli).Resources().Create(ctx, &apiv1.Resource{Name: "pvc-self", NodeName: "node-1"}); err != nil {
		t.Fatalf("create the replica: %v", err)
	}

	var dst blockstoriov1alpha1.Resource
	if err := cli.Get(ctx, types.NamespacedName{Name: "pvc-self.node-1"}, &dst); err != nil {
		t.Fatalf("get the replica: %v", err)
	}

	if err := (&ResourceMigrationReconciler{Client: cli}).pruneSrc(ctx, &dst, "Node-1"); err != nil {
		t.Fatalf("pruneSrc: %v", err)
	}

	var left blockstoriov1alpha1.Resource
	if err := cli.Get(ctx, types.NamespacedName{Name: dst.Name}, &left); err != nil || !left.DeletionTimestamp.IsZero() {
		t.Errorf("pruneSrc deleted the destination it was handed: %v", err)
	}
}
