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

package controller_test

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// A node registered as Node-1 is reconciled under its CR name, node-1, while
// the store shows its replicas on Node-1. The drain still counts the lone
// replica it is evacuating: it gap-fills a replacement and keeps the source,
// instead of reading the source as a replica on some other draining node and
// deriving a drain target of zero.
func TestEvacuatingANodeRegisteredInMixedCaseKeepsItsLoneSource(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)
	st := store.NewInMemory()

	for _, name := range []string{"Node-1", "n2", "n3"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{Name: name, Type: apiv1.NodeTypeSatellite}); err != nil {
			t.Fatalf("seed node: %v", err)
		}

		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			StoragePoolName: "pool", NodeName: name, ProviderKind: apiv1.StoragePoolKindLVMThin,
		}); err != nil {
			t.Fatalf("seed pool: %v", err)
		}
	}

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "DfltRscGrp", SelectFilter: apiv1.AutoSelectFilter{StoragePool: "pool"},
	}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-solo", ResourceGroupName: "DfltRscGrp"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{Name: "pvc-solo", NodeName: "Node-1"}); err != nil {
		t.Fatalf("seed resource: %v", err)
	}

	if err := st.Nodes().Update(ctx, &apiv1.Node{Name: "Node-1", Type: apiv1.NodeTypeSatellite, Flags: []string{apiv1.NodeFlagEvicted}}); err != nil {
		t.Fatalf("flag Node-1 evicted: %v", err)
	}

	srcCRD := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-solo.node-1"},
		Spec:       blockstoriov1alpha1.ResourceSpec{ResourceDefinitionName: "pvc-solo", NodeName: "node-1"},
	}
	nodeCRD := &blockstoriov1alpha1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-1", Annotations: map[string]string{"blockstor.io/linstor-name": "Node-1"}},
		Spec:       blockstoriov1alpha1.NodeSpec{Type: apiv1.NodeTypeSatellite, Flags: []string{apiv1.NodeFlagEvicted}},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}).
		WithObjects(nodeCRD, srcCRD).
		Build()

	rec := &controllerpkg.NodeReconciler{Client: cli, Scheme: scheme, Store: st}

	if _, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "node-1"}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	replicas, err := st.Resources().ListByDefinition(ctx, "pvc-solo")
	if err != nil {
		t.Fatalf("list replicas: %v", err)
	}

	if !hasDiskfulOn(replicas, "n2") && !hasDiskfulOn(replicas, "n3") {
		t.Errorf("no replacement gap-filled for the lone replica on Node-1; replicas=%+v", replicas)
	}

	if err := cli.Get(ctx, types.NamespacedName{Name: "pvc-solo.node-1"}, &blockstoriov1alpha1.Resource{}); err != nil {
		t.Errorf("the lone source was pruned before a replacement exists: %v", err)
	}
}

// The drain target leaves out replicas on other draining nodes, whichever
// spelling a replica row keeps for its node: a row stored as n2 is on the node
// registered as N2, which is LOST.
func TestTheDrainTargetSkipsADrainingNodeInAnySpelling(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewInMemory()

	for _, n := range []apiv1.Node{
		{Name: "Node-1", Type: apiv1.NodeTypeSatellite, Flags: []string{apiv1.NodeFlagEvicted}},
		{Name: "N2", Type: apiv1.NodeTypeSatellite, Flags: []string{apiv1.NodeFlagLost}},
		{Name: "n3", Type: apiv1.NodeTypeSatellite},
		{Name: "n4", Type: apiv1.NodeTypeSatellite},
		{Name: "n5", Type: apiv1.NodeTypeSatellite},
	} {
		if err := st.Nodes().Create(ctx, &n); err != nil {
			t.Fatalf("seed node: %v", err)
		}
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-t"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	for _, node := range []string{"Node-1", "n2", "n3"} {
		if err := st.Resources().Create(ctx, &apiv1.Resource{Name: "pvc-t", NodeName: node}); err != nil {
			t.Fatalf("seed replica on %s: %v", node, err)
		}
	}

	rec := &controllerpkg.NodeReconciler{Store: st}

	got, err := rec.CurrentDiskfulTarget(ctx, "pvc-t", "node-1")
	if err != nil {
		t.Fatalf("CurrentDiskfulTarget: %v", err)
	}

	if got != 2 {
		t.Errorf("drain target %d, want 2: the evacuated replica and the one on n3, not the one on LOST N2", got)
	}
}
