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

// TestNodeReconciler_EvictedTriggersMigration: when a Node is flagged
// EVICTED, every Resource on it gets a replacement created on a
// non-evicted node. Existing peer count + new replica must match the
// RG's place_count.
func TestNodeReconciler_EvictedTriggersMigration(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()

	st := store.NewInMemory()

	// Two storage pools on three nodes; n1 is the evicted one.
	for _, name := range []string{"n1", "n2", "n3"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{Name: name, Type: apiv1.NodeTypeSatellite}); err != nil {
			t.Fatalf("seed node: %v", err)
		}

		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			StoragePoolName: "pool",
			NodeName:        name,
			ProviderKind:    apiv1.StoragePoolKindLVMThin,
		}); err != nil {
			t.Fatalf("seed pool: %v", err)
		}
	}

	// RG with place_count=2.
	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "rg",
		SelectFilter: apiv1.AutoSelectFilter{
			PlaceCount:  2,
			StoragePool: "pool",
		},
	}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:              "pvc-1",
		ResourceGroupName: "rg",
	}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	// Two replicas exist: n1 + n2. We're going to evict n1.
	for _, node := range []string{"n1", "n2"} {
		if err := st.Resources().Create(ctx, &apiv1.Resource{Name: "pvc-1", NodeName: node}); err != nil {
			t.Fatalf("seed resource: %v", err)
		}
	}

	// Mark n1 evicted via the store + Node CRD on the fake client.
	n1 := &apiv1.Node{
		Name:  "n1",
		Type:  apiv1.NodeTypeSatellite,
		Flags: []string{apiv1.NodeFlagEvicted},
	}

	if err := st.Nodes().Update(ctx, n1); err != nil {
		t.Fatalf("flag n1 evicted: %v", err)
	}

	if err := cli.Create(ctx, &blockstoriov1alpha1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "n1"},
		Spec: blockstoriov1alpha1.NodeSpec{
			Type:  apiv1.NodeTypeSatellite,
			Flags: []string{apiv1.NodeFlagEvicted},
		},
	}); err != nil {
		t.Fatalf("create n1 CRD: %v", err)
	}

	rec := &controllerpkg.NodeReconciler{Client: cli, Scheme: scheme, Store: st}

	_, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "n1"}})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got, err := st.Resources().ListByDefinition(ctx, "pvc-1")
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	// EVICTED is the soft "drain me" hint: the migration adds a
	// replacement on a healthy node but leaves the source replica
	// in place — the operator decides when to actually remove it
	// (typically once the new replica is UpToDate). For LOST, the
	// source is deleted in the same reconcile pass.
	//
	// So after one EVICTED reconcile we expect 3 replicas: original
	// n1 + n2 + the freshly placed replacement on n3.
	if len(got) != 3 {
		t.Fatalf("replica count: got %d, want 3; entries=%v", len(got), got)
	}

	gotNodes := map[string]bool{}
	for _, r := range got {
		gotNodes[r.NodeName] = true
	}

	for _, want := range []string{"n1", "n2", "n3"} {
		if !gotNodes[want] {
			t.Errorf("expected replica on %s after migration; got %v", want, got)
		}
	}
}

// TestNodeReconciler_LostDeletesSourceResource: when a Node is flagged
// LOST, the Resource on it is deleted via the K8s API in addition to
// the migration trigger.
func TestNodeReconciler_LostDeletesSourceResource(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)

	resCRD := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.n1"},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "pvc-1",
			NodeName:               "n1",
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(
			&blockstoriov1alpha1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "n1"},
				Spec: blockstoriov1alpha1.NodeSpec{
					Type:  apiv1.NodeTypeSatellite,
					Flags: []string{apiv1.NodeFlagEvicted, apiv1.NodeFlagLost},
				},
			},
			resCRD,
		).
		Build()

	st := store.NewInMemory()

	for _, name := range []string{"n1", "n2"} {
		_ = st.Nodes().Create(ctx, &apiv1.Node{Name: name, Type: apiv1.NodeTypeSatellite})
		_ = st.StoragePools().Create(ctx, &apiv1.StoragePool{
			StoragePoolName: "pool",
			NodeName:        name,
			ProviderKind:    apiv1.StoragePoolKindLVMThin,
		})
	}

	_ = st.Nodes().Update(ctx, &apiv1.Node{
		Name:  "n1",
		Type:  apiv1.NodeTypeSatellite,
		Flags: []string{apiv1.NodeFlagEvicted, apiv1.NodeFlagLost},
	})
	_ = st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-1"})
	_ = st.Resources().Create(ctx, &apiv1.Resource{Name: "pvc-1", NodeName: "n1"})

	rec := &controllerpkg.NodeReconciler{Client: cli, Scheme: scheme, Store: st}

	_, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "n1"}})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	// The Resource CRD on the lost node must be gone (or have
	// DeletionTimestamp set if it had a finalizer).
	got := &blockstoriov1alpha1.Resource{}

	err = cli.Get(ctx, types.NamespacedName{Name: "pvc-1.n1"}, got)
	if err == nil && got.DeletionTimestamp.IsZero() {
		t.Errorf("expected Resource on LOST node to be deleted; still present: %v", got)
	}
}
