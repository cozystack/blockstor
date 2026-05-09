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

	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	controllerpkg "github.com/cozystack/blockstor/internal/controller"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestRGPlaceCountBumpFillsGap: bumping place_count from 2 to 3 on
// the parent RG triggers the placer to backfill the missing replica
// for every spawned RD on the next RG reconcile. Existing replicas
// keep their nodes — placer.Place treats them as already-placed.
func TestRGPlaceCountBumpFillsGap(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()
	st := store.NewInMemory()

	for _, n := range []string{"n1", "n2", "n3"} {
		_ = st.Nodes().Create(ctx, &apiv1.Node{Name: n, Type: apiv1.NodeTypeSatellite})
		_ = st.StoragePools().Create(ctx, &apiv1.StoragePool{
			StoragePoolName: "pool",
			NodeName:        n,
			ProviderKind:    apiv1.StoragePoolKindLVMThin,
		})
	}

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "rg",
		SelectFilter: apiv1.AutoSelectFilter{
			PlaceCount:  2,
			StoragePool: "pool",
		},
	}); err != nil {
		t.Fatalf("seed rg: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:              "pvc-prop",
		ResourceGroupName: "rg",
	}); err != nil {
		t.Fatalf("seed rd: %v", err)
	}

	for _, n := range []string{"n1", "n2"} {
		_ = st.Resources().Create(ctx, &apiv1.Resource{Name: "pvc-prop", NodeName: n})
	}

	// Bump place_count on the RG.
	updated := apiv1.ResourceGroup{
		Name: "rg",
		SelectFilter: apiv1.AutoSelectFilter{
			PlaceCount:  3,
			StoragePool: "pool",
		},
	}

	if err := st.ResourceGroups().Update(ctx, &updated); err != nil {
		t.Fatalf("update rg: %v", err)
	}

	rec := &controllerpkg.ResourceGroupReconciler{Client: cli, Scheme: scheme, Store: st}

	_, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "rg"}})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got, err := st.Resources().ListByDefinition(ctx, "pvc-prop")
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	if len(got) != 3 {
		t.Fatalf("replica count after bump: got %d, want 3; entries=%v", len(got), got)
	}

	nodes := map[string]bool{}
	for _, r := range got {
		nodes[r.NodeName] = true
	}

	if !nodes["n1"] || !nodes["n2"] || !nodes["n3"] {
		t.Errorf("expected replicas on n1+n2+n3 after bump; got %v", got)
	}
}

// TestRGUpdateNoChangeNoOp: re-running the reconciler on an unchanged
// RG doesn't churn anything. Idempotency for the periodic resync.
func TestRGUpdateNoChangeNoOp(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()
	st := store.NewInMemory()

	for _, n := range []string{"n1", "n2"} {
		_ = st.Nodes().Create(ctx, &apiv1.Node{Name: n, Type: apiv1.NodeTypeSatellite})
		_ = st.StoragePools().Create(ctx, &apiv1.StoragePool{
			StoragePoolName: "pool",
			NodeName:        n,
			ProviderKind:    apiv1.StoragePoolKindLVMThin,
		})
	}

	_ = st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name:         "rg",
		SelectFilter: apiv1.AutoSelectFilter{PlaceCount: 2, StoragePool: "pool"},
	})
	_ = st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:              "pvc-stable",
		ResourceGroupName: "rg",
	})

	for _, n := range []string{"n1", "n2"} {
		_ = st.Resources().Create(ctx, &apiv1.Resource{Name: "pvc-stable", NodeName: n})
	}

	rec := &controllerpkg.ResourceGroupReconciler{Client: cli, Scheme: scheme, Store: st}

	for range 3 {
		_, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "rg"}})
		if err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
	}

	got, _ := st.Resources().ListByDefinition(ctx, "pvc-stable")
	if len(got) != 2 {
		t.Errorf("replica count drifted: got %d, want 2; entries=%v", len(got), got)
	}
}
