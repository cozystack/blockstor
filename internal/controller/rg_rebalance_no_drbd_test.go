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

	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	controllerpkg "github.com/cozystack/blockstor/internal/controller"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// seedNoDRBDRebalance builds three online nodes, a group with the given
// stack and annotations, and one child definition with no replica, the
// state a storage-only volume is in once its only replica is deleted.
func seedNoDRBDRebalance(t *testing.T, ctx context.Context, st store.Store, rgStack, rdStack []string, annotations map[string]string) {
	t.Helper()

	for _, n := range []string{"n1", "n2", "n3"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{
			Name:             n,
			Type:             apiv1.NodeTypeSatellite,
			ConnectionStatus: apiv1.NodeTypeOnline,
		}); err != nil {
			t.Fatalf("seed node %q: %v", n, err)
		}

		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			StoragePoolName: "pool",
			NodeName:        n,
			ProviderKind:    apiv1.StoragePoolKindLVMThin,
		}); err != nil {
			t.Fatalf("seed pool %q: %v", n, err)
		}
	}

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "rg",
		SelectFilter: apiv1.AutoSelectFilter{
			PlaceCount:  1,
			StoragePool: "pool",
			LayerStack:  rgStack,
		},
		Annotations: annotations,
	}); err != nil {
		t.Fatalf("seed rg: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:              "pvc-local",
		ResourceGroupName: "rg",
		LayerStack:        rdStack,
	}); err != nil {
		t.Fatalf("seed rd: %v", err)
	}
}

func replicasOf(t *testing.T, ctx context.Context, st store.Store, rd string) int {
	t.Helper()

	got, err := st.Resources().ListByDefinition(ctx, rd)
	if err != nil {
		t.Fatalf("list replicas of %q: %v", rd, err)
	}

	return len(got)
}

// TestRGRebalanceScheduledSkipsDefinitionWithoutDRBD: LINSTOR's balance
// task ignores a definition with no DRBD layer, so the scheduled pass
// must not give a storage-only definition a fresh, empty replica. Both
// the definition's own stack and one it takes from its group count.
func TestRGRebalanceScheduledSkipsDefinitionWithoutDRBD(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		rgStack, rdStack []string
	}{
		"stack on the definition": {rgStack: nil, rdStack: []string{apiv1.LayerKindStorage}},
		"stack from the group":    {rgStack: []string{apiv1.LayerKindStorage}, rdStack: nil},
		"LUKS over storage":       {rgStack: nil, rdStack: []string{apiv1.LayerKindLUKS, apiv1.LayerKindStorage}},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			scheme := newScheme(t)
			cli := fake.NewClientBuilder().WithScheme(scheme).Build()
			st := store.NewInMemory()

			seedNoDRBDRebalance(t, ctx, st, tc.rgStack, tc.rdStack, nil)

			rec := &controllerpkg.RGRebalanceReconciler{Client: cli, Scheme: scheme, Store: st}

			if _, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "rg"}}); err != nil {
				t.Fatalf("Reconcile: %v", err)
			}

			if n := replicasOf(t, ctx, st, "pvc-local"); n != 0 {
				t.Errorf("scheduled pass placed %d replicas on a definition without DRBD, want 0", n)
			}
		})
	}
}

// TestRGRebalanceScheduledStillPlacesDRBDDefinition is the control: the
// same fixture with the default stack is topped up as before.
func TestRGRebalanceScheduledStillPlacesDRBDDefinition(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()
	st := store.NewInMemory()

	seedNoDRBDRebalance(t, ctx, st, nil, nil, nil)

	rec := &controllerpkg.RGRebalanceReconciler{Client: cli, Scheme: scheme, Store: st}

	if _, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "rg"}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if n := replicasOf(t, ctx, st, "pvc-local"); n != 1 {
		t.Errorf("scheduled pass placed %d replicas on a DRBD definition, want 1", n)
	}
}

// TestRGRebalanceAnnotationPlacesDefinitionWithoutDRBD: an explicit
// `rg modify` is not the balance task. LINSTOR autoplaces every child
// definition on it whatever its stack, and so does blockstor.
func TestRGRebalanceAnnotationPlacesDefinitionWithoutDRBD(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()
	st := store.NewInMemory()

	seedNoDRBDRebalance(t, ctx, st, []string{apiv1.LayerKindStorage}, nil, map[string]string{
		apiv1.AnnotationRGRebalancePending: "2026-05-14T00:00:00Z",
	})

	rec := &controllerpkg.RGRebalanceReconciler{Client: cli, Scheme: scheme, Store: st}

	if _, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "rg"}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if n := replicasOf(t, ctx, st, "pvc-local"); n != 1 {
		t.Errorf("rg modify placed %d replicas on a definition without DRBD, want 1", n)
	}
}

// TestRGEventSkipsDefinitionWithoutDRBD: the RG reconciler places on
// every group event, a controller start included, with nobody asking.
// It must leave a storage-only definition alone the way the balance
// pass does.
func TestRGEventSkipsDefinitionWithoutDRBD(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()
	st := store.NewInMemory()

	seedNoDRBDRebalance(t, ctx, st, []string{apiv1.LayerKindStorage}, nil, nil)

	rec := &controllerpkg.ResourceGroupReconciler{Client: cli, Scheme: scheme, Store: st}

	if _, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "rg"}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if n := replicasOf(t, ctx, st, "pvc-local"); n != 0 {
		t.Errorf("RG event placed %d replicas on a definition without DRBD, want 0", n)
	}
}
