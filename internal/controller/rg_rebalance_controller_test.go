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

// seedRebalanceFixture builds a 3-node store with one RG and one RD,
// optionally pre-placing a list of replica nodes. The RG carries the
// rebalance-pending annotation so the reconciler will act on the
// first pass.
func seedRebalanceFixture(t *testing.T, ctx context.Context, st store.Store, placeCount int32, existingReplicaNodes []string) {
	t.Helper()

	for _, n := range []string{"n1", "n2", "n3"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{Name: n, Type: apiv1.NodeTypeSatellite}); err != nil {
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
			PlaceCount:  apiv1.LaxInt32(placeCount),
			StoragePool: "pool",
		},
		Annotations: map[string]string{
			apiv1.AnnotationRGRebalancePending: "2026-05-14T00:00:00Z",
		},
	}); err != nil {
		t.Fatalf("seed rg: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:              "pvc-rebalance",
		ResourceGroupName: "rg",
	}); err != nil {
		t.Fatalf("seed rd: %v", err)
	}

	for _, n := range existingReplicaNodes {
		if err := st.Resources().Create(ctx, &apiv1.Resource{Name: "pvc-rebalance", NodeName: n}); err != nil {
			t.Fatalf("seed existing replica on %q: %v", n, err)
		}
	}
}

// TestRGRebalanceReconcilerSpawnsAdditionalReplicas: the explicit
// annotation-driven path matches upstream LINSTOR's
// RescheduleAutoPlace behaviour — a 2→3 PlaceCount bump on the RG
// fills the gap on every child RD.
func TestRGRebalanceReconcilerSpawnsAdditionalReplicas(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()
	st := store.NewInMemory()

	seedRebalanceFixture(t, ctx, st, 3, []string{"n1", "n2"})

	rec := &controllerpkg.RGRebalanceReconciler{Client: cli, Scheme: scheme, Store: st}

	if _, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "rg"}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got, err := st.Resources().ListByDefinition(ctx, "pvc-rebalance")
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	if len(got) != 3 {
		t.Fatalf("replica count after rebalance: got %d, want 3; entries=%v", len(got), got)
	}

	nodes := map[string]bool{}
	for _, r := range got {
		nodes[r.NodeName] = true
	}

	if !nodes["n1"] || !nodes["n2"] || !nodes["n3"] {
		t.Errorf("expected replicas on n1+n2+n3 after rebalance; got %v", got)
	}
}

// TestRGRebalanceReconcilerStripsAnnotationAfter: once the rebalance
// pass completes the marker is removed so a re-watch event doesn't
// loop forever. Required for the explicit-trigger contract — the
// next REST modify re-stamps the annotation if (and only if) another
// rebalance is wanted.
func TestRGRebalanceReconcilerStripsAnnotationAfter(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()
	st := store.NewInMemory()

	seedRebalanceFixture(t, ctx, st, 3, []string{"n1", "n2"})

	rec := &controllerpkg.RGRebalanceReconciler{Client: cli, Scheme: scheme, Store: st}

	if _, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "rg"}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	rg, err := st.ResourceGroups().Get(ctx, "rg")
	if err != nil {
		t.Fatalf("Get rg: %v", err)
	}

	if _, ok := rg.Annotations[apiv1.AnnotationRGRebalancePending]; ok {
		t.Errorf("rebalance annotation must be stripped after pass; got %v", rg.Annotations)
	}

	// A second pass with the annotation already gone must be a
	// clean no-op (idempotency for periodic resync).
	if _, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "rg"}}); err != nil {
		t.Fatalf("second Reconcile: %v", err)
	}

	got, _ := st.Resources().ListByDefinition(ctx, "pvc-rebalance")
	if len(got) != 3 {
		t.Errorf("second reconcile churned replicas: got %d, want 3", len(got))
	}
}

// TestRGRebalanceReconcilerIsAdditiveOnly: a 3-replica RD whose
// parent RG drops PlaceCount to 2 (with the rebalance annotation
// stamped, as if by a bug or misuse of the REST handler) must NOT
// shed any replica. Pins the additive-only contract: replica removal
// is the operator's explicit responsibility via `linstor r d`.
func TestRGRebalanceReconcilerIsAdditiveOnly(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()
	st := store.NewInMemory()

	seedRebalanceFixture(t, ctx, st, 2, []string{"n1", "n2", "n3"})

	rec := &controllerpkg.RGRebalanceReconciler{Client: cli, Scheme: scheme, Store: st}

	if _, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "rg"}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got, err := st.Resources().ListByDefinition(ctx, "pvc-rebalance")
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	if len(got) != 3 {
		t.Errorf("rebalance must be additive only; got %d replicas (want 3), entries=%v", len(got), got)
	}
}

// TestRGRebalanceReconcilerHonoursBalanceResourcesDisabled: scenario
// 2.W02 — operator sets controller-scope
// `BalanceResourcesEnabled=false` to suppress the periodic rebalance
// pass cluster-wide. Even with the `rebalance-pending` annotation
// stamped on the RG, the reconciler MUST NOT spawn additional
// replicas; existing placements are left untouched. The annotation
// is stripped so a re-watch on the same event doesn't loop, matching
// the no-op return contract.
func TestRGRebalanceReconcilerHonoursBalanceResourcesDisabled(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()
	st := store.NewInMemory()

	// Two existing replicas, RG wants three — without the kill-switch
	// the reconciler would add a third. The seed annotation simulates
	// a fresh `linstor rg modify` increasing PlaceCount.
	seedRebalanceFixture(t, ctx, st, 3, []string{"n1", "n2"})

	// Flip the controller-scope kill-switch BEFORE the reconcile fires.
	if err := st.ControllerProps().Set(ctx, map[string]string{
		apiv1.PropBalanceResourcesEnabled: "false",
	}); err != nil {
		t.Fatalf("set BalanceResourcesEnabled=false: %v", err)
	}

	rec := &controllerpkg.RGRebalanceReconciler{Client: cli, Scheme: scheme, Store: st}

	if _, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "rg"}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got, err := st.Resources().ListByDefinition(ctx, "pvc-rebalance")
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("BalanceResourcesEnabled=false must short-circuit; got %d replicas, want 2 (entries=%v)", len(got), got)
	}

	// Annotation must still be stripped — a stale marker that survives
	// a kill-switched pass would loop forever on the next RG event.
	rg, err := st.ResourceGroups().Get(ctx, "rg")
	if err != nil {
		t.Fatalf("Get rg: %v", err)
	}

	if _, ok := rg.Annotations[apiv1.AnnotationRGRebalancePending]; ok {
		t.Errorf("annotation must be stripped after a disabled-pass; got %v", rg.Annotations)
	}
}

// TestRGRebalanceReconcilerIgnoresNonFalseValues: 2.W02 contract pin
// — only the explicit string "false" disables the reconciler. Other
// values (empty, "true", "0", typo) keep it armed, matching upstream
// LINSTOR's "default = enabled" semantics so an unconfigured cluster
// behaves the same way it did before the kill-switch landed.
func TestRGRebalanceReconcilerIgnoresNonFalseValues(t *testing.T) {
	t.Parallel()

	for _, val := range []string{"", "true", "True", "0", "FALSE", "no"} {
		t.Run("value="+val, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			scheme := newScheme(t)
			cli := fake.NewClientBuilder().WithScheme(scheme).Build()
			st := store.NewInMemory()

			seedRebalanceFixture(t, ctx, st, 3, []string{"n1", "n2"})

			if val != "" {
				if err := st.ControllerProps().Set(ctx, map[string]string{
					apiv1.PropBalanceResourcesEnabled: val,
				}); err != nil {
					t.Fatalf("set prop: %v", err)
				}
			}

			rec := &controllerpkg.RGRebalanceReconciler{Client: cli, Scheme: scheme, Store: st}

			if _, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "rg"}}); err != nil {
				t.Fatalf("Reconcile: %v", err)
			}

			got, _ := st.Resources().ListByDefinition(ctx, "pvc-rebalance")
			if len(got) != 3 {
				t.Errorf("BalanceResourcesEnabled=%q must NOT disable rebalance; got %d replicas, want 3", val, len(got))
			}
		})
	}
}

// TestRGRebalanceReconcilerNoAnnotationStillFiresScheduledTick:
// scenario 2.15 broadened the no-annotation contract — the scheduled
// tick now ALWAYS refills a place-count deficit cluster-wide, so an
// RG event arriving without the rebalance-pending marker still acts
// when there is genuine work to do. The placer's idempotent
// already-placed accounting keeps a clean cluster a no-op; this test
// pins the deficit-fill side. A subsequent test (TestRebalanceHonours
// Disabled) pins the kill-switch suppression path.
func TestRGRebalanceReconcilerNoAnnotationStillFiresScheduledTick(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()
	st := store.NewInMemory()

	for _, n := range []string{"n1", "n2", "n3"} {
		_ = st.Nodes().Create(ctx, &apiv1.Node{
			Name:             n,
			Type:             apiv1.NodeTypeSatellite,
			ConnectionStatus: apiv1.NodeTypeOnline,
		})
		_ = st.StoragePools().Create(ctx, &apiv1.StoragePool{
			StoragePoolName: "pool",
			NodeName:        n,
			ProviderKind:    apiv1.StoragePoolKindLVMThin,
		})
	}

	_ = st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "rg",
		SelectFilter: apiv1.AutoSelectFilter{
			PlaceCount:  3,
			StoragePool: "pool",
		},
		// No Annotations — scheduled tick fires anyway under 2.15.
	})
	_ = st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:              "pvc-no-annot",
		ResourceGroupName: "rg",
	})

	rec := &controllerpkg.RGRebalanceReconciler{Client: cli, Scheme: scheme, Store: st}

	if _, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "rg"}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got, _ := st.Resources().ListByDefinition(ctx, "pvc-no-annot")
	if len(got) != 3 {
		t.Errorf("scheduled tick must refill deficit even without annotation; got %d replicas, want 3", len(got))
	}
}
