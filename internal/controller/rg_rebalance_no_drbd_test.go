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

// withOneOfTwoReplicas raises the group's place count to two and gives
// pvc-local one UpToDate replica on n1: a definition the balance task
// tops up, once its stack has DRBD, since it still holds its data and
// is one replica short.
func withOneOfTwoReplicas(t *testing.T, ctx context.Context, st store.Store) {
	t.Helper()

	rg, err := st.ResourceGroups().Get(ctx, "rg")
	if err != nil {
		t.Fatalf("get rg: %v", err)
	}

	rg.SelectFilter.PlaceCount = 2

	if err := st.ResourceGroups().Update(ctx, &rg); err != nil {
		t.Fatalf("raise the place count: %v", err)
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name:     "pvc-local",
		NodeName: "n1",
		Props:    map[string]string{"StorPoolName": "pool"},
		Volumes:  upToDateVolumes(),
	}); err != nil {
		t.Fatalf("seed the UpToDate replica: %v", err)
	}
}

// placeCountModify and constraintOnlyModify return the markers the REST
// handler leaves for an `rg modify` that named a place count, and for
// one that changed only a placement constraint.
func placeCountModify() map[string]string {
	return map[string]string{
		apiv1.AnnotationRGRebalancePending:    "2026-05-14T00:00:00Z",
		apiv1.AnnotationRGRebalancePlaceCount: "2026-05-14T00:00:00Z",
	}
}

func constraintOnlyModify() map[string]string {
	return map[string]string{apiv1.AnnotationRGRebalancePending: "2026-05-14T00:00:00Z"}
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
			withOneOfTwoReplicas(t, ctx, st)

			rec := &controllerpkg.RGRebalanceReconciler{Client: cli, Scheme: scheme, Store: st}

			if _, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "rg"}}); err != nil {
				t.Fatalf("Reconcile: %v", err)
			}

			if n := replicasOf(t, ctx, st, "pvc-local"); n != 1 {
				t.Errorf("scheduled pass left %d replicas on a definition without DRBD, want its 1", n)
			}
		})
	}
}

// TestRGRebalanceScheduledStillPlacesDRBDDefinition is the control: the
// same fixture with the default stack is topped up as before. Its one
// replica is UpToDate, or the balance checks would skip it (a
// definition with no data left is never topped up).
func TestRGRebalanceScheduledStillPlacesDRBDDefinition(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()
	st := store.NewInMemory()

	seedNoDRBDRebalance(t, ctx, st, nil, nil, nil)
	withOneOfTwoReplicas(t, ctx, st)

	rec := &controllerpkg.RGRebalanceReconciler{Client: cli, Scheme: scheme, Store: st}

	if _, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "rg"}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if n := replicasOf(t, ctx, st, "pvc-local"); n != 2 {
		t.Errorf("scheduled pass left %d replicas on a DRBD definition, want 2", n)
	}
}

// TestRGRebalanceAnnotationPlacesDefinitionWithoutDRBD: an `rg modify`
// that names a place count is not the balance task. LINSTOR autoplaces
// every child definition short of the count whatever its stack, and so
// does blockstor.
func TestRGRebalanceAnnotationPlacesDefinitionWithoutDRBD(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()
	st := store.NewInMemory()

	seedNoDRBDRebalance(t, ctx, st, []string{apiv1.LayerKindStorage}, nil, placeCountModify())

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
	withOneOfTwoReplicas(t, ctx, st)

	rec := &controllerpkg.ResourceGroupReconciler{Client: cli, Scheme: scheme, Store: st}

	if _, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "rg"}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if n := replicasOf(t, ctx, st, "pvc-local"); n != 1 {
		t.Errorf("RG event left %d replicas on a definition without DRBD, want its 1", n)
	}
}

// seedModifyWithBalancingOff extends the no-DRBD fixture (pvc-local,
// storage only, no replica) with a DRBD child without a replica
// (pvc-drbd), a child already holding two replicas against the place
// count of one (pvc-full), and BalanceResourcesEnabled=false on the
// controller.
func seedModifyWithBalancingOff(t *testing.T, ctx context.Context, st store.Store, annotations map[string]string) {
	t.Helper()

	seedNoDRBDRebalance(t, ctx, st, nil, []string{apiv1.LayerKindStorage}, annotations)

	for _, rd := range []string{"pvc-drbd", "pvc-full"} {
		if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
			Name:              rd,
			ResourceGroupName: "rg",
		}); err != nil {
			t.Fatalf("seed rd %q: %v", rd, err)
		}
	}

	for _, n := range []string{"n1", "n2"} {
		if err := st.Resources().Create(ctx, &apiv1.Resource{
			Name:     "pvc-full",
			NodeName: n,
			Props:    map[string]string{"StorPoolName": "pool"},
		}); err != nil {
			t.Fatalf("seed pvc-full on %q: %v", n, err)
		}
	}

	if err := st.ControllerProps().Set(ctx, map[string]string{
		apiv1.PropBalanceResourcesEnabled: "false",
	}); err != nil {
		t.Fatalf("set BalanceResourcesEnabled=false: %v", err)
	}
}

// reconcileModify runs the RG event and the rebalance reconciler on
// the group, in either order.
func reconcileModify(t *testing.T, ctx context.Context, st store.Store, rgEventFirst bool) {
	t.Helper()

	type reconciler interface {
		Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error)
	}

	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()

	order := []reconciler{
		&controllerpkg.RGRebalanceReconciler{Client: cli, Scheme: scheme, Store: st},
		&controllerpkg.ResourceGroupReconciler{Client: cli, Scheme: scheme, Store: st},
	}
	if rgEventFirst {
		order[0], order[1] = order[1], order[0]
	}

	for _, rec := range order {
		if _, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "rg"}}); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
	}

	rg, err := st.ResourceGroups().Get(ctx, "rg")
	if err != nil {
		t.Fatalf("get rg: %v", err)
	}

	if _, ok := rg.Annotations[apiv1.AnnotationRGRebalancePending]; ok {
		t.Errorf("annotation not stripped with balancing off: %v", rg.Annotations)
	}
}

// TestRGModifyPlacesEveryChildWithBalancingOff: `BalanceResourcesEnabled`
// switches off LINSTOR's balance task, not an `rg modify` that names a
// place count, which autoplaces every child with fewer non-tiebreaker
// replicas than the count, whatever its stack, and leaves the others
// alone (CtrlRscGrpApiCallHandler.java:577-620 in LINSTOR v1.33.1).
// Each short child ends up with its replica, in whichever order the RG
// event and the rebalance reconciler run.
func TestRGModifyPlacesEveryChildWithBalancingOff(t *testing.T) {
	t.Parallel()

	for name, rgEventFirst := range map[string]bool{
		"RG event first":  true,
		"rebalance first": false,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			st := store.NewInMemory()

			seedModifyWithBalancingOff(t, ctx, st, placeCountModify())
			reconcileModify(t, ctx, st, rgEventFirst)

			for rd, want := range map[string]int{"pvc-local": 1, "pvc-drbd": 1, "pvc-full": 2} {
				if n := replicasOf(t, ctx, st, rd); n != want {
					t.Errorf("rg modify with balancing off left %d replicas on %q, want %d", n, rd, want)
				}
			}
		})
	}
}

// TestRGConstraintOnlyModifyPlacesNothingWithBalancingOff: LINSTOR's
// `rg modify` autoplaces only when the request carries a replica count
// (CtrlRscGrpApiCallHandler.java:577-620 in v1.33.1). A modify that
// changed only a placement constraint places nothing, and with the
// balance task switched off nothing else places either, on any child.
func TestRGConstraintOnlyModifyPlacesNothingWithBalancingOff(t *testing.T) {
	t.Parallel()

	for name, rgEventFirst := range map[string]bool{
		"RG event first":  true,
		"rebalance first": false,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			st := store.NewInMemory()

			seedModifyWithBalancingOff(t, ctx, st, constraintOnlyModify())
			reconcileModify(t, ctx, st, rgEventFirst)

			for rd, want := range map[string]int{"pvc-local": 0, "pvc-drbd": 0, "pvc-full": 2} {
				if n := replicasOf(t, ctx, st, rd); n != want {
					t.Errorf("constraint-only rg modify with balancing off left %d replicas on %q, want %d", n, rd, want)
				}
			}
		})
	}
}

// TestRGRebalanceDisabledPlacesDRBDChildItself: with balancing off the
// rebalance reconciler places a place-count modify on a DRBD child too,
// without the RG event path. Leaving that child to the RG event path
// made the placement depend on a path LINSTOR does not place from, and
// that path may make the balance checks and skip it.
func TestRGRebalanceDisabledPlacesDRBDChildItself(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()
	st := store.NewInMemory()

	seedNoDRBDRebalance(t, ctx, st, nil, nil, placeCountModify())

	if err := st.ControllerProps().Set(ctx, map[string]string{
		apiv1.PropBalanceResourcesEnabled: "false",
	}); err != nil {
		t.Fatalf("set BalanceResourcesEnabled=false: %v", err)
	}

	rec := &controllerpkg.RGRebalanceReconciler{Client: cli, Scheme: scheme, Store: st}

	if _, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "rg"}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if n := replicasOf(t, ctx, st, "pvc-local"); n != 1 {
		t.Errorf("rg modify with balancing off placed %d replicas on a DRBD definition, want 1", n)
	}
}
