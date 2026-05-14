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
	"time"

	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	controllerpkg "github.com/cozystack/blockstor/internal/controller"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestSpawnShortfallReplaysAfterGracePeriod pins scenario 2.20: an RD
// stamped with `blockstor.io/spawn-shortfall=<RFC3339>` by 9.W05's
// partial-fail spawn path gets retried on the rebalance reconciler's
// next scheduled tick, but only after `BalanceResourcesGracePeriod`
// has elapsed since the stamp. On a successful retry the annotation
// is stripped so a subsequent tick is a clean no-op.
func TestSpawnShortfallReplaysAfterGracePeriod(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()
	st := store.NewInMemory()

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
			PlaceCount:  3,
			StoragePool: "pool",
		},
	}); err != nil {
		t.Fatalf("seed rg: %v", err)
	}

	t0 := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)

	// RD stamped by the (hypothetical, post-9.W05) partial-fail path:
	// spawn landed only 2 of the 3 requested replicas and the REST
	// handler dropped the shortfall annotation. The reconciler must
	// retry once the grace window elapses.
	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:              "pvc-shortfall",
		ResourceGroupName: "rg",
		Annotations: map[string]string{
			apiv1.RDSpawnShortfallAnnotation: t0.Format(time.RFC3339),
		},
	}); err != nil {
		t.Fatalf("seed rd: %v", err)
	}

	for _, n := range []string{"n1", "n2"} {
		if err := st.Resources().Create(ctx, &apiv1.Resource{
			Name:     "pvc-shortfall",
			NodeName: n,
		}); err != nil {
			t.Fatalf("seed existing replica on %q: %v", n, err)
		}
	}

	if err := st.ControllerProps().Set(ctx, map[string]string{
		apiv1.PropBalanceResourcesInterval:    "5",
		apiv1.PropBalanceResourcesGracePeriod: "60",
	}); err != nil {
		t.Fatalf("set BalanceResources props: %v", err)
	}

	now := t0

	rec := &controllerpkg.RGRebalanceReconciler{
		Client: cli,
		Scheme: scheme,
		Store:  st,
		Now:    func() time.Time { return now },
	}

	// Inside the grace window — the shortfall annotation is observed
	// but the retry must wait. Replica count stays at 2 and the
	// annotation survives.
	now = t0.Add(5 * time.Minute)
	if _, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "rg"}}); err != nil {
		t.Fatalf("Reconcile @T0+5m: %v", err)
	}

	got, _ := st.Resources().ListByDefinition(ctx, "pvc-shortfall")
	if len(got) != 2 {
		t.Fatalf("shortfall retry fired inside grace window: got %d replicas, want 2 (entries=%v)", len(got), got)
	}

	rd, err := st.ResourceDefinitions().Get(ctx, "pvc-shortfall")
	if err != nil {
		t.Fatalf("get rd @T0+5m: %v", err)
	}

	if _, ok := rd.Annotations[apiv1.RDSpawnShortfallAnnotation]; !ok {
		t.Errorf("shortfall annotation stripped prematurely; got %v", rd.Annotations)
	}

	// Beyond the grace window — the placer pass runs and refills the
	// missing replica. Successful replay strips the annotation so a
	// subsequent tick is a clean no-op.
	now = t0.Add(61 * time.Minute)
	if _, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "rg"}}); err != nil {
		t.Fatalf("Reconcile @T0+61m: %v", err)
	}

	got, _ = st.Resources().ListByDefinition(ctx, "pvc-shortfall")
	if len(got) != 3 {
		t.Fatalf("shortfall retry must fire after grace window: got %d replicas, want 3 (entries=%v)", len(got), got)
	}

	rd, err = st.ResourceDefinitions().Get(ctx, "pvc-shortfall")
	if err != nil {
		t.Fatalf("get rd @T0+61m: %v", err)
	}

	if _, ok := rd.Annotations[apiv1.RDSpawnShortfallAnnotation]; ok {
		t.Errorf("shortfall annotation must be stripped on successful replay; got %v", rd.Annotations)
	}
}

// TestSpawnShortfallSkippedWithoutAnnotation pins the gate: RDs
// without the spawn-shortfall annotation are NOT touched by the
// shortfall-replay path on a scheduled tick. The annotation-driven
// rebalance pass (rg.Annotations[rebalance-pending]) is the only
// other way to spawn replicas — preventing this path from firing on
// a clean RD keeps prop-only RG / RD updates from re-running the
// placer cluster-wide on every periodic wake.
func TestSpawnShortfallSkippedWithoutAnnotation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()
	st := store.NewInMemory()

	seedIntervalFixture(t, ctx, st, 3, []string{"n1", "n2"})

	if err := st.ControllerProps().Set(ctx, map[string]string{
		apiv1.PropBalanceResourcesInterval:    "5",
		apiv1.PropBalanceResourcesGracePeriod: "60",
	}); err != nil {
		t.Fatalf("set BalanceResources props: %v", err)
	}

	t0 := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)

	rec := &controllerpkg.RGRebalanceReconciler{
		Client: cli,
		Scheme: scheme,
		Store:  st,
		Now:    func() time.Time { return t0.Add(2 * time.Hour) },
	}

	// No shortfall annotation. The scheduled tick fires the normal
	// rebalance pass (every node ONLINE, no grace gate), so the
	// 2-of-3 deficit DOES get refilled by the periodic path — that
	// is the interval contract. What this test pins is that the
	// shortfall-specific code path is independent: even if the
	// rebalance pass were the kill-switched variant, the shortfall
	// replay would still skip an RD without the annotation. We assert
	// the steady-state outcome by flipping the kill-switch on so only
	// the shortfall replay COULD have fired — and verify that nothing
	// did.
	if err := st.ControllerProps().Set(ctx, map[string]string{
		apiv1.PropBalanceResourcesEnabled:     "false",
		apiv1.PropBalanceResourcesInterval:    "5",
		apiv1.PropBalanceResourcesGracePeriod: "60",
	}); err != nil {
		t.Fatalf("set kill-switch: %v", err)
	}

	if _, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "rg"}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got, _ := st.Resources().ListByDefinition(ctx, "pvc-interval")
	if len(got) != 2 {
		t.Errorf("shortfall replay fired without annotation; got %d replicas, want 2", len(got))
	}
}
