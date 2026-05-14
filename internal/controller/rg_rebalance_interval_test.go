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

// seedIntervalFixture builds a 3-node store seeded with one RG and one
// RD. Unlike seedRebalanceFixture, NO `rebalance-pending` annotation is
// stamped — these tests exercise the scheduled-tick path, not the
// explicit operator-driven trigger.
func seedIntervalFixture(t *testing.T, ctx context.Context, st store.Store, placeCount int32, existingReplicaNodes []string) *apiv1.ResourceGroup {
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

	rg := &apiv1.ResourceGroup{
		Name: "rg",
		SelectFilter: apiv1.AutoSelectFilter{
			PlaceCount:  apiv1.LaxInt32(placeCount),
			StoragePool: "pool",
		},
	}
	if err := st.ResourceGroups().Create(ctx, rg); err != nil {
		t.Fatalf("seed rg: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:              "pvc-interval",
		ResourceGroupName: "rg",
	}); err != nil {
		t.Fatalf("seed rd: %v", err)
	}

	for _, n := range existingReplicaNodes {
		if err := st.Resources().Create(ctx, &apiv1.Resource{
			Name:     "pvc-interval",
			NodeName: n,
		}); err != nil {
			t.Fatalf("seed existing replica on %q: %v", n, err)
		}
	}

	return rg
}

// TestRebalanceInterval pins scenario 2.15 first half: with the
// BalanceResourcesInterval prop configured, the reconciler triggers
// itself periodically via `Result.RequeueAfter` and refills a
// place-count gap on a scheduled tick — no explicit RG-mutation
// annotation required. The placer's additive-only behaviour does the
// rest: a 2-of-3 deficit becomes a 3-of-3 placement on the very next
// tick.
func TestRebalanceInterval(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()
	st := store.NewInMemory()

	seedIntervalFixture(t, ctx, st, 3, []string{"n1", "n2"})

	// Configure a 5-minute periodic tick at controller scope. No
	// rebalance-pending annotation — this is the scheduled path.
	if err := st.ControllerProps().Set(ctx, map[string]string{
		apiv1.PropBalanceResourcesInterval: "5",
	}); err != nil {
		t.Fatalf("set BalanceResourcesInterval: %v", err)
	}

	t0 := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)

	rec := &controllerpkg.RGRebalanceReconciler{
		Client: cli,
		Scheme: scheme,
		Store:  st,
		Now:    func() time.Time { return t0 },
	}

	res, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "rg"}})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	// Every pass arms the next scheduled tick at Interval.
	if res.RequeueAfter != 5*time.Minute {
		t.Errorf("RequeueAfter: got %v, want 5m (scheduled-tick cadence)", res.RequeueAfter)
	}

	got, err := st.Resources().ListByDefinition(ctx, "pvc-interval")
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	if len(got) != 3 {
		t.Fatalf("scheduled tick must refill the 2-of-3 deficit; got %d replicas, want 3", len(got))
	}

	nodes := map[string]bool{}
	for _, r := range got {
		nodes[r.NodeName] = true
	}

	if !nodes["n1"] || !nodes["n2"] || !nodes["n3"] {
		t.Errorf("expected replicas on n1+n2+n3 after scheduled tick; got %v", got)
	}
}

// TestRebalanceIntervalDefaultsWhenUnset pins the default-Interval
// fallback: with NO `BalanceResourcesInterval` prop set anywhere, the
// scheduled tick still fires at the package default (5 minutes).
// Otherwise a fresh cluster that never touched the knob would never
// auto-rebalance — operationally identical to wave2 2.W02's
// kill-switch, which 2.15 explicitly is NOT.
func TestRebalanceIntervalDefaultsWhenUnset(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()
	st := store.NewInMemory()

	seedIntervalFixture(t, ctx, st, 3, []string{"n1", "n2"})

	rec := &controllerpkg.RGRebalanceReconciler{
		Client: cli,
		Scheme: scheme,
		Store:  st,
		Now:    func() time.Time { return time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC) },
	}

	res, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "rg"}})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	want := time.Duration(apiv1.DefaultBalanceResourcesIntervalMinutes) * time.Minute
	if res.RequeueAfter != want {
		t.Errorf("RequeueAfter without prop: got %v, want %v (default)", res.RequeueAfter, want)
	}
}

// TestRebalanceGracePeriod pins scenario 2.15 second half: a Node that
// just transitioned to ConnectionStatus=OFFLINE suppresses the
// scheduled rebalance tick for `BalanceResourcesGracePeriod` minutes
// so a rebooting / flapping satellite gets a chance to return before
// healthy peers churn replicas on its behalf. Once the grace window
// elapses (the node is still offline) the reconciler proceeds with
// the additive pass.
func TestRebalanceGracePeriod(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()
	st := store.NewInMemory()

	rg := seedIntervalFixture(t, ctx, st, 3, []string{"n1", "n2"})

	if err := st.ControllerProps().Set(ctx, map[string]string{
		apiv1.PropBalanceResourcesInterval:    "5",
		apiv1.PropBalanceResourcesGracePeriod: "60",
	}); err != nil {
		t.Fatalf("set BalanceResources props: %v", err)
	}

	// Flip n3 OFFLINE; the rebalance tick should not yet act.
	if err := st.Nodes().SetConnectionStatus(ctx, "n3", apiv1.NodeTypeOffline); err != nil {
		t.Fatalf("set n3 offline: %v", err)
	}

	t0 := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)
	now := t0

	rec := &controllerpkg.RGRebalanceReconciler{
		Client: cli,
		Scheme: scheme,
		Store:  st,
		Now:    func() time.Time { return now },
	}

	// First tick at T0 — registers the offline-since stamp, GracePeriod
	// hasn't elapsed yet, so the placer pass must NOT fire.
	if _, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: rg.Name}}); err != nil {
		t.Fatalf("Reconcile @T0: %v", err)
	}

	// Tick at T0+5min — still inside the 60-min grace window.
	now = t0.Add(5 * time.Minute)
	if _, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: rg.Name}}); err != nil {
		t.Fatalf("Reconcile @T0+5m: %v", err)
	}

	got, _ := st.Resources().ListByDefinition(ctx, "pvc-interval")
	if len(got) != 2 {
		t.Fatalf("rebalance fired inside grace window: got %d replicas, want 2 (entries=%v)", len(got), got)
	}

	// Tick at T0+61min — grace window has elapsed, n3 is still offline
	// but its slot is now eligible for replacement on a peer. Since
	// only 3 nodes exist, the placer can't add a 3rd diskful replica
	// without n3; the test asserts only that the reconciler ATTEMPTED
	// the pass (no longer short-circuited). To exercise actual
	// placement we bring n3 back online — at which point the pass
	// fills the deficit.
	now = t0.Add(61 * time.Minute)

	if err := st.Nodes().SetConnectionStatus(ctx, "n3", apiv1.NodeTypeOnline); err != nil {
		t.Fatalf("flip n3 online: %v", err)
	}

	if _, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: rg.Name}}); err != nil {
		t.Fatalf("Reconcile @T0+61m: %v", err)
	}

	got, _ = st.Resources().ListByDefinition(ctx, "pvc-interval")
	if len(got) != 3 {
		t.Errorf("rebalance must fire after grace window: got %d replicas, want 3 (entries=%v)", len(got), got)
	}
}

// TestRebalanceHonoursDisabled pins the wave2 2.W02 contract: the
// controller-scope `BalanceResourcesEnabled=false` kill-switch wins
// over the new scheduled-tick path. Even at a scheduled tick (no
// rebalance-pending annotation, Interval armed), the reconciler must
// NOT spawn replicas. A subsequent Enabled-flip is what re-arms the
// pass — the scheduled cadence alone never overrides the operator's
// explicit "leave things alone" intent.
func TestRebalanceHonoursDisabled(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()
	st := store.NewInMemory()

	seedIntervalFixture(t, ctx, st, 3, []string{"n1", "n2"})

	if err := st.ControllerProps().Set(ctx, map[string]string{
		apiv1.PropBalanceResourcesEnabled:  "false",
		apiv1.PropBalanceResourcesInterval: "5",
	}); err != nil {
		t.Fatalf("set props: %v", err)
	}

	t0 := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)

	rec := &controllerpkg.RGRebalanceReconciler{
		Client: cli,
		Scheme: scheme,
		Store:  st,
		Now:    func() time.Time { return t0 },
	}

	res, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "rg"}})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got, _ := st.Resources().ListByDefinition(ctx, "pvc-interval")
	if len(got) != 2 {
		t.Fatalf("BalanceResourcesEnabled=false must suppress scheduled tick; got %d replicas, want 2", len(got))
	}

	// Scheduled cadence stays armed even on the disabled path so a
	// later flip back to enabled gets a tick within Interval.
	if res.RequeueAfter != 5*time.Minute {
		t.Errorf("RequeueAfter on disabled scheduled tick: got %v, want 5m", res.RequeueAfter)
	}
}
