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
	"slices"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	controllerpkg "github.com/cozystack/blockstor/internal/controller"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// seedAutoDiskfulFixture builds a 3-node store with one RG (placeCount=2),
// one RD parented to it, and the requested mix of diskful + diskless
// replicas. ControllerProps and rdProps are written verbatim so each
// test exercises the precedence path it cares about.
func seedAutoDiskfulFixture(
	t *testing.T,
	ctx context.Context,
	st store.Store,
	placeCount int32,
	ctrlPropMinutes string,
	rdPropMinutes string,
	diskfulNodes []string,
	disklessNodes []string,
) *apiv1.ResourceDefinition {
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

	if ctrlPropMinutes != "" {
		err := st.ControllerProps().Set(ctx, map[string]string{
			apiv1.AutoDiskfulPropKey: ctrlPropMinutes,
		})
		if err != nil {
			t.Fatalf("seed controller props: %v", err)
		}
	}

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "rg",
		SelectFilter: apiv1.AutoSelectFilter{
			PlaceCount:  apiv1.LaxInt32(placeCount),
			StoragePool: "pool",
		},
	}); err != nil {
		t.Fatalf("seed rg: %v", err)
	}

	rdProps := map[string]string{}
	if rdPropMinutes != "" {
		rdProps[apiv1.AutoDiskfulPropKey] = rdPropMinutes
	}

	rd := &apiv1.ResourceDefinition{
		Name:              "pvc-auto-diskful",
		ResourceGroupName: "rg",
		Props:             rdProps,
	}

	if err := st.ResourceDefinitions().Create(ctx, rd); err != nil {
		t.Fatalf("seed rd: %v", err)
	}

	for _, n := range diskfulNodes {
		err := st.Resources().Create(ctx, &apiv1.Resource{Name: rd.Name, NodeName: n})
		if err != nil {
			t.Fatalf("seed diskful replica on %q: %v", n, err)
		}
	}

	for _, n := range disklessNodes {
		err := st.Resources().Create(ctx, &apiv1.Resource{
			Name:     rd.Name,
			NodeName: n,
			Flags:    []string{apiv1.ResourceFlagDiskless},
		})
		if err != nil {
			t.Fatalf("seed diskless replica on %q: %v", n, err)
		}
	}

	return rd
}

// TestAutoDiskfulTimerArmsDeadline: scenario 7.W03 first phase — when
// the reconciler observes a diskful-replica deficit and a positive
// `DrbdOptions/auto-diskful` prop, it stamps the deadline annotation
// and requeues at the fire time, WITHOUT touching the diskless flag.
// A second reconcile before the deadline fires must remain idempotent
// (annotation unchanged, DISKLESS still present).
func TestAutoDiskfulTimerArmsDeadline(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewInMemory()

	rd := seedAutoDiskfulFixture(t, ctx, st,
		2 /* placeCount */, "" /* ctrlMinutes */, "5", /* rdMinutes */
		[]string{"n1"},       /* diskful */
		[]string{"n2", "n3"}, /* diskless */
	)

	t0 := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)

	rec := &controllerpkg.AutoDiskfulReconciler{
		Store: st,
		Now:   func() time.Time { return t0 },
	}

	res, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: rd.Name}})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if res.RequeueAfter != 5*time.Minute {
		t.Errorf("RequeueAfter: got %v, want 5m", res.RequeueAfter)
	}

	got, err := st.ResourceDefinitions().Get(ctx, rd.Name)
	if err != nil {
		t.Fatalf("get rd: %v", err)
	}

	want := t0.Add(5 * time.Minute).Format(time.RFC3339)

	if got.Annotations[apiv1.AutoDiskfulDeadlineAnnotation] != want {
		t.Errorf("deadline annotation: got %q, want %q",
			got.Annotations[apiv1.AutoDiskfulDeadlineAnnotation], want)
	}

	// Second reconcile a minute later must NOT re-stamp the deadline
	// (timer would never fire if every reconcile pushed it forward).
	rec.Now = func() time.Time { return t0.Add(time.Minute) }

	_, err = rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: rd.Name}})
	if err != nil {
		t.Fatalf("Reconcile (2): %v", err)
	}

	got, err = st.ResourceDefinitions().Get(ctx, rd.Name)
	if err != nil {
		t.Fatalf("get rd (2): %v", err)
	}

	if got.Annotations[apiv1.AutoDiskfulDeadlineAnnotation] != want {
		t.Errorf("deadline re-stamped on idempotent pass: got %q, want %q",
			got.Annotations[apiv1.AutoDiskfulDeadlineAnnotation], want)
	}

	// Replicas untouched until the deadline fires.
	disklessN2, err := st.Resources().Get(ctx, rd.Name, "n2")
	if err != nil {
		t.Fatalf("get n2: %v", err)
	}

	if !slices.Contains(disklessN2.Flags, apiv1.ResourceFlagDiskless) {
		t.Errorf("n2 promoted prematurely: flags=%v", disklessN2.Flags)
	}
}

// TestAutoDiskfulTimerPromotesAfterDeadline: scenario 7.W03 second
// phase — once the wall clock crosses the stamped deadline, the
// reconciler promotes a non-tiebreaker DISKLESS replica by dropping
// the flag and stamping Spec.Props["StorPoolName"]. The deadline
// annotation is stripped on success so the next deficit starts fresh.
func TestAutoDiskfulTimerPromotesAfterDeadline(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewInMemory()

	rd := seedAutoDiskfulFixture(t, ctx, st,
		2, "", "5",
		[]string{"n1"},
		[]string{"n2", "n3"},
	)

	t0 := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)

	rec := &controllerpkg.AutoDiskfulReconciler{
		Store: st,
		Now:   func() time.Time { return t0 },
	}

	// Arm.
	_, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: rd.Name}})
	if err != nil {
		t.Fatalf("Reconcile (arm): %v", err)
	}

	// Move past deadline.
	rec.Now = func() time.Time { return t0.Add(6 * time.Minute) }

	_, err = rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: rd.Name}})
	if err != nil {
		t.Fatalf("Reconcile (fire): %v", err)
	}

	// Exactly one of n2/n3 must have been promoted; the other stays
	// diskless because placeCount=2 and we already had 1 diskful on n1.
	var promoted int

	for _, node := range []string{"n2", "n3"} {
		rep, err := st.Resources().Get(ctx, rd.Name, node)
		if err != nil {
			t.Fatalf("get %s: %v", node, err)
		}

		if slices.Contains(rep.Flags, apiv1.ResourceFlagDiskless) {
			continue
		}

		promoted++

		if rep.Props["StorPoolName"] != "pool" {
			t.Errorf("%s promoted without StorPoolName: %v", node, rep.Props)
		}
	}

	if promoted != 1 {
		t.Fatalf("expected exactly 1 promotion, got %d", promoted)
	}

	// Annotation stripped on success.
	got, err := st.ResourceDefinitions().Get(ctx, rd.Name)
	if err != nil {
		t.Fatalf("get rd: %v", err)
	}

	if _, ok := got.Annotations[apiv1.AutoDiskfulDeadlineAnnotation]; ok {
		t.Errorf("deadline annotation not stripped after promotion: %v",
			got.Annotations)
	}
}

// TestAutoDiskfulTimerSkipsTiebreaker: a TIE_BREAKER witness must not
// be promoted — its sole purpose is the network-only quorum vote, and
// promoting it would defeat the semantic and waste storage. With
// placeCount=2 and only one viable (non-tiebreaker) diskless missing,
// no promotion occurs and the deadline stays armed for retry.
func TestAutoDiskfulTimerSkipsTiebreaker(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewInMemory()

	for _, n := range []string{"n1", "n2", "n3"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{Name: n, Type: apiv1.NodeTypeSatellite}); err != nil {
			t.Fatalf("seed node: %v", err)
		}
	}

	// Only n1 has a usable storage pool — n2 is a pure tiebreaker
	// witness with no local storage anyway.
	err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		StoragePoolName: "pool",
		NodeName:        "n1",
		ProviderKind:    apiv1.StoragePoolKindLVMThin,
	})
	if err != nil {
		t.Fatalf("seed pool: %v", err)
	}

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "rg",
		SelectFilter: apiv1.AutoSelectFilter{
			PlaceCount: apiv1.LaxInt32(2),
		},
	}); err != nil {
		t.Fatalf("seed rg: %v", err)
	}

	rd := &apiv1.ResourceDefinition{
		Name:              "pvc-tb-skip",
		ResourceGroupName: "rg",
		Props:             map[string]string{apiv1.AutoDiskfulPropKey: "5"},
	}
	if err := st.ResourceDefinitions().Create(ctx, rd); err != nil {
		t.Fatalf("seed rd: %v", err)
	}

	// One diskful + one TIE_BREAKER diskless witness — deficit is 1
	// but the only diskless candidate is a tiebreaker.
	err = st.Resources().Create(ctx, &apiv1.Resource{Name: rd.Name, NodeName: "n1"})
	if err != nil {
		t.Fatalf("seed diskful: %v", err)
	}

	err = st.Resources().Create(ctx, &apiv1.Resource{
		Name:     rd.Name,
		NodeName: "n2",
		Flags:    []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker},
	})
	if err != nil {
		t.Fatalf("seed witness: %v", err)
	}

	t0 := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)

	rec := &controllerpkg.AutoDiskfulReconciler{
		Store: st,
		Now:   func() time.Time { return t0 },
	}

	// Arm.
	_, err = rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: rd.Name}})
	if err != nil {
		t.Fatalf("Reconcile (arm): %v", err)
	}

	// Past deadline — but no viable candidate exists.
	rec.Now = func() time.Time { return t0.Add(6 * time.Minute) }

	_, err = rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: rd.Name}})
	if err != nil {
		t.Fatalf("Reconcile (fire): %v", err)
	}

	got, err := st.Resources().Get(ctx, rd.Name, "n2")
	if err != nil {
		t.Fatalf("get n2: %v", err)
	}

	if !slices.Contains(got.Flags, apiv1.ResourceFlagTieBreaker) {
		t.Errorf("tiebreaker flag dropped: %v", got.Flags)
	}

	if !slices.Contains(got.Flags, apiv1.ResourceFlagDiskless) {
		t.Errorf("tiebreaker promoted to diskful: %v", got.Flags)
	}
}

// TestAutoDiskfulTimerStripsOnRecovery: when the cluster returns to
// full diskful health (operator manually added a replica, or
// place_count was lowered), the stale deadline annotation is dropped
// so a future deficit starts a fresh timer rather than firing
// immediately against a past timestamp.
func TestAutoDiskfulTimerStripsOnRecovery(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewInMemory()

	rd := seedAutoDiskfulFixture(t, ctx, st,
		2, "", "5",
		[]string{"n1", "n2"},
		[]string{"n3"},
	)

	// Pre-arm the deadline annotation as if the reconciler had
	// observed a deficit earlier and then a manual operator add
	// brought us back to placeCount=2 diskful.
	rd.Annotations = map[string]string{
		apiv1.AutoDiskfulDeadlineAnnotation: time.Now().Add(time.Hour).Format(time.RFC3339),
	}
	if err := st.ResourceDefinitions().Update(ctx, rd); err != nil {
		t.Fatalf("pre-arm: %v", err)
	}

	rec := &controllerpkg.AutoDiskfulReconciler{
		Store: st,
		Now:   func() time.Time { return time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC) },
	}

	_, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: rd.Name}})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got, err := st.ResourceDefinitions().Get(ctx, rd.Name)
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if _, ok := got.Annotations[apiv1.AutoDiskfulDeadlineAnnotation]; ok {
		t.Errorf("stale deadline not stripped on recovery: %v", got.Annotations)
	}
}

// TestAutoDiskfulPropHierarchyRDWins: the RD-scope prop overrides the
// cluster-scope ControllerProps default. Verifies the lower-scope-wins
// precedence the UG9 hierarchy spec mandates.
func TestAutoDiskfulPropHierarchyRDWins(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewInMemory()

	rd := seedAutoDiskfulFixture(t, ctx, st,
		2, "30" /* ctrl=30min */, "2", /* rd=2min wins */
		[]string{"n1"},
		[]string{"n2", "n3"},
	)

	t0 := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)

	rec := &controllerpkg.AutoDiskfulReconciler{
		Store: st,
		Now:   func() time.Time { return t0 },
	}

	res, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: rd.Name}})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if res.RequeueAfter != 2*time.Minute {
		t.Errorf("requeue used ControllerProps minutes, expected RD scope: got %v, want 2m",
			res.RequeueAfter)
	}

	got, err := st.ResourceDefinitions().Get(ctx, rd.Name)
	if err != nil {
		t.Fatalf("get rd: %v", err)
	}

	want := t0.Add(2 * time.Minute).Format(time.RFC3339)

	if got.Annotations[apiv1.AutoDiskfulDeadlineAnnotation] != want {
		t.Errorf("deadline used cluster prop: got %q, want %q (RD scope wins)",
			got.Annotations[apiv1.AutoDiskfulDeadlineAnnotation], want)
	}
}

// TestAutoDiskfulDisabledByZeroProp: a non-positive / unparseable
// prop disables the feature at every scope. No deadline is stamped;
// any stale deadline is stripped.
func TestAutoDiskfulDisabledByZeroProp(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewInMemory()

	rd := seedAutoDiskfulFixture(t, ctx, st,
		2, "" /* no ctrl prop */, "" /* no rd prop */, /* feature disabled */
		[]string{"n1"},
		[]string{"n2"},
	)

	t0 := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)

	rec := &controllerpkg.AutoDiskfulReconciler{
		Store: st,
		Now:   func() time.Time { return t0 },
	}

	_, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: rd.Name}})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got, err := st.ResourceDefinitions().Get(ctx, rd.Name)
	if err != nil {
		t.Fatalf("get rd: %v", err)
	}

	if _, ok := got.Annotations[apiv1.AutoDiskfulDeadlineAnnotation]; ok {
		t.Errorf("deadline stamped while feature disabled: %v",
			got.Annotations)
	}

	// And the diskless replica untouched.
	disklessN2, err := st.Resources().Get(ctx, rd.Name, "n2")
	if err != nil {
		t.Fatalf("get n2: %v", err)
	}

	if !slices.Contains(disklessN2.Flags, apiv1.ResourceFlagDiskless) {
		t.Errorf("n2 promoted while feature disabled: %v", disklessN2.Flags)
	}
}
