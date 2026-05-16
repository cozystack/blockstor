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
	stderrors "errors"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Bug 130 — concurrent `linstor rd d` operations race the RD
// reconciler's ensureTiebreaker; a phantom TIE_BREAKER Resource
// survives the RD delete on the third (tiebreaker) node, blocking
// reuse of the RD name and showing up on every subsequent `r l`.
//
// The race window: handleRDDelete.cascadeDeleteResources takes a
// snapshot of children via ListByDefinition; while the snapshot is
// being walked, the RD reconciler's ensureTiebreaker enumerates
// child Resources, sees diskful=2 / witness=0, picks a third node,
// and Creates a brand-new TIE_BREAKER Resource. The cascade only
// deletes what it saw, the RD itself is then dropped, and the
// witness lingers.
//
// We exercise the race directly against the Store + Reconciler the
// production REST handler and controller share. The Store is
// thread-safe, so an interleaved Create-from-controller while the
// handler is between List and Delete is a faithful reproducer of
// the in-cluster race.

// cascadeDeleteMaxPassesForTest mirrors the production constant in
// pkg/rest/resource_definitions.go::cascadeDeleteMaxPasses. The
// controller package doesn't import pkg/rest (no dependency arrow
// in that direction; it would be circular), so we replicate the
// value here. A regression that diverged the cap on either side
// would surface as a phantom — the test then catches it via the
// post-cascade list assertions.
const cascadeDeleteMaxPassesForTest = 5

// cascadeDeleteResourcesForTest mirrors the production
// pkg/rest/resource_definitions.go::cascadeDeleteResources sequence
// the REST handler runs on `linstor rd d`: re-snapshot children
// until ListByDefinition returns empty (or the iteration cap fires),
// then drop the RD itself. The retry-until-empty loop is the
// cascade-side half of the Bug 130 fix (REST package); this helper
// stays in lock-step with it so the controller-package tests
// faithfully exercise the same end-to-end shape `rd d` runs in
// production.
//
// The `betweenListAndDelete` hook is the deterministic
// synchronisation point a real cluster only hits under concurrent
// rd-delete pressure. Tests block in the hook on the FIRST pass
// until the controller thread has created the witness, then resume
// the cascade. With the Bug 130 fix in place, the controller's
// rdIsDeleting guard refuses to create when the RD is gone, AND
// the cascade's second pass reaps any witness that landed during
// the race window.
//
// Pre-fix (HEAD 508293534): the production cascade was single-pass
// (one List + Delete sweep, then drop the RD), and the controller's
// ensureTiebreaker had no deletion-aware guard. Either a single-
// pass helper or the missing guard alone is enough to surface the
// phantom; the test pins the contract that BOTH fixes must hold
// for `rd d` to be race-free.
func cascadeDeleteResourcesForTest(
	t *testing.T,
	ctx context.Context,
	st store.Store,
	rdName string,
	betweenListAndDelete func(),
) {
	t.Helper()

	hookFired := false

	for range cascadeDeleteMaxPassesForTest {
		children, err := st.Resources().ListByDefinition(ctx, rdName)
		if err != nil && !stderrors.Is(err, store.ErrNotFound) {
			t.Fatalf("cascade list: %v", err)
		}

		if betweenListAndDelete != nil && !hookFired {
			betweenListAndDelete()

			hookFired = true
		}

		if len(children) == 0 {
			break
		}

		for i := range children {
			err := st.Resources().Delete(ctx, rdName, children[i].NodeName)
			if err != nil && !stderrors.Is(err, store.ErrNotFound) {
				t.Fatalf("cascade delete %s/%s: %v", rdName, children[i].NodeName, err)
			}
		}
	}

	err := st.ResourceDefinitions().Delete(ctx, rdName)
	if err != nil && !stderrors.Is(err, store.ErrNotFound) {
		t.Fatalf("rd delete: %v", err)
	}
}

// seedNodes is a tiny helper to keep the Bug 130 fixtures readable —
// every test seeds the same n1/n2/n3 satellite shape.
func seedNodes(t *testing.T, ctx context.Context, st store.Store, names ...string) {
	t.Helper()

	for _, n := range names {
		err := st.Nodes().Create(ctx, &apiv1.Node{
			Name: n, Type: apiv1.NodeTypeSatellite,
		})
		if err != nil {
			t.Fatalf("seed node %s: %v", n, err)
		}
	}
}

// TestBug130ConcurrentRDDeleteDoesntLeavePhantomTiebreaker pins the
// core invariant of the v3 report: an RD-delete that races against
// ensureTiebreaker must NOT leave a phantom TIE_BREAKER Resource
// behind. The race window is the gap between ListByDefinition (the
// cascade's snapshot pass) and the per-child Delete loop — that's
// the moment the controller can squeeze a new witness in.
//
// Seed: 2 diskful replicas, no witness. Controller about to create
// it; cascade about to enumerate.
//
// Expectation: after the cascade finishes, EVERY Resource that
// references the deleted RD is gone — no `r l` row, no Resource
// CRD on any node. The RD's name is then free to reuse.
func TestBug130ConcurrentRDDeleteDoesntLeavePhantomTiebreaker(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	st := store.NewInMemory()
	ctx := context.Background()

	seedNodes(t, ctx, st, "n1", "n2", "n3")

	err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: "phantom130",
	})
	if err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	for _, n := range []string{"n1", "n2"} {
		err := st.Resources().Create(ctx, &apiv1.Resource{
			Name: "phantom130", NodeName: n,
		})
		if err != nil {
			t.Fatalf("seed diskful %s: %v", n, err)
		}
	}

	rdCRD := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "phantom130"},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rdCRD).Build()

	rec := &controllerpkg.ResourceDefinitionReconciler{
		Client: cli,
		Scheme: scheme,
		Store:  st,
	}

	// Synchronisation primitives that deterministically reproduce the
	// race the v3 report observed in the cluster:
	//
	//  1. The cascade thread snapshots the children list (sees n1, n2).
	//  2. The cascade signals "I have my snapshot, controller go" via
	//     `cascadeListed`.
	//  3. The controller thread runs ensureTiebreaker — observes 2
	//     diskful + 0 witness, creates the witness on n3 via the same
	//     shared Store.
	//  4. The controller signals "witness committed" via `witnessDone`.
	//  5. The cascade thread resumes and deletes only what it saw.
	//
	// Pre-fix: witness on n3 survives. Post-fix: the cascade is
	// resilient — either it picks up the late-landing witness in a
	// second pass, or ensureTiebreaker refuses to create when the RD
	// is being deleted, or both.
	cascadeListed := make(chan struct{})
	witnessDone := make(chan struct{})

	var wg sync.WaitGroup

	wg.Add(2)

	go func() {
		defer wg.Done()

		cascadeDeleteResourcesForTest(t, ctx, st, "phantom130", func() {
			close(cascadeListed)
			<-witnessDone
		})
	}()

	go func() {
		defer wg.Done()

		<-cascadeListed

		// EnsureTiebreaker runs as if fired by a Resource watch event
		// that landed just before the rd-delete request arrived. In
		// production this would happen via Reconcile; we call the
		// exposed entrypoint directly so the test stays focused on
		// the race window the fix must close.
		_ = rec.EnsureTiebreaker(ctx, rdCRD)

		close(witnessDone)
	}()

	wg.Wait()

	// Bug 130 invariant: every Resource that referenced phantom130
	// must be gone. A surviving row is the phantom.
	remaining, err := st.Resources().ListByDefinition(ctx, "phantom130")
	if err != nil {
		t.Fatalf("post-cascade list: %v", err)
	}

	if len(remaining) != 0 {
		t.Errorf("Bug 130: %d phantom Resource(s) survived rd-delete: %+v",
			len(remaining), remaining)
	}
}

// TestBug130RDDeleteRacesEnsureTiebreakerCreate pins the controller-
// side guard: ensureTiebreaker MUST refuse to create a witness when
// the RD is mid-delete (DeletionTimestamp set on the CRD, OR the RD
// has already vanished from the Store).
//
// Even with the cascade-side retry-until-empty fix, the controller
// can still wake up AFTER the RD-row is gone — a Resource watch
// event lingers in the workqueue for a few ms after the cascade
// drops the row. Without the guard, ensureTiebreaker enumerates,
// sees `len(diskful) == 1` (the toggle-race shape), picks n3 from
// pickTiebreakerNode, and races back into the empty Store with a
// witness Create. That row then has no parent RD — the classic
// phantom shape Bug 130 reports.
//
// Pre-fix: the controller is happy to Create a witness regardless
// of RD presence. Post-fix: the controller checks the Store /
// DeletionTimestamp before creating.
func TestBug130RDDeleteRacesEnsureTiebreakerCreate(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	st := store.NewInMemory()
	ctx := context.Background()

	seedNodes(t, ctx, st, "n1", "n2", "n3")

	// 1 diskful replica (place_count=2 in progress) + no witness.
	// This is the post-toggle-race shape Bug 108 also touches.
	err := st.Resources().Create(ctx, &apiv1.Resource{
		Name: "phantom130b", NodeName: "n1",
	})
	if err != nil {
		t.Fatalf("seed diskful: %v", err)
	}

	// 1 user-diskless on n2 — completes the 2-voter "post-toggle race"
	// shape the Bug 108 repair branch recognises. Without this, the
	// keep-branch wouldn't even decide to create a witness; with it,
	// the controller WOULD normally create one — but Bug 130 says
	// "not when the RD is being deleted".
	err = st.Resources().Create(ctx, &apiv1.Resource{
		Name: "phantom130b", NodeName: "n2",
		Flags: []string{apiv1.ResourceFlagDiskless},
	})
	if err != nil {
		t.Fatalf("seed diskless: %v", err)
	}

	// The RD has been dropped from the Store — the cascade got there
	// first. The controller's queued reconcile fires next.
	// (No ResourceDefinitions().Create call: Store is the source of
	// truth; if the row isn't there, the RD is "gone" from the
	// controller's perspective.)

	// CRD still exists in the fake client (informer cache trails the
	// real deletion by a few ms) but with DeletionTimestamp set —
	// mirrors the kubectl-side state right after `kubectl delete rd`.
	now := metav1.NewTime(time.Now())
	rdCRD := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "phantom130b",
			DeletionTimestamp: &now,
			Finalizers:        []string{"blockstor.io/cleanup"},
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rdCRD).Build()

	rec := &controllerpkg.ResourceDefinitionReconciler{
		Client: cli,
		Scheme: scheme,
		Store:  st,
	}

	// The controller invokes EnsureTiebreaker for an RD whose Store-
	// side row is gone. Pre-fix: pickTiebreakerNode returns n3,
	// witness Created on n3 → phantom. Post-fix: ensureTiebreaker
	// short-circuits because the RD is mid-delete.
	_ = rec.EnsureTiebreaker(ctx, rdCRD)

	remaining, err := st.Resources().ListByDefinition(ctx, "phantom130b")
	if err != nil {
		t.Fatalf("post-list: %v", err)
	}

	// The pre-existing seeds on n1 + n2 are fine — Bug 130 is only
	// about the witness Create that races the delete. The phantom is
	// any Resource on n3 (the picked tiebreaker node).
	for _, rep := range remaining {
		if rep.NodeName == "n3" {
			t.Errorf("Bug 130: phantom witness created on n3 mid-rd-delete: %+v", rep)
		}
	}
}

// TestBug130PostDeleteCRDNameCanBeReused is the user-visible
// validation: after `linstor rd d phantom130` returns SUCCESS, the
// very next `linstor rd c phantom130` must succeed. A phantom row
// blocks the recreate because the in-memory Store's per-resource
// Create checks the rKey (rdName, node) — a leftover row on n3
// makes the controller's next ensureTiebreaker hit ErrAlreadyExists.
//
// More importantly, the operator-visible repro from the v3 report
// is "phantom blocks reuse" — this test pins the end-to-end UX
// contract.
func TestBug130PostDeleteCRDNameCanBeReused(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	st := store.NewInMemory()
	ctx := context.Background()

	seedNodes(t, ctx, st, "n1", "n2", "n3")

	err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: "phantom130c",
	})
	if err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	for _, n := range []string{"n1", "n2"} {
		err := st.Resources().Create(ctx, &apiv1.Resource{
			Name: "phantom130c", NodeName: n,
		})
		if err != nil {
			t.Fatalf("seed diskful %s: %v", n, err)
		}
	}

	rdCRD := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "phantom130c"},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rdCRD).Build()

	rec := &controllerpkg.ResourceDefinitionReconciler{
		Client: cli,
		Scheme: scheme,
		Store:  st,
	}

	cascadeListed := make(chan struct{})
	witnessDone := make(chan struct{})

	var wg sync.WaitGroup

	wg.Add(2)

	go func() {
		defer wg.Done()

		cascadeDeleteResourcesForTest(t, ctx, st, "phantom130c", func() {
			close(cascadeListed)
			<-witnessDone
		})
	}()

	go func() {
		defer wg.Done()

		<-cascadeListed
		_ = rec.EnsureTiebreaker(ctx, rdCRD)

		close(witnessDone)
	}()

	wg.Wait()

	// `linstor rd c phantom130c` — exact same name, fresh RD.
	err = st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: "phantom130c",
	})
	if err != nil {
		t.Fatalf("re-create RD with same name: %v "+
			"(Bug 130: phantom Resource row blocked the reuse)", err)
	}

	// And the recreate must not have inherited any phantom replicas.
	rs, err := st.Resources().ListByDefinition(ctx, "phantom130c")
	if err != nil {
		t.Fatalf("list after recreate: %v", err)
	}

	if len(rs) != 0 {
		t.Errorf("recreated RD inherited %d phantom replica(s): %+v",
			len(rs), rs)
	}
}
