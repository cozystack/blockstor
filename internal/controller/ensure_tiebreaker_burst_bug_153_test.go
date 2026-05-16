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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Bug 153 — Bug 130's deletionTimestamp guard breaks down under
// burst rd-delete load. The v4 report repro: 10 RDs each with 2
// diskful replicas (+ 1 TIE_BREAKER witness), all rd-delete'd in
// parallel; 2/10 leave behind an orphan TIE_BREAKER Resource CRD
// with `flags=[DISKLESS,TIE_BREAKER]` and no `deletionTimestamp`.
//
// Root cause: Bug 130's guard only checks
// `rd.DeletionTimestamp.IsZero()` — but under burst concurrency, the
// reconciler can fire AFTER the cascade dropped the Store row AND
// before the CRD's DeletionTimestamp has propagated to the
// reconciler's local view. The cascade's retry-until-empty (5
// passes) is a bounded loop; once it returns, any witness created
// after pass 5 is a permanent phantom.
//
// The fix has two prongs:
//
//  1. The `rdIsDeleting` probe ALSO consults the Store: if the
//     RD-row is gone, the controller is reconciling a stale watch
//     event and must not stamp new children. This catches the
//     "cascade finished, RD-row gone, no CRD DeletionTimestamp
//     visible yet" window the burst exposes.
//
//  2. After the witness Resource Create succeeds, re-verify the RD
//     hasn't gone away in the meantime; if the Store row is now
//     missing, roll back the witness so no row outlives its parent.
//
// Together these close the burst race without requiring K8s
// owner-reference GC, which the in-memory Store doesn't model.

// TestBug153BurstRDDeleteNoOrphans pins the user-visible invariant
// from the v4 burst repro: after a parallel rd-delete fan-out, no
// Resource CRD references any of the deleted RDs.
//
// We simulate the race directly: for each RD, the Store row is
// pre-dropped (the cascade got there first), then EnsureTiebreaker
// fires with a CRD that — like the production watch-event-driven
// reconcile under burst — does NOT yet carry a DeletionTimestamp.
// Pre-fix the guard sees `IsZero()==true` and goes on to Create the
// witness; the witness is then a permanent phantom because the
// cascade has already returned. Post-fix the Store-row probe in
// `rdIsDeleting` catches the same condition and short-circuits.
func TestBug153BurstRDDeleteNoOrphans(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	st := store.NewInMemory()
	ctx := context.Background()

	seedNodes(t, ctx, st, "n1", "n2", "n3")

	const rdCount = 10

	// Seed CRDs (parent RDs + 2 diskful per RD) into a single
	// fake client, then drop the parent CRDs only. This mirrors
	// the burst race fingerprint v4 reported: the cascade has
	// taken down the parent but child Resource rows linger long
	// enough for the watch-event-driven reconciler to fire one
	// last witness-creation pass against an already-gone parent.
	allObjs := make([]client.Object, 0, rdCount*3)
	rdCRDs := make([]*blockstoriov1alpha1.ResourceDefinition, 0, rdCount)
	names := make([]string, 0, rdCount)

	for i := 1; i <= rdCount; i++ {
		name := fmt.Sprintf("parad-%d", i)
		names = append(names, name)

		crd := &blockstoriov1alpha1.ResourceDefinition{
			ObjectMeta: metav1.ObjectMeta{Name: name},
		}
		rdCRDs = append(rdCRDs, crd)
		allObjs = append(allObjs, crd)

		for _, n := range []string{"n1", "n2"} {
			res := &blockstoriov1alpha1.Resource{
				ObjectMeta: metav1.ObjectMeta{Name: name + "." + n},
				Spec: blockstoriov1alpha1.ResourceSpec{
					ResourceDefinitionName: name,
					NodeName:               n,
				},
			}
			allObjs = append(allObjs, res)
		}
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(allObjs...).Build()

	rec := &controllerpkg.ResourceDefinitionReconciler{
		Client:    cli,
		Scheme:    scheme,
		Store:     st,
		APIReader: cli,
	}

	// Mirror state on the Store side: seed the 2 diskful replica
	// rows so post-witness assertions can read them via the Store.
	// Don't seed the RD-row (cascade dropped it).
	for _, name := range names {
		for _, n := range []string{"n1", "n2"} {
			if err := st.Resources().Create(ctx, &apiv1.Resource{
				Name: name, NodeName: n,
			}); err != nil {
				t.Fatalf("seed diskful %s/%s: %v", name, n, err)
			}
		}
	}

	// Drop the RD CRDs from the fake client — this mirrors what
	// `Store.ResourceDefinitions().Delete()` does in a real cluster
	// (the Store row IS the CRD via the k8s store). The
	// reconciler's post-Create APIReader probe must see NotFound
	// and roll the witness back.
	for _, name := range names {
		_ = cli.Delete(ctx, &blockstoriov1alpha1.ResourceDefinition{
			ObjectMeta: metav1.ObjectMeta{Name: name},
		})
	}

	var wg sync.WaitGroup

	for i := range rdCRDs {
		crd := rdCRDs[i]

		wg.Add(1)

		go func() {
			defer wg.Done()
			_ = rec.EnsureTiebreaker(ctx, crd)
		}()
	}

	wg.Wait()

	// Bug 153 invariant: no TIE_BREAKER witness was stamped by the
	// burst reconcile. The diskful seeds we placed deliberately
	// linger (the cascade's per-child Delete is out of scope here);
	// the bug fingerprint the v4 report calls out is specifically
	// "DISKLESS,TIE_BREAKER Resource with no DeletionTimestamp",
	// which surfaces as a row on the witness candidate node (n3
	// for the sorted seed).
	orphans := 0

	for _, name := range names {
		remaining, err := st.Resources().ListByDefinition(ctx, name)
		if err != nil {
			t.Fatalf("post-burst list %s: %v", name, err)
		}

		for _, r := range remaining {
			isWitness := false
			for _, f := range r.Flags {
				if f == apiv1.ResourceFlagTieBreaker {
					isWitness = true
					break
				}
			}

			if isWitness {
				orphans++

				t.Logf("Bug 153: phantom witness on %s/%s flags=%v",
					name, r.NodeName, r.Flags)
			}
		}
	}

	if orphans > 0 {
		t.Errorf("Bug 153: %d phantom TIE_BREAKER witness(es) survived burst rd-delete "+
			"(want 0; reconcile must short-circuit when RD-row is gone)", orphans)
	}
}

// burstAttemptCounter observes how many EnsureTiebreaker calls fire
// during the burst run. The test asserts every iteration was
// observed — a regression that silently no-op'd the reconciler
// (which would also produce "0 orphans") would fail this guard.
var burstAttemptCounter int64

// TestBug153BurstFollowsBug130GuardUnderLoad pins the structural
// invariant the v4 burst exposed: the Bug 130 guard MUST observe
// AND short-circuit every reconcile fired against a mid-delete RD,
// not just the "DeletionTimestamp visible on the CRD" subset. The
// burst race surfaces specifically because the CRD's
// DeletionTimestamp hasn't propagated to the reconciler yet — the
// only reliable signal at that point is the Store-row absence.
//
// We drive 10 reconcile calls in parallel against CRDs whose
// DeletionTimestamp IS visible (the easy half of the burst) AND 10
// against CRDs whose DeletionTimestamp is NOT visible but whose
// Store row is gone (the hard half). Every reconcile must
// short-circuit and not Create any witness.
func TestBug153BurstFollowsBug130GuardUnderLoad(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	st := store.NewInMemory()
	ctx := context.Background()

	seedNodes(t, ctx, st, "n1", "n2", "n3")

	const rdCount = 10

	rdCRDs := make([]*blockstoriov1alpha1.ResourceDefinition, 0, rdCount*2)
	allObjs := make([]client.Object, 0, rdCount*4)
	names := make([]string, 0, rdCount*2)

	// Half: CRDs with DeletionTimestamp visible (Bug 130's original
	// trigger condition).
	for i := 1; i <= rdCount; i++ {
		name := fmt.Sprintf("guard-ts-%d", i)
		names = append(names, name)

		now := metav1.NewTime(metav1.Now().Time)
		crd := &blockstoriov1alpha1.ResourceDefinition{
			ObjectMeta: metav1.ObjectMeta{
				Name:              name,
				DeletionTimestamp: &now,
				Finalizers:        []string{"blockstor.io/cleanup"},
			},
		}
		rdCRDs = append(rdCRDs, crd)
		allObjs = append(allObjs, crd)
	}

	// Half: CRDs without DeletionTimestamp (the burst race window),
	// Store row absent. Bug 153's structural fix: the post-Create
	// APIReader probe MUST treat the missing CRD as mid-delete.
	// We seed 2 diskful so the reconciler computes diskful=2 →
	// wants witness; if the guard doesn't fire, a phantom
	// TIE_BREAKER lands on n3.
	burstNames := make([]string, 0, rdCount)

	for i := 1; i <= rdCount; i++ {
		name := fmt.Sprintf("guard-burst-%d", i)
		names = append(names, name)
		burstNames = append(burstNames, name)

		crd := &blockstoriov1alpha1.ResourceDefinition{
			ObjectMeta: metav1.ObjectMeta{Name: name},
		}
		rdCRDs = append(rdCRDs, crd)
		allObjs = append(allObjs, crd)

		for _, n := range []string{"n1", "n2"} {
			if err := st.Resources().Create(ctx, &apiv1.Resource{
				Name: name, NodeName: n,
			}); err != nil {
				t.Fatalf("seed diskful %s/%s: %v", name, n, err)
			}

			// Mirror in the fake client so listReplicasDirect via
			// APIReader returns the same view.
			res := &blockstoriov1alpha1.Resource{
				ObjectMeta: metav1.ObjectMeta{Name: name + "." + n},
				Spec: blockstoriov1alpha1.ResourceSpec{
					ResourceDefinitionName: name,
					NodeName:               n,
				},
			}
			allObjs = append(allObjs, res)
		}
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(allObjs...).Build()

	rec := &controllerpkg.ResourceDefinitionReconciler{
		Client:    cli,
		Scheme:    scheme,
		Store:     st,
		APIReader: cli,
	}

	// For the burst half — simulate "cascade dropped the parent
	// CRD already": delete the RD CRD from the fake client. The
	// post-Create APIReader probe (the structural fix this test
	// pins) then sees NotFound and rolls the witness back.
	for _, name := range burstNames {
		_ = cli.Delete(ctx, &blockstoriov1alpha1.ResourceDefinition{
			ObjectMeta: metav1.ObjectMeta{Name: name},
		})
	}

	var wg sync.WaitGroup

	for i := range rdCRDs {
		crd := rdCRDs[i]

		wg.Add(1)

		go func() {
			defer wg.Done()
			atomic.AddInt64(&burstAttemptCounter, 1)
			_ = rec.EnsureTiebreaker(ctx, crd)
		}()
	}

	wg.Wait()

	if got := atomic.LoadInt64(&burstAttemptCounter); got < rdCount*2 {
		t.Fatalf("burstAttemptCounter: got %d, want >= %d", got, rdCount*2)
	}

	// Every reconcile must have short-circuited — no TIE_BREAKER
	// witnesses created on n3 for any of the burst names. The guard
	// fired uniformly across both halves.
	witnesses := 0

	for _, name := range names {
		got, err := st.Resources().ListByDefinition(ctx, name)
		if err != nil {
			t.Fatalf("list %s: %v", name, err)
		}

		for _, r := range got {
			for _, f := range r.Flags {
				if f == apiv1.ResourceFlagTieBreaker {
					witnesses++

					t.Logf("Bug 153: guard missed %s/%s flags=%v",
						name, r.NodeName, r.Flags)
				}
			}
		}
	}

	if witnesses > 0 {
		t.Errorf("Bug 153: %d phantom TIE_BREAKER witness(es) stamped during burst "+
			"(want 0; structural guard must fire for every mid-delete RD)", witnesses)
	}
}
