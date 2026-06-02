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
	"slices"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Bug 385 (P1) — `linstor n e <node>` (node evict) misbehaves when the
// evicted node hosts the free TieBreaker replica. Operator stand repro
// (dev cluster):
//
//	Before evict: worker-1 UpToDate, worker-2 UpToDate, worker-3 TieBreaker.
//	After `linstor n e dev-worker-3`:
//	  worker-2 wrongly demoted UpToDate → TieBreaker,
//	  worker-3 (EVICTED) still holds a diskful resource.
//
// Operator summary: "Если евиктить ноду, при свободной TieBreaker
// реплике, то евикт не произойдёт" — the evict does not actually take
// effect.
//
// Root cause: ensureTiebreaker counted ALL replicas of the RD, including
// the TIE_BREAKER witness sitting on the just-EVICTED node. The witness
// invariant therefore read as already-satisfied (diskful=2 + witness=1)
// and the reconciler never relocated the witness off the drained node —
// leaving it pinned to the evicted satellite. Worse, downstream the
// stranded witness blocked a fresh witness from landing on a healthy
// spare, which is how the operator observed a healthy diskful drift into
// the tiebreaker role.
//
// Fix (Bug 385): replicas on EVICTED / LOST nodes are draining
// placements, not live ones — the witness / quorum decision must ignore
// them (mirroring the placer's documented `disabledNodes` semantic), and
// a TIE_BREAKER stranded on a disabled node must be reaped so a fresh one
// can land on a healthy spare or quorum falls back to off. A healthy
// diskful must NEVER be demoted to TIE_BREAKER.

// TestBug385EvictTiebreakerNodeReapsStrandedWitness pins the exact stand
// repro: 2 diskful (n1, n2) + 1 TIE_BREAKER witness (n3), then evict n3
// (the witness node). After the reconcile:
//
//  1. The stranded TIE_BREAKER on the EVICTED n3 MUST be gone (the evict
//     takes effect — the witness no longer pins the drained node).
//  2. Both diskful replicas (n1, n2) MUST stay diskful — a healthy
//     replica is NEVER demoted to TIE_BREAKER.
//  3. No witness lands back on n3, and no spare exists (n1/n2 are
//     diskful) so no new witness is created.
func TestBug385EvictTiebreakerNodeReapsStrandedWitness(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	st := store.NewInMemory()
	ctx := context.Background()

	// n1, n2 healthy; n3 EVICTED (the operator just ran `n e n3`).
	if err := st.Nodes().Create(ctx, &apiv1.Node{
		Name: "n1", Type: apiv1.NodeTypeSatellite,
	}); err != nil {
		t.Fatalf("seed n1: %v", err)
	}

	if err := st.Nodes().Create(ctx, &apiv1.Node{
		Name: "n2", Type: apiv1.NodeTypeSatellite,
	}); err != nil {
		t.Fatalf("seed n2: %v", err)
	}

	if err := st.Nodes().Create(ctx, &apiv1.Node{
		Name: "n3", Type: apiv1.NodeTypeSatellite,
		Flags: []string{apiv1.NodeFlagEvicted},
	}); err != nil {
		t.Fatalf("seed n3 (evicted): %v", err)
	}

	// Steady state before evict: n1 + n2 diskful, n3 TIE_BREAKER.
	for _, n := range []string{"n1", "n2"} {
		if err := st.Resources().Create(ctx, &apiv1.Resource{
			Name: "pvc-bug385", NodeName: n,
		}); err != nil {
			t.Fatalf("seed diskful %s: %v", n, err)
		}
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name: "pvc-bug385", NodeName: "n3",
		Flags: []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker},
	}); err != nil {
		t.Fatalf("seed n3 witness: %v", err)
	}

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-bug385"},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rd).Build()

	rec := &controllerpkg.ResourceDefinitionReconciler{
		Client: cli,
		Scheme: scheme,
		Store:  st,
	}

	if err := rec.EnsureTiebreaker(ctx, rd); err != nil {
		t.Fatalf("EnsureTiebreaker: %v", err)
	}

	all, err := st.Resources().ListByDefinition(ctx, "pvc-bug385")
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	// Invariant 1: the stranded TIE_BREAKER on the EVICTED n3 is gone.
	if _, err := st.Resources().Get(ctx, "pvc-bug385", "n3"); err == nil {
		t.Errorf("Bug 385: TIE_BREAKER witness still pinned to EVICTED n3; entries=%v", all)
	}

	// Invariant 2: both diskful replicas survive unchanged — NEVER demoted.
	for _, n := range []string{"n1", "n2"} {
		got, getErr := st.Resources().Get(ctx, "pvc-bug385", n)
		if getErr != nil {
			t.Fatalf("Bug 385: diskful replica on %s vanished: %v", n, getErr)
		}

		if slices.Contains(got.Flags, apiv1.ResourceFlagTieBreaker) {
			t.Errorf("Bug 385: healthy diskful on %s was demoted to TIE_BREAKER; flags=%v", n, got.Flags)
		}

		if slices.Contains(got.Flags, apiv1.ResourceFlagDiskless) {
			t.Errorf("Bug 385: healthy diskful on %s gained DISKLESS; flags=%v", n, got.Flags)
		}
	}

	// Invariant 3: exactly the two diskful replicas remain (no spare to
	// host a fresh witness — n1/n2 are diskful, n3 is evicted).
	if len(all) != 2 {
		t.Fatalf("Bug 385: replica count=%d, want 2 (both diskful, no stranded/relocated witness); entries=%v",
			len(all), all)
	}

	diskful, diskless, witness := classifyReplicas(all)
	if diskful != 2 || diskless != 0 || witness != 0 {
		t.Errorf("Bug 385: composition diskful=%d diskless=%d witness=%d, want 2/0/0; entries=%v",
			diskful, diskless, witness, all)
	}
}

// TestBug385EvictTiebreakerNodeRelocatesWitnessToSpare extends the repro
// with a spare healthy node (n4). Evicting the witness node (n3) must
// relocate the witness onto the spare — not leave it stranded on the
// drained node and not demote a healthy diskful.
//
// Expected convergence: n1 + n2 diskful, witness lands on n4, nothing on
// the EVICTED n3.
func TestBug385EvictTiebreakerNodeRelocatesWitnessToSpare(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	st := store.NewInMemory()
	ctx := context.Background()

	for _, n := range []string{"n1", "n2", "n4"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{
			Name: n, Type: apiv1.NodeTypeSatellite,
		}); err != nil {
			t.Fatalf("seed node %s: %v", n, err)
		}
	}

	if err := st.Nodes().Create(ctx, &apiv1.Node{
		Name: "n3", Type: apiv1.NodeTypeSatellite,
		Flags: []string{apiv1.NodeFlagEvicted},
	}); err != nil {
		t.Fatalf("seed n3 (evicted): %v", err)
	}

	for _, n := range []string{"n1", "n2"} {
		if err := st.Resources().Create(ctx, &apiv1.Resource{
			Name: "pvc-bug385b", NodeName: n,
		}); err != nil {
			t.Fatalf("seed diskful %s: %v", n, err)
		}
	}

	// Witness stranded on the evicted n3.
	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name: "pvc-bug385b", NodeName: "n3",
		Flags: []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker},
	}); err != nil {
		t.Fatalf("seed n3 witness: %v", err)
	}

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-bug385b"},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rd).Build()

	rec := &controllerpkg.ResourceDefinitionReconciler{
		Client: cli,
		Scheme: scheme,
		Store:  st,
	}

	if err := rec.EnsureTiebreaker(ctx, rd); err != nil {
		t.Fatalf("EnsureTiebreaker: %v", err)
	}

	all, err := st.Resources().ListByDefinition(ctx, "pvc-bug385b")
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	// The stranded witness on the EVICTED n3 must be gone.
	if _, err := st.Resources().Get(ctx, "pvc-bug385b", "n3"); err == nil {
		t.Errorf("Bug 385: witness still on EVICTED n3; entries=%v", all)
	}

	// The witness must have relocated to the healthy spare n4.
	n4, err := st.Resources().Get(ctx, "pvc-bug385b", "n4")
	if err != nil {
		t.Fatalf("Bug 385: witness did not relocate to healthy spare n4: %v; entries=%v", err, all)
	}

	if !slices.Contains(n4.Flags, apiv1.ResourceFlagTieBreaker) {
		t.Errorf("Bug 385: relocated replica on n4 lacks TIE_BREAKER flag; flags=%v", n4.Flags)
	}

	// Diskful replicas untouched.
	for _, n := range []string{"n1", "n2"} {
		got, getErr := st.Resources().Get(ctx, "pvc-bug385b", n)
		if getErr != nil {
			t.Fatalf("Bug 385: diskful on %s vanished: %v", n, getErr)
		}

		if slices.Contains(got.Flags, apiv1.ResourceFlagTieBreaker) ||
			slices.Contains(got.Flags, apiv1.ResourceFlagDiskless) {
			t.Errorf("Bug 385: healthy diskful on %s changed class; flags=%v", n, got.Flags)
		}
	}

	diskful, diskless, witness := classifyReplicas(all)
	if diskful != 2 || diskless != 0 || witness != 1 {
		t.Errorf("Bug 385: composition diskful=%d diskless=%d witness=%d, want 2/0/1; entries=%v",
			diskful, diskless, witness, all)
	}
}

// TestBug385EvictDiskfulNodeDoesNotCountStrandedDiskful pins the other
// half of the bug: evicting a DISKFUL node must not let its stranded
// diskful replica keep the witness invariant satisfied. Topology: n1 + n2
// diskful + n3 witness, then evict n2 (a diskful node). With n2's diskful
// no longer counted, the live diskful count drops to 1 and the orphaned
// witness on n3 must collapse (Bug-338 carve-out, now reached via the
// disabled-node filter) — the lone diskful runs quorum=off. Crucially,
// the witness on n3 is NOT promoted/demoted onto a healthy diskful.
func TestBug385EvictDiskfulNodeDoesNotCountStrandedDiskful(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	st := store.NewInMemory()
	ctx := context.Background()

	for _, n := range []string{"n1", "n3"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{
			Name: n, Type: apiv1.NodeTypeSatellite,
		}); err != nil {
			t.Fatalf("seed node %s: %v", n, err)
		}
	}

	// n2 is the evicted DISKFUL node.
	if err := st.Nodes().Create(ctx, &apiv1.Node{
		Name: "n2", Type: apiv1.NodeTypeSatellite,
		Flags: []string{apiv1.NodeFlagEvicted},
	}); err != nil {
		t.Fatalf("seed n2 (evicted): %v", err)
	}

	for _, n := range []string{"n1", "n2"} {
		if err := st.Resources().Create(ctx, &apiv1.Resource{
			Name: "pvc-bug385c", NodeName: n,
		}); err != nil {
			t.Fatalf("seed diskful %s: %v", n, err)
		}
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name: "pvc-bug385c", NodeName: "n3",
		Flags: []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker},
	}); err != nil {
		t.Fatalf("seed n3 witness: %v", err)
	}

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-bug385c"},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rd).Build()

	rec := &controllerpkg.ResourceDefinitionReconciler{
		Client: cli,
		Scheme: scheme,
		Store:  st,
	}

	if err := rec.EnsureTiebreaker(ctx, rd); err != nil {
		t.Fatalf("EnsureTiebreaker: %v", err)
	}

	// The lone live diskful on n1 must survive, still diskful.
	n1, err := st.Resources().Get(ctx, "pvc-bug385c", "n1")
	if err != nil {
		t.Fatalf("Bug 385: live diskful on n1 vanished: %v", err)
	}

	if slices.Contains(n1.Flags, apiv1.ResourceFlagTieBreaker) ||
		slices.Contains(n1.Flags, apiv1.ResourceFlagDiskless) {
		t.Errorf("Bug 385: healthy diskful on n1 changed class; flags=%v", n1.Flags)
	}

	// The orphaned witness on n3 must collapse — live diskful dropped to
	// 1, no user-diskless, so the witness is meaningless.
	if _, err := st.Resources().Get(ctx, "pvc-bug385c", "n3"); err == nil {
		t.Errorf("Bug 385: orphaned witness on n3 survived after evicting the second diskful")
	}

	// The stranded diskful on the EVICTED n2 is the NodeReconciler's
	// migration responsibility — the witness path must leave it alone.
	if _, err := st.Resources().Get(ctx, "pvc-bug385c", "n2"); err != nil {
		t.Errorf("Bug 385: witness path must not delete the stranded diskful on EVICTED n2: %v", err)
	}
}
