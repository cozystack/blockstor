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
	"errors"
	"slices"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Bug-024 (release-gate, deadlock-class) — after `n lost` cascades a
// 3-diskful RD down to 2 diskful, ensureTiebreaker re-created the
// witness `<rd>.<lost-node>` `[DISKLESS TIE_BREAKER]` ON THE JUST-
// DELETED NODE: the witness placement picked candidates from the
// manager's lagging informer cache, and nothing ever reaped the
// ghost (the node row is gone — no DeletionTimestamp event, no
// finalizer pass). Deterministic on a warm process; observed >30s.
//
// The fix has two legs, both pinned here:
//
//  1. Placement guard: re-validate the chosen witness node against
//     the authoritative reader right before the Create (the
//     controller-side mirror of REST's Bug 174 node-deleted-race
//     guard) and defer to the next reconcile on a miss.
//  2. Repair leg: a witness Resource referencing a node that no
//     longer exists is reaped by the reconciler — which also covers
//     ghosts created before the fix.

// staleListStore wraps an inner store and, while `stale` is true,
// appends a phantom node to Nodes().List — modelling the lagging
// informer cache that still serves a node `n lost` already deleted.
// Get / every other call passes through to the inner (authoritative)
// store, which is exactly the split the fix exploits.
type staleListStore struct {
	store.Store

	stale *bool
	ghost apiv1.Node
}

func (s *staleListStore) Nodes() store.NodeStore {
	return &staleNodeLister{NodeStore: s.Store.Nodes(), stale: s.stale, ghost: s.ghost}
}

type staleNodeLister struct {
	store.NodeStore

	stale *bool
	ghost apiv1.Node
}

func (l *staleNodeLister) List(ctx context.Context) ([]apiv1.Node, error) {
	nodes, err := l.NodeStore.List(ctx)
	if err != nil || !*l.stale {
		return nodes, err
	}

	return append(nodes, l.ghost), nil
}

// TestBug024WitnessNeverCreatedOnDeletedNode pins the placement
// guard. Topology: diskful on n1 + n2, one real spare (n3), and the
// cached node list still serving the just-deleted "a-lost" (lex-
// first, so the deterministic picker selects it). Pre-fix the
// reconciler stamped the `[DISKLESS TIE_BREAKER]` ghost on a-lost;
// post-fix the authoritative probe reports the node gone, the create
// is deferred, and the next reconcile (fresh list) lands the witness
// on the real spare.
func TestBug024WitnessNeverCreatedOnDeletedNode(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	inner := store.NewInMemory()
	ctx := context.Background()

	for _, n := range []string{"n1", "n2", "n3"} {
		if err := inner.Nodes().Create(ctx, &apiv1.Node{
			Name: n, Type: apiv1.NodeTypeSatellite,
		}); err != nil {
			t.Fatalf("seed node %s: %v", n, err)
		}
	}

	for _, n := range []string{"n1", "n2"} {
		if err := inner.Resources().Create(ctx, &apiv1.Resource{
			Name: "pvc-bug024-guard", NodeName: n,
		}); err != nil {
			t.Fatalf("seed diskful %s: %v", n, err)
		}
	}

	stale := true
	st := &staleListStore{
		Store: inner,
		stale: &stale,
		// "a-lost" sorts before "n3", so the picker WILL choose it
		// off the stale list — the guard is the only thing standing
		// between the pick and the ghost create.
		ghost: apiv1.Node{Name: "a-lost", Type: apiv1.NodeTypeSatellite},
	}

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-bug024-guard"},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rd).Build()

	rec := &controllerpkg.ResourceDefinitionReconciler{
		Client: cli,
		Scheme: scheme,
		Store:  st,
	}

	// Pass 1: stale cache. The witness create must be DEFERRED, not
	// land on the deleted node.
	if err := rec.EnsureTiebreaker(ctx, rd); err != nil {
		t.Fatalf("EnsureTiebreaker (stale pass): %v", err)
	}

	if _, err := inner.Resources().Get(ctx, "pvc-bug024-guard", "a-lost"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Bug-024 regression: witness ghost created on deleted node a-lost (err=%v)", err)
	}

	// Pass 2: cache caught up. The witness converges onto the real
	// spare — the deferral is a delay, not a permanent skip.
	stale = false

	if err := rec.EnsureTiebreaker(ctx, rd); err != nil {
		t.Fatalf("EnsureTiebreaker (fresh pass): %v", err)
	}

	got, err := inner.Resources().Get(ctx, "pvc-bug024-guard", "n3")
	if err != nil {
		t.Fatalf("witness not created on healthy spare n3 after fresh list: %v", err)
	}

	if !slices.Contains(got.Flags, apiv1.ResourceFlagTieBreaker) {
		t.Errorf("n3 replica must carry TIE_BREAKER; got %v", got.Flags)
	}
}

// TestBug024GhostWitnessOnMissingNodeIsReaped pins the repair leg:
// a pre-existing `[DISKLESS TIE_BREAKER]` row referencing a node
// with no Node object (the exact artifact pre-fix processes left
// behind, or a crash between `n lost`'s node delete and the
// cascade) must be reaped, and the fresh witness must land on a
// healthy spare. The diskful pair must never be touched.
func TestBug024GhostWitnessOnMissingNodeIsReaped(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	st := store.NewInMemory()
	ctx := context.Background()

	// n1, n2 host diskful; n3 is the healthy spare. NOTE: no node
	// row exists for "w-gone" — the witness below is a ghost.
	for _, n := range []string{"n1", "n2", "n3"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{
			Name: n, Type: apiv1.NodeTypeSatellite,
		}); err != nil {
			t.Fatalf("seed node %s: %v", n, err)
		}
	}

	for _, n := range []string{"n1", "n2"} {
		if err := st.Resources().Create(ctx, &apiv1.Resource{
			Name: "pvc-bug024-reap", NodeName: n,
		}); err != nil {
			t.Fatalf("seed diskful %s: %v", n, err)
		}
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name: "pvc-bug024-reap", NodeName: "w-gone",
		Flags: []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker},
	}); err != nil {
		t.Fatalf("seed ghost witness: %v", err)
	}

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-bug024-reap"},
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

	// 1. The ghost is reaped.
	if _, err := st.Resources().Get(ctx, "pvc-bug024-reap", "w-gone"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("ghost witness on missing node w-gone must be reaped (err=%v)", err)
	}

	// 2. A fresh witness lands on the healthy spare (diskful==2, so
	// the witness invariant still wants one).
	got, err := st.Resources().Get(ctx, "pvc-bug024-reap", "n3")
	if err != nil {
		t.Fatalf("fresh witness not created on n3: %v", err)
	}

	if !slices.Contains(got.Flags, apiv1.ResourceFlagTieBreaker) {
		t.Errorf("n3 replica must carry TIE_BREAKER; got %v", got.Flags)
	}

	// 3. The diskful pair is untouched — a healthy replica is never
	// demoted or deleted by the repair leg.
	for _, n := range []string{"n1", "n2"} {
		diskful, err := st.Resources().Get(ctx, "pvc-bug024-reap", n)
		if err != nil {
			t.Fatalf("diskful %s vanished: %v", n, err)
		}

		if slices.Contains(diskful.Flags, apiv1.ResourceFlagDiskless) {
			t.Errorf("diskful %s demoted: %v", n, diskful.Flags)
		}
	}
}

// TestBug024GhostDiskfulExcludedFromCountButNotDeleted pins the
// boundary of the repair leg: a DISKFUL replica on a missing node
// (mid-flight `n lost` cascade) is excluded from the voting count —
// so a 3-diskful RD that lost a node behaves as 2-diskful and grows
// a witness on the spare — but the row itself is NOT deleted here;
// the cascade owns diskful teardown.
func TestBug024GhostDiskfulExcludedFromCountButNotDeleted(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	st := store.NewInMemory()
	ctx := context.Background()

	for _, n := range []string{"n1", "n2", "n3"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{
			Name: n, Type: apiv1.NodeTypeSatellite,
		}); err != nil {
			t.Fatalf("seed node %s: %v", n, err)
		}
	}

	// 3 diskful, but "lost-n"'s node row is already gone.
	for _, n := range []string{"n1", "n2", "lost-n"} {
		if err := st.Resources().Create(ctx, &apiv1.Resource{
			Name: "pvc-bug024-diskful", NodeName: n,
		}); err != nil {
			t.Fatalf("seed diskful %s: %v", n, err)
		}
	}

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-bug024-diskful"},
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

	// The stranded diskful survives — its teardown belongs to the
	// `n lost` cascade, not the witness invariant.
	if _, err := st.Resources().Get(ctx, "pvc-bug024-diskful", "lost-n"); err != nil {
		t.Errorf("stranded diskful must NOT be reaped by the witness pass: %v", err)
	}

	// Voting count is 2 → the invariant grows a witness on n3.
	got, err := st.Resources().Get(ctx, "pvc-bug024-diskful", "n3")
	if err != nil {
		t.Fatalf("witness not created on n3 (ghost diskful still counted as live?): %v", err)
	}

	if !slices.Contains(got.Flags, apiv1.ResourceFlagTieBreaker) {
		t.Errorf("n3 replica must carry TIE_BREAKER; got %v", got.Flags)
	}
}
