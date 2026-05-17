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

// Bug 261 (P1, data-loss class) — ensureTiebreaker MUST NEVER
// downgrade an existing diskful Resource to TIE_BREAKER. Stand-
// caught on dev-kvaps: after the operator dropped the witness via
// `linstor r d <node> <rd>` on a 2-diskful + 1-witness RD, the
// reconciler saw `diskful=2, witness=0` and chose a witness
// candidate from `pickTiebreakerNode`. The pre-fix selector only
// excluded the per-call `hostingReplica` map — a snapshot built
// from `listReplicasDirect` at the top of `ensureTiebreaker`. When
// the caller passed a stale snapshot (Resource watch races, REST
// cache lag on a sibling apiserver replica), `pickTiebreakerNode`
// would return a node already hosting a diskful Resource. The
// downstream `Store.Resources().Create(<witness>)` then either
// raced into a silent overwrite (k8s store: pre-Bug-130 Resource
// CRDs had no `Create` precondition on Spec content) or — at
// minimum — left the operator one race-window away from a silent
// `r td --diskless` without consent.
//
// The fix: explicit defense-in-depth at the selector. Instead of
// trusting the caller's pre-built hostingReplica map, re-probe the
// store for diskful Resources of the RD inside the selector and
// hard-exclude their nodes. The selector now refuses to return a
// diskful node EVER, even when the caller's hostingReplica map is
// empty / stale.
//
// The test exposes the bug at the selector contract level: build a
// cluster where worker-1 is diskful, then call PickTiebreakerNode
// with an EMPTY hostingReplica map (simulating a stale snapshot)
// and assert that worker-1 is never returned.

// TestBug261PickTiebreakerNeverPicksDiskfulEvenWithStaleHostingMap
// is the failing-pre-fix selector contract test. The current
// signature only honours the hostingReplica map; the fix must
// either (a) widen the selector signature to also accept an
// `rdName` and re-probe the store, or (b) cross-check the picked
// candidate against the store before returning it.
func TestBug261PickTiebreakerNeverPicksDiskfulEvenWithStaleHostingMap(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	st := store.NewInMemory()
	ctx := context.Background()

	// 3 nodes. worker-1 and worker-2 host diskful Resources on
	// the RD `pvc-bug261`. worker-3 is the only safe witness
	// target.
	for _, n := range []string{"worker-1", "worker-2", "worker-3"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{
			Name: n, Type: apiv1.NodeTypeSatellite,
		}); err != nil {
			t.Fatalf("seed node %s: %v", n, err)
		}
	}

	for _, n := range []string{"worker-1", "worker-2"} {
		if err := st.Resources().Create(ctx, &apiv1.Resource{
			Name: "pvc-bug261", NodeName: n,
		}); err != nil {
			t.Fatalf("seed diskful %s: %v", n, err)
		}
	}

	rec := &controllerpkg.ResourceDefinitionReconciler{
		Scheme: scheme,
		Store:  st,
	}

	// EMPTY hostingReplica map — simulates a stale caller
	// snapshot. Pre-fix, the selector trusts the empty map and
	// freely returns any healthy node — including worker-1 (the
	// lowest-name diskful node). The fix must re-probe the store
	// for diskful Resources of the RD and exclude them
	// unconditionally.
	picked, err := rec.PickTiebreakerNodeForRD(ctx, "pvc-bug261", map[string]bool{})
	if err != nil {
		t.Fatalf("PickTiebreakerNodeForRD: %v", err)
	}

	if picked == "worker-1" || picked == "worker-2" {
		t.Fatalf("Bug 261 regression: selector picked diskful node %q for the witness; "+
			"only worker-3 is safe (worker-1+worker-2 are diskful)", picked)
	}

	if picked != "worker-3" {
		t.Fatalf("selector picked %q; want worker-3 (only spare node)", picked)
	}
}

// TestBug261PickTiebreakerReturnsEmptyWhenAllNodesAreDiskful pins
// the "no candidate → skip" half of the contract: when EVERY
// healthy node already hosts a diskful Resource (e.g. 2-node
// cluster lost its witness), the selector must return "" so
// createWitness skips. Pre-fix, a 2-node cluster where both
// nodes are diskful would still pick one of them (lowest-name
// first) when the caller passed an empty hostingReplica map —
// effectively a silent downgrade target.
func TestBug261PickTiebreakerReturnsEmptyWhenAllNodesAreDiskful(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	st := store.NewInMemory()
	ctx := context.Background()

	for _, n := range []string{"worker-1", "worker-2"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{
			Name: n, Type: apiv1.NodeTypeSatellite,
		}); err != nil {
			t.Fatalf("seed node %s: %v", n, err)
		}

		if err := st.Resources().Create(ctx, &apiv1.Resource{
			Name: "pvc-bug261b", NodeName: n,
		}); err != nil {
			t.Fatalf("seed diskful %s: %v", n, err)
		}
	}

	rec := &controllerpkg.ResourceDefinitionReconciler{
		Scheme: scheme,
		Store:  st,
	}

	// Stale hostingReplica map.
	picked, err := rec.PickTiebreakerNodeForRD(ctx, "pvc-bug261b", map[string]bool{})
	if err != nil {
		t.Fatalf("PickTiebreakerNodeForRD: %v", err)
	}

	if picked != "" {
		t.Fatalf("selector returned %q; want empty (every healthy node is diskful → no safe witness)",
			picked)
	}
}

// TestBug261EnsureTiebreakerNoDowngradeWhenNoSpareNode is the
// end-to-end variant: full `ensureTiebreaker` pipeline on a 2-node
// cluster where both nodes are diskful. No downgrade may occur,
// and no rogue witness may land on either node. Acts as a
// regression pin for the selector-level fix above.
func TestBug261EnsureTiebreakerNoDowngradeWhenNoSpareNode(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	st := store.NewInMemory()
	ctx := context.Background()

	for _, n := range []string{"worker-1", "worker-2"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{
			Name: n, Type: apiv1.NodeTypeSatellite,
		}); err != nil {
			t.Fatalf("seed node %s: %v", n, err)
		}

		if err := st.Resources().Create(ctx, &apiv1.Resource{
			Name: "pvc-bug261e", NodeName: n,
		}); err != nil {
			t.Fatalf("seed diskful %s: %v", n, err)
		}
	}

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-bug261e"},
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

	for _, n := range []string{"worker-1", "worker-2"} {
		got, err := st.Resources().Get(ctx, "pvc-bug261e", n)
		if err != nil {
			t.Fatalf("get %s: %v", n, err)
		}

		if slices.Contains(got.Flags, apiv1.ResourceFlagTieBreaker) {
			t.Errorf("Bug 261: diskful %s downgraded to TIE_BREAKER; flags=%v", n, got.Flags)
		}

		if slices.Contains(got.Flags, apiv1.ResourceFlagDiskless) {
			t.Errorf("Bug 261: diskful %s gained DISKLESS; flags=%v", n, got.Flags)
		}
	}

	all, err := st.Resources().ListByDefinition(ctx, "pvc-bug261e")
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	if len(all) != 2 {
		t.Errorf("replica count: got %d, want 2 (no 3rd node → no witness); entries=%v", len(all), all)
	}
}
