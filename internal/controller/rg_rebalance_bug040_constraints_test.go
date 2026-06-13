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

// BUG-040 placement-constraint pins for the RGRebalanceReconciler's
// refill path. The refill rides the shared placer, which already
// enforces the constraints — these tests pin the WIRING so a future
// refactor that hands the placer a different filter (or bypasses it)
// trips a unit test instead of a stand sweep:
//
//  1. refill never lands a replica on an AutoplaceTarget=false node;
//  2. refill never exceeds the RG place_count (strictly additive,
//     gap-fill only);
//  3. refill never lands a replica on an EVICTED node (and the evicted
//     node's stranded replica does not mask the deficit — the gap is
//     filled on a healthy spare).
//
// The kill-switch (`BalanceResourcesEnabled=false`) is pinned by
// TestRGRebalanceReconcilerHonoursBalanceResourcesDisabled and
// TestRebalanceHonoursDisabled alongside this file.

// seedBug040RebalanceFixture is seedRebalanceFixture with per-node
// control over flags and props. 4 nodes (n1..n4) so the EVICTED case
// has both a forbidden node and a healthy spare.
func seedBug040RebalanceFixture(
	t *testing.T,
	ctx context.Context,
	st store.Store,
	placeCount int32,
	nodeFlags map[string][]string,
	nodeProps map[string]map[string]string,
	existingReplicaNodes []string,
) {
	t.Helper()

	for _, n := range []string{"n1", "n2", "n3", "n4"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{
			Name:  n,
			Type:  apiv1.NodeTypeSatellite,
			Flags: nodeFlags[n],
			Props: nodeProps[n],
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
			PlaceCount:  apiv1.LaxInt32(placeCount),
			StoragePool: "pool",
		},
		Annotations: map[string]string{
			apiv1.AnnotationRGRebalancePending: "2026-06-13T00:00:00Z",
		},
	}); err != nil {
		t.Fatalf("seed rg: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:              "pvc-bug040-rebalance",
		ResourceGroupName: "rg",
	}); err != nil {
		t.Fatalf("seed rd: %v", err)
	}

	for _, n := range existingReplicaNodes {
		if err := st.Resources().Create(ctx, &apiv1.Resource{
			Name: "pvc-bug040-rebalance", NodeName: n,
		}); err != nil {
			t.Fatalf("seed existing replica on %q: %v", n, err)
		}
	}
}

// runBug040Rebalance drives one annotation-armed reconcile and returns
// the post-pass replica set keyed by node.
func runBug040Rebalance(t *testing.T, ctx context.Context, st store.Store) map[string]apiv1.Resource {
	t.Helper()

	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()
	rec := &controllerpkg.RGRebalanceReconciler{Client: cli, Scheme: scheme, Store: st}

	if _, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "rg"}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got, err := st.Resources().ListByDefinition(ctx, "pvc-bug040-rebalance")
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	byNode := make(map[string]apiv1.Resource, len(got))
	for i := range got {
		byNode[got[i].NodeName] = got[i]
	}

	return byNode
}

// TestRGRebalanceRefillSkipsAutoplaceExcludedNode: place_count=3 with
// replicas on n1+n2 and BOTH spares drained via AutoplaceTarget=false.
// The refill must leave the deficit unfilled rather than violate the
// operator's drain.
func TestRGRebalanceRefillSkipsAutoplaceExcludedNode(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewInMemory()

	drained := map[string]string{apiv1.PropAutoplaceTarget: "false"}
	seedBug040RebalanceFixture(t, ctx, st, 3,
		nil,
		map[string]map[string]string{"n3": drained, "n4": drained},
		[]string{"n1", "n2"})

	byNode := runBug040Rebalance(t, ctx, st)

	for _, forbidden := range []string{"n3", "n4"} {
		if _, hit := byNode[forbidden]; hit {
			t.Errorf("BUG-040: rebalance refill landed a replica on AutoplaceTarget=false node %s; got %v",
				forbidden, byNode)
		}
	}

	if len(byNode) != 2 {
		t.Errorf("replica count after refill: got %d, want 2 (deficit must stay unfilled); got %v",
			len(byNode), byNode)
	}
}

// TestRGRebalanceRefillNeverExceedsPlaceCount: an RD already AT the RG
// place_count must come through an annotation-armed pass untouched —
// the refill is gap-fill only, never an overshoot.
func TestRGRebalanceRefillNeverExceedsPlaceCount(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewInMemory()

	seedBug040RebalanceFixture(t, ctx, st, 2, nil, nil, []string{"n1", "n2"})

	byNode := runBug040Rebalance(t, ctx, st)

	if len(byNode) != 2 {
		t.Fatalf("BUG-040: refill overshot place_count=2: got %d replicas (%v)", len(byNode), byNode)
	}

	for _, n := range []string{"n1", "n2"} {
		if _, ok := byNode[n]; !ok {
			t.Errorf("pre-existing replica on %s vanished during the no-op pass; got %v", n, byNode)
		}
	}
}

// TestRGRebalanceRefillSkipsEvictedNode: place_count=3, replicas on
// n1+n2+n3 with n3 EVICTED. The stranded replica must not mask the
// deficit (placer accounting drops it) and the refill must land on the
// healthy spare n4 — never back on the EVICTED node.
func TestRGRebalanceRefillSkipsEvictedNode(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewInMemory()

	seedBug040RebalanceFixture(t, ctx, st, 3,
		map[string][]string{"n3": {apiv1.NodeFlagEvicted}},
		nil,
		[]string{"n1", "n2", "n3"})

	byNode := runBug040Rebalance(t, ctx, st)

	if _, ok := byNode["n4"]; !ok {
		t.Errorf("BUG-040: deficit behind the EVICTED n3 was not refilled on the healthy spare n4; got %v", byNode)
	}

	// The stranded replica on the EVICTED node is left for the
	// eviction machinery to prune — the rebalance pass itself is
	// additive and must not have created a SECOND row there.
	if len(byNode) != 4 {
		t.Errorf("replica set after refill: got %d rows (%v), want 4 (n1+n2+stranded n3+refilled n4)",
			len(byNode), byNode)
	}
}
