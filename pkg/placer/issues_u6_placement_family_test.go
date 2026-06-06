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

package placer_test

import (
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/placer"
	"github.com/cozystack/blockstor/pkg/store"
)

// seedPoolWithFree registers a node + one LVM_THIN pool with the given
// free / total capacity. Used by the U6 placement-family pins to build
// deliberately unequal pools (the hot-spotting re-exam from upstream
// issue U89 / corner D9).
func seedPoolWithFree(t *testing.T, st store.Store, node string, free, total int64) {
	t.Helper()

	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: node, Type: apiv1.NodeTypeSatellite}); err != nil {
		t.Fatalf("seed node %s: %v", node, err)
	}

	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		NodeName:        node,
		StoragePoolName: "pool",
		ProviderKind:    apiv1.StoragePoolKindLVMThin,
		FreeCapacity:    free,
		TotalCapacity:   total,
	}); err != nil {
		t.Fatalf("seed pool %s: %v", node, err)
	}
}

// TestU89CeterisParibusEmptierPoolWins is the re-examination pin for
// upstream issue U89 / corner-case D9. The operator flagged BS's
// all-weights=1.0 default (every Autoplacer/Weights/* strategy weighted
// equally, vs upstream's MaxFreeSpace=1-only default) for re-examination
// after upstream users reported hot-spotting when free space is
// under-weighted.
//
// The re-exam verdict is MATCHES: even with the equal-weight composition,
// the MaxFreeSpace strategy (FreeCapacity/TotalCapacity) is the ONLY
// strategy that discriminates between two otherwise-identical pools, so
// ceteris paribus the emptier pool always scores higher and is picked
// first. BS therefore does NOT hot-spot a full node — the D9 delta is a
// richer-but-compatible default, not a placement bug.
//
// Setup: two nodes with the SAME total capacity but different free
// (n-empty has 90% free, n-full has 10% free), identical in every other
// respect (no reserved prop, no throughput hint, no existing resources).
// A single-replica autoplace MUST land on n-empty.
func TestU89CeterisParibusEmptierPoolWins(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()

	// Same total, different free → the only discriminating strategy is
	// MaxFreeSpace. n-empty is emptier and must win.
	seedPoolWithFree(t, st, "n-full", 100, 1000)  // 10% free
	seedPoolWithFree(t, st, "n-empty", 900, 1000) // 90% free

	p := placer.New(st)

	placed, want, err := p.Place(t.Context(), "pvc-1", &apiv1.AutoSelectFilter{PlaceCount: 1})
	if err != nil {
		t.Fatalf("Place: %v", err)
	}

	if placed != 1 || want != 1 {
		t.Fatalf("placed/want: got %d/%d, want 1/1", placed, want)
	}

	got, _ := st.Resources().ListByDefinition(t.Context(), "pvc-1")
	if len(got) != 1 {
		t.Fatalf("expected exactly 1 replica, got %d: %+v", len(got), got)
	}

	if got[0].NodeName != "n-empty" {
		t.Errorf("ceteris paribus the emptier pool must win: got %s, want n-empty (hot-spotting regression)",
			got[0].NodeName)
	}
}

// TestU89FirstPlacementPrefersEmptierThenBalances documents the FULL
// U89 / D9 re-exam outcome, including the nuance that surfaced on the
// stand. With the equal-weight default, MaxFreeSpace is the ONLY
// discriminator on a fresh cluster, so the FIRST replica lands on the
// emptier node (no hot-spotting). But MinRscCount is also weighted 1.0,
// so as the emptier node accumulates resources the scorer eventually
// balances toward the (statically) fuller-but-idle node — the placer
// SPREADS load instead of hot-spotting a single node, which is the
// opposite of the upstream complaint.
//
// On the real stand each placement consumes free space (the in-memory
// store's FreeCapacity is static), so the free-space term keeps tracking
// reality; here we deliberately freeze free space to isolate the
// resource-count balancing effect. The invariant we pin is the
// operator-meaningful one: (a) the first placement prefers the emptier
// node, and (b) over many placements BOTH nodes receive replicas — the
// fuller node is never starved AND the emptier node is never the sole
// hot-spot. This is the evidence that BS's equal-weight default does not
// reproduce the U89 hot-spotting symptom.
func TestU89FirstPlacementPrefersEmptierThenBalances(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()

	seedPoolWithFree(t, st, "n-full", 100, 1000)  // 10% free
	seedPoolWithFree(t, st, "n-empty", 900, 1000) // 90% free

	p := placer.New(st)

	perNode := map[string]int{}

	for _, rd := range []string{"r1", "r2", "r3", "r4", "r5", "r6", "r7", "r8"} {
		placed, _, err := p.Place(t.Context(), rd, &apiv1.AutoSelectFilter{PlaceCount: 1})
		if err != nil {
			t.Fatalf("Place(%s): %v", rd, err)
		}

		if placed != 1 {
			t.Fatalf("Place(%s): placed %d, want 1", rd, placed)
		}

		got, _ := st.Resources().ListByDefinition(t.Context(), rd)
		if len(got) != 1 {
			t.Fatalf("Place(%s): expected 1 replica, got %+v", rd, got)
		}

		perNode[got[0].NodeName]++

		if rd == "r1" && got[0].NodeName != "n-empty" {
			t.Errorf("first placement must prefer the emptier node, got %s", got[0].NodeName)
		}
	}

	// Anti-hot-spotting invariant: neither node is starved. A scorer
	// that ignored free space would pile everything on one node; the
	// equal-weight composite spreads.
	if perNode["n-empty"] == 0 || perNode["n-full"] == 0 {
		t.Errorf("equal-weight scorer must spread across both nodes (no hot-spot): n-empty=%d n-full=%d",
			perNode["n-empty"], perNode["n-full"])
	}

	// The emptier node still receives at least as many replicas as the
	// fuller one — free space biases the early rounds in its favour.
	if perNode["n-empty"] < perNode["n-full"] {
		t.Errorf("emptier node must not receive fewer replicas than the fuller one: n-empty=%d n-full=%d",
			perNode["n-empty"], perNode["n-full"])
	}
}

// TestU88ReplicasOnDifferentHonoredOnExtension pins upstream issue U88 /
// U113: a constraint supplied on an EXTENSION of an existing resource is
// still honored. An RD already carries one diskful replica on a node in
// zone-a; an autoplace bump to place_count=2 with
// `--replicas-on-different zone` MUST land the second replica in a
// DIFFERENT zone (never a second node in zone-a), even though the first
// replica was placed in an earlier call.
//
// The placer seeds its anti-affinity seen-set from `existing` (see
// newState → topologySeen), so the extension respects the constraint
// exactly as a from-scratch 2-replica placement would.
func TestU88ReplicasOnDifferentHonoredOnExtension(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	mk := func(name, zone string) {
		if err := st.Nodes().Create(ctx, &apiv1.Node{
			Name:  name,
			Type:  apiv1.NodeTypeSatellite,
			Props: map[string]string{"Aux/zone": zone},
		}); err != nil {
			t.Fatalf("seed node %s: %v", name, err)
		}

		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			NodeName: name, StoragePoolName: "pool",
			ProviderKind: apiv1.StoragePoolKindLVMThin,
			FreeCapacity: 1000, TotalCapacity: 1000,
		}); err != nil {
			t.Fatalf("seed pool %s: %v", name, err)
		}
	}

	mk("a1", "zone-a")
	mk("a2", "zone-a") // second node in the SAME zone as the existing replica
	mk("b1", "zone-b")

	// Pre-existing diskful replica on a1 (zone-a), as if from an earlier
	// place_count=1 call.
	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name: "pvc-1", NodeName: "a1",
		Props: map[string]string{"StorPoolName": "pool"},
	}); err != nil {
		t.Fatalf("seed existing replica: %v", err)
	}

	p := placer.New(st)

	placed, want, err := p.Place(ctx, "pvc-1", &apiv1.AutoSelectFilter{
		PlaceCount:          2,
		ReplicasOnDifferent: []string{"zone"},
	})
	if err != nil {
		t.Fatalf("Place: %v", err)
	}

	if placed != 2 || want != 2 {
		t.Fatalf("placed/want: got %d/%d, want 2/2", placed, want)
	}

	got, _ := st.Resources().ListByDefinition(ctx, "pvc-1")

	zones := map[string]int{"zone-a": 0, "zone-b": 0}
	for _, r := range got {
		switch r.NodeName {
		case "a1", "a2":
			zones["zone-a"]++
		case "b1":
			zones["zone-b"]++
		}
	}

	if zones["zone-a"] != 1 || zones["zone-b"] != 1 {
		t.Errorf("extension must honor replicas-on-different zone: got zone-a=%d zone-b=%d (placement %+v)",
			zones["zone-a"], zones["zone-b"], got)
	}
}

// TestU139ContradictoryConstraintsPlaceZeroNeverSilentSuccess is the
// FIX-CANDIDATE pin for upstream issue U139 / U94: "successfully
// autoplaced on 0 nodes" must NEVER be reported as success when the
// caller asked for replicas. Contradictory topology constraints
// (`--replicas-on-same zone` AND `--replicas-on-different zone` on the
// same key) make every candidate after the first replica unsatisfiable.
//
// At the placer layer the correct shape is placed < want (NOT placed ==
// want == 0). The REST layer (runPlaceAndReport) turns placed < want
// into a 409 FAIL_NOT_ENOUGH_NODES envelope, so the operator sees an
// error, never a misleading SUCCESS. This pin asserts the placer never
// silently collapses `want` to 0 to manufacture a success.
func TestU139ContradictoryConstraintsPlaceZeroNeverSilentSuccess(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	mk := func(name, zone string) {
		if err := st.Nodes().Create(ctx, &apiv1.Node{
			Name:  name,
			Type:  apiv1.NodeTypeSatellite,
			Props: map[string]string{"Aux/zone": zone},
		}); err != nil {
			t.Fatalf("seed node %s: %v", name, err)
		}

		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			NodeName: name, StoragePoolName: "pool",
			ProviderKind: apiv1.StoragePoolKindLVMThin,
			FreeCapacity: 1000, TotalCapacity: 1000,
		}); err != nil {
			t.Fatalf("seed pool %s: %v", name, err)
		}
	}

	mk("a1", "zone-a")
	mk("b1", "zone-b")

	p := placer.New(st)

	// replicas-on-same zone AND replicas-on-different zone is
	// self-contradictory: the first replica pins a zone, the same-tuple
	// rule then requires every other replica to share it, while the
	// different rule forbids exactly that. The second replica can never
	// land.
	placed, want, err := p.Place(ctx, "pvc-1", &apiv1.AutoSelectFilter{
		PlaceCount:          2,
		ReplicasOnSame:      []string{"zone"},
		ReplicasOnDifferent: []string{"zone"},
	})
	if err != nil {
		t.Fatalf("Place: %v", err)
	}

	if want != 2 {
		t.Errorf("want must reflect the operator's request (2), not be collapsed: got %d", want)
	}

	if placed >= want {
		t.Errorf("contradictory constraints must under-place (placed<want), got placed=%d want=%d "+
			"(silent success-on-zero is the U139/U94 bug)", placed, want)
	}

	// Exactly one replica can satisfy a pin-the-first-zone rule; the
	// contradiction only bites the SECOND. Either way the REST 409 fires
	// because placed (<=1) < want (2).
	got, _ := st.Resources().ListByDefinition(ctx, "pvc-1")
	if len(got) >= 2 {
		t.Errorf("contradictory constraints must not yield 2 replicas: %+v", got)
	}
}

// TestU139UnsatisfiableNodeListUnderPlaces pins the second U139/U94
// envelope: an autoplace whose node-name list resolves to zero eligible
// pools (the named node has no pool of the requested provider) must
// under-place, not report success-on-zero. place_count=1, the only
// listed node carries a DISKLESS-only pool (excluded by matchesPoolFilter)
// → placed=0, want=1.
func TestU139UnsatisfiableNodeListUnderPlaces(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n1", Type: apiv1.NodeTypeSatellite}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	// Only a diskless pool on n1 — matchesPoolFilter drops it, so no
	// diskful candidate survives.
	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		NodeName: "n1", StoragePoolName: "DfltDisklessStorPool",
		ProviderKind: apiv1.StoragePoolKindDiskless,
		FreeCapacity: 0, TotalCapacity: 0,
	}); err != nil {
		t.Fatalf("seed diskless pool: %v", err)
	}

	p := placer.New(st)

	placed, want, err := p.Place(ctx, "pvc-1", &apiv1.AutoSelectFilter{
		PlaceCount:   1,
		NodeNameList: []string{"n1"},
	})
	if err != nil {
		t.Fatalf("Place: %v", err)
	}

	if want != 1 {
		t.Errorf("want must stay 1, got %d", want)
	}

	if placed != 0 {
		t.Errorf("no diskful pool on the only candidate node → placed must be 0, got %d (success-on-zero bug)", placed)
	}
}

// TestU83PoolPinnedAutoplaceLandsOnlyOnPoolNodes pins upstream issue
// U83 / U21: `--auto-place 1 -s <pool>` (filter.StoragePool) must land
// the replica ONLY on a node that actually hosts that pool. Two nodes:
// n-has carries `fast`, n-lacks carries `slow`. A pool-pinned autoplace
// to `fast` must pick n-has even though n-lacks has more free space —
// the pool pin is a hard filter (matchesPoolFilter), not a soft
// preference.
func TestU83PoolPinnedAutoplaceLandsOnlyOnPoolNodes(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	mkPool := func(node, pool string, free int64) {
		if _, err := st.Nodes().Get(ctx, node); err != nil {
			if cerr := st.Nodes().Create(ctx, &apiv1.Node{Name: node, Type: apiv1.NodeTypeSatellite}); cerr != nil {
				t.Fatalf("seed node %s: %v", node, cerr)
			}
		}

		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			NodeName: node, StoragePoolName: pool,
			ProviderKind: apiv1.StoragePoolKindLVMThin,
			FreeCapacity: free, TotalCapacity: 1000,
		}); err != nil {
			t.Fatalf("seed pool %s/%s: %v", node, pool, err)
		}
	}

	mkPool("n-has", "fast", 100)    // hosts the pinned pool, less free
	mkPool("n-lacks", "slow", 1000) // more free but WRONG pool

	p := placer.New(st)

	placed, want, err := p.Place(ctx, "pvc-1", &apiv1.AutoSelectFilter{
		PlaceCount:  1,
		StoragePool: "fast",
	})
	if err != nil {
		t.Fatalf("Place: %v", err)
	}

	if placed != 1 || want != 1 {
		t.Fatalf("placed/want: got %d/%d, want 1/1", placed, want)
	}

	got, _ := st.Resources().ListByDefinition(ctx, "pvc-1")
	if len(got) != 1 || got[0].NodeName != "n-has" {
		t.Errorf("pool-pinned autoplace must land only on the pool's node: got %+v, want n-has/fast", got)
	}

	if got[0].Props["StorPoolName"] != "fast" {
		t.Errorf("replica must be stamped with the pinned pool: got %q, want fast", got[0].Props["StorPoolName"])
	}
}
