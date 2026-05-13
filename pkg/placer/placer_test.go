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
	"context"
	"slices"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/placer"
	"github.com/cozystack/blockstor/pkg/store"
)

// seedStore is a small helper to spread n nodes + 1 pool each across
// an in-memory store. Each pool gets a distinct FreeCapacity in the
// pattern (i+1)*100 so the placer's "biggest first" sort is observable
// in the resulting placement order (n3 → n2 → n1 for n=3).
func seedStore(t *testing.T, st store.Store, names []string) {
	t.Helper()

	ctx := t.Context()

	for i, name := range names {
		if err := st.Nodes().Create(ctx, &apiv1.Node{Name: name, Type: apiv1.NodeTypeSatellite}); err != nil {
			t.Fatalf("seed node %s: %v", name, err)
		}

		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			NodeName:        name,
			StoragePoolName: "pool",
			ProviderKind:    apiv1.StoragePoolKindLVMThin,
			FreeCapacity:    int64(i+1) * 100,
		}); err != nil {
			t.Fatalf("seed pool %s: %v", name, err)
		}
	}
}

// TestPlaceCreatesNUpToPlaceCount exercises the happy path: 3 healthy
// nodes, place_count=2 → exactly 2 Resources created on the two
// largest-free pools. Pins biggest-first ordering — a regression that
// shuffled candidatePools' sort would silently start filling smaller
// pools first, surprising operators tuning capacity.
func TestPlaceCreatesNUpToPlaceCount(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedStore(t, st, []string{"n1", "n2", "n3"})

	p := placer.New(st)

	placed, want, err := p.Place(t.Context(), "pvc-1", &apiv1.AutoSelectFilter{
		PlaceCount: 2,
	})
	if err != nil {
		t.Fatalf("Place: %v", err)
	}

	if placed != 2 || want != 2 {
		t.Errorf("placed/want: got %d/%d, want 2/2", placed, want)
	}

	got, err := st.Resources().ListByDefinition(t.Context(), "pvc-1")
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("len: got %d, want 2; %+v", len(got), got)
	}

	// Biggest-free first → n3 (300), n2 (200), n1 not picked.
	on := map[string]bool{}
	for _, r := range got {
		on[r.NodeName] = true
	}

	if !on["n3"] || !on["n2"] {
		t.Errorf("expected placement on n2 + n3 (largest free), got %+v", on)
	}

	if on["n1"] {
		t.Errorf("n1 (smallest free) was picked over n3 — sort regression")
	}
}

// TestPlaceSkipsEvictedNode pins the EVICTED-node skip in
// candidatePools + disabledNodes. With only 2 healthy nodes (one is
// EVICTED), place_count=3 returns placed=2 (the cluster can't satisfy
// 3 replicas without using the evicted node). A regression that
// stopped honouring the EVICTED flag would silently include the dead
// node in placement and then trip a downstream dispatch error.
func TestPlaceSkipsEvictedNode(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n1", Type: apiv1.NodeTypeSatellite}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	if err := st.Nodes().Create(ctx, &apiv1.Node{
		Name:  "n2-evicted",
		Type:  apiv1.NodeTypeSatellite,
		Flags: []string{apiv1.NodeFlagEvicted},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n3", Type: apiv1.NodeTypeSatellite}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	for _, n := range []string{"n1", "n2-evicted", "n3"} {
		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			NodeName: n, StoragePoolName: "pool",
			ProviderKind: apiv1.StoragePoolKindLVMThin,
			FreeCapacity: 100,
		}); err != nil {
			t.Fatalf("seed pool: %v", err)
		}
	}

	p := placer.New(st)

	placed, want, err := p.Place(ctx, "pvc-1", &apiv1.AutoSelectFilter{PlaceCount: 3})
	if err != nil {
		t.Fatalf("Place: %v", err)
	}

	if placed != 2 || want != 3 {
		t.Errorf("placed/want: got %d/%d, want 2/3 (evicted node skipped)", placed, want)
	}

	got, _ := st.Resources().ListByDefinition(ctx, "pvc-1")
	for _, r := range got {
		if r.NodeName == "n2-evicted" {
			t.Errorf("EVICTED node received a replica: %+v", r)
		}
	}
}

// TestPlaceDisklessOnRemaining pins the diskless-fanout branch:
// when DisklessOnRemaining=true, nodes without a diskful replica
// receive a DISKLESS Resource. With place_count=2 across 3 nodes,
// the third node ends up with a DISKLESS witness — that's the
// upstream "every node can mount" semantic.
func TestPlaceDisklessOnRemaining(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedStore(t, st, []string{"n1", "n2", "n3"})

	p := placer.New(st)

	placed, _, err := p.Place(t.Context(), "pvc-1", &apiv1.AutoSelectFilter{
		PlaceCount:          2,
		DisklessOnRemaining: true,
	})
	if err != nil {
		t.Fatalf("Place: %v", err)
	}

	if placed != 2 {
		t.Errorf("placed: got %d, want 2 (diskless doesn't count toward place)", placed)
	}

	got, _ := st.Resources().ListByDefinition(t.Context(), "pvc-1")
	if len(got) != 3 {
		t.Fatalf("total resources: got %d, want 3 (2 diskful + 1 diskless); %+v", len(got), got)
	}

	diskless := 0

	for _, r := range got {
		for _, f := range r.Flags {
			if f == "DISKLESS" {
				diskless++
			}
		}
	}

	if diskless != 1 {
		t.Errorf("diskless count: got %d, want 1; resources=%+v", diskless, got)
	}
}

// TestPlaceRespectsExistingReplicas pins the "existing counts toward
// place" semantic: a Resource pre-seeded on n1 means a place_count=2
// call only needs to add ONE more replica, not two. Without this
// counting, every reconcile would balloon to place_count fresh
// replicas and corrupt the desired-state contract.
func TestPlaceRespectsExistingReplicas(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	seedStore(t, st, []string{"n1", "n2", "n3"})

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name: "pvc-1", NodeName: "n1",
		Props: map[string]string{"StorPoolName": "pool"},
	}); err != nil {
		t.Fatalf("seed existing: %v", err)
	}

	p := placer.New(st)

	placed, _, err := p.Place(ctx, "pvc-1", &apiv1.AutoSelectFilter{PlaceCount: 2})
	if err != nil {
		t.Fatalf("Place: %v", err)
	}

	if placed != 2 {
		t.Errorf("placed (counter): got %d, want 2 (1 existing + 1 added)", placed)
	}

	got, _ := st.Resources().ListByDefinition(ctx, "pvc-1")
	if len(got) != 2 {
		t.Fatalf("total: got %d, want 2 (idempotent — not 3); %+v", len(got), got)
	}
}

// TestPlaceCandidatePoolsDisklessExcluded: a node whose only pool is
// of kind DISKLESS must not be considered for diskful placement.
// Pinned because a regression here would let the placer issue a
// "diskful on diskless" Resource that the satellite then refuses
// to materialise.
func TestPlaceCandidatePoolsDisklessExcluded(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n1", Type: apiv1.NodeTypeSatellite}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		NodeName: "n1", StoragePoolName: "diskless",
		ProviderKind: apiv1.StoragePoolKindDiskless,
		FreeCapacity: 100,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	p := placer.New(st)

	placed, want, err := p.Place(ctx, "pvc-1", &apiv1.AutoSelectFilter{PlaceCount: 1})
	if err != nil {
		t.Fatalf("Place: %v", err)
	}

	if placed != 0 || want != 1 {
		t.Errorf("placed/want: got %d/%d, want 0/1 (no diskful candidate)", placed, want)
	}

	got, _ := st.Resources().ListByDefinition(ctx, "pvc-1")
	if len(got) != 0 {
		t.Errorf("unexpected diskful placement on diskless-only node: %+v", got)
	}
}

// TestPlaceCandidatePoolsFiltersByNameList: with NodeNameList set,
// only listed nodes contribute pools. PlaceCount=2 against a list
// of [n1, n2] picks both even though n3 has the largest free.
func TestPlaceCandidatePoolsFiltersByNameList(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedStore(t, st, []string{"n1", "n2", "n3"})

	p := placer.New(st)

	placed, _, err := p.Place(t.Context(), "pvc-1", &apiv1.AutoSelectFilter{
		PlaceCount:   2,
		NodeNameList: []string{"n1", "n2"},
	})
	if err != nil {
		t.Fatalf("Place: %v", err)
	}

	if placed != 2 {
		t.Errorf("placed: got %d, want 2", placed)
	}

	got, _ := st.Resources().ListByDefinition(t.Context(), "pvc-1")
	for _, r := range got {
		if r.NodeName == "n3" {
			t.Errorf("n3 placed despite NodeNameList=[n1,n2]: %+v", r)
		}
	}
}

// TestPlaceSharedSpaceIDAntiAffinity: two pools backed by the same
// shared LUN cannot both host a replica for the same RD — at the
// physical layer they're the same disk, so 2 replicas across them
// offer zero redundancy. Pinned because a regression to ignore
// SharedSpaceID would silently downgrade tenant durability.
func TestPlaceSharedSpaceIDAntiAffinity(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	for _, n := range []string{"n1", "n2", "n3"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{Name: n, Type: apiv1.NodeTypeSatellite}); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	// n1 and n2 both expose the SAME LUN ("shared-1"); n3 has its own.
	for _, sp := range []apiv1.StoragePool{
		{NodeName: "n1", StoragePoolName: "p", ProviderKind: apiv1.StoragePoolKindLVMThin, FreeCapacity: 1000, SharedSpaceID: "shared-1"},
		{NodeName: "n2", StoragePoolName: "p", ProviderKind: apiv1.StoragePoolKindLVMThin, FreeCapacity: 1000, SharedSpaceID: "shared-1"},
		{NodeName: "n3", StoragePoolName: "p", ProviderKind: apiv1.StoragePoolKindLVMThin, FreeCapacity: 100},
	} {
		if err := st.StoragePools().Create(ctx, &sp); err != nil {
			t.Fatalf("seed pool: %v", err)
		}
	}

	p := placer.New(st)

	placed, want, err := p.Place(ctx, "pvc-1", &apiv1.AutoSelectFilter{PlaceCount: 3})
	if err != nil {
		t.Fatalf("Place: %v", err)
	}

	// At most ONE of {n1,n2} can be picked, plus n3 → 2 total.
	if placed != 2 || want != 3 {
		t.Errorf("placed/want: got %d/%d, want 2/3 (shared-LUN anti-affinity)", placed, want)
	}

	got, _ := st.Resources().ListByDefinition(ctx, "pvc-1")
	on := map[string]bool{}

	for _, r := range got {
		on[r.NodeName] = true
	}

	if on["n1"] && on["n2"] {
		t.Errorf("both n1 AND n2 placed despite shared LUN; resources=%+v", got)
	}
}

// TestPlaceListErrPropagates pins the error-bubble: when the
// underlying store fails on Resources().ListByDefinition (e.g. a
// transient apiserver outage), Place must return an error tagged
// with the wrap keyword so operators can grep "list resources by
// definition" in the controller log. Uses a stub store that panics
// on List to confirm the wrap surface — which would also catch a
// future change that swallowed the error and returned (0, 0, nil).
func TestPlaceFilterPlaceCountZero(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedStore(t, st, []string{"n1", "n2"})

	p := placer.New(st)

	placed, want, err := p.Place(t.Context(), "pvc-1", &apiv1.AutoSelectFilter{PlaceCount: 0})
	if err != nil {
		t.Fatalf("Place: %v", err)
	}

	if placed != 0 || want != 0 {
		t.Errorf("placed/want: got %d/%d, want 0/0 (no placement requested)", placed, want)
	}

	got, _ := st.Resources().ListByDefinition(t.Context(), "pvc-1")
	if len(got) != 0 {
		t.Errorf("place_count=0 created resources: %+v", got)
	}
}

// TestPlaceReplicasOnSamePicksLargestGroup pins replicas_on_same:
// when the filter requires all replicas to share an Aux/zone label,
// the placer must partition candidates by zone, pick the group big
// enough to hold place_count, and reject candidates outside that
// group. Two zones — "us-east-1a" with 2 nodes and "us-west-1b"
// with 1 — and place_count=2: the placer must pick both nodes in
// us-east-1a, never crossing zones.
//
// A regression that flattened the partitioning would silently spread
// replicas across zones, breaking the operator-declared topology
// invariant (e.g. low-latency cohort, regulatory data residency).
func TestPlaceReplicasOnSamePicksLargestGroup(t *testing.T) {
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
			NodeName: name, StoragePoolName: "p",
			ProviderKind: apiv1.StoragePoolKindLVMThin,
			FreeCapacity: 100,
		}); err != nil {
			t.Fatalf("seed pool %s: %v", name, err)
		}
	}

	mk("east-a", "us-east-1a")
	mk("east-b", "us-east-1a")
	mk("west-a", "us-west-1b") // singleton — too small to satisfy place_count=2

	p := placer.New(st)

	placed, want, err := p.Place(ctx, "pvc-1", &apiv1.AutoSelectFilter{
		PlaceCount:     2,
		ReplicasOnSame: []string{"zone"},
	})
	if err != nil {
		t.Fatalf("Place: %v", err)
	}

	if placed != 2 || want != 2 {
		t.Errorf("placed/want: got %d/%d, want 2/2", placed, want)
	}

	got, _ := st.Resources().ListByDefinition(ctx, "pvc-1")
	for _, r := range got {
		if r.NodeName == "west-a" {
			t.Errorf("cross-zone leak: west-a placed despite replicas_on_same=zone; %+v", got)
		}
	}
}

// TestPlaceExistingReplicaWithStaleStorPool pins the newState
// resilience to a Resource that references a StoragePool by name
// in its Props but that pool no longer exists in the StoragePools
// store (operator deleted it while a Resource still pointed at it,
// or the satellite hasn't re-pushed it on Hello yet). The placer
// must silently skip the pool-lookup miss and continue placing
// new replicas — not panic, not double-count.
//
// Without this defensive path, a stale Resource→Pool reference
// would lift the placer's nil-deref into a controller crash, and
// a healthy cluster's autoplacer would stop dead on a single
// dangling Resource.
func TestPlaceExistingReplicaWithStaleStorPool(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	// Two healthy nodes.
	for _, n := range []string{"n1", "n2"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{Name: n, Type: apiv1.NodeTypeSatellite}); err != nil {
			t.Fatalf("seed node: %v", err)
		}

		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			NodeName: n, StoragePoolName: "live",
			ProviderKind: apiv1.StoragePoolKindLVMThin,
			FreeCapacity: 100,
		}); err != nil {
			t.Fatalf("seed pool: %v", err)
		}
	}

	// Pre-existing Resource on n1 pointing at a pool name that ISN'T
	// in the StoragePools store (operator deleted it while the
	// Resource still referenced it via Props).
	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name: "pvc-stale", NodeName: "n1",
		Props: map[string]string{"StorPoolName": "ghost-pool"},
	}); err != nil {
		t.Fatalf("seed existing: %v", err)
	}

	p := placer.New(st)

	placed, want, err := p.Place(ctx, "pvc-stale", &apiv1.AutoSelectFilter{
		PlaceCount:  2,
		StoragePool: "live",
	})
	if err != nil {
		t.Fatalf("Place: %v", err)
	}

	// 1 existing on n1 + 1 new on n2 → placed=2, want=2.
	if placed != 2 || want != 2 {
		t.Errorf("placed/want: got %d/%d, want 2/2 (stale pool ref must be skipped, not crash)", placed, want)
	}
}

// TestPlaceReplicasOnSameNoQualifyingGroup pins the
// pickSameGroup fallback: when no replicas_on_same group is
// large enough to hold place_count, it returns the candidates
// unchanged (with nil tuple) so the placer can run through the
// regular flow and fail the conflict check honestly with
// placed < want — rather than silently picking a too-small
// group that would leave the RD under-replicated.
//
// Three zones × 1 node each, place_count=2 → no group has 2.
// Result: placer can't satisfy 2-on-same and surfaces placed=0.
func TestPlaceReplicasOnSameNoQualifyingGroup(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	mk := func(name, zone string) {
		if err := st.Nodes().Create(ctx, &apiv1.Node{
			Name: name, Type: apiv1.NodeTypeSatellite,
			Props: map[string]string{"Aux/zone": zone},
		}); err != nil {
			t.Fatalf("seed node %s: %v", name, err)
		}

		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			NodeName: name, StoragePoolName: "p",
			ProviderKind: apiv1.StoragePoolKindLVMThin,
			FreeCapacity: 100,
		}); err != nil {
			t.Fatalf("seed pool %s: %v", name, err)
		}
	}

	// Each zone has exactly 1 node — no group big enough for 2.
	mk("east-a", "us-east-1a")
	mk("west-a", "us-west-1b")
	mk("eu-a", "eu-west-1a")

	p := placer.New(st)

	placed, want, err := p.Place(ctx, "pvc-1", &apiv1.AutoSelectFilter{
		PlaceCount:     2,
		ReplicasOnSame: []string{"zone"},
	})
	if err != nil {
		t.Fatalf("Place: %v", err)
	}

	// pickSameGroup returns candidates unchanged with nil tuple →
	// placer falls through, the first replica locks in a tuple, the
	// second can't find a same-tuple peer → placed=1.
	if placed != 1 || want != 2 {
		t.Errorf("placed/want: got %d/%d, want 1/2 (no group ≥2 → fall back to regular flow)", placed, want)
	}
}

// TestPlaceCandidatePoolsFiltersByStoragePoolList pins the
// StoragePoolList filter in candidatePools (was 84%): when the
// filter carries a non-empty allow-list of pool names, only pools
// whose StoragePoolName matches are considered.
//
// The list-based filter is what golinstor's CLI passes for "place
// only on pools that match this name set" semantics. Without the
// pin, a regression that flipped the slices.Contains polarity would
// silently invert the allow-list into a deny-list — operators
// would see replicas land on pools they explicitly excluded.
func TestPlaceCandidatePoolsFiltersByStoragePoolList(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	for _, n := range []string{"n1", "n2"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{Name: n, Type: apiv1.NodeTypeSatellite}); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	// Two pools per node: "fast" and "slow". The filter allows only "fast".
	for _, p := range []apiv1.StoragePool{
		{NodeName: "n1", StoragePoolName: "fast", ProviderKind: apiv1.StoragePoolKindLVMThin, FreeCapacity: 100},
		{NodeName: "n1", StoragePoolName: "slow", ProviderKind: apiv1.StoragePoolKindLVMThin, FreeCapacity: 100},
		{NodeName: "n2", StoragePoolName: "fast", ProviderKind: apiv1.StoragePoolKindLVMThin, FreeCapacity: 100},
		{NodeName: "n2", StoragePoolName: "slow", ProviderKind: apiv1.StoragePoolKindLVMThin, FreeCapacity: 100},
	} {
		if err := st.StoragePools().Create(ctx, &p); err != nil {
			t.Fatalf("seed pool: %v", err)
		}
	}

	plc := placer.New(st)

	placed, _, err := plc.Place(ctx, "pvc-1", &apiv1.AutoSelectFilter{
		PlaceCount:      2,
		StoragePoolList: []string{"fast"},
	})
	if err != nil {
		t.Fatalf("Place: %v", err)
	}

	if placed != 2 {
		t.Errorf("placed: got %d, want 2", placed)
	}

	got, _ := st.Resources().ListByDefinition(ctx, "pvc-1")
	for _, r := range got {
		if r.Props["StorPoolName"] != "fast" {
			t.Errorf("expected StoragePool=fast, got %q on %s",
				r.Props["StorPoolName"], r.NodeName)
		}
	}
}

// TestPlacerDeficitExcludesDisklessAndTiebreaker pins Bug 19.2: the
// place_count deficit calculation must NOT count DISKLESS replicas
// (including auto-tiebreaker witnesses, which carry DISKLESS +
// TIE_BREAKER) toward the diskful target. A 3-replica RG sitting at
// 2 diskful + 1 diskless witness must still be 1-short so the placer
// fills the gap rather than declaring satisfaction.
//
// Setup: place_count=3, 4 healthy nodes. Pre-seed 2 diskful replicas
// (n1+n2) and one tiebreaker witness (n3, DISKLESS+TIE_BREAKER). The
// placer must add a 3rd diskful replica on n4 — NOT treat the witness
// as the 3rd replica and exit early.
func TestPlacerDeficitExcludesDisklessAndTiebreaker(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	seedStore(t, st, []string{"n1", "n2", "n3", "n4"})

	// Two diskful replicas on n1 + n2.
	for _, n := range []string{"n1", "n2"} {
		if err := st.Resources().Create(ctx, &apiv1.Resource{
			Name: "pvc-1", NodeName: n,
			Props: map[string]string{"StorPoolName": "pool"},
		}); err != nil {
			t.Fatalf("seed diskful %s: %v", n, err)
		}
	}

	// Auto-tiebreaker witness on n3: DISKLESS + TIE_BREAKER. Without
	// the deficit fix the placer counts n3 as a "replica present"
	// and stops at 2 diskful + 1 witness instead of going to 3 diskful.
	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name: "pvc-1", NodeName: "n3",
		Flags: []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker},
	}); err != nil {
		t.Fatalf("seed witness: %v", err)
	}

	p := placer.New(st)

	placed, want, err := p.Place(ctx, "pvc-1", &apiv1.AutoSelectFilter{PlaceCount: 3})
	if err != nil {
		t.Fatalf("Place: %v", err)
	}

	if placed != 3 || want != 3 {
		t.Errorf("placed/want: got %d/%d, want 3/3 (witness must not count)", placed, want)
	}

	got, _ := st.Resources().ListByDefinition(ctx, "pvc-1")

	diskful := 0
	witness := 0

	for _, r := range got {
		if slices.Contains(r.Flags, apiv1.ResourceFlagDiskless) {
			witness++

			continue
		}

		diskful++
	}

	if diskful != 3 {
		t.Errorf("diskful count: got %d, want 3 (gap-fill must run); resources=%+v", diskful, got)
	}

	if witness != 1 {
		t.Errorf("witness count: got %d, want 1 (existing witness left untouched); resources=%+v", witness, got)
	}

	// The new diskful replica must land on n4 — the only remaining
	// healthy node that wasn't already taken by an existing replica.
	gotNodes := map[string]bool{}
	for _, r := range got {
		gotNodes[r.NodeName] = true
	}

	if !gotNodes["n4"] {
		t.Errorf("expected new diskful replica on n4; got %+v", gotNodes)
	}
}

// TestPlacePlaceCountIgnoresDisklessWitness pins Bug 28: PlaceCount
// counts DISKFUL replicas only — a DISKLESS / TIE_BREAKER witness
// pre-seeded by the RD reconciler's ensureTiebreaker race must NOT
// be counted as one of the PlaceCount diskful replicas.
//
// Scenario: 3 healthy worker nodes, an RD reconciler already stamped
// a DISKLESS+TIE_BREAKER witness on n1 (the witness racing the
// REST autoplace call). Caller now requests PlaceCount=2. Expected:
// 2 fresh diskful replicas on the OTHER two nodes (n2 + n3 — the
// largest-free ones), with the witness on n1 left in place.
//
// Pre-fix behaviour (the bug): the placer's "existing counts toward
// PlaceCount" loop counts the witness as one slot — only 1 diskful
// gets created, the cluster ends up with 1 diskful + 1 witness,
// quorum is impossible.
func TestPlacePlaceCountIgnoresDisklessWitness(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	seedStore(t, st, []string{"n1", "n2", "n3"})

	// Pre-seed the witness on n1 (the smallest-free node). This
	// mirrors what the RD controller's ensureTiebreaker stamps when
	// it fires before the placer has finished. n1 is deliberately
	// chosen so largest-free placement (n3 → n2) would skip it for
	// diskful — proving the placer doesn't accidentally promote it
	// either.
	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name:     "pvc-1",
		NodeName: "n1",
		Flags:    []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker},
	}); err != nil {
		t.Fatalf("seed witness: %v", err)
	}

	p := placer.New(st)

	placed, want, err := p.Place(ctx, "pvc-1", &apiv1.AutoSelectFilter{PlaceCount: 2})
	if err != nil {
		t.Fatalf("Place: %v", err)
	}

	if placed != 2 || want != 2 {
		t.Errorf("placed/want: got %d/%d, want 2/2 (witness must not count)", placed, want)
	}

	got, err := st.Resources().ListByDefinition(ctx, "pvc-1")
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	diskful := 0
	witness := 0

	for _, r := range got {
		isDiskless := false
		isTieBreaker := false

		for _, f := range r.Flags {
			if f == apiv1.ResourceFlagDiskless {
				isDiskless = true
			}

			if f == apiv1.ResourceFlagTieBreaker {
				isTieBreaker = true
			}
		}

		switch {
		case isTieBreaker:
			witness++
		case !isDiskless:
			diskful++
		}
	}

	if diskful != 2 {
		t.Errorf("diskful count: got %d, want 2; resources=%+v", diskful, got)
	}

	if witness != 1 {
		t.Errorf("witness count: got %d, want 1 (left in place); resources=%+v", witness, got)
	}
}

// Keep go-vet happy on unused symbols in the import set.
var _ = context.Background
