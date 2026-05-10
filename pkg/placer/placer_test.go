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

// Keep go-vet happy on unused symbols in the import set.
var _ = context.Background
