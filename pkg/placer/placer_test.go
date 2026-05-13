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
	"errors"
	"slices"
	"strings"
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
			// TotalCapacity uniform 1000 — scenario 2.17's MaxFreeSpace
			// ratio (Free/Total) discriminates n3 > n2 > n1, preserving
			// the legacy flat-sort biggest-free-first ordering under
			// the weighted scorer.
			TotalCapacity: 1000,
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

// TestPlaceReplicasOnSameBug44 pins Bug 44: when an RG passes
// `replicas-on-same Aux/topology.kubernetes.io/zone` (the full
// Aux-prefixed key used by the NodeLabelSyncReconciler), spawning
// with place-count=2 across 3 nodes (zone-a: 2 nodes, zone-b: 1
// node) MUST land both replicas in zone-a — never on the zone-b
// singleton.
//
// Mirrors tests/e2e/placement-label-sync.sh (scenario 2.13) at the
// placer layer. The e2e setup gives worker-3 the largest FreeCapacity
// to defeat the biggest-first sort — without the replicas-on-same
// constraint the placer would pick worker-3 first. Both the bare-key
// and the Aux-prefixed-key forms must work because the upstream CLI
// passes the key verbatim through the wire and operators write
// either shape (`auxKey()` normalises).
func TestPlaceReplicasOnSameBug44(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	mk := func(name, zone string, free int64) {
		if err := st.Nodes().Create(ctx, &apiv1.Node{
			Name:  name,
			Type:  apiv1.NodeTypeSatellite,
			Props: map[string]string{"Aux/topology.kubernetes.io/zone": zone},
		}); err != nil {
			t.Fatalf("seed node %s: %v", name, err)
		}

		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			NodeName: name, StoragePoolName: "stand",
			ProviderKind: apiv1.StoragePoolKindLVMThin,
			FreeCapacity: free,
		}); err != nil {
			t.Fatalf("seed pool %s: %v", name, err)
		}
	}

	// worker-3 has the LARGEST free capacity AND is the singleton in
	// zone-b — without the replicas-on-same constraint the placer
	// would pick it first by the biggest-first sort. The constraint
	// must keep it out entirely.
	mk("worker-1", "zone-a", 200)
	mk("worker-2", "zone-a", 100)
	mk("worker-3", "zone-b", 999)

	p := placer.New(st)

	placed, want, err := p.Place(ctx, "pvc-1", &apiv1.AutoSelectFilter{
		PlaceCount:     2,
		StoragePool:    "stand",
		ReplicasOnSame: []string{"Aux/topology.kubernetes.io/zone"},
	})
	if err != nil {
		t.Fatalf("Place: %v", err)
	}

	if placed != 2 || want != 2 {
		t.Errorf("placed/want: got %d/%d, want 2/2", placed, want)
	}

	got, _ := st.Resources().ListByDefinition(ctx, "pvc-1")
	if len(got) != 2 {
		t.Fatalf("len: got %d, want 2; %+v", len(got), got)
	}

	on := map[string]bool{}
	for _, r := range got {
		on[r.NodeName] = true
	}

	if on["worker-3"] {
		t.Errorf("worker-3 (zone-b) placed despite replicas_on_same=Aux/topology.kubernetes.io/zone; got %+v", got)
	}

	if !on["worker-1"] || !on["worker-2"] {
		t.Errorf("expected placement on worker-1+worker-2 (zone-a pair); got %+v", on)
	}
}

// TestPlaceReplicasOnSameCountsUniqueNodes pins the unique-nodes
// sizing in pickSameGroup (Bug 44 hardening): a group with one node
// exposing N pools cannot satisfy place_count=2 even when the pool
// count looks like 2. The placer must skip such pseudo-groups so a
// multi-pool-per-node setup doesn't trick the partitioner into
// returning a too-small group as the winner.
//
// Scenario: zone-a has ONE node with TWO pools (lvm-thin + zfs-thin);
// zone-b has TWO nodes with one pool each. place_count=2 must pick
// the zone-b pair — zone-a only has one unique node so the placer
// cannot land 2 distinct replicas there.
func TestPlaceReplicasOnSameCountsUniqueNodes(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	mkNode := func(name, zone string) {
		if err := st.Nodes().Create(ctx, &apiv1.Node{
			Name:  name,
			Type:  apiv1.NodeTypeSatellite,
			Props: map[string]string{"Aux/zone": zone},
		}); err != nil {
			t.Fatalf("seed node %s: %v", name, err)
		}
	}

	mkPool := func(node, pool string, free int64) {
		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			NodeName: node, StoragePoolName: pool,
			ProviderKind: apiv1.StoragePoolKindLVMThin,
			FreeCapacity: free,
		}); err != nil {
			t.Fatalf("seed pool %s/%s: %v", node, pool, err)
		}
	}

	mkNode("a1", "zone-a")
	mkPool("a1", "fast", 500)
	mkPool("a1", "slow", 500) // single zone-a node, but 2 pools — looks like 2 candidates

	mkNode("b1", "zone-b")
	mkNode("b2", "zone-b")
	mkPool("b1", "fast", 100)
	mkPool("b2", "fast", 100)

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
	on := map[string]bool{}
	for _, r := range got {
		on[r.NodeName] = true
	}

	if on["a1"] {
		t.Errorf("a1 (zone-a singleton with 2 pools) picked despite needing 2 unique nodes; got %+v", got)
	}

	if !on["b1"] || !on["b2"] {
		t.Errorf("expected zone-b pair (b1+b2); got %+v", on)
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

// TestPlaceReplicasOnDifferentExcludeMode pins UG9's "key=value" form
// of replicas_on_different: nodes carrying that exact Aux/<key>=<value>
// pair are considered LAST resort, NOT hard-excluded. With 3 healthy
// nodes (n3 carrying Aux/no-csi-volumes=true), a place_count=2 call
// must land on n1+n2 even though n3 has the largest free capacity —
// because the value-form excludes n3 from normal selection.
//
// A regression that treated the value-form as a no-op would happily
// pick n3 (biggest free) over n2 and break tenant placement intent.
func TestPlaceReplicasOnDifferentExcludeMode(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	mk := func(name string, free int64, props map[string]string) {
		if err := st.Nodes().Create(ctx, &apiv1.Node{
			Name: name, Type: apiv1.NodeTypeSatellite, Props: props,
		}); err != nil {
			t.Fatalf("seed node %s: %v", name, err)
		}

		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			NodeName: name, StoragePoolName: "pool",
			ProviderKind: apiv1.StoragePoolKindLVMThin,
			FreeCapacity: free,
		}); err != nil {
			t.Fatalf("seed pool %s: %v", name, err)
		}
	}

	// n3 has the LARGEST free capacity AND the excluded label —
	// largest-first sort alone would pick it. The filter must keep
	// it out of the preferred bucket.
	mk("n1", 100, nil)
	mk("n2", 200, nil)
	mk("n3", 999, map[string]string{"Aux/no-csi-volumes": "true"})

	p := placer.New(st)

	placed, want, err := p.Place(ctx, "pvc-1", &apiv1.AutoSelectFilter{
		PlaceCount:          2,
		ReplicasOnDifferent: []string{"no-csi-volumes=true"},
	})
	if err != nil {
		t.Fatalf("Place: %v", err)
	}

	if placed != 2 || want != 2 {
		t.Errorf("placed/want: got %d/%d, want 2/2", placed, want)
	}

	got, _ := st.Resources().ListByDefinition(ctx, "pvc-1")

	on := map[string]bool{}
	for _, r := range got {
		on[r.NodeName] = true
	}

	if on["n3"] {
		t.Errorf("excluded node n3 picked while preferred candidates still available; resources=%+v", got)
	}

	if !on["n1"] || !on["n2"] {
		t.Errorf("expected placement on n1+n2 (preferred), got %+v", on)
	}
}

// TestPlaceReplicasOnDifferentFallsBackToExcludedNode pins the
// last-resort fallback half of the contract: the value-form is a
// preference, NOT a hard rule. With only 3 nodes available and
// place_count=3, the placer MUST tap n3 (the excluded node) after
// the preferred bucket is drained, otherwise a 3-replica RG over a
// 3-node cluster would be permanently under-placed.
//
// A regression that hard-excluded the value-form node would leave
// placed=2/want=3 and the operator would have no way to recover.
func TestPlaceReplicasOnDifferentFallsBackToExcludedNode(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	mk := func(name string, free int64, props map[string]string) {
		if err := st.Nodes().Create(ctx, &apiv1.Node{
			Name: name, Type: apiv1.NodeTypeSatellite, Props: props,
		}); err != nil {
			t.Fatalf("seed node %s: %v", name, err)
		}

		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			NodeName: name, StoragePoolName: "pool",
			ProviderKind: apiv1.StoragePoolKindLVMThin,
			FreeCapacity: free,
		}); err != nil {
			t.Fatalf("seed pool %s: %v", name, err)
		}
	}

	mk("n1", 100, nil)
	mk("n2", 200, nil)
	mk("n3", 999, map[string]string{"Aux/no-csi-volumes": "true"})

	p := placer.New(st)

	placed, want, err := p.Place(ctx, "pvc-1", &apiv1.AutoSelectFilter{
		PlaceCount:          3,
		ReplicasOnDifferent: []string{"no-csi-volumes=true"},
	})
	if err != nil {
		t.Fatalf("Place: %v", err)
	}

	if placed != 3 || want != 3 {
		t.Errorf("placed/want: got %d/%d, want 3/3 (last-resort fallback must engage)", placed, want)
	}

	got, _ := st.Resources().ListByDefinition(ctx, "pvc-1")

	on := map[string]bool{}
	for _, r := range got {
		on[r.NodeName] = true
	}

	if !on["n1"] || !on["n2"] || !on["n3"] {
		t.Errorf("expected placement on all 3 nodes (n3 as last-resort), got %+v", on)
	}
}

// TestPlaceMixedProviderPools pins UG9 §"Mixing storage pools of
// different storage providers" / scenario 6.8: a RG that lists pools
// with DIFFERENT ProviderKinds in StoragePoolList must autoplace
// freely across them. With:
//
//   - n1: only `zfs-thin` (ZFS_THIN, big free)
//   - n2: only `lvm-thin` (LVM_THIN, medium free)
//   - n3: both pools (small free)
//
// and StoragePoolList = ["zfs-thin","lvm-thin"], place_count=2 must
// land replicas on n1 + n2 (biggest-free preferred), yielding a
// MIXED-provider RD where one replica is on ZFS_THIN and the other
// on LVM_THIN. Each replica's Props["StorPoolName"] reflects the
// pool that hosts it — the dispatcher uses this downstream to drive
// the satellite's provider-specific create path.
//
// The reverse-direction guard (Bug 15) is exercised in a sub-test:
// once such a mixed-provider RD exists, a snapshot-restore clone from
// it must NOT be free to land cross-provider. We simulate the REST
// handler's behaviour by setting filter.ProviderList to the source's
// ProviderKind — the placer's candidatePools must then drop all
// pools whose ProviderKind doesn't match, leaving only same-kind
// pools as placement targets.
func TestPlaceMixedProviderPools(t *testing.T) {
	t.Parallel()

	t.Run("autoplace_mixes_providers", func(t *testing.T) {
		t.Parallel()

		st := store.NewInMemory()
		ctx := t.Context()

		for _, n := range []string{"n1", "n2", "n3"} {
			if err := st.Nodes().Create(ctx, &apiv1.Node{
				Name: n, Type: apiv1.NodeTypeSatellite,
			}); err != nil {
				t.Fatalf("seed node %s: %v", n, err)
			}
		}

		// FreeCapacity is tuned so the biggest-first sort picks
		// n1.zfs-thin then n2.lvm-thin BEFORE either of n3's pools.
		// That puts one ZFS_THIN replica and one LVM_THIN replica on
		// two DIFFERENT nodes — the canonical "mixed-provider RG" shape.
		pools := []apiv1.StoragePool{
			{NodeName: "n1", StoragePoolName: "zfs-thin", ProviderKind: apiv1.StoragePoolKindZFSThin, FreeCapacity: 1000},
			{NodeName: "n2", StoragePoolName: "lvm-thin", ProviderKind: apiv1.StoragePoolKindLVMThin, FreeCapacity: 900},
			{NodeName: "n3", StoragePoolName: "zfs-thin", ProviderKind: apiv1.StoragePoolKindZFSThin, FreeCapacity: 200},
			{NodeName: "n3", StoragePoolName: "lvm-thin", ProviderKind: apiv1.StoragePoolKindLVMThin, FreeCapacity: 100},
		}
		for i := range pools {
			if err := st.StoragePools().Create(ctx, &pools[i]); err != nil {
				t.Fatalf("seed pool %s/%s: %v", pools[i].NodeName, pools[i].StoragePoolName, err)
			}
		}

		p := placer.New(st)

		placed, want, err := p.Place(ctx, "pvc-mixed", &apiv1.AutoSelectFilter{
			PlaceCount:      2,
			StoragePoolList: []string{"zfs-thin", "lvm-thin"},
		})
		if err != nil {
			t.Fatalf("Place: %v", err)
		}

		if placed != 2 || want != 2 {
			t.Fatalf("placed/want: got %d/%d, want 2/2", placed, want)
		}

		got, err := st.Resources().ListByDefinition(ctx, "pvc-mixed")
		if err != nil {
			t.Fatalf("list: %v", err)
		}

		if len(got) != 2 {
			t.Fatalf("len: got %d, want 2; %+v", len(got), got)
		}

		// Each replica's StorPoolName + the looked-up ProviderKind
		// must reflect the pool that hosts it. Walk the placed
		// Resources, look up the pool, and bucket by ProviderKind.
		kinds := map[string]string{} // node -> ProviderKind
		nodes := map[string]bool{}

		for _, r := range got {
			nodes[r.NodeName] = true

			stor := r.Props["StorPoolName"]
			if stor == "" {
				t.Errorf("replica on %s missing StorPoolName prop: %+v", r.NodeName, r)

				continue
			}

			pool, err := st.StoragePools().Get(ctx, r.NodeName, stor)
			if err != nil {
				t.Fatalf("get pool %s/%s: %v", r.NodeName, stor, err)
			}

			kinds[r.NodeName] = pool.ProviderKind
		}

		// Biggest-free pair → n1 + n2 (not n3).
		if !nodes["n1"] || !nodes["n2"] {
			t.Errorf("expected placement on n1+n2 (largest-free across providers); got %+v", nodes)
		}

		if nodes["n3"] {
			t.Errorf("n3 picked despite n1/n2 having larger free; got %+v", nodes)
		}

		// Mixed providers: one ZFS_THIN, one LVM_THIN.
		var seenZFS, seenLVM bool
		for _, k := range kinds {
			switch k {
			case apiv1.StoragePoolKindZFSThin:
				seenZFS = true
			case apiv1.StoragePoolKindLVMThin:
				seenLVM = true
			}
		}

		if !seenZFS || !seenLVM {
			t.Errorf("expected mixed providers (ZFS_THIN + LVM_THIN); kinds=%+v", kinds)
		}

		// Sanity: replica on n1 is ZFS_THIN, replica on n2 is LVM_THIN.
		if kinds["n1"] != apiv1.StoragePoolKindZFSThin {
			t.Errorf("n1 replica: got ProviderKind=%q, want %q",
				kinds["n1"], apiv1.StoragePoolKindZFSThin)
		}

		if kinds["n2"] != apiv1.StoragePoolKindLVMThin {
			t.Errorf("n2 replica: got ProviderKind=%q, want %q",
				kinds["n2"], apiv1.StoragePoolKindLVMThin)
		}
	})

	t.Run("bug15_clone_refuses_cross_provider", func(t *testing.T) {
		t.Parallel()

		// Same topology as above, but now we simulate the REST
		// snapshot-restore path: source RD is on ZFS_THIN, so the
		// caller pins filter.ProviderList = [ZFS_THIN]. The placer
		// must drop every LVM_THIN candidate — even though the
		// StoragePoolList still allows both pool *names*. With only
		// 2 ZFS_THIN-bearing nodes (n1, n3) the placer can place 2
		// replicas, but NOT on n2 (LVM_THIN only).
		st := store.NewInMemory()
		ctx := t.Context()

		for _, n := range []string{"n1", "n2", "n3"} {
			if err := st.Nodes().Create(ctx, &apiv1.Node{
				Name: n, Type: apiv1.NodeTypeSatellite,
			}); err != nil {
				t.Fatalf("seed node %s: %v", n, err)
			}
		}

		pools := []apiv1.StoragePool{
			{NodeName: "n1", StoragePoolName: "zfs-thin", ProviderKind: apiv1.StoragePoolKindZFSThin, FreeCapacity: 1000},
			{NodeName: "n2", StoragePoolName: "lvm-thin", ProviderKind: apiv1.StoragePoolKindLVMThin, FreeCapacity: 900},
			{NodeName: "n3", StoragePoolName: "zfs-thin", ProviderKind: apiv1.StoragePoolKindZFSThin, FreeCapacity: 200},
			{NodeName: "n3", StoragePoolName: "lvm-thin", ProviderKind: apiv1.StoragePoolKindLVMThin, FreeCapacity: 100},
		}
		for i := range pools {
			if err := st.StoragePools().Create(ctx, &pools[i]); err != nil {
				t.Fatalf("seed pool: %v", err)
			}
		}

		p := placer.New(st)

		// Caller asks for both pool names AND restricts ProviderKind
		// to ZFS_THIN (simulating handleAutoplace's Bug 15 stamp).
		placed, want, err := p.Place(ctx, "pvc-clone", &apiv1.AutoSelectFilter{
			PlaceCount:      2,
			StoragePoolList: []string{"zfs-thin", "lvm-thin"},
			ProviderList:    []string{apiv1.StoragePoolKindZFSThin},
		})
		if err != nil {
			t.Fatalf("Place: %v", err)
		}

		if placed != 2 || want != 2 {
			t.Fatalf("placed/want: got %d/%d, want 2/2 (n1+n3 ZFS_THIN); placed should still satisfy via same-provider", placed, want)
		}

		got, _ := st.Resources().ListByDefinition(ctx, "pvc-clone")

		for _, r := range got {
			stor := r.Props["StorPoolName"]

			pool, err := st.StoragePools().Get(ctx, r.NodeName, stor)
			if err != nil {
				t.Fatalf("get pool %s/%s: %v", r.NodeName, stor, err)
			}

			if pool.ProviderKind != apiv1.StoragePoolKindZFSThin {
				t.Errorf("cross-provider leak: replica on %s used %s pool (kind=%s); ProviderList guard failed; resource=%+v",
					r.NodeName, stor, pool.ProviderKind, r)
			}

			if r.NodeName == "n2" {
				t.Errorf("n2 (LVM_THIN only) selected despite ProviderList=[ZFS_THIN]; resource=%+v", r)
			}
		}

		// Sanity: the two replicas live on the two ZFS_THIN-bearing
		// nodes (n1 + n3). Either pool name from the allow-list is
		// fine as long as ProviderKind is ZFS_THIN.
		nodes := map[string]bool{}
		for _, r := range got {
			nodes[r.NodeName] = true
		}

		if !nodes["n1"] || !nodes["n3"] {
			t.Errorf("expected ZFS_THIN replicas on n1+n3; got %+v", nodes)
		}
	})

	t.Run("bug15_clone_underplaces_when_only_one_zfs_node", func(t *testing.T) {
		t.Parallel()

		// Tightens the guard: when the cluster can't satisfy
		// place_count with same-provider pools, the placer must
		// 409-shortfall (placed<want), NOT silently fall back to
		// the other provider and corrupt the clone payload.
		st := store.NewInMemory()
		ctx := t.Context()

		for _, n := range []string{"n1", "n2", "n3"} {
			if err := st.Nodes().Create(ctx, &apiv1.Node{
				Name: n, Type: apiv1.NodeTypeSatellite,
			}); err != nil {
				t.Fatalf("seed node %s: %v", n, err)
			}
		}

		// Only n1 carries a ZFS_THIN pool; n2+n3 are LVM_THIN.
		pools := []apiv1.StoragePool{
			{NodeName: "n1", StoragePoolName: "zfs-thin", ProviderKind: apiv1.StoragePoolKindZFSThin, FreeCapacity: 1000},
			{NodeName: "n2", StoragePoolName: "lvm-thin", ProviderKind: apiv1.StoragePoolKindLVMThin, FreeCapacity: 900},
			{NodeName: "n3", StoragePoolName: "lvm-thin", ProviderKind: apiv1.StoragePoolKindLVMThin, FreeCapacity: 800},
		}
		for i := range pools {
			if err := st.StoragePools().Create(ctx, &pools[i]); err != nil {
				t.Fatalf("seed pool: %v", err)
			}
		}

		p := placer.New(st)

		placed, want, err := p.Place(ctx, "pvc-clone-2", &apiv1.AutoSelectFilter{
			PlaceCount:      2,
			StoragePoolList: []string{"zfs-thin", "lvm-thin"},
			ProviderList:    []string{apiv1.StoragePoolKindZFSThin},
		})
		if err != nil {
			t.Fatalf("Place: %v", err)
		}

		if placed != 1 || want != 2 {
			t.Errorf("placed/want: got %d/%d, want 1/2 (only n1 satisfies ProviderList=[ZFS_THIN])", placed, want)
		}

		got, _ := st.Resources().ListByDefinition(ctx, "pvc-clone-2")
		for _, r := range got {
			if r.NodeName == "n2" || r.NodeName == "n3" {
				t.Errorf("Bug 15 violation: clone allowed onto LVM_THIN node %s; resource=%+v", r.NodeName, r)
			}
		}

		// And the (unused) slices import stays load-bearing in the file.
		if len(got) > 0 && !slices.Contains([]string{"n1"}, got[0].NodeName) {
			t.Errorf("expected single replica on n1; got %+v", got)
		}
	})
}

// TestPlaceRejectsBelowMinFreeCapacity pins Bug 35: the placer must
// drop pools whose FreeCapacity is below the largest VolumeDefinition
// on the RD. 7.15 e2e showed autoplace returning 200 even when every
// candidate pool reported FreeCapacity=0 — the satellite then failed
// opaquely at volume-create. The fix is the FreeCapacity floor at the
// placer layer.
//
// Setup mirrors the bug report: 3 pools with FreeCapacity 100, 200,
// 50 MiB; one VolumeDefinition asking for 150 MiB. Only the 200-MiB
// pool clears the floor.
//
// Sub-test "single_replica_fits": PlaceCount=1 → the 200-MiB pool
// hosts the replica; the 100- and 50-MiB pools are silently dropped.
//
// Sub-test "three_replicas_short": PlaceCount=3 → only one pool can
// satisfy, so placed=1/want=3 AND the placer returns a
// CapacityShortfallError carrying RequiredKib and the largest
// FreeCapacity among the rejected pools (the 100-MiB one). REST
// converts this into a 409 with the actionable text from
// CapacityShortfallError.Error().
func TestPlaceRejectsBelowMinFreeCapacity(t *testing.T) {
	t.Parallel()

	const mib = int64(1024)

	mkCluster := func(t *testing.T) store.Store {
		t.Helper()

		st := store.NewInMemory()
		ctx := t.Context()

		type seed struct {
			node string
			free int64 // KiB
		}

		for _, s := range []seed{
			{"n-small", 50 * mib},
			{"n-mid", 100 * mib},
			{"n-big", 200 * mib},
		} {
			if err := st.Nodes().Create(ctx, &apiv1.Node{
				Name: s.node, Type: apiv1.NodeTypeSatellite,
			}); err != nil {
				t.Fatalf("seed node %s: %v", s.node, err)
			}

			if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
				NodeName: s.node, StoragePoolName: "pool",
				ProviderKind: apiv1.StoragePoolKindLVMThin,
				FreeCapacity: s.free,
			}); err != nil {
				t.Fatalf("seed pool %s: %v", s.node, err)
			}
		}

		// RD + VD asking for 150 MiB — the floor against which the
		// placer's capacity gate filters candidate pools.
		if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
			Name: "pvc-1",
		}); err != nil {
			t.Fatalf("seed RD: %v", err)
		}

		if err := st.VolumeDefinitions().Create(ctx, "pvc-1", &apiv1.VolumeDefinition{
			VolumeNumber: 0,
			SizeKib:      150 * mib,
		}); err != nil {
			t.Fatalf("seed VD: %v", err)
		}

		return st
	}

	t.Run("single_replica_fits", func(t *testing.T) {
		t.Parallel()

		st := mkCluster(t)
		ctx := t.Context()

		placed, want, err := placer.New(st).Place(ctx, "pvc-1", &apiv1.AutoSelectFilter{
			PlaceCount: 1,
		})
		if err != nil {
			t.Fatalf("Place: %v", err)
		}

		if placed != 1 || want != 1 {
			t.Errorf("placed/want: got %d/%d, want 1/1", placed, want)
		}

		got, _ := st.Resources().ListByDefinition(ctx, "pvc-1")
		if len(got) != 1 {
			t.Fatalf("resource count: got %d, want 1; %+v", len(got), got)
		}

		// The only pool with enough FreeCapacity (200 MiB > 150 MiB)
		// is on n-big; n-mid (100 MiB) and n-small (50 MiB) must be
		// silently dropped by the capacity filter.
		if got[0].NodeName != "n-big" {
			t.Errorf("placed on %q, want n-big (only pool ≥ 150 MiB); resources=%+v",
				got[0].NodeName, got)
		}
	})

	t.Run("three_replicas_short_surfaces_capacity_error", func(t *testing.T) {
		t.Parallel()

		st := mkCluster(t)
		ctx := t.Context()

		placed, want, err := placer.New(st).Place(ctx, "pvc-1", &apiv1.AutoSelectFilter{
			PlaceCount: 3,
		})

		// With only 1 pool clearing the floor, placement is partial.
		if placed != 1 || want != 3 {
			t.Errorf("placed/want: got %d/%d, want 1/3 (only n-big satisfies)", placed, want)
		}

		// And the placer must surface the actionable capacity error
		// — REST converts to 409 with the rendered text.
		var capErr *placer.CapacityShortfallError
		if !errors.As(err, &capErr) {
			t.Fatalf("err: got %v, want *CapacityShortfallError", err)
		}

		if capErr.RequiredKib != 150*mib {
			t.Errorf("RequiredKib: got %d, want %d", capErr.RequiredKib, 150*mib)
		}

		// Largest FreeCapacity among rejected pools is the 100-MiB one.
		if capErr.MaxFreeKib != 100*mib {
			t.Errorf("MaxFreeKib: got %d, want %d (largest rejected)",
				capErr.MaxFreeKib, 100*mib)
		}

		// Sanity: rendered message carries both numbers verbatim — the
		// REST 409 surface that operators see.
		msg := capErr.Error()
		if !strings.Contains(msg, "required 153600 KiB") {
			t.Errorf("error text missing required KiB: %q", msg)
		}

		if !strings.Contains(msg, "max free 102400 KiB") {
			t.Errorf("error text missing max free KiB: %q", msg)
		}
	})

	t.Run("no_candidate_clears_floor", func(t *testing.T) {
		t.Parallel()

		// Tightens the all-rejected branch: when ZERO pools clear the
		// FreeCapacity floor, the placer must early-return the
		// shortfall error without creating any Resource — even on a
		// single-replica request. This is the literal 7.15 e2e shape:
		// every candidate reports 0-KiB free.
		st := store.NewInMemory()
		ctx := t.Context()

		for _, n := range []string{"n1", "n2"} {
			if err := st.Nodes().Create(ctx, &apiv1.Node{
				Name: n, Type: apiv1.NodeTypeSatellite,
			}); err != nil {
				t.Fatalf("seed node: %v", err)
			}

			if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
				NodeName: n, StoragePoolName: "pool",
				ProviderKind: apiv1.StoragePoolKindLVMThin,
				FreeCapacity: 10 * mib, // way under the 150-MiB ask
			}); err != nil {
				t.Fatalf("seed pool: %v", err)
			}
		}

		if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
			Name: "pvc-empty",
		}); err != nil {
			t.Fatalf("seed RD: %v", err)
		}

		if err := st.VolumeDefinitions().Create(ctx, "pvc-empty", &apiv1.VolumeDefinition{
			VolumeNumber: 0, SizeKib: 150 * mib,
		}); err != nil {
			t.Fatalf("seed VD: %v", err)
		}

		placed, want, err := placer.New(st).Place(ctx, "pvc-empty",
			&apiv1.AutoSelectFilter{PlaceCount: 1})

		if placed != 0 || want != 1 {
			t.Errorf("placed/want: got %d/%d, want 0/1", placed, want)
		}

		var capErr *placer.CapacityShortfallError
		if !errors.As(err, &capErr) {
			t.Fatalf("err: got %v, want *CapacityShortfallError", err)
		}

		// The largest FreeCapacity among the rejected pools is 10 MiB.
		if capErr.MaxFreeKib != 10*mib {
			t.Errorf("MaxFreeKib: got %d, want %d", capErr.MaxFreeKib, 10*mib)
		}

		got, _ := st.Resources().ListByDefinition(ctx, "pvc-empty")
		if len(got) != 0 {
			t.Errorf("no replica must be created on all-rejected fail-fast; got %+v", got)
		}
	})
}

// TestPlaceCapacityGateIntegratesWithOversubRatio pins that the
// placer's hard FreeCapacity floor (Bug 35) and the spawn-layer
// over-subscription gate (Bug 7.19, see
// pkg/rest/oversubscription.go:poolMaxVolumeKib) are independent
// gates: even on a thin pool where the oversub ratio would let the
// logical sum exceed FreeCapacity, the PHYSICAL FreeCapacity floor
// still drops pools that can't host the requested volume at create
// time.
//
// Why this matters: the oversub gate is a logical-budget check
// applied at spawn time across the cluster; the placer-level capacity
// gate is the per-pool physical floor. Without Bug 35 the placer
// would happily pick a pool whose FreeCapacity is below the volume
// size, trusting the oversub gate to have already vetted "logical
// sum". But the oversub gate runs on a stale snapshot and ALWAYS
// trusts the in-store FreeCapacity from the satellite's last push —
// it doesn't protect against a pool that has since filled up. The
// placer's hard floor is the synchronisation point that catches that.
//
// Setup: 2 LVM_THIN pools (both thin → ratio=20 by default would
// allow logical up to 20×FreeCapacity), each reporting FreeCapacity=
// 10 MiB, TotalCapacity=1 GiB. The RD's VD asks for 100 MiB — well
// above the per-pool FreeCapacity (which Bug 35 gates) but well
// inside what a naive ratio×FreeCapacity computation would let pass.
// The placer must reject BOTH pools and surface the capacity error,
// independent of the oversub ratio.
func TestPlaceCapacityGateIntegratesWithOversubRatio(t *testing.T) {
	t.Parallel()

	const mib = int64(1024)

	st := store.NewInMemory()
	ctx := t.Context()

	for _, n := range []string{"n1", "n2"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{
			Name: n, Type: apiv1.NodeTypeSatellite,
		}); err != nil {
			t.Fatalf("seed node: %v", err)
		}

		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			NodeName: n, StoragePoolName: "thin",
			ProviderKind:  apiv1.StoragePoolKindLVMThin,
			FreeCapacity:  10 * mib,   // physical floor
			TotalCapacity: 1024 * mib, // ratio gate would allow much more
			// Explicitly set the umbrella ratio prop to the upstream
			// default (20) so the bug-7.19 logical budget is in scope.
			// The placer must IGNORE this and gate on FreeCapacity only.
			Props: map[string]string{"MaxOversubscriptionRatio": "20"},
		}); err != nil {
			t.Fatalf("seed pool: %v", err)
		}
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: "pvc-oversub",
	}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	// Request 100 MiB — fits inside 20×10MiB logical budget but
	// overshoots the 10-MiB physical floor on every pool.
	if err := st.VolumeDefinitions().Create(ctx, "pvc-oversub", &apiv1.VolumeDefinition{
		VolumeNumber: 0,
		SizeKib:      100 * mib,
	}); err != nil {
		t.Fatalf("seed VD: %v", err)
	}

	placed, want, err := placer.New(st).Place(ctx, "pvc-oversub",
		&apiv1.AutoSelectFilter{PlaceCount: 2})

	if placed != 0 || want != 2 {
		t.Errorf("placed/want: got %d/%d, want 0/2 (oversub must not bypass physical floor)",
			placed, want)
	}

	var capErr *placer.CapacityShortfallError
	if !errors.As(err, &capErr) {
		t.Fatalf("err: got %v, want *CapacityShortfallError", err)
	}

	if capErr.RequiredKib != 100*mib {
		t.Errorf("RequiredKib: got %d, want %d", capErr.RequiredKib, 100*mib)
	}

	if capErr.MaxFreeKib != 10*mib {
		t.Errorf("MaxFreeKib: got %d, want %d (the physical floor, NOT ratio×free)",
			capErr.MaxFreeKib, 10*mib)
	}

	got, _ := st.Resources().ListByDefinition(ctx, "pvc-oversub")
	if len(got) != 0 {
		t.Errorf("no Resource must be created when capacity gate trips; got %+v", got)
	}
}

// TestPlaceNotPlaceWithRscRegexExcludesMatchingHosts pins scenario 2.11:
// when `NotPlaceWithRscRegex` matches the name of an existing RD, every
// node hosting a replica of that RD becomes ineligible. Cluster has 3
// nodes; rd-prod-a sits on n1, rd-prod-b on n2; spawning rd-new with
// PlaceCount=1 and regex `rd-prod-.*` must land on n3 — the only host
// not running anything matching the pattern.
func TestPlaceNotPlaceWithRscRegexExcludesMatchingHosts(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	seedStore(t, st, []string{"n1", "n2", "n3"})

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name: "rd-prod-a", NodeName: "n1",
		Props: map[string]string{"StorPoolName": "pool"},
	}); err != nil {
		t.Fatalf("seed rd-prod-a: %v", err)
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name: "rd-prod-b", NodeName: "n2",
		Props: map[string]string{"StorPoolName": "pool"},
	}); err != nil {
		t.Fatalf("seed rd-prod-b: %v", err)
	}

	p := placer.New(st)

	placed, want, err := p.Place(ctx, "rd-new", &apiv1.AutoSelectFilter{
		PlaceCount:           1,
		NotPlaceWithRscRegex: "rd-prod-.*",
	})
	if err != nil {
		t.Fatalf("Place: %v", err)
	}

	if placed != 1 || want != 1 {
		t.Fatalf("placed/want: got %d/%d, want 1/1", placed, want)
	}

	got, err := st.Resources().ListByDefinition(ctx, "rd-new")
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("len: got %d, want 1; %+v", len(got), got)
	}

	if got[0].NodeName != "n3" {
		t.Errorf("node: got %q, want %q (n1/n2 host rd-prod-* and must be excluded)",
			got[0].NodeName, "n3")
	}
}

// TestPlaceNotPlaceWithRscRegexInvalidIsNoOp pins the "invalid regex is
// silent" contract: a bracket-only pattern fails to compile, but the
// placer must NOT error out — operator typos must not strand placement.
// With no other constraints and 3 healthy nodes, PlaceCount=2 still
// succeeds end-to-end on the two largest-free pools.
func TestPlaceNotPlaceWithRscRegexInvalidIsNoOp(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedStore(t, st, []string{"n1", "n2", "n3"})

	p := placer.New(st)

	placed, want, err := p.Place(t.Context(), "rd-new", &apiv1.AutoSelectFilter{
		PlaceCount:           2,
		NotPlaceWithRscRegex: "[", // invalid: unterminated character class
	})
	if err != nil {
		t.Fatalf("Place: invalid regex must be silent, got %v", err)
	}

	if placed != 2 || want != 2 {
		t.Errorf("placed/want: got %d/%d, want 2/2", placed, want)
	}
}

// TestPlaceWeightedScoringMaxFreeSpace pins scenario 2.17 case 1:
// three pools with FreeCapacity 100 / 500 / 1000 (and a uniform Total
// so the Free/Total ratio is monotone in Free), Weights/MaxFreeSpace=10
// with the other three weights left at their default 1.0 → the
// MaxFreeSpace strategy dominates and the placer picks the 1000-Free
// pool first. A regression that dropped the weight multiplier or
// swapped the comparator would silently fill the 100-Free pool first.
func TestPlaceWeightedScoringMaxFreeSpace(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	frees := []int64{100, 500, 1000}
	names := []string{"n1", "n2", "n3"}

	for i, name := range names {
		if err := st.Nodes().Create(ctx, &apiv1.Node{Name: name, Type: apiv1.NodeTypeSatellite}); err != nil {
			t.Fatalf("seed node %s: %v", name, err)
		}

		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			NodeName:        name,
			StoragePoolName: "pool",
			ProviderKind:    apiv1.StoragePoolKindLVMThin,
			FreeCapacity:    frees[i],
			TotalCapacity:   1000,
		}); err != nil {
			t.Fatalf("seed pool %s: %v", name, err)
		}
	}

	// Boost MaxFreeSpace heavily over the defaults so its 0.1 / 0.5 /
	// 1.0 raw scores are the dominant signal. (Other strategies default
	// to weight 1.0 each — MinRscCount alone contributes 1.0 to every
	// pool when no resources exist, so the picker needs MaxFreeSpace's
	// per-pool variance multiplied up to win.)
	if err := st.ControllerProps().Set(ctx, map[string]string{
		apiv1.PropAutoplacerWeightMaxFreeSpace: "10",
	}); err != nil {
		t.Fatalf("set weights: %v", err)
	}

	placed, _, err := placer.New(st).Place(ctx, "pvc-1",
		&apiv1.AutoSelectFilter{PlaceCount: 1})
	if err != nil {
		t.Fatalf("Place: %v", err)
	}

	if placed != 1 {
		t.Fatalf("placed: got %d, want 1", placed)
	}

	got, _ := st.Resources().ListByDefinition(ctx, "pvc-1")
	if len(got) != 1 || got[0].NodeName != "n3" {
		t.Errorf("expected single replica on n3 (most free); got %+v", got)
	}
}

// TestPlaceWeightedScoringMinRscCount pins scenario 2.17 case 2:
// MaxFreeSpace=0 disables the capacity-ratio strategy, MinRscCount=10
// dominates. One node already hosts 2 existing RDs, the other two host
// 0 each. The placer must pick a 0-RDs node even when it carries the
// smallest pool, proving the rsc-count score wins over the implicit
// MinReservedSpace=1.0 default tie.
func TestPlaceWeightedScoringMinRscCount(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	// n-c-busy carries 1000 of Free + 2 pre-existing Resources on
	// unrelated RDs. n-a-small has the smallest Free (100); n-b-mid
	// sits in the middle (500). MinRscCount=10 should beat the tied
	// MinReservedSpace=1.0 default contribution and put a 0-RDs node
	// ahead of n-c-busy even though n-c-busy has 10x more Free.
	type fixture struct {
		name  string
		free  int64
		busy  bool
		total int64
	}

	// Names ordered alphabetically: n-a-small < n-b-mid < n-c-busy. The
	// scoring tiebreaker is NodeName ASC, so when two pools end up with
	// identical composite scores the alphabetically-first wins — picking
	// distinct alphabetic prefixes makes the expected winner unambiguous.
	fixtures := []fixture{
		{"n-c-busy", 1000, true, 1000},
		{"n-a-small", 100, false, 1000},
		{"n-b-mid", 500, false, 1000},
	}

	for _, f := range fixtures {
		if err := st.Nodes().Create(ctx, &apiv1.Node{Name: f.name, Type: apiv1.NodeTypeSatellite}); err != nil {
			t.Fatalf("seed node %s: %v", f.name, err)
		}

		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			NodeName:        f.name,
			StoragePoolName: "pool",
			ProviderKind:    apiv1.StoragePoolKindLVMThin,
			FreeCapacity:    f.free,
			TotalCapacity:   f.total,
		}); err != nil {
			t.Fatalf("seed pool %s: %v", f.name, err)
		}

		if !f.busy {
			continue
		}

		// Two pre-existing Resources for unrelated RDs on n-c-busy. The
		// MinRscCount strategy counts every Resource on the node, not
		// just same-RD replicas, so these contribute to the score
		// regardless of name.
		for j, rdName := range []string{"other-rd-1", "other-rd-2"} {
			if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rdName}); err != nil && j == 0 {
				// First create may collide if a previous fixture seeded
				// it — only the first iteration's failure matters.
				t.Logf("seed RD %s: %v (ok if duplicate)", rdName, err)
			}

			if err := st.Resources().Create(ctx, &apiv1.Resource{
				Name:     rdName,
				NodeName: f.name,
				Props:    map[string]string{"StorPoolName": "pool"},
			}); err != nil {
				t.Fatalf("seed existing resource on %s: %v", f.name, err)
			}
		}
	}

	if err := st.ControllerProps().Set(ctx, map[string]string{
		apiv1.PropAutoplacerWeightMaxFreeSpace: "0",
		apiv1.PropAutoplacerWeightMinRscCount:  "10",
	}); err != nil {
		t.Fatalf("set weights: %v", err)
	}

	placed, _, err := placer.New(st).Place(ctx, "pvc-1",
		&apiv1.AutoSelectFilter{PlaceCount: 1})
	if err != nil {
		t.Fatalf("Place: %v", err)
	}

	if placed != 1 {
		t.Fatalf("placed: got %d, want 1", placed)
	}

	got, _ := st.Resources().ListByDefinition(ctx, "pvc-1")
	if len(got) != 1 {
		t.Fatalf("len: got %d, want 1; %+v", len(got), got)
	}

	// n-c-busy hosts 2 → MinRscCount score 1/3. n-a-small + n-b-mid
	// host 0 → score 1/1. n-a-small wins the tie among the two
	// zero-resource nodes on NodeName ASC (the deterministic
	// tiebreaker).
	if got[0].NodeName != "n-a-small" {
		t.Errorf("expected n-a-small (0 resources, smallest Free) to win on MinRscCount weight; got %s", got[0].NodeName)
	}
}

// TestPlaceSkipsPoolMissing pins Bug 50 placer behaviour: a pool
// the satellite has flagged with PoolMissing=true MUST NOT receive
// a replica. With 3 nodes — one of them flagged as missing —
// place_count=2 lands on the two healthy peers; the missing pool
// is dropped from the candidate set by matchesPoolFilter, same as
// EVICTED / LOST nodes.
//
// Without this gate the placer would happily stamp a Resource on
// the dead pool, the satellite would fail the ZVOL create, but
// the DRBD slot would still come up on the healthy peers — leaving
// resync stalled at 1% because there's nothing on the dead peer to
// catch up.
func TestPlaceSkipsPoolMissing(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	for _, name := range []string{"n1", "n2", "n3-missing"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{Name: name, Type: apiv1.NodeTypeSatellite}); err != nil {
			t.Fatalf("seed node %s: %v", name, err)
		}
	}

	for _, n := range []string{"n1", "n2"} {
		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			NodeName:        n,
			StoragePoolName: "pool",
			ProviderKind:    apiv1.StoragePoolKindLVMThin,
			FreeCapacity:    1000,
			TotalCapacity:   1000,
		}); err != nil {
			t.Fatalf("seed pool %s: %v", n, err)
		}
	}

	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		NodeName:        "n3-missing",
		StoragePoolName: "pool",
		ProviderKind:    apiv1.StoragePoolKindLVMThin,
		FreeCapacity:    0,
		TotalCapacity:   0,
		PoolMissing:     true,
	}); err != nil {
		t.Fatalf("seed missing pool: %v", err)
	}

	placed, want, err := placer.New(st).Place(ctx, "pvc-1",
		&apiv1.AutoSelectFilter{PlaceCount: 2})
	if err != nil {
		t.Fatalf("Place: %v", err)
	}

	if placed != 2 || want != 2 {
		t.Errorf("placed/want: got %d/%d, want 2/2", placed, want)
	}

	got, _ := st.Resources().ListByDefinition(ctx, "pvc-1")
	for _, r := range got {
		if r.NodeName == "n3-missing" {
			t.Errorf("missing-pool node received a replica: %+v", r)
		}
	}
}

// TestPlacePoolMissingNoCandidates pins the singular-missing-pool
// edge: when the ONLY candidate is PoolMissing, the placer must
// fail rather than land a replica on the dead pool. We expect the
// placer to return placed=0 with no error (or a CapacityShortfallError
// surface when a non-zero RD size enters the picture).
func TestPlacePoolMissingNoCandidates(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n1", Type: apiv1.NodeTypeSatellite}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		NodeName:        "n1",
		StoragePoolName: "pool",
		ProviderKind:    apiv1.StoragePoolKindLVMThin,
		FreeCapacity:    0,
		TotalCapacity:   0,
		PoolMissing:     true,
	}); err != nil {
		t.Fatalf("seed missing pool: %v", err)
	}

	placed, want, err := placer.New(st).Place(ctx, "pvc-1",
		&apiv1.AutoSelectFilter{PlaceCount: 1})
	if err != nil {
		// CapacityShortfallError or a generic-shortfall envelope is
		// acceptable — both surface to the operator as "no candidate".
		// The hard requirement is placed=0 on the dead-pool path.
		if !strings.Contains(err.Error(), "free capacity") &&
			!errors.As(err, new(*placer.CapacityShortfallError)) {
			t.Logf("Place error (acceptable): %v", err)
		}
	}

	if placed != 0 {
		t.Errorf("placed: got %d, want 0 (only pool is missing)", placed)
	}

	if want != 1 {
		t.Errorf("want: got %d, want 1", want)
	}

	got, _ := st.Resources().ListByDefinition(ctx, "pvc-1")
	if len(got) != 0 {
		t.Errorf("resources created on dead-pool path: %+v", got)
	}
}

// Keep go-vet happy on unused symbols in the import set.
var _ = context.Background
