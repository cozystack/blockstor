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

// Corner-case campaign D3 (UG9 linstor-administration.adoc ~1025-1199):
// `--x-replicas-on-different <key> N` caps the number of replicas that
// may share any single value of Aux/<key> at N. Two facets of the
// upstream contract were implemented in the placer (state.exceedsXBucket
// / xBucketKey) but had ZERO regression coverage:
//
//   1. Nodes WITHOUT the aux property are NOT special-cased — they all
//      hash into the empty-value bucket ("<key>=") and therefore count
//      as one shared group, capped just like any concrete value.
//
//   2. `--x-replicas-on-different X 1` is equivalent to the bare
//      `--replicas-on-different X` anti-affinity: a per-value cap of 1
//      means "at most one replica per value", i.e. all-different.
//
// These tests pin both so a future refactor of the bucket logic can't
// silently regress the X-replicas surface.

// xrdNode seeds a satellite node carrying Aux/<key>=<value> (when value
// is non-empty) plus one LVM_THIN pool with the given free capacity.
func xrdNode(t *testing.T, st store.Store, name, key, value string, free int64) {
	t.Helper()

	ctx := t.Context()

	props := map[string]string{}
	if value != "" {
		props["Aux/"+key] = value
	}

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

func xrdValueCounts(t *testing.T, st store.Store, rd, key string) map[string]int {
	t.Helper()

	ctx := t.Context()

	got, err := st.Resources().ListByDefinition(ctx, rd)
	if err != nil {
		t.Fatalf("list resources: %v", err)
	}

	counts := map[string]int{}

	for _, r := range got {
		node, err := st.Nodes().Get(ctx, r.NodeName)
		if err != nil {
			t.Fatalf("get node %s: %v", r.NodeName, err)
		}

		counts[node.Props["Aux/"+key]]++
	}

	return counts
}

// TestXReplicasOnDifferentCapTwoPerValue pins the core bucket semantic:
// with two sites (east/west) each holding two nodes, a place-count of 4
// under `--x-replicas-on-different site 2` must land exactly two replicas
// per site — never three-on-one which a cap of 2 forbids.
func TestXReplicasOnDifferentCapTwoPerValue(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	xrdNode(t, st, "e1", "site", "east", 900)
	xrdNode(t, st, "e2", "site", "east", 800)
	xrdNode(t, st, "w1", "site", "west", 700)
	xrdNode(t, st, "w2", "site", "west", 600)

	p := placer.New(st)

	placed, want, err := p.Place(ctx, "pvc-x2", &apiv1.AutoSelectFilter{
		PlaceCount:              4,
		XReplicasOnDifferentMap: map[string]int32{"site": 2},
	})
	if err != nil {
		t.Fatalf("Place: %v", err)
	}

	if placed != 4 || want != 4 {
		t.Fatalf("placed/want: got %d/%d, want 4/4", placed, want)
	}

	counts := xrdValueCounts(t, st, "pvc-x2", "site")
	if counts["east"] != 2 || counts["west"] != 2 {
		t.Errorf("expected 2 replicas per site, got %+v", counts)
	}
}

// TestXReplicasOnDifferentNoPropNodesShareEmptyBucket pins facet 1:
// nodes that do NOT carry Aux/site all hash into the empty-value bucket
// and are capped together. With cap=1 and three prop-less nodes, only
// ONE replica may land — the other two are rejected because the empty
// bucket is full. This is the "nodes without the aux property count as
// their own group" rule (NOT "each gets its own group").
func TestXReplicasOnDifferentNoPropNodesShareEmptyBucket(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	// None of these carry Aux/site.
	xrdNode(t, st, "n1", "site", "", 900)
	xrdNode(t, st, "n2", "site", "", 800)
	xrdNode(t, st, "n3", "site", "", 700)

	p := placer.New(st)

	placed, want, err := p.Place(ctx, "pvc-empty", &apiv1.AutoSelectFilter{
		PlaceCount:              3,
		XReplicasOnDifferentMap: map[string]int32{"site": 1},
	})
	if err != nil {
		t.Fatalf("Place: %v", err)
	}

	// Want stays 3 (the operator asked for 3) but only 1 can be placed
	// because all candidates collide in the single empty-value bucket.
	if want != 3 {
		t.Errorf("want: got %d, expected 3 (operator target unchanged)", want)
	}

	if placed != 1 {
		t.Errorf("placed: got %d, expected 1 (prop-less nodes share the empty bucket, cap=1)", placed)
	}

	counts := xrdValueCounts(t, st, "pvc-empty", "site")
	if counts[""] != 1 {
		t.Errorf("expected exactly 1 replica in the empty-value bucket, got %+v", counts)
	}
}

// TestXReplicasOnDifferentCapOneEqualsBareReplicasOnDifferent pins the
// D3 equivalence claim: `--x-replicas-on-different X 1` behaves exactly
// like the bare `--replicas-on-different X` anti-affinity. Same topology,
// two filters, identical placement outcome (one replica per distinct
// value, and a prop-less node counts as the empty-value group).
func TestXReplicasOnDifferentCapOneEqualsBareReplicasOnDifferent(t *testing.T) {
	t.Parallel()

	// Topology: two east nodes, one west node, one prop-less node.
	// place_count=3 must spread across {east, west, ""} — exactly one
	// per value — under BOTH filter spellings.
	seed := func() store.Store {
		st := store.NewInMemory()
		xrdNode(t, st, "e1", "site", "east", 900)
		xrdNode(t, st, "e2", "site", "east", 850)
		xrdNode(t, st, "w1", "site", "west", 800)
		xrdNode(t, st, "x1", "site", "", 750)

		return st
	}

	run := func(t *testing.T, st store.Store, filter *apiv1.AutoSelectFilter) map[string]int {
		t.Helper()

		placed, _, err := placer.New(st).Place(t.Context(), "pvc-eq", filter)
		if err != nil {
			t.Fatalf("Place: %v", err)
		}

		if placed != 3 {
			t.Fatalf("placed: got %d, want 3", placed)
		}

		return xrdValueCounts(t, st, "pvc-eq", "site")
	}

	bare := run(t, seed(), &apiv1.AutoSelectFilter{
		PlaceCount:          3,
		ReplicasOnDifferent: []string{"site"},
	})

	xrd := run(t, seed(), &apiv1.AutoSelectFilter{
		PlaceCount:              3,
		XReplicasOnDifferentMap: map[string]int32{"site": 1},
	})

	// Both must place exactly one replica per distinct site value, and
	// the prop-less node (empty value) is its own group in both cases.
	for _, v := range []string{"east", "west", ""} {
		if bare[v] != 1 {
			t.Errorf("bare --replicas-on-different: value %q got %d replicas, want 1 (counts=%+v)", v, bare[v], bare)
		}

		if xrd[v] != 1 {
			t.Errorf("--x-replicas-on-different X 1: value %q got %d replicas, want 1 (counts=%+v)", v, xrd[v], xrd)
		}
	}

	if len(bare) != len(xrd) {
		t.Errorf("placement distribution differs: bare=%+v xrd=%+v", bare, xrd)
	}
}
