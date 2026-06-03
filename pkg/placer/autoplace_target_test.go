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

// seedNodeWithPool registers one satellite node + one LVM_THIN pool with
// the given free capacity, plus optional props on the node. Mirrors the
// shape of seedStore but lets a single node carry an AutoplaceTarget
// prop so the exclusion gate can be exercised in isolation.
func seedNodeWithPool(t *testing.T, st store.Store, name string, free int64, props map[string]string) {
	t.Helper()

	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{
		Name:  name,
		Type:  apiv1.NodeTypeSatellite,
		Props: props,
	}); err != nil {
		t.Fatalf("seed node %s: %v", name, err)
	}

	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		NodeName:        name,
		StoragePoolName: "pool",
		ProviderKind:    apiv1.StoragePoolKindLVMThin,
		FreeCapacity:    free,
		TotalCapacity:   1000,
	}); err != nil {
		t.Fatalf("seed pool %s: %v", name, err)
	}
}

// TestPlaceSkipsAutoplaceTargetFalseNode pins the F4 contract: a node
// carrying `AutoplaceTarget=false` is never selected for a NEW replica,
// even when it has the most free space (and would otherwise be the
// first pick under the biggest-free-first sort). Two eligible nodes +
// one AutoplaceTarget=false node, place_count=3 → only 2 placed, none
// on the excluded node. Regression here = the maintenance-drain node
// silently receives new placements (kb.linbit.com preventing-placement).
func TestPlaceSkipsAutoplaceTargetFalseNode(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()

	// n-drain has the LARGEST free space (300) so the only reason it
	// isn't picked is the AutoplaceTarget=false gate, not the scorer.
	seedNodeWithPool(t, st, "n1", 100, nil)
	seedNodeWithPool(t, st, "n2", 200, nil)
	seedNodeWithPool(t, st, "n-drain", 300, map[string]string{
		apiv1.PropAutoplaceTarget: "false",
	})

	p := placer.New(st)

	placed, want, err := p.Place(t.Context(), "pvc-1", &apiv1.AutoSelectFilter{PlaceCount: 3})
	if err != nil {
		t.Fatalf("Place: %v", err)
	}

	if placed != 2 || want != 3 {
		t.Errorf("placed/want: got %d/%d, want 2/3 (AutoplaceTarget=false node skipped)", placed, want)
	}

	got, _ := st.Resources().ListByDefinition(t.Context(), "pvc-1")
	for _, r := range got {
		if r.NodeName == "n-drain" {
			t.Errorf("AutoplaceTarget=false node received a replica: %+v", r)
		}
	}
}

// TestPlaceAutoplaceTargetFalseLeavesExistingReplica pins the half of
// the F4 contract that distinguishes it from EVICTED/LOST: an EXISTING
// replica on an AutoplaceTarget=false node still counts toward
// place_count and is NOT migrated. A pre-seeded replica on the
// excluded node + place_count=2 must result in exactly ONE more
// replica on a healthy node — total 2, with the excluded node's
// replica untouched. If the placer treated AutoplaceTarget=false like
// EVICTED it would drop the existing replica from the count and
// gap-fill a third one, draining the node — exactly the behaviour the
// prop is meant to AVOID.
func TestPlaceAutoplaceTargetFalseLeavesExistingReplica(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	seedNodeWithPool(t, st, "n1", 100, nil)
	seedNodeWithPool(t, st, "n2", 200, nil)
	seedNodeWithPool(t, st, "n-drain", 300, map[string]string{
		apiv1.PropAutoplaceTarget: "false",
	})

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name: "pvc-1", NodeName: "n-drain",
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
		t.Errorf("placed: got %d, want 2 (1 existing on drain node counts + 1 added)", placed)
	}

	got, _ := st.Resources().ListByDefinition(ctx, "pvc-1")
	if len(got) != 2 {
		t.Fatalf("total: got %d, want 2 (existing replica preserved, not migrated); %+v", len(got), got)
	}

	var onDrain bool

	for _, r := range got {
		if r.NodeName == "n-drain" {
			onDrain = true
		}
	}

	if !onDrain {
		t.Errorf("existing replica on AutoplaceTarget=false node was migrated away; %+v", got)
	}
}

// TestPlaceAutoplaceTargetTrueAndTypoStayEligible guards the opt-out
// semantics: only a parseable false excludes the node. AutoplaceTarget
// set to an explicit "true" — and a fat-fingered non-bool value —
// both leave the node eligible. Two nodes, one AutoplaceTarget=true and
// one with a typo value, place_count=2 → both get a replica. A
// regression that treated "any AutoplaceTarget prop present" as an
// exclusion would silently drain the cluster off these nodes.
func TestPlaceAutoplaceTargetTrueAndTypoStayEligible(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()

	seedNodeWithPool(t, st, "n-true", 200, map[string]string{
		apiv1.PropAutoplaceTarget: "true",
	})
	seedNodeWithPool(t, st, "n-typo", 100, map[string]string{
		apiv1.PropAutoplaceTarget: "yes-please", // unparseable → not an exclusion
	})

	p := placer.New(st)

	placed, want, err := p.Place(t.Context(), "pvc-1", &apiv1.AutoSelectFilter{PlaceCount: 2})
	if err != nil {
		t.Fatalf("Place: %v", err)
	}

	if placed != 2 || want != 2 {
		t.Errorf("placed/want: got %d/%d, want 2/2 (true + typo both eligible)", placed, want)
	}
}
