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
	"slices"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/placer"
	"github.com/cozystack/blockstor/pkg/store"
)

// seedTwoDiskfulPlusWitness builds the standard place-count-2 steady
// state on a 3-node cluster: a diskful replica on n1 and n2, plus an
// auto-tiebreaker witness (DISKLESS + TIE_BREAKER) on n3. This is the
// exact topology the corner-D2b bug reproduced on.
func seedTwoDiskfulPlusWitness(t *testing.T, st store.Store) {
	t.Helper()

	ctx := t.Context()

	seedStore(t, st, []string{"n1", "n2", "n3"})

	for _, n := range []string{"n1", "n2"} {
		if err := st.Resources().Create(ctx, &apiv1.Resource{
			Name: "pvc-1", NodeName: n,
			Props: map[string]string{"StorPoolName": "pool"},
		}); err != nil {
			t.Fatalf("seed diskful %s: %v", n, err)
		}
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name: "pvc-1", NodeName: "n3",
		Flags: []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker},
	}); err != nil {
		t.Fatalf("seed witness n3: %v", err)
	}
}

// assertWitnessUpgraded asserts that after the placer ran, n3's replica
// is diskful: DISKLESS and TIE_BREAKER are gone and StorPoolName is set.
// It also asserts there are exactly 3 Resources (the witness was
// upgraded IN PLACE, not duplicated by a 4th replica somewhere).
func assertWitnessUpgraded(t *testing.T, st store.Store) {
	t.Helper()

	got, _ := st.Resources().ListByDefinition(t.Context(), "pvc-1")
	if len(got) != 3 {
		t.Fatalf("total resources: got %d, want 3 (witness upgraded in place, not duplicated); %+v", len(got), got)
	}

	var n3 *apiv1.Resource

	for i := range got {
		if got[i].NodeName == "n3" {
			n3 = &got[i]
		}
	}

	if n3 == nil {
		t.Fatalf("n3 replica missing after placement; %+v", got)
	}

	if slices.Contains(n3.Flags, apiv1.ResourceFlagDiskless) {
		t.Errorf("n3 still DISKLESS after upgrade: %+v", n3.Flags)
	}

	if slices.Contains(n3.Flags, apiv1.ResourceFlagTieBreaker) {
		t.Errorf("n3 still TIE_BREAKER after upgrade: %+v", n3.Flags)
	}

	if n3.Props["StorPoolName"] != "pool" {
		t.Errorf("n3 StorPoolName: got %q, want \"pool\" (backing disk stamped)", n3.Props["StorPoolName"])
	}
}

// TestPlaceUpgradesTiebreakerAbsolutePlaceCount is corner-D2b's
// absolute-form pin: the standard 2-diskful + 1-auto-tiebreaker shape,
// then `--auto-place 3` (PlaceCount=3, the value the REST handler hands
// the placer for both `--auto-place 3` and the `+1` shorthand). Before
// the fix the placer treated the witness-holding node as fully "taken"
// and reported a shortfall ("Not enough nodes ... Replica count: 3")
// even though n3 was available to host a diskful replica. The fix marks
// the witness node as an UPGRADE candidate so the placer promotes the
// witness in place.
func TestPlaceUpgradesTiebreakerAbsolutePlaceCount(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedTwoDiskfulPlusWitness(t, st)

	placed, want, err := placer.New(st).Place(t.Context(), "pvc-1", &apiv1.AutoSelectFilter{PlaceCount: 3})
	if err != nil {
		t.Fatalf("Place: %v", err)
	}

	if placed != 3 || want != 3 {
		t.Errorf("placed/want: got %d/%d, want 3/3 (witness upgraded fills the gap)", placed, want)
	}

	assertWitnessUpgraded(t, st)
}

// TestPlaceUpgradesTiebreakerNotDoubleCounted guards the gap-fill
// arithmetic for the `+1` shorthand. The REST handler lowers
// `--auto-place +1` on a 2-diskful RD to PlaceCount=3 (existing diskful
// count + 1). The witness on n3 must NOT count as one of the 2 existing
// diskful (place_count is diskful-only — countDiskfulReplicas already
// skips DISKLESS), so the effective target is exactly 3 and the witness
// fills slot 3. This pins that the promote lands precisely one new
// diskful and never spills to a (non-existent) 4th node.
func TestPlaceUpgradesTiebreakerNotDoubleCounted(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedTwoDiskfulPlusWitness(t, st)

	// PlaceCount=3 == count(existing diskful)=2 + additional 1, the
	// value resolveAdditionalPlaceCount computes for `--auto-place +1`.
	placed, _, err := placer.New(st).Place(t.Context(), "pvc-1", &apiv1.AutoSelectFilter{PlaceCount: 3})
	if err != nil {
		t.Fatalf("Place: %v", err)
	}

	if placed != 3 {
		t.Errorf("placed: got %d, want 3", placed)
	}

	assertWitnessUpgraded(t, st)

	// Idempotency: re-running the same target must not create a 4th
	// replica or flip anything back to diskless.
	placed2, _, err := placer.New(st).Place(t.Context(), "pvc-1", &apiv1.AutoSelectFilter{PlaceCount: 3})
	if err != nil {
		t.Fatalf("Place (re-run): %v", err)
	}

	if placed2 != 3 {
		t.Errorf("placed (re-run): got %d, want 3 (idempotent)", placed2)
	}

	assertWitnessUpgraded(t, st)
}

// TestPlaceDoesNotUpgradeTiebreakerWhenTargetMet guards the no-op edge:
// the same 2-diskful + 1-witness shape but place_count=2 (already
// satisfied). The witness must stay a witness — the placer must not
// opportunistically upgrade it just because it became an upgrade
// candidate. Pins that the upgrade only fires to fill a real gap.
func TestPlaceDoesNotUpgradeTiebreakerWhenTargetMet(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedTwoDiskfulPlusWitness(t, st)

	placed, _, err := placer.New(st).Place(t.Context(), "pvc-1", &apiv1.AutoSelectFilter{PlaceCount: 2})
	if err != nil {
		t.Fatalf("Place: %v", err)
	}

	if placed != 2 {
		t.Errorf("placed: got %d, want 2 (target already met)", placed)
	}

	got, _ := st.Resources().ListByDefinition(t.Context(), "pvc-1")

	var n3 *apiv1.Resource

	for i := range got {
		if got[i].NodeName == "n3" {
			n3 = &got[i]
		}
	}

	if n3 == nil {
		t.Fatalf("n3 witness vanished; %+v", got)
	}

	if !slices.Contains(n3.Flags, apiv1.ResourceFlagTieBreaker) {
		t.Errorf("n3 witness was upgraded despite target being met: %+v", n3.Flags)
	}
}
