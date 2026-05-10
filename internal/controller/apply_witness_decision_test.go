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

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestApplyWitnessDecisionRemoveDropsWitnessesFromDisklessSlice
// pins the !wantWitness && len(witness) > 0 branch (was 78.6%):
// when the cluster shrinks below the auto-witness threshold (e.g.
// the operator added a 3rd diskful replica so the 2-replica
// witness invariant no longer applies), removeWitnesses must:
//
//  1. Delete every TIE_BREAKER replica from the store.
//  2. Return a diskless slice with TIE_BREAKER replicas dropped
//     so the caller's quorum computation sees the post-remove
//     state, not the pre-remove count.
//
// Without (2), the next reconcile would see the witness still in
// the diskless slice and emit a stale "majority" decision when
// the actual peer count says "off" — split-brain risk on a 2-node
// partition.
func TestApplyWitnessDecisionRemoveDropsWitnessesFromDisklessSlice(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := context.Background()

	// Pre-existing TIE_BREAKER witness on n3.
	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name: "pvc-1", NodeName: "n3",
		Flags: []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker},
	}); err != nil {
		t.Fatalf("seed witness: %v", err)
	}

	rec := &controllerpkg.ResourceDefinitionReconciler{Store: st}

	rd := &blockstoriov1alpha1.ResourceDefinition{}
	rd.Name = "pvc-1"

	diskless := []apiv1.Resource{
		// User-added diskless on n4 (NOT a witness).
		{Name: "pvc-1", NodeName: "n4", Flags: []string{apiv1.ResourceFlagDiskless}},
		// Witness on n3.
		{Name: "pvc-1", NodeName: "n3", Flags: []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker}},
	}
	witness := []apiv1.Resource{diskless[1]}

	out, err := rec.ApplyWitnessDecision(ctx, rd, nil, diskless, witness, false)
	if err != nil {
		t.Fatalf("ApplyWitnessDecision: %v", err)
	}

	if len(out) != 1 {
		t.Fatalf("post-remove diskless: got %d, want 1 (witness must be dropped); %+v", len(out), out)
	}

	if out[0].NodeName != "n4" {
		t.Errorf("survivor: got %q, want n4 (user-added diskless must persist)", out[0].NodeName)
	}

	for _, r := range out {
		if slices.Contains(r.Flags, apiv1.ResourceFlagTieBreaker) {
			t.Errorf("TIE_BREAKER flag survived removal: %+v", r)
		}
	}

	// Witness Resource itself must be gone from the store too.
	if _, err := st.Resources().Get(ctx, "pvc-1", "n3"); err == nil {
		t.Errorf("witness Resource n3 survived removeWitnesses")
	}
}
