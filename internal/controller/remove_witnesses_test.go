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

	controllerpkg "github.com/cozystack/blockstor/internal/controller"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestRemoveWitnessesIdempotent pins the swallow-NotFound branch
// of removeWitnesses (was 80%): when a concurrent reconcile races
// us and deletes a witness Resource between our enumeration and
// our delete, the per-witness Delete returns ErrNotFound — which
// must be silently swallowed so the rest of the witness list still
// gets processed.
//
// Without this defensive swallow, two reconcilers fighting over a
// 2-witness RD could leave one witness alive (the second
// reconciler errored out before reaching it) and the auto-quorum
// invariant would silently break.
func TestRemoveWitnessesIdempotent(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := context.Background()

	// Two witnesses recorded; one of them is missing in the store
	// (concurrent reconciler already deleted it).
	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name: "pvc-1", NodeName: "n1",
		Flags: []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker},
	}); err != nil {
		t.Fatalf("seed witness on n1: %v", err)
	}

	witnesses := []apiv1.Resource{
		{Name: "pvc-1", NodeName: "n1"},
		{Name: "pvc-1", NodeName: "ghost"}, // not in store → ErrNotFound on delete
	}

	rec := &controllerpkg.ResourceDefinitionReconciler{Store: st}

	if err := rec.RemoveWitnesses(ctx, "pvc-1", witnesses); err != nil {
		t.Fatalf("RemoveWitnesses: got %v, want nil (NotFound must be swallowed)", err)
	}

	// n1's witness must be gone after the call.
	if _, err := st.Resources().Get(ctx, "pvc-1", "n1"); err == nil {
		t.Errorf("n1 witness survived removeWitnesses")
	}
}
