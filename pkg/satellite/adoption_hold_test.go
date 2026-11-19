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

package satellite

import (
	"slices"
	"testing"
	"time"
)

// TestAdoptedPeersWithoutResource_HoldsUntilPeersArrive is the render
// half of issue #266: worker-0's Resource arrived with only worker-1's
// beside it, while the kernel still runs the mesh LINSTOR built. The
// first render would drop worker-2 from the .res and adjust would
// disconnect it, so the render is held until worker-2's Resource is
// observed, whatever the spelling of its node name.
func TestAdoptedPeersWithoutResource_HoldsUntilPeersArrive(t *testing.T) {
	r, _, _ := newPruneReconciler(t, adoptedMeshFixture)
	now := time.Now()

	hold := r.AdoptedPeersWithoutResource(t.Context(), "pvc-1", []string{"worker-1"}, now)
	if !hold.HoldsRender() || !slices.Equal(hold.Missing, []string{"worker-2"}) {
		t.Fatalf("one peer observed: hold = %+v, want worker-2 holding the render", hold)
	}

	hold = r.AdoptedPeersWithoutResource(t.Context(), "pvc-1", []string{"worker-1", "Worker-2"}, now)
	if hold.HoldsRender() || len(hold.Missing) != 0 {
		t.Fatalf("every peer observed: hold = %+v, want none", hold)
	}
}

// TestAdoptedPeersWithoutResource_OwnedSlotIsNotHeld: once blockstor
// has rendered the resource here, its own .res is the record of the
// peer set and a missing Resource is a removal the prune and adjust
// carry out, as before.
func TestAdoptedPeersWithoutResource_OwnedSlotIsNotHeld(t *testing.T) {
	r, _, dir := newPruneReconciler(t, adoptedMeshFixture)

	writeOwnedSlot(t, dir, "pvc-1", nil)

	hold := r.AdoptedPeersWithoutResource(t.Context(), "pvc-1", []string{"worker-1"}, time.Now())
	if hold.HoldsRender() || len(hold.Missing) != 0 {
		t.Fatalf("owned slot: hold = %+v, want none", hold)
	}
}

// TestAdoptedPeersWithoutResource_HoldIsBounded: a peer whose Resource
// never comes must not keep the replica unapplied for good. The wait
// counts from the first held reconcile, not from the latest one, and
// starts over once the hold clears.
func TestAdoptedPeersWithoutResource_HoldIsBounded(t *testing.T) {
	r, _, _ := newPruneReconciler(t, adoptedMeshFixture)
	start := time.Now()
	observed := []string{"worker-1"}

	r.AdoptedPeersWithoutResource(t.Context(), "pvc-1", observed, start)

	hold := r.AdoptedPeersWithoutResource(t.Context(), "pvc-1", observed, start.Add(defaultAdoptionPeerWait-time.Second))
	if !hold.HoldsRender() || !hold.Since.Equal(start) {
		t.Fatalf("inside the wait: hold = %+v, want held since %s", hold, start)
	}

	hold = r.AdoptedPeersWithoutResource(t.Context(), "pvc-1", observed, start.Add(defaultAdoptionPeerWait))
	if hold.HoldsRender() || !hold.Expired || !slices.Equal(hold.Missing, []string{"worker-2"}) {
		t.Fatalf("wait elapsed: hold = %+v, want expired with worker-2 missing", hold)
	}

	r.AdoptedPeersWithoutResource(t.Context(), "pvc-1", []string{"worker-1", "worker-2"}, start.Add(defaultAdoptionPeerWait))

	later := start.Add(2 * defaultAdoptionPeerWait)

	hold = r.AdoptedPeersWithoutResource(t.Context(), "pvc-1", observed, later)
	if !hold.HoldsRender() || !hold.Since.Equal(later) {
		t.Fatalf("after the hold cleared: hold = %+v, want a fresh wait from %s", hold, later)
	}
}
