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

package rest

import (
	"context"
	"testing"
	"time"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestCascadeOrphansForLostNodeFlagsBeforeReap pins spec §4.2 +
// §6 phase 1 — the controller MUST stamp `DELETE` on each doomed
// Resource's `Spec.Flags` AND bump the `PeerChangedAnnotation` on
// every surviving sibling BEFORE physically dropping the doomed
// Resource row from the store.
//
// Why this matters: without the phase-1 flag-and-broadcast,
// surviving satellites lose the "flagged for deletion" signal that
// triggers their per-peer `forget-peer` cleanup. The pre-fix path
// hard-deleted the doomed Resource synchronously; survivors saw a
// peer that simply vanished between two reconciles and never
// reached the §4.1 `if P is NOT flagged DELETE/DRBD_DELETE`
// trigger condition. The kernel slot for the departed peer
// survived with its day-0 bitmap, and a subsequent autoplace
// re-using the same numeric `node-id` wedged in `Connecting`
// because DRBD-9 refused to handshake a fresh peer over a slot
// that still carried the predecessor's bitmap (spec §4.3).
//
// We can't easily observe the FLAG between phase 1 and phase 2 in
// a unit test (the cascade runs synchronously to completion and
// the doomed rows are gone at the end of the call). What we CAN
// observe is the side-effect that proves phase 1 happened: the
// surviving siblings carry a fresh PeerChangedAnnotation
// timestamp stamped by `bumpPeerChangedOnSiblings`. The
// pre-fix path called `bumpPeerChangedOnSiblings` only AFTER the
// physical delete on the regular `r d` route; the `n lost` cascade
// path did NOT call it at all — so a PeerChangedAnnotation on the
// surviving sibling after `cascadeOrphansForLostNode` is positive
// proof that the new two-phase cascade ran.
func TestCascadeOrphansForLostNodeFlagsBeforeReap(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := context.Background()

	// Two RDs, each with two replicas on the doomed node + the
	// surviving peer node. The doomed node has its satellite
	// down (no ConnectionStatus stamp → isNodeOnline returns
	// false, so the ACK wait skips it instantly — keeps this
	// unit test from running into peerDeleteAckTimeout).
	for _, name := range []string{"lost-node", "peer-node"} {
		err := st.Nodes().Create(ctx, &apiv1.Node{Name: name})
		if err != nil {
			t.Fatalf("seed node %s: %v", name, err)
		}
	}

	for _, rd := range []string{"rd-a", "rd-b"} {
		err := st.Resources().Create(ctx, &apiv1.Resource{Name: rd, NodeName: "lost-node"})
		if err != nil {
			t.Fatalf("seed lost replica %s: %v", rd, err)
		}

		err = st.Resources().Create(ctx, &apiv1.Resource{Name: rd, NodeName: "peer-node"})
		if err != nil {
			t.Fatalf("seed peer replica %s: %v", rd, err)
		}
	}

	srv := &Server{Store: st}

	start := time.Now()

	err := srv.cascadeOrphansForLostNode(ctx, "lost-node")
	if err != nil {
		t.Fatalf("cascadeOrphansForLostNode: %v", err)
	}

	// Spec §4.2 phase 1 confirmation: each surviving sibling on
	// peer-node now carries the PeerChangedAnnotation, freshly
	// stamped during this call.
	for _, rd := range []string{"rd-a", "rd-b"} {
		sib, getErr := st.Resources().Get(ctx, rd, "peer-node")
		if getErr != nil {
			t.Fatalf("surviving sibling %s.peer-node: %v", rd, getErr)
		}

		stamp, ok := sib.Annotations[apiv1.PeerChangedAnnotation]
		if !ok {
			t.Errorf("surviving %s.peer-node missing PeerChangedAnnotation — phase 1 broadcast did not fire", rd)

			continue
		}

		// Parse the stamp and require it to be no older than the
		// test's wall-clock start — pinning that this call wrote
		// it, not some leftover from a previous test.
		ts, parseErr := time.Parse(time.RFC3339Nano, stamp)
		if parseErr != nil {
			t.Errorf("PeerChangedAnnotation on %s.peer-node not RFC3339Nano: %q (%v)",
				rd, stamp, parseErr)

			continue
		}

		if ts.Before(start) {
			t.Errorf("PeerChangedAnnotation on %s.peer-node is stale: %v < start=%v",
				rd, ts, start)
		}
	}

	// Spec §6 phase 2 confirmation: the doomed Resources are
	// physically gone after the cascade.
	for _, rd := range []string{"rd-a", "rd-b"} {
		_, getErr := st.Resources().Get(ctx, rd, "lost-node")
		if getErr == nil {
			t.Errorf("doomed %s.lost-node still present after cascade", rd)
		}
	}
}

// TestFlagResourceDeletedStampsDeleteFlag is the unit-level pin on
// `flagResourceDeleted`'s job: stamp the upstream-LINSTOR `DELETE`
// flag onto a Resource's Spec.Flags. Idempotent — repeat calls
// converge on the same flag set. Reads back the post-stamp Resource
// from the store to assert the wire shape directly rather than
// trusting only the patch return value.
func TestFlagResourceDeletedStampsDeleteFlag(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := context.Background()

	err := st.Resources().Create(ctx, &apiv1.Resource{Name: "rd-1", NodeName: "doomed-node"})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	srv := &Server{Store: st}

	err = srv.flagResourceDeleted(ctx, "rd-1", "doomed-node")
	if err != nil {
		t.Fatalf("flagResourceDeleted: %v", err)
	}

	got, getErr := st.Resources().Get(ctx, "rd-1", "doomed-node")
	if getErr != nil {
		t.Fatalf("get: %v", getErr)
	}

	if !containsFlag(got.Flags, rscFlagDelete) {
		t.Fatalf("DELETE flag not stamped: got=%v", got.Flags)
	}

	// Idempotence: stamp twice, flag must not duplicate.
	err = srv.flagResourceDeleted(ctx, "rd-1", "doomed-node")
	if err != nil {
		t.Fatalf("second flagResourceDeleted: %v", err)
	}

	got2, _ := st.Resources().Get(ctx, "rd-1", "doomed-node")

	count := 0

	for _, f := range got2.Flags {
		if f == rscFlagDelete {
			count++
		}
	}

	if count != 1 {
		t.Errorf("DELETE flag duplicated on idempotent stamp: count=%d", count)
	}
}

// containsFlag is the local helper that keeps the test bodies
// compact. slices.Contains would do here too, but keeping the
// helper in-package avoids the extra import for one call site.
func containsFlag(flags []string, want string) bool {
	for _, f := range flags {
		if f == want {
			return true
		}
	}

	return false
}
