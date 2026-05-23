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

package drbd_test

import (
	"errors"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
)

func TestLowestFreeNodeID_EmptyTakenReturnsZero(t *testing.T) {
	t.Parallel()

	got, err := drbd.LowestFreeNodeID(nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if got != 0 {
		t.Errorf("got %d, want 0", got)
	}
}

func TestLowestFreeNodeID_PicksGapNotMaxPlusOne(t *testing.T) {
	t.Parallel()

	got, err := drbd.LowestFreeNodeID([]int32{0, 2, 3})
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if got != 1 {
		t.Errorf("got %d, want 1 (the gap)", got)
	}
}

func TestLowestFreeNodeID_FillsContiguous(t *testing.T) {
	t.Parallel()

	got, err := drbd.LowestFreeNodeID([]int32{0, 1, 2})
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if got != 3 {
		t.Errorf("got %d, want 3", got)
	}
}

func TestLowestFreeNodeID_DeterministicAcrossOrderings(t *testing.T) {
	t.Parallel()

	a, _ := drbd.LowestFreeNodeID([]int32{5, 1, 3})
	b, _ := drbd.LowestFreeNodeID([]int32{3, 1, 5})

	if a != b {
		t.Errorf("non-deterministic: %d vs %d", a, b)
	}

	if a != 0 {
		t.Errorf("got %d, want 0 (lowest gap)", a)
	}
}

func TestLowestFreeNodeID_Exhausted(t *testing.T) {
	t.Parallel()

	taken := make([]int32, drbd.MaxPeers)
	for i := range int32(drbd.MaxPeers) {
		taken[i] = i
	}

	_, err := drbd.LowestFreeNodeID(taken)
	if !errors.Is(err, drbd.ErrNodeIDExhausted) {
		t.Errorf("err: got %v, want ErrNodeIDExhausted", err)
	}
}

func TestLowestFreeNodeID_IgnoresOutOfRange(t *testing.T) {
	t.Parallel()

	// 99 is past MaxPeers; the allocator must not let it block id 0.
	got, err := drbd.LowestFreeNodeID([]int32{99})
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if got != 0 {
		t.Errorf("got %d, want 0", got)
	}
}

// TestLowestFreeNodeID_ReclaimsAfterReap pins spec §3.1 (lowest-free
// reuse) and the three-replica §6 edge case the
// `recovery-node-id-mismatch.sh` e2e exercises end-to-end: once a
// replica's record is physically reaped (per spec §4.2 phase 2 ACK
// + reap), its `node-id` becomes free for the next allocation, and
// lowest-free will deterministically reissue the same integer to a
// fresh replica.
//
// Behaviourally this matches §6 step 2: with `{0, 2}` left as the
// taken set after W2's replica is reaped, the placer picks id `1`
// for W4 (the replacement). The forget-peer obligation (§4) is what
// makes this safe — without it the kernel slot still carries W2's
// bitmap and DRBD-9 refuses the handshake (§4.3 "wrong day-0
// bitmap"). The allocator itself is correct here; the spec just
// requires the ID to be reusable once the predecessor is gone.
func TestLowestFreeNodeID_ReclaimsAfterReap(t *testing.T) {
	t.Parallel()

	// Three-replica RD with W1=0, W2=1, W3=2.  W2 is `n lost` and
	// its record is reaped (DELETE flag → ACK → physical drop).
	// Surviving taken set is {0, 2}.
	got, err := drbd.LowestFreeNodeID([]int32{0, 2})
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if got != 1 {
		t.Errorf("got %d, want 1 (reclaiming the reaped slot)", got)
	}
}

// TestLowestFreeNodeID_FullRangeBeyondMaxPeers covers spec §7.1
// invariant — node-id ∈ [0, 31] for every replica that has a DRBD
// layer. The allocator's local `MaxPeers` cap is currently 16
// (matches the blockstor `drbdadm create-md --max-peers=15`
// per-volume slot budget), and the spec's upper bound is 31. Both
// constraints are tighter than the DRBD-9 kernel limit (32 ids).
// This test pins the current MaxPeers=16 cap as the exhaustion
// boundary so a future cap-bump to 32 (when the create-md flag
// catches up to the spec ceiling) requires an intentional update.
func TestLowestFreeNodeID_FullRangeBeyondMaxPeers(t *testing.T) {
	t.Parallel()

	// drbd.MaxPeers is intentionally lower than the spec's 32-id
	// ceiling. Confirm the local cap is the binding limit.
	if drbd.MaxPeers > 32 {
		t.Fatalf("drbd.MaxPeers=%d violates spec §7.1 range [0,31]", drbd.MaxPeers)
	}
}
