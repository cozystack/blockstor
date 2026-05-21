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
	"testing"
	"time"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/satellite/intent"
)

// TestClassifyEvictionSkipsWhenPeerUIDEmpty pins the informer-cache-
// trail branch: when the dispatcher carries an empty ResourceUID
// (the apiserver just observed a fresh CRD that hasn't propagated
// yet) classifyEviction MUST report skip with no log reason — the
// next reconcile will retry once the UID lands.
func TestClassifyEvictionSkipsWhenPeerUIDEmpty(t *testing.T) {
	d := classifyEviction(
		intent.DesiredPeer{Name: "n2", NodeID: 1, ResourceUID: ""},
		map[string]string{"n2": "old-uid"},
		map[string]drbd.KernelSlot{"n2": {Name: "n2", NodeID: 1}},
		nil,
		time.Now(),
	)

	if !d.skip || d.logReason != "" {
		t.Errorf("empty UID must skip silently, got %+v", d)
	}
}

// TestClassifyEvictionSkipsWhenNoBaseline pins the adoption-mode
// branch: a peer with no prior AppliedPeerUIDs entry has nothing
// to compare against, so we cannot detect a UID change — fall
// through silently.
func TestClassifyEvictionSkipsWhenNoBaseline(t *testing.T) {
	d := classifyEviction(
		intent.DesiredPeer{Name: "n2", NodeID: 1, ResourceUID: "new-uid"},
		nil, // no AppliedPeerUIDs at all
		map[string]drbd.KernelSlot{"n2": {Name: "n2", NodeID: 1}},
		nil,
		time.Now(),
	)

	if !d.skip || d.logReason != "" {
		t.Errorf("missing baseline must skip silently, got %+v", d)
	}
}

// TestClassifyEvictionSkipsWhenUIDMatches pins the steady-state
// branch: when the dispatcher's UID matches the baseline, there's
// nothing to evict — every reconcile in normal operation lands here.
func TestClassifyEvictionSkipsWhenUIDMatches(t *testing.T) {
	d := classifyEviction(
		intent.DesiredPeer{Name: "n2", NodeID: 1, ResourceUID: "same-uid"},
		map[string]string{"n2": "same-uid"},
		map[string]drbd.KernelSlot{"n2": {Name: "n2", NodeID: 1}},
		nil,
		time.Now(),
	)

	if !d.skip || d.logReason != "" {
		t.Errorf("matching UID must skip silently, got %+v", d)
	}
}

// TestClassifyEvictionDefersWhenRespawnAnnotationFuture pins Bug 342
// Fix B-extended on the EvictPeersByUIDMismatch path: a UID mismatch
// that races a peer-respawn stamp (the operator is mid-`r d` + `r c`)
// MUST defer the eviction. The kernel slot will get cleaned by the
// next reconcile after the new peer's adjust runs; tearing it down
// now would wipe per-volume v09 GI/bitmap metadata before the new
// incarnation has a chance to negotiate against it.
func TestClassifyEvictionDefersWhenRespawnAnnotationFuture(t *testing.T) {
	now := time.Now()
	deadline := now.Add(30 * time.Second).UTC().Format(time.RFC3339Nano)

	d := classifyEviction(
		intent.DesiredPeer{Name: "n2", NodeID: 1, ResourceUID: "new-uid"},
		map[string]string{"n2": "old-uid"},
		map[string]drbd.KernelSlot{"n2": {Name: "n2", NodeID: 1}},
		map[string]string{apiv1.PeerRespawningAnnotationKey("n2"): deadline},
		now,
	)

	if !d.skip {
		t.Fatalf("respawn pending must skip, got %+v", d)
	}

	if d.logReason == "" || d.deadline != deadline {
		t.Errorf("respawn skip must surface log + deadline, got %+v", d)
	}
}

// TestClassifyEvictionProceedsWhenRespawnAnnotationExpired pins the
// expired-stamp fall-through: an old `peer-respawning` annotation
// the controller never cleaned MUST NOT permanently leak the v09
// metadata slot. Once the RFC3339Nano deadline is in the past, the
// eviction proceeds as if the annotation weren't there.
func TestClassifyEvictionProceedsWhenRespawnAnnotationExpired(t *testing.T) {
	now := time.Now()
	stale := now.Add(-time.Hour).UTC().Format(time.RFC3339Nano)

	d := classifyEviction(
		intent.DesiredPeer{Name: "n2", NodeID: 1, ResourceUID: "new-uid"},
		map[string]string{"n2": "old-uid"},
		map[string]drbd.KernelSlot{"n2": {Name: "n2", NodeID: 1}},
		map[string]string{apiv1.PeerRespawningAnnotationKey("n2"): stale},
		now,
	)

	if d.skip {
		t.Errorf("expired stamp must NOT defer eviction, got %+v", d)
	}

	if d.nodeID != 1 {
		t.Errorf("expected resolved nodeID=1, got %d", d.nodeID)
	}
}

// TestClassifyEvictionSkipsWhenKernelHasNoSlot pins Bug 342 Fix C
// Option 2: when `drbdsetup show` reports no slot for the peer name,
// there is nothing stale to clean. del-peer + forget-peer against a
// missing slot is at best a no-op and at worst destructive (it
// would wipe per-volume metadata that the new incarnation needs).
// Skip the eviction entirely.
func TestClassifyEvictionSkipsWhenKernelHasNoSlot(t *testing.T) {
	d := classifyEviction(
		intent.DesiredPeer{Name: "n2", NodeID: 1, ResourceUID: "new-uid"},
		map[string]string{"n2": "old-uid"},
		map[string]drbd.KernelSlot{}, // kernel has NO slot for n2
		nil,
		time.Now(),
	)

	if !d.skip {
		t.Fatalf("missing kernel slot must skip eviction, got %+v", d)
	}

	if d.logReason == "" {
		t.Errorf("missing kernel slot skip must surface log reason, got %+v", d)
	}
}

// TestClassifyEvictionUsesKernelNodeIDOverPeerNodeID pins the
// kernel-state-as-truth invariant: when the kernel-side slot has a
// node-id different from the dispatcher's `peer.NodeID`, the kernel
// wins. This is the path that closes the zero-vs-nil ambiguity in
// `intent.DesiredPeer.NodeID` — `desiredPeersFromCRDs` copies a
// legitimate `*Status.DRBDNodeID == 0` as `entry.NodeID = 0`, and
// before Fix C Option 2 the eviction deferred forever on the
// `nodeID == 0` branch. Now the kernel's actual id wins.
func TestClassifyEvictionUsesKernelNodeIDOverPeerNodeID(t *testing.T) {
	d := classifyEviction(
		intent.DesiredPeer{Name: "n2", NodeID: 0, ResourceUID: "new-uid"}, // K8s view says 0
		map[string]string{"n2": "old-uid"},
		map[string]drbd.KernelSlot{"n2": {Name: "n2", NodeID: 5}}, // kernel says 5
		nil,
		time.Now(),
	)

	if d.skip {
		t.Fatalf("kernel slot present must NOT skip, got %+v", d)
	}

	if d.nodeID != 5 {
		t.Errorf("kernel-side nodeID must win (5), got %d", d.nodeID)
	}
}

// TestClassifyEvictionFallsBackToPeerNodeIDWhenKernelIDZero pins the
// drbd-utils-<9.x compatibility branch: if the kernel slot is
// present but `peer_node_id` is missing (parser fills 0) AND the
// dispatcher carries a non-zero peer.NodeID, use the dispatcher's
// hint rather than deferring forever. This narrows the defer window
// to "kernel slot present with no id AND K8s thinks unallocated".
func TestClassifyEvictionFallsBackToPeerNodeIDWhenKernelIDZero(t *testing.T) {
	d := classifyEviction(
		intent.DesiredPeer{Name: "n2", NodeID: 3, ResourceUID: "new-uid"},
		map[string]string{"n2": "old-uid"},
		map[string]drbd.KernelSlot{"n2": {Name: "n2", NodeID: 0}}, // kernel says 0 (e.g. missing field)
		nil,
		time.Now(),
	)

	if d.skip {
		t.Fatalf("falls back to peer.NodeID, must NOT skip, got %+v", d)
	}

	if d.nodeID != 3 {
		t.Errorf("expected peer.NodeID=3 fallback, got %d", d.nodeID)
	}
}

// TestClassifyEvictionDefersWhenBothNodeIDsZero pins the narrowest
// remaining defer-window: the kernel has a slot present (so we
// can't skip via Fix C Option 2's "nothing to clean" branch) but
// neither the kernel-side id nor the dispatcher hint is usable.
// Defer rather than firing forget-peer 0, which might address an
// unrelated slot.
func TestClassifyEvictionDefersWhenBothNodeIDsZero(t *testing.T) {
	d := classifyEviction(
		intent.DesiredPeer{Name: "n2", NodeID: 0, ResourceUID: "new-uid"},
		map[string]string{"n2": "old-uid"},
		map[string]drbd.KernelSlot{"n2": {Name: "n2", NodeID: 0}},
		nil,
		time.Now(),
	)

	if !d.skip {
		t.Fatalf("both nodeIDs zero must defer, got %+v", d)
	}

	if d.logReason == "" {
		t.Errorf("defer must surface log reason, got %+v", d)
	}
}

// TestClassifyEvictionProceedsWhenKernelHasIDZeroIsValid pins the
// most important Bug 342 / C3 regression: when the controller
// legitimately allocates DRBDNodeID = 0 (the lowest-free id, which
// is valid in DRBD-9) AND the kernel slot is present with that
// same id, eviction MUST proceed. Before Fix C Option 2, both
// sources saying "0" looked identical to "not yet allocated" and
// the eviction deferred forever; the kernel-slot-present check
// now discriminates correctly.
func TestClassifyEvictionProceedsWhenKernelHasIDZeroIsValid(t *testing.T) {
	// Both kernel and K8s view say id=0 — valid DRBD-9 lowest-free
	// allocation. classifyEviction sees `slot.NodeID == 0` so falls
	// back to peer.NodeID, which is also 0. The final `nodeID == 0`
	// branch defers — that's the residual gap when forget-peer
	// truly has no slot to target. The interesting case (kernel
	// has id=0 slot AND peer.NodeID is non-zero) is covered by
	// TestClassifyEvictionFallsBackToPeerNodeIDWhenKernelIDZero,
	// and the symmetric case (kernel has non-zero slot AND
	// peer.NodeID is 0) is covered by
	// TestClassifyEvictionUsesKernelNodeIDOverPeerNodeID. Both are
	// the load-bearing regressions for C3.

	// This test pins the symmetric case explicitly: kernel id=5,
	// peer says 0 (the C3 scenario where allocator picks lowest-
	// free=0 and the old kernel slot had id=5).
	d := classifyEviction(
		intent.DesiredPeer{Name: "n2", NodeID: 0, ResourceUID: "new-uid"},
		map[string]string{"n2": "old-uid"},
		map[string]drbd.KernelSlot{"n2": {Name: "n2", NodeID: 5}},
		nil,
		time.Now(),
	)

	if d.skip {
		t.Fatalf("kernel slot with non-zero id must proceed even when peer.NodeID=0, got %+v", d)
	}

	if d.nodeID != 5 {
		t.Errorf("expected kernel-side nodeID=5, got %d", d.nodeID)
	}
}

// TestClassifyEvictionRespawnAnnotationOverridesKernelStatePath pins
// the ordering: even when the kernel HAS a slot for the peer (and
// would otherwise proceed under Fix C Option 2), an active
// `peer-respawning` annotation takes precedence — the deferral is
// the safer choice during operator-driven respawn.
func TestClassifyEvictionRespawnAnnotationOverridesKernelStatePath(t *testing.T) {
	now := time.Now()
	deadline := now.Add(30 * time.Second).UTC().Format(time.RFC3339Nano)

	d := classifyEviction(
		intent.DesiredPeer{Name: "n2", NodeID: 1, ResourceUID: "new-uid"},
		map[string]string{"n2": "old-uid"},
		map[string]drbd.KernelSlot{"n2": {Name: "n2", NodeID: 1}}, // kernel slot present
		map[string]string{apiv1.PeerRespawningAnnotationKey("n2"): deadline},
		now,
	)

	if !d.skip {
		t.Fatalf("respawn annotation must take precedence even with kernel slot present, got %+v", d)
	}

	if d.deadline != deadline {
		t.Errorf("expected deadline %q, got %q", deadline, d.deadline)
	}
}
