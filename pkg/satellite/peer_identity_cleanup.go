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
	"context"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/log"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/satellite/intent"
)

// EvictPeersByUIDMismatch is the Bug 342 v4 deterministic teardown
// hook. It compares each desired peer's current Resource metadata.uid
// against the satellite's last-stamped Resource.Status.AppliedPeerUIDs
// and force-evicts any peer whose UID changed since the previous
// successful adjust.
//
// Why this exists: a rapid `r d <peer>` + `r c <peer>` sequence (Phase
// 3 of the r-full-lifecycle catcher) coalesces into a single satellite
// reconcile pass. The dispatcher rebuilds the .res with the same peer
// name set, so .res-file-driven diffs (tearDownRemovedPeers) see no
// removed peers. The kernel slot for the old <peer> incarnation, with
// its stale GI epoch + PSK + endpoint mapping, survives the new
// `drbdadm adjust` — the new <peer> brings up fresh DRBD with a new
// GI epoch, handshake fails (incompatible epochs), the slot wedges
// in Connecting forever (the `disk='' rep='Off'` symptom).
//
// State-based detection (v3 PruneStaleKernelSlots Pass 3) can in
// principle clean this up after the 30s zombie grace, but it depends
// on the satellite getting a reconcile during the wedge window, which
// for sub-second r d / r c churn never reliably happens — observer
// triggers on transient state changes that the controller-runtime
// workqueue dedupes away.
//
// Identity-based detection has none of those timing dependencies:
// dispatcher already knows the current peer UID (it watches the
// apiserver), the satellite's last-applied UID is persisted in its
// own Status, comparison is exact. Cleanup runs synchronously on
// every reconcile, regardless of what state the kernel slot is in,
// regardless of whether the allocation gate would otherwise hold.
//
// Critical placement: this MUST run BEFORE the satellite's
// `waitForControllerAllocation` gate (Bug 87 path). A relocated
// peer's Status.DRBDNodeID is nil for a brief window, the gate
// short-circuits Apply.Apply, and reconcilePeers / tearDownRemovedPeers
// never see the mismatch. Identity-eviction doesn't need peer
// DRBDNodeID allocation; it operates on metadata.uid + the kernel's
// already-known node-id from `drbdsetup show`.
//
// Bug 342 attempt #16 — TWO guards layer on top of the original v4
// UID-mismatch trigger:
//
//   - Fix B-extended: when `rdAnnotations` carries a
//     `apiv1.PeerRespawningAnnotationKey(<peer>)` whose deadline is
//     still in the future, defer the eviction. The operator is mid-
//     `r d` + `r c` and tearing down the slot now would wipe per-
//     volume v09 GI/bitmap metadata before the new incarnation
//     hands off. The same gate already protects tearDownRemovedPeers;
//     this is the second forget-peer caller attempt #15 missed.
//
//   - Fix C Option 2: kernel-state-as-truth. When `drbdsetup show`
//     reports NO slot for the peer name, there is nothing stale to
//     clean — skip the eviction. When the slot IS present, use the
//     slot's NodeID as the authoritative source rather than the
//     dispatcher-carried `peer.NodeID`; this eliminates the zero-vs-
//     nil ambiguity in `intent.DesiredPeer.NodeID` (where a valid
//     id=0 allocation looked identical to "not yet allocated", and
//     the prior implementation deferred the eviction forever).
//
// Returns a `cleaned` map of {peerName: newResourceUID} pairs that
// the controller layer MUST then patch into
// res.Status.AppliedPeerUIDs so subsequent reconciles don't re-evict
// the same peer in a loop.
//
// Best-effort design: del-peer errors bubble (a live kernel slot for
// a known-stale peer is a faster correctness issue than a slow
// metadata leak); forget-peer errors stay inside teardownKernelSlot
// and log without bubbling.
func (r *Reconciler) EvictPeersByUIDMismatch(
	ctx context.Context,
	rdName string,
	desiredPeers []intent.DesiredPeer,
	appliedPeerUIDs map[string]string,
	vols []int32,
	devices map[int32]string,
	rdAnnotations map[string]string,
) (map[string]string, error) {
	if r.cfg.Adm == nil {
		return nil, nil
	}

	logger := log.FromContext(ctx).WithValues("rd", rdName, "v4uidevict", true)

	// Read kernel state once. Bug 342 Fix C Option 2: the slot map is
	// the authoritative view of which peer slots the kernel currently
	// holds. classifyEviction skips peers with no slot (nothing stale
	// to clean) and prefers the slot's node-id over the dispatcher's
	// `peer.NodeID` to discriminate the valid id=0 case from the
	// "not yet allocated" case.
	slots, showErr := r.cfg.Adm.Show(ctx, rdName)
	if showErr != nil {
		logger.Info("EvictPeersByUIDMismatch: drbdsetup show failed; falling back to peer.NodeID",
			"err", showErr.Error())

		slots = nil
	}

	now := time.Now()

	var cleaned map[string]string

	for _, peer := range desiredPeers {
		decision := classifyEviction(peer, appliedPeerUIDs, slots, rdAnnotations, now)
		if decision.skip {
			if decision.logReason != "" {
				logger.Info(decision.logReason,
					"peer", peer.Name,
					"oldUID", decision.oldUID,
					"newUID", peer.ResourceUID,
					"deadline", decision.deadline)
			}

			continue
		}

		logger.Info("UID mismatch — evicting kernel slot for re-incarnated peer",
			"peer", peer.Name,
			"oldUID", decision.oldUID,
			"newUID", peer.ResourceUID,
			"nodeID", decision.nodeID)

		if err := r.cfg.Adm.DelPeer(ctx, rdName, peer.Name); err != nil {
			return cleaned, errors.Wrapf(err, "drbdadm del-peer %s:%s", peer.Name, rdName)
		}

		r.forgetPeerSlots(ctx, rdName, peer.Name, decision.nodeID, devices, logger)

		if cleaned == nil {
			cleaned = make(map[string]string)
		}

		cleaned[peer.Name] = peer.ResourceUID
	}

	return cleaned, nil
}

// evictionDecision is the verdict of classifyEviction for one peer:
// either skip (with a log reason) or proceed (with a resolved nodeID).
// Decoupled so the EvictPeersByUIDMismatch hot loop stays inside the
// cognitive-complexity budget the linter enforces on satellite hot
// paths.
type evictionDecision struct {
	skip      bool
	logReason string
	oldUID    string
	deadline  string
	nodeID    int32
}

// classifyEviction returns whether a peer should be skipped this
// reconcile and the kernel-resolved node-id to use if it should be
// evicted. Pure function — testable in isolation.
//
// Skip reasons (in order, highest precedence first):
//   - peer's ResourceUID is empty (informer cache trail)
//   - no prior baseline or UID matches the baseline (nothing changed)
//   - peer-respawn annotation deadline is in the future (Fix B-extended)
//   - kernel has no slot for the peer name (Fix C Option 2 — nothing
//     to clean)
//   - slot node-id and K8s peer.NodeID both zero (no resolvable id
//     to feed forget-peer)
func classifyEviction(
	peer intent.DesiredPeer,
	appliedPeerUIDs map[string]string,
	slots map[string]drbd.KernelSlot,
	rdAnnotations map[string]string,
	now time.Time,
) evictionDecision {
	if peer.ResourceUID == "" {
		return evictionDecision{skip: true}
	}

	last, hasLast := appliedPeerUIDs[peer.Name]
	if !hasLast || last == peer.ResourceUID {
		return evictionDecision{skip: true}
	}

	// Bug 342 Fix B-extended: REST stamps
	// `apiv1.PeerRespawningAnnotationKey(<peer>)` on the parent RD
	// whenever `r d <peer> <rd>` lands. While the RFC3339Nano deadline
	// is in the future, the operator's intent is "this peer is coming
	// back on the same node sub-second from now" — running del-peer +
	// forget-peer right now would wipe the per-volume v09 GI/bitmap
	// metadata BEFORE the new incarnation hands off, identical to
	// the C2 root cause that tearDownRemovedPeers' fix targets. Defer
	// eviction; the next reconcile (after the deadline OR after the
	// new peer's Status.DRBDNodeID lands) will retry.
	// tearDownRemovedPeers already honours the same annotation; we
	// mirror the gate here because the UID-mismatch path is a SECOND
	// forget-peer caller attempt #15 missed.
	if peerRespawnPending(rdAnnotations, peer.Name, now) {
		return evictionDecision{
			skip:      true,
			logReason: "UID mismatch but peer-respawn annotation present — deferring eviction",
			oldUID:    last,
			deadline:  rdAnnotations[apiv1.PeerRespawningAnnotationKey(peer.Name)],
		}
	}

	// Bug 342 Fix C Option 2: if the kernel has no slot for this
	// peer name, there is nothing stale to clean. del-peer +
	// forget-peer against a missing slot is at best a no-op and at
	// worst wipes per-volume metadata the new incarnation will need.
	slot, slotPresent := slots[peer.Name]
	if !slotPresent {
		return evictionDecision{
			skip:      true,
			logReason: "UID mismatch but kernel has no slot for peer — nothing stale to clean",
			oldUID:    last,
		}
	}

	// Kernel slot is the authoritative source for the node-id
	// (Fix C Option 2): it discriminates the valid id=0 case from
	// the unallocated case, eliminating the zero-vs-nil ambiguity in
	// peer.NodeID that previously deferred eviction forever when the
	// controller allocated DRBDNodeID=0. Fall back to peer.NodeID
	// only when the slot has no usable id.
	nodeID := slot.NodeID
	if nodeID == 0 && peer.NodeID != 0 {
		nodeID = peer.NodeID
	}

	if nodeID == 0 {
		return evictionDecision{
			skip:      true,
			logReason: "UID mismatch detected but slot node-id unresolved — deferring eviction",
			oldUID:    last,
		}
	}

	return evictionDecision{oldUID: last, nodeID: nodeID}
}

// forgetPeerSlots iterates the per-volume v09 metadata slots for the
// given peer and calls drbdmeta forget-peer for each volume that has a
// backing device. Skips DISKLESS local-replica entries (no metadata
// block to clean) and tolerates per-volume forget-peer errors
// (logged, not bubbled): a stale on-disk slot is a slow leak, while
// a live kernel connection (del-peer's domain) is the faster
// correctness risk and already errored above.
func (r *Reconciler) forgetPeerSlots(
	ctx context.Context,
	rdName string,
	peerName string,
	nodeID int32,
	devices map[int32]string,
	logger logr.Logger,
) {
	if nodeID == 0 {
		return
	}

	for volNum, device := range devices {
		if device == "" {
			continue
		}

		if forgetErr := r.cfg.Adm.ForgetPeer(ctx, rdName, volNum, device, nodeID); forgetErr != nil {
			logger.Info("EvictPeersByUIDMismatch: forget-peer failed (non-fatal)",
				"peer", peerName,
				"vol", volNum,
				"nodeID", nodeID,
				"err", forgetErr.Error())
		}
	}
}
