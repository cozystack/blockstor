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

	"github.com/cockroachdb/errors"
	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/log"

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
// in Connecting forever (the `disk=” rep='Off'` symptom).
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
	_ []int32,
	devices map[int32]string,
) (map[string]string, error) {
	if r.cfg.Adm == nil {
		// No drbdadm wired (unit-test fast path). Caller treats a
		// nil cleaned-map as no-op; surface a sentinel so the
		// signature stays informative without nudging callers
		// (production wiring always sets cfg.Adm).
		return nil, nil //nolint:nilnil // intentional no-op signal for tests
	}

	logger := log.FromContext(ctx).WithValues("rd", rdName, "v4uidevict", true)

	// Read kernel state once so forget-peer can use the kernel-side
	// node-id (Bug 87 lesson: the K8s-side allocator may reissue
	// the same name with a different node-id; the zombie metadata
	// slot is bound to the OLD id which only the kernel knows).
	slots, showErr := r.cfg.Adm.Show(ctx, rdName)
	if showErr != nil {
		logger.Info("EvictPeersByUIDMismatch: drbdsetup show failed; falling back to peer.NodeID",
			"err", showErr.Error())

		slots = nil
	}

	var cleaned map[string]string

	for _, peer := range desiredPeers {
		evicted, err := r.evictOnePeerByUIDMismatch(ctx, logger, rdName, peer,
			appliedPeerUIDs, slots, devices)
		if err != nil {
			return cleaned, err
		}

		if !evicted {
			continue
		}

		if cleaned == nil {
			cleaned = make(map[string]string)
		}

		cleaned[peer.Name] = peer.ResourceUID
	}

	return cleaned, nil
}

// evictOnePeerByUIDMismatch performs the per-peer half of
// EvictPeersByUIDMismatch: classify whether the peer needs eviction,
// resolve the kernel-side node-id (preferring the kernel's
// observation), and run del-peer + per-volume forget-peer when so.
//
// Returns (true, nil) when this peer was evicted and the caller
// should record it in `cleaned`; (false, nil) when the peer was
// skipped (no UID known yet / no prior baseline / baseline matches
// / node-id unresolved); (false, err) on a fatal del-peer error.
// Pulled out of EvictPeersByUIDMismatch so the orchestrator stays
// under the gocyclo budget; the cascade is byte-identical.
func (r *Reconciler) evictOnePeerByUIDMismatch(
	ctx context.Context,
	logger logr.Logger,
	rdName string,
	peer intent.DesiredPeer,
	appliedPeerUIDs map[string]string,
	slots map[string]drbd.KernelSlot,
	devices map[int32]string,
) (bool, error) {
	if peer.ResourceUID == "" {
		// Peer's UID not yet known to the dispatcher (informer
		// cache trail / fresh CRD just hit apiserver). Skip —
		// next reconcile will retry with a populated UID.
		return false, nil
	}

	last, hasLast := appliedPeerUIDs[peer.Name]
	if !hasLast || last == peer.ResourceUID {
		// No prior baseline (rollout window / first apply) OR
		// baseline matches — nothing to evict.
		return false, nil
	}

	// UID mismatch — force-evict the kernel slot for this peer.
	// forget-peer needs a node-id; resolvePeerNodeIDForEviction
	// applies the Bug 342 C3 resolution order (kernel slot wins, else
	// K8s-allocated id incl. a valid 0, else unresolved -> defer).
	nodeID, resolved := resolvePeerNodeIDForEviction(peer, slots)
	if !resolved {
		logger.Info("UID mismatch detected but peer node-id unresolved — deferring eviction",
			"peer", peer.Name,
			"oldUID", last,
			"newUID", peer.ResourceUID)

		return false, nil
	}

	logger.Info("UID mismatch — evicting kernel slot for re-incarnated peer",
		"peer", peer.Name,
		"oldUID", last,
		"newUID", peer.ResourceUID,
		"nodeID", nodeID)

	err := r.cfg.Adm.DelPeer(ctx, rdName, peer.Name)
	if err != nil {
		return false, errors.Wrapf(err, "drbdadm del-peer %s:%s", peer.Name, rdName)
	}

	// forget-peer is per-volume because v09 metadata lives in the
	// per-volume block. Skip volumes without a device path
	// (DISKLESS local replica — no metadata to clean).
	for volNum, device := range devices {
		if device == "" {
			continue
		}

		forgetErr := r.cfg.Adm.ForgetPeer(ctx, rdName, volNum, device, nodeID)
		if forgetErr != nil {
			logger.Info("EvictPeersByUIDMismatch: forget-peer failed (non-fatal)",
				"peer", peer.Name,
				"vol", volNum,
				"nodeID", nodeID,
				"err", forgetErr.Error())
		}
	}

	return true, nil
}

// resolvePeerNodeIDForEviction picks the DRBD node-id forget-peer must
// target when evicting a re-incarnated peer (Bug 342 C3). Resolution
// order:
//  1. kernel-observed slot (drbdsetup show) ALWAYS wins when present —
//     the zombie v09 metadata slot is bound to the id the kernel still
//     holds, which may differ from the K8s-side allocation after a
//     relocate. Presence is the map-lookup `ok`; a slot legitimately
//     holding node-id 0 is real and must NOT be skipped (the pre-C3
//     `slot.NodeID != 0` guard wrongly dropped it).
//  2. else the dispatcher's K8s-side id when allocated (peer.NodeID !=
//     nil, including a valid 0).
//  3. else genuinely unresolved (nil pointer AND no kernel slot) ->
//     (0,false): the caller DEFERS. A del-peer without a matching
//     forget-peer drops the connection but leaves stale per-volume
//     GI/bitmap metadata, and the next handshake regresses the LOCAL
//     stable peer to Inconsistent/Outdated.
//
// Why pointer-nil and not `nodeID == 0`: an allocated id 0 is a
// legitimate value (LowestFreeNodeID hands out 0 for the first or a
// freed slot). The pre-C3 `nodeID == 0` test conflated that valid id 0
// with "unresolved" and deferred eviction forever — the exact stall
// behind the re-spawned-worker wedge.
func resolvePeerNodeIDForEviction(peer intent.DesiredPeer, slots map[string]drbd.KernelSlot) (int32, bool) {
	if slot, ok := slots[peer.Name]; ok {
		return slot.NodeID, true
	}

	if peer.NodeID != nil {
		return *peer.NodeID, true
	}

	return 0, false
}
