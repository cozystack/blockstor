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
	"sigs.k8s.io/controller-runtime/pkg/log"

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
) (map[string]string, error) {
	if r.cfg.Adm == nil {
		return nil, nil
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
		if peer.ResourceUID == "" {
			// Peer's UID not yet known to the dispatcher
			// (informer cache trail / fresh CRD just hit
			// apiserver). Skip — next reconcile will retry
			// with a populated UID.
			continue
		}

		last, hasLast := appliedPeerUIDs[peer.Name]
		if !hasLast || last == peer.ResourceUID {
			// No prior baseline (rollout window / first
			// apply) OR baseline matches — nothing to evict.
			continue
		}

		// UID mismatch — force-evict the kernel slot for this
		// peer. Forget-peer needs a node-id; prefer the kernel's
		// observation, fall back to the dispatcher's.
		//
		// Bug 342 v8 (narrow scope): only the -1 sentinel check
		// below is needed. Do NOT broaden the slot preference to
		// take slot.NodeID==0 — when the satellite's drbdsetup
		// show parse for an inbound peer hasn't fully populated
		// node-id (transient state during fresh-peer handshake),
		// the slot map can report 0 erroneously while the
		// dispatcher's peer.NodeID has the correct value from
		// controller-side allocation. Keep the original
		// `slot.NodeID != 0` filter so we don't overwrite a good
		// dispatcher-side id with a transient kernel zero.
		nodeID := peer.NodeID

		if slot, ok := slots[peer.Name]; ok && slot.NodeID != 0 {
			nodeID = slot.NodeID
		}

		// Bug 342 v5: when neither the kernel-observed slot nor
		// the K8s-allocated peer NodeID is available, DEFER
		// eviction to a future reconcile. A del-peer without
		// matching forget-peer drops the kernel connection but
		// leaves stale per-volume GI/bitmap metadata; the
		// subsequent new-peer handshake (after the relocated
		// peer brings DRBD up with a fresh GI epoch) exposes
		// the mismatch and the LOCAL stable peer regresses its
		// own disk_state to Inconsistent / Outdated. Skipping
		// this reconcile is safe: the next one (after kernel
		// loads OR allocation lands) will see the same UID
		// mismatch and try again with a resolvable node-id.
		//
		// Bug 342 v8: compare against -1 sentinel (set by
		// desiredPeersFromCRDs). Previously the comparison was
		// `nodeID == 0`, which incorrectly treated allocated
		// node-id 0 as "missing" and deferred indefinitely —
		// causing the Phase 3 r-full-lifecycle wedge.
		if nodeID < 0 {
			logger.Info("UID mismatch detected but peer node-id unresolved — deferring eviction",
				"peer", peer.Name,
				"oldUID", last,
				"newUID", peer.ResourceUID)

			continue
		}

		logger.Info("UID mismatch — evicting kernel slot for re-incarnated peer",
			"peer", peer.Name,
			"oldUID", last,
			"newUID", peer.ResourceUID,
			"nodeID", nodeID)

		if err := r.cfg.Adm.DelPeer(ctx, rdName, peer.Name); err != nil {
			return cleaned, errors.Wrapf(err, "drbdadm del-peer %s:%s", peer.Name, rdName)
		}

		// forget-peer is per-volume because v09 metadata lives
		// in the per-volume block. Skip volumes without a device
		// path (DISKLESS local replica — no metadata to clean).
		// Skip when nodeID is zero (no resolvable id — leaves a
		// slow-leak slot, recoverable later).
		if nodeID != 0 {
			for volNum, device := range devices {
				if device == "" {
					continue
				}

				if forgetErr := r.cfg.Adm.ForgetPeer(ctx, rdName, volNum, device, nodeID); forgetErr != nil {
					logger.Info("EvictPeersByUIDMismatch: forget-peer failed (non-fatal)",
						"peer", peer.Name,
						"vol", volNum,
						"nodeID", nodeID,
						"err", forgetErr.Error())
				}
			}
		}

		if cleaned == nil {
			cleaned = make(map[string]string)
		}

		cleaned[peer.Name] = peer.ResourceUID
	}

	return cleaned, nil
}
