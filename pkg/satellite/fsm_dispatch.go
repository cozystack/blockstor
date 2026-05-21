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

// Phase 11.2.c Stage 3d: the shadow-dispatch router. Maps each
// FSM-recommended Action onto its corresponding extracted helper
// (renderResFile / createMetadata / bringUpResource / adjustResource).
// The legacy chain in applyDRBD still runs after this dispatch fires
// — every helper is content-idempotent, so the second call is a
// near-no-op stat-and-skip path. Once dashboards confirm every
// transition has been FSM-dispatched in steady state over a full
// burnin window, Stage 4 retires the legacy gate chain entirely.

package satellite

import (
	"context"

	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// dispatchFsmAction routes a FSM-recommended Action to its
// corresponding extracted helper. Returns nil for unknown actions
// (the legacy chain handles them), for the explicit "no-op" action
// (FSM says nothing to do), and for actions the FSM can't safely
// double-fire on (ActionDecommission — delete-path territory the
// legacy chain still owns through tearDownRemovedPeers and the
// dedicated DeleteResource pipeline).
//
// Phase 11.2.c Stage 3d: shadow-dispatches every action. The helpers
// are content-idempotent so re-running the same work via the legacy
// chain below is safe. ActionCreateMd is gated inside the dispatch
// to mirror the legacy `firstActivation && !diskless && !MdExists`
// invariant — defense-in-depth in case the FSM lookup ever drifts
// from the legacy gate ordering.
//
// Phase 11.2.c Stage 4 step 1: renderResFile is invoked as a
// preamble for every action that needs an up-to-date .res on disk
// (createMd, up, adjust, adjustSkipDisk). This makes the FSM
// dispatch the sole writer of .res — the legacy unconditional
// renderResFile call inside applyDRBD's body has been retired.
// renderResFile is content-idempotent (Bug 315), so a no-op preamble
// pass when content is unchanged is a single stat+compare with no
// mtime bump. The ActionRenderRes arm still exists for the cold-
// start path where it is the only action (PhaseUnprovisioned →
// MetadataPending). Decommission and Noop deliberately skip the
// preamble: Decommission is delete-path territory (no need to
// freshen .res for a resource being torn down) and Noop must remain
// a true no-op.
//
//nolint:cyclop // declarative router; each case is one arm of the FSM action enum
func (r *Reconciler) dispatchFsmAction(ctx context.Context, dr *intent.DesiredResource, devices map[int32]string, action string, obs Observation) error {
	// Phase 11.2.c Stage 4 step 1: renderResFile preamble for every
	// action that consumes the .res file (createMd reads it via
	// drbdadm dump-md; up/adjust/adjustSkipDisk re-load it into the
	// kernel). The legacy unconditional renderResFile call inside
	// applyDRBD's body has been retired — this preamble takes over
	// that role and makes the FSM dispatch the sole writer of .res.
	// renderResFile is content-idempotent (Bug 315) so the preamble
	// is a stat+compare no-op when the rendered body already matches
	// what is on disk. Skipped for Decommission (delete-path) and
	// Noop (must remain a true no-op).
	switch action {
	case ActionCreateMd, ActionUp, ActionAdjust, ActionAdjustSkipDisk:
		if err := r.renderResFile(ctx, dr, devices); err != nil {
			return err
		}
	}

	switch action {
	case ActionRenderRes:
		return r.renderResFile(ctx, dr, devices)
	case ActionCreateMd:
		// Defense-in-depth gates:
		//   - !SpecHasResource: Spec hasn't materialized yet
		//   - MetadataExists: nothing to do, marker already stamped
		//     (`create-md --force` would wipe operator-stamped GI +
		//     bitmap state)
		//   - SpecFlagsHasDiskless: never seed metadata on a Diskless
		//     replica (no lower disk to stamp)
		//   - KernelLoaded && KernelHasDiskless: diskful-flip path —
		//     legacy routes through ensureMetadata(firstActivation=false)
		//     (no GI-seed); the shadow's createMetadata calls
		//     firstActivation=true which seeds GI via seedInitialGi
		//     and corrupts the in-flight handshake. Stage 4 will own
		//     the flip path end-to-end; for now defer to legacy.
		if !obs.SpecHasResource || obs.MetadataExists ||
			obs.SpecFlagsHasDiskless ||
			(obs.KernelLoaded && obs.KernelHasDiskless) {
			return nil
		}

		return r.createMetadata(ctx, dr, devices)
	case ActionUp:
		return r.bringUpResource(ctx, dr)
	case ActionAdjust, ActionAdjustSkipDisk:
		// adjustResource computes the SkipDisk variant internally
		// from operator prop + kernel state. Pass diskfulFlip=false —
		// the Bug 319 flip is gate-level state the legacy chain still
		// owns (it has the .res-file stat + .md-created marker reads
		// the FSM Observation can't reproduce on its own).
		return r.adjustResource(ctx, dr, false)
	case ActionDecommission:
		// Decommission is delete-path territory; the legacy chain
		// owns tearDownRemovedPeers + storage cleanup via the
		// dedicated DeleteResource pipeline. Skip in shadow.
		return nil
	case ActionForgetPeer:
		return r.dispatchForgetPeer(ctx, dr, devices, obs)
	case ActionNoop:
		return nil
	default:
		return nil
	}
}

// dispatchForgetPeer runs `drbdadm del-peer` + `drbdmeta
// forget-peer` per (peer, volume) for every entry in
// obs.PeersDeleting, then stamps the peer-forget ACK annotation
// on the local Resource CRD so the REST handler's
// waitForPeerDeletionAcks loop unblocks. 342-v10 Phase 2.
//
// Best-effort throughout: del-peer / forget-peer errors are
// logged but do not bubble — the REST handler's 15s timeout is
// the upper bound on the wait, and the next reconcile retries.
// The ACK stamp is also best-effort; a missing stamp falls back
// to the pre-v10 behaviour (REST waits 15s then proceeds with a
// warning).
func (r *Reconciler) dispatchForgetPeer(ctx context.Context, dr *intent.DesiredResource, devices map[int32]string, obs Observation) error {
	logger := log.FromContext(ctx).WithName("forget-peer").WithValues("rd", dr.GetName())

	if r.cfg.Adm == nil || len(obs.PeersDeleting) == 0 {
		return nil
	}

	for _, peer := range obs.PeersDeleting {
		if peer.NodeID < 0 {
			continue
		}

		if err := r.cfg.Adm.DelPeer(ctx, dr.GetName(), peer.Name); err != nil {
			logger.Info("del-peer failed (non-fatal)",
				"peer", peer.Name,
				"err", err.Error())
		}

		for volNum, device := range devices {
			if device == "" {
				continue
			}

			if err := r.cfg.Adm.ForgetPeer(ctx, dr.GetName(), volNum, device, peer.NodeID); err != nil {
				logger.Info("forget-peer failed (non-fatal)",
					"peer", peer.Name,
					"vol", volNum,
					"nodeID", peer.NodeID,
					"err", err.Error())
			}
		}

		// Stamp the peer-forget ACK annotation so the REST
		// handler can unblock. Stamper nil under unit-test wiring
		// — the FSM helpers still run del-peer/forget-peer (the
		// kernel-side outcome) and the lack of an apiserver write
		// degrades to "REST waits the full 15s timeout".
		//
		// Resource CRD name is `<rd>.<node>` (per-node sharding);
		// matches the same shape Phase 11.3 MetadataCreated
		// stamper uses (see reconciler.go::resourceCRDName).
		if r.cfg.PeerForgetAckStamper != nil {
			resourceCRDName := dr.GetName() + "." + r.cfg.NodeName
			stampErr := r.cfg.PeerForgetAckStamper.StampPeerForgetAck(ctx, resourceCRDName, peer.Name)
			if stampErr != nil {
				logger.Info("peer-forget ACK stamp failed (non-fatal)",
					"peer", peer.Name,
					"err", stampErr.Error())
			}
		}
	}

	return nil
}
