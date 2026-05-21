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

package controller

import (
	"context"
	"strings"
	"time"

	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// PendingPeerCleanupStaleWindow bounds how long a pending-peer-cleanup
// marker survives without an ACK before the reaper drops it anyway.
// Matches the Resource controller's allocator-gate escape hatch — both
// values are deliberately the same so the system converges to the
// same "give up on the ACK and proceed" decision regardless of which
// side observes the timeout first. Issue 342 v12c.
const PendingPeerCleanupStaleWindow = 10 * time.Second

// pendingPeerCleanupGateActive reports whether the parent RD carries
// an active per-peer pending-cleanup marker for `nodeName`. The Resource
// controller's `ensureDRBDIDs` consults this BEFORE acquiring the
// per-RD allocation mutex: an active marker means a sub-second
// `r d $node` then `r c $node` is in flight, and bringing the new
// replica's DRBD up against siblings still holding the OLD kernel
// slot would wedge the handshake — better to short-circuit allocation
// until siblings finish their del-peer + forget-peer pass and the RD
// reconciler reaps the marker.
//
// A marker older than PendingPeerCleanupStaleWindow is treated as
// inactive (escape hatch) — matches the symmetrical timeout in
// reapPendingPeerCleanup so both sides agree on when to give up on
// the ACK and proceed. Issue 342 v12c.
func pendingPeerCleanupGateActive(rd *blockstoriov1alpha1.ResourceDefinition, nodeName string) bool {
	if rd == nil || len(rd.Annotations) == 0 {
		return false
	}

	value, ok := rd.Annotations[apiv1.PendingPeerCleanupAnnotationPrefix+nodeName]
	if !ok {
		return false
	}

	stamped, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		// Unparseable marker → treat as inactive. A bad value
		// is worth a single allocation pass under the
		// pre-v12c behaviour rather than an indefinite gate.
		return false
	}

	return time.Since(stamped) < PendingPeerCleanupStaleWindow
}

// reapPendingPeerCleanup drops PendingPeerCleanupAnnotationPrefix
// markers from rd.Annotations when one of two conditions holds for
// each marker:
//
//  1. Every online surviving Resource of this RD has stamped
//     Status.ClearedPeers[departedPeer] with a timestamp >= the
//     marker's value (the satellites finished del-peer + forget-peer
//     and ACK'd).
//  2. The marker is older than PendingPeerCleanupStaleWindow (the
//     escape hatch — a wedged satellite that never ACKs would
//     otherwise hold the allocator gate forever; matches the
//     Resource controller's symmetrical stale-marker timeout).
//
// Idempotent — when no markers are present this is a no-op. Errors
// from APIReader Get / List are bubbled so the caller's Reconcile
// requeues; the inner Patch retries on conflict for the case where a
// concurrent RD-modify on the same Annotations map races us.
func (r *ResourceDefinitionReconciler) reapPendingPeerCleanup(ctx context.Context, rd *blockstoriov1alpha1.ResourceDefinition) error {
	pending := extractPendingPeerCleanupMarkers(rd.Annotations)
	if len(pending) == 0 {
		return nil
	}

	logger := logf.FromContext(ctx)

	// Walk the marker set; collect peer names whose markers are
	// safe to drop. The Store-driven sibling read is wrapped per
	// marker because there's no benefit to caching: a single RD
	// typically carries 0-1 markers at steady state, and the
	// pending set rarely fans out.
	var toDrop []string

	now := time.Now()

	for peer, stamped := range pending {
		stale := now.Sub(stamped) >= PendingPeerCleanupStaleWindow

		acked, ackErr := r.allOnlineSurvivorsAcked(ctx, rd.Name, peer, stamped)
		if ackErr != nil {
			logger.Error(ackErr, "check ClearedPeers ACK", "rd", rd.Name, "peer", peer)

			// Don't drop on read error — the next reconcile
			// retries. The stale-window safety net still
			// fires on its own schedule.
			continue
		}

		if acked || stale {
			toDrop = append(toDrop, peer)
		}
	}

	if len(toDrop) == 0 {
		return nil
	}

	return r.dropPendingPeerCleanupMarkers(ctx, rd.Name, toDrop)
}

// extractPendingPeerCleanupMarkers walks the RD's Annotations and
// returns the parsed (peerNodeName, stampedAt) pairs for every entry
// matching the PendingPeerCleanupAnnotationPrefix. Malformed values
// (unparseable timestamp) are skipped silently — a bad marker is
// treated as "no signal" so the reaper doesn't loop on a corrupted
// entry.
func extractPendingPeerCleanupMarkers(annotations map[string]string) map[string]time.Time {
	if len(annotations) == 0 {
		return nil
	}

	out := map[string]time.Time{}

	for key, value := range annotations {
		peer, ok := strings.CutPrefix(key, apiv1.PendingPeerCleanupAnnotationPrefix)
		if !ok || peer == "" {
			continue
		}

		stamped, err := time.Parse(time.RFC3339Nano, value)
		if err != nil {
			continue
		}

		out[peer] = stamped
	}

	if len(out) == 0 {
		return nil
	}

	return out
}

// allOnlineSurvivorsAcked returns true when every Resource of `rdName`
// hosted on an online node carries
// Status.ClearedPeers[peer] >= stampedAt. Offline siblings are skipped
// (matches `pkg/rest/peer_delete_sync.go::isNodeOnline`'s convention:
// an OFFLINE satellite cannot ACK, so blocking on it would deadlock
// the cluster invariant). Returns true on an empty survivor set —
// nothing to wait for.
func (r *ResourceDefinitionReconciler) allOnlineSurvivorsAcked(ctx context.Context, rdName, peer string, stampedAt time.Time) (bool, error) {
	reader := r.directOrCached()

	var resList blockstoriov1alpha1.ResourceList
	if err := reader.List(ctx, &resList); err != nil {
		return false, err
	}

	var nodeList blockstoriov1alpha1.NodeList
	if err := reader.List(ctx, &nodeList); err != nil {
		return false, err
	}

	onlineNodes := buildOnlineNodeSet(nodeList.Items)

	for i := range resList.Items {
		sib := &resList.Items[i]
		if sib.Spec.ResourceDefinitionName != rdName {
			continue
		}

		// Skip the doomed peer itself — it's already gone from
		// the apiserver by the time we get here, but a racing
		// reconcile could observe a CRD that hasn't been
		// GC-finalised yet.
		if sib.Spec.NodeName == peer {
			continue
		}

		if !onlineNodes[sib.Spec.NodeName] {
			continue
		}

		acked, ok := sib.Status.ClearedPeers[peer]
		if !ok {
			return false, nil
		}

		ackedAt, parseErr := time.Parse(time.RFC3339Nano, acked)
		if parseErr != nil {
			// Treat unparseable as "not ACK'd" — a stamp
			// is supposed to be RFC3339Nano. Avoid burning
			// the stale-window budget on a malformed entry.
			//nolint:nilerr // intentional: swallow parse error and treat as still-pending
			return false, nil
		}

		if ackedAt.Before(stampedAt) {
			return false, nil
		}
	}

	return true, nil
}

// buildOnlineNodeSet returns the set of node names whose Status
// ConnectionStatus is not OFFLINE. An empty / unset ConnectionStatus
// is treated as online (best-effort: matches
// `pkg/rest/peer_delete_sync.go::isNodeOnline`).
func buildOnlineNodeSet(nodes []blockstoriov1alpha1.Node) map[string]bool {
	out := make(map[string]bool, len(nodes))
	for i := range nodes {
		if nodes[i].Status.ConnectionStatus == blockstoriov1alpha1.NodeConnectionStatusOffline {
			continue
		}

		out[nodes[i].Name] = true
	}

	return out
}

// dropPendingPeerCleanupMarkers removes the named pending-peer-cleanup
// annotations from the RD via PatchResourceDefinitionSpec (the same
// retry-on-conflict path the REST stamp uses, so concurrent stamps
// and reaps converge instead of clobbering each other). NotFound on
// the RD is fine — the cascade has already started; the markers are
// gone-by-deletion. Other errors bubble for the caller to requeue.
func (r *ResourceDefinitionReconciler) dropPendingPeerCleanupMarkers(ctx context.Context, rdName string, peers []string) error {
	// Patch via Store so the wire-side annotation map is the
	// source of truth (matches REST's stamp path; the CRD
	// annotations get re-derived from the Store on the next
	// dispatcher pass).
	if r.Store != nil {
		return r.dropMarkersViaStore(ctx, rdName, peers)
	}

	// Fallback for unit tests that wire the reconciler directly
	// against a fake client without a Store. Strip the annotations
	// in-place via APIReader-fresh Get + Patch.
	reader := r.directOrCached()

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var fresh blockstoriov1alpha1.ResourceDefinition
		if err := reader.Get(ctx, client.ObjectKey{Name: rdName}, &fresh); err != nil {
			return client.IgnoreNotFound(err)
		}

		if fresh.Annotations == nil {
			return nil
		}

		changed := false

		for _, peer := range peers {
			key := apiv1.PendingPeerCleanupAnnotationPrefix + peer
			if _, ok := fresh.Annotations[key]; ok {
				delete(fresh.Annotations, key)

				changed = true
			}
		}

		if !changed {
			return nil
		}

		return r.Update(ctx, &fresh)
	})
}

// dropMarkersViaStore drops the per-peer markers from the RD's
// Annotations via Store's PatchResourceDefinitionSpec (the same path
// REST writes use; retry-on-conflict is built into the Store impl).
func (r *ResourceDefinitionReconciler) dropMarkersViaStore(ctx context.Context, rdName string, peers []string) error {
	err := r.Store.ResourceDefinitions().PatchResourceDefinitionSpec(ctx, rdName, func(rd *apiv1.ResourceDefinition) error {
		if rd.Annotations == nil {
			return nil
		}

		for _, peer := range peers {
			delete(rd.Annotations, apiv1.PendingPeerCleanupAnnotationPrefix+peer)
		}

		return nil
	})
	if err != nil {
		// NotFound — the RD got cascade-deleted between our
		// Get and Patch. Markers are gone by-deletion; fine.
		return client.IgnoreNotFound(err)
	}

	return nil
}
