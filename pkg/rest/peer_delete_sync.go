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

package rest

import (
	"context"
	"errors"
	"time"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// peerDeleteAckTimeout bounds how long handleResourceDelete waits for
// surviving siblings to acknowledge a peer-deletion before falling
// back to physical Delete with a warning. Sized to comfortably cover
// one satellite reconcile + del-peer/forget-peer roundtrip (~1-3s in
// practice) plus a generous safety margin for kernel-slot teardown
// under load. Phase 2's `wait_status_state` budget (120s in
// r-full-lifecycle.sh) absorbs this without test-side adjustment.
//
//nolint:gochecknoglobals // tunable; CHANGELOG-tracked
var peerDeleteAckTimeout = 15 * time.Second

// peerDeletePollInterval is the cadence for re-reading sibling
// Annotations while waiting for the ACK. Short enough that happy-path
// (~1s satellite reconcile) returns promptly; long enough that a slow
// apiserver (Bug 124 cache-trail class) doesn't get hammered.
//
//nolint:gochecknoglobals // tunable
var peerDeletePollInterval = 250 * time.Millisecond

// peerForgetAckAnnotationPrefix names the per-peer ACK key the
// satellite stamps after completing ActionForgetPeer for a given peer
// name. Full key shape: `blockstor.io/peer-forget-acked.<peerNode>`
// with an RFC3339Nano timestamp as the value. The REST handler's
// waitForPeerDeletionAcks polls the key for presence as the implicit
// confirmation that del-peer + forget-peer have completed against
// the OLD peer incarnation.
//
// An annotation is the chosen ACK transport because annotations are
// round-tripped through Store and already serve as the channel for
// cross-satellite signals (Bug 67 PeerChangedAnnotation), so reusing
// the same transport keeps the fix additive — no Store schema change.
const peerForgetAckAnnotationPrefix = "blockstor.io/peer-forget-acked."

// waitForPeerDeletionAcks blocks until every online sibling Resource
// of `rdName` has stamped a peer-forget ACK annotation for
// `removedNode` — confirming the satellite FSM has run
// ActionForgetPeer (del-peer + forget-peer) against the doomed
// peer. Returns when all ACKs land OR the peerDeleteAckTimeout
// deadline fires.
//
// Bug 342 v10: matches upstream LINSTOR's behaviour of blocking the
// REST call until satellites confirm peer cleanup, without copying
// GPL code. Per-sibling independent — if one of three siblings is
// OFFLINE, the other two still produce timely ACKs and only the
// OFFLINE one is skipped.
//
// Best-effort semantics on the timeout path: the REST handler MUST
// proceed to physical Delete even if ACKs don't land. The fallback
// is pre-v10 behaviour (satellite gets a peer-changed annotation
// bump after Delete; cleanup happens late, possibly racing the next
// `r c`). This is degraded but not data-loss — a wedged DRBD
// connection self-recovers when the operator reissues the test or
// the next reconcile pass picks up the missing forget-peer.
//
// OFFLINE-node skip: a satellite whose Node.ConnectionStatus is
// `OFFLINE` is unreachable; waiting for its ACK would block the
// entire operation. Treated as "ACK satisfied" — the cluster
// invariant the caller cares about is "every reachable satellite
// has cleaned up" which is exactly what this loop guarantees.
func (s *Server) waitForPeerDeletionAcks(ctx context.Context, rdName, removedNode string) {
	siblings, err := s.Store.Resources().ListByDefinition(ctx, rdName)
	if err != nil {
		return
	}

	// Subset down to online siblings on nodes other than the
	// doomed one. We capture node names here once so a racing
	// satellite-side annotation patch doesn't re-enter the wait
	// loop with a different sibling set.
	type sibKey struct {
		rdName   string
		nodeName string
	}

	wait := make([]sibKey, 0, len(siblings))

	for i := range siblings {
		sib := &siblings[i]
		if sib.NodeName == removedNode {
			continue
		}

		if !isNodeOnline(ctx, s.Store.Nodes(), sib.NodeName) {
			continue
		}

		wait = append(wait, sibKey{rdName: rdName, nodeName: sib.NodeName})
	}

	if len(wait) == 0 {
		return
	}

	ackKey := peerForgetAckAnnotationPrefix + removedNode
	deadline := time.Now().Add(peerDeleteAckTimeout)

	for {
		remaining := wait[:0]

		for _, k := range wait {
			acked, ackErr := siblingHasPeerForgetAck(ctx, s.Store.Resources(), k.rdName, k.nodeName, ackKey)
			if ackErr != nil {
				// Treat read errors as "still pending" — the
				// next poll iteration may succeed. The timeout
				// is the safety net.
				remaining = append(remaining, k)

				continue
			}

			if !acked {
				remaining = append(remaining, k)
			}
		}

		if len(remaining) == 0 {
			return
		}

		if time.Now().After(deadline) {
			// Timeout: fall through to physical Delete with the
			// pre-v10 annotation-bump fallback. Operator sees a
			// 200 (the caller writes the envelope after this
			// function returns) — pending wedge will surface in
			// the next iteration if reproducible.
			return
		}

		wait = remaining

		select {
		case <-ctx.Done():
			return
		case <-time.After(peerDeletePollInterval):
		}
	}
}

// isNodeOnline returns true when the named Node is currently reachable.
// Treats lookup errors and missing nodes as "online" (best-effort: the
// REST caller would rather wait briefly and time out than skip a node
// that might in fact be reachable). Only an explicit
// `ConnectionStatus == OFFLINE` triggers the skip path.
func isNodeOnline(ctx context.Context, nodes store.NodeStore, nodeName string) bool {
	node, err := nodes.Get(ctx, nodeName)
	if err != nil {
		return true
	}

	return node.ConnectionStatus != apiv1.NodeTypeOffline
}

// siblingHasPeerForgetAck returns true when the sibling Resource
// carries an annotation matching `ackKey`, i.e. the satellite has
// stamped the peer-forget acknowledgment.
//
// NotFound on the sibling Resource is treated as ACK-satisfied: if
// the sibling just disappeared (concurrent cascade delete), there's
// no peer to forget-peer from, so the caller's invariant ("every
// reachable satellite has cleaned up") degenerates trivially true.
func siblingHasPeerForgetAck(ctx context.Context, resources store.ResourceStore, rdName, nodeName, ackKey string) (bool, error) {
	sib, err := resources.Get(ctx, rdName, nodeName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return true, nil
		}

		return false, err
	}

	if _, present := sib.Annotations[ackKey]; present {
		return true, nil
	}

	return false, nil
}
