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

package controllers

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// PeerForgetAckStamper implements satellite.PeerForgetAckStamper.
// JSON-merge-patches a single annotation
// `blockstor.io/peer-forget-acked.<peerNodeName>` onto the Resource
// CRD's metadata.annotations after `tearDownRemovedPeers` has issued
// `drbdadm del-peer` (and `drbdmeta forget-peer` when local is
// diskful) for a departing peer.
//
// The REST handler's `waitForPeerDeletionAcks` polls this annotation
// on every surviving sibling Resource as the cluster-wide signal
// that the per-peer kernel cleanup has completed before the
// controller is free to physically reap the doomed Resource CRD and
// let the allocator reuse its `node-id`. Spec §4.2 ("two-phase
// delete") + §6 ("three-replica n-lost + autoplace edge case").
//
// One instance per satellite — the agent wires this in after the
// controller-runtime manager is built (the cached client lives
// there).
type PeerForgetAckStamper struct {
	// Client is the controller-runtime cached client. Reads + writes
	// flow through the same client the rest of the controllers use,
	// so the merge patch lands on the same apiserver round-trip the
	// other annotation writers (PeerChanged bumper, volume-numbers
	// stamper) share.
	Client client.Client
}

// StampPeerForgetAck merge-patches a single annotation
// `<PeerForgetAckAnnotationPrefix><peerNodeName>` onto the Resource
// CRD's metadata.annotations with the current wall-clock RFC3339Nano
// timestamp. Idempotent — a repeat call for the same peer simply
// re-advances the timestamp; the REST poller only cares about
// presence, not value.
//
// JSON merge-patch (not SSA Apply) is used because:
//
//   - we are stamping ONE key under metadata.annotations; SSA's
//     listMap merging only applies inside lists and Conditions, so
//     a map-key patch is what kube-apiserver wants here
//   - other annotation writers (`bumpPeerChangedOnSiblings`,
//     `stampVolumeNumbersAnnotation`) already use merge-patch on
//     metadata.annotations; mirroring their shape keeps the
//     field-owner story uniform
//   - the patch body is tiny and deterministic, so there's no
//     benefit to SSA's structural diff
//
// NotFound on the Resource CRD is silently swallowed — concurrent
// cascade-deletes are the most common cause, and the REST poller
// already folds NotFound to "ACK satisfied".
func (s *PeerForgetAckStamper) StampPeerForgetAck(ctx context.Context, resourceName, peerNodeName string) error {
	if s == nil || s.Client == nil {
		// Defensive: unit-test wiring may construct the stamper
		// struct without a Client. Surface a no-op rather than a
		// nil-pointer panic — the caller is fire-and-forget anyway.
		return nil
	}

	key := apiv1.PeerForgetAckAnnotationPrefix + peerNodeName
	stamp := time.Now().UTC().Format(time.RFC3339Nano)

	// One annotation, one round-trip. The patch body only touches
	// metadata.annotations[<key>]; every other annotation + every
	// other field on the Resource CRD survives untouched by
	// merge-patch semantics.
	body := []byte(`{"metadata":{"annotations":{"` + key + `":"` + stamp + `"}}}`)

	target := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: resourceName},
	}

	err := s.Client.Patch(ctx, target, client.RawPatch(types.MergePatchType, body))
	if err != nil {
		if apierrors.IsNotFound(err) {
			// Resource CRD already gone — the REST poller treats
			// NotFound as ACK-satisfied
			// (`siblingHasPeerForgetAck` folds NotFound → true),
			// so the convergent outcome is the same.
			return nil
		}

		return errors.Wrapf(err, "merge-patch peer-forget-acked annotation for %s on %s", peerNodeName, resourceName)
	}

	return nil
}
