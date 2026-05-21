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
	"encoding/json"
	"time"

	"github.com/cockroachdb/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// peerForgetAckAnnotationPrefix matches the prefix the REST
// handler's waitForPeerDeletionAcks polls. Key shape is
// `blockstor.io/peer-forget-acked.<peerName>`; the value is an
// RFC3339Nano timestamp so repeated stamps produce strictly
// monotonic values (the REST waiter only checks presence, but
// distinct values keep apiserver / controller-runtime watch
// events firing for each refresh).
//
// Must mirror pkg/rest/peer_delete_sync.go::peerForgetAckAnnotationPrefix.
const peerForgetAckAnnotationPrefix = "blockstor.io/peer-forget-acked."

// PeerForgetAckStamper implements satellite.PeerForgetAckStamper.
// JSON-merge patches an annotation
// `blockstor.io/peer-forget-acked.<peerName>` onto the local
// Resource CRD's metadata.annotations after the FSM's
// ActionForgetPeer arm finishes del-peer + forget-peer. The REST
// handler polls for the annotation via Store.Resources().Get so
// the round-trip through the apiserver-watched CRD is what
// closes the 2-phase delete handshake.
//
// One instance per satellite — the agent wires this in after the
// controller-runtime manager is built (cached client lives there).
type PeerForgetAckStamper struct {
	// Client is the controller-runtime cached client. JSON-merge
	// patches go through the same client as the Phase 11.3 Status
	// Condition stampers — same apiserver round-trip lane.
	Client client.Client
}

// StampPeerForgetAck JSON-merge patches an annotation onto
// Resource <resourceName>.metadata.annotations. Idempotent —
// each call refreshes the timestamp value; the REST poller only
// inspects presence.
//
// NotFound is treated as "nothing to stamp, ACK satisfied
// trivially" — the local Resource may have been deleted in a
// concurrent cascade (e.g. the parent RD was torn down mid-
// reconcile) and there's no peer to forget either way.
func (s *PeerForgetAckStamper) StampPeerForgetAck(ctx context.Context, resourceName, peerName string) error {
	stamp := time.Now().UTC().Format(time.RFC3339Nano)
	key := peerForgetAckAnnotationPrefix + peerName

	// Build the JSON-merge body via encoding/json so the peer
	// name (untrusted by linting purposes) and the timestamp are
	// safely quoted. The metadata sub-document is `{"annotations":
	// {"<key>":"<value>"}}` which kube-apiserver applies as a
	// recursive merge against the live object's annotation map.
	annotations := map[string]string{key: stamp}

	body, err := json.Marshal(map[string]any{
		"metadata": map[string]any{"annotations": annotations},
	})
	if err != nil {
		return errors.Wrap(err, "marshal peer-forget ACK patch body")
	}

	target := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: resourceName},
	}

	patchErr := s.Client.Patch(ctx, target, client.RawPatch(types.MergePatchType, body))
	if patchErr != nil {
		if apierrors.IsNotFound(patchErr) {
			// Local Resource gone — concurrent cascade. Nothing
			// to ACK; REST waiter's NotFound path treats it as
			// trivially satisfied.
			return nil
		}

		return errors.Wrapf(patchErr, "merge-patch annotation %s on Resource %s", key, resourceName)
	}

	return nil
}
