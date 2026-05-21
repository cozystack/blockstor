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

	"github.com/cockroachdb/errors"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// ClearedPeersStamper implements satellite.ClearedPeersStamper.
// Merges an entry into Resource.Status.ClearedPeers under the
// departed peer's node name, using RetryOnConflict + APIReader-fresh
// Get + Status().Update — same pattern as stampAppliedPeerUIDs. SSA
// Apply isn't used here because the field is a plain map (not a
// listMap with a discriminator) so the apiserver doesn't merge
// independent writers' map keys; a wholesale Update under retry
// covers the equivalent of "this writer's entry wins, peer entries
// stay put" because every retry re-reads the live object.
//
// Lives behind the satellite.ClearedPeersStamper interface so the
// satellite Reconciler stays free of a controller-runtime client
// dependency — see other stamper impls in the same package. Issue
// 342 v12c.
type ClearedPeersStamper struct {
	// Client is the controller-runtime cached client for Status
	// writes.
	Client client.Client

	// APIReader is the uncached client used for the in-retry Get.
	// Controller-runtime's cached client trails the apiserver by
	// hundreds of ms after a satellite-side Status write; an
	// APIReader-fresh read inside the retry loop avoids the
	// stampAppliedPeerUIDs-class 409 storm on hot Resources.
	APIReader client.Reader
}

// StampClearedPeer merges `Status.ClearedPeers[departedPeer] = stamp`
// onto the Resource named `resourceName`. Idempotent — a repeat call
// with the same args is a no-op because the inner closure short-
// circuits when the map entry already matches.
func (s *ClearedPeersStamper) StampClearedPeer(ctx context.Context, resourceName, departedPeer, stamp string) error {
	reader := client.Reader(s.Client)
	if s.APIReader != nil {
		reader = s.APIReader
	}

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var fresh blockstoriov1alpha1.Resource

		if getErr := reader.Get(ctx, client.ObjectKey{Name: resourceName}, &fresh); getErr != nil {
			return getErr
		}

		if fresh.Status.ClearedPeers == nil {
			fresh.Status.ClearedPeers = map[string]string{}
		}

		if fresh.Status.ClearedPeers[departedPeer] == stamp {
			// Already at the desired state — short-circuit
			// before the apiserver round-trip.
			return nil
		}

		fresh.Status.ClearedPeers[departedPeer] = stamp

		return s.Client.Status().Update(ctx, &fresh)
	})
	if err != nil {
		return errors.Wrapf(err, "stamp Resource %s ClearedPeers[%s]", resourceName, departedPeer)
	}

	return nil
}
