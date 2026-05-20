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
	"maps"

	"github.com/cockroachdb/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// appliedPeerUIDsFieldOwner is the SSA field-manager the satellite
// uses when it writes Status.AppliedPeerUIDs. Distinct from the
// MetadataCreated / FilesystemFormatted / volume-status / observer
// owners so the apiserver merges all claims cleanly under their
// respective listMapKey paths.
const appliedPeerUIDsFieldOwner = "blockstor-satellite-applied-peer-uids"

// AppliedPeerUIDsStamper implements satellite.AppliedPeerUIDsStamper.
// SSA-patches the Status.AppliedPeerUIDs map onto the per-replica
// Resource CRD after the satellite's reconcilePeers + adjust succeed.
// See pkg/satellite/reconcile_peers.go for the full UID-aware diff
// algorithm that consumes this map on every subsequent reconcile.
//
// One instance per satellite — the agent wires this in after the
// controller-runtime manager is built (cached client lives there).
type AppliedPeerUIDsStamper struct {
	// Client is the controller-runtime cached client. Reads + writes
	// flow through the same client the other Status writers
	// (volume-status, observer, MetadataCreatedStamper,
	// FilesystemFormattedStamper) share so the SSA patch lands on
	// the same apiserver round-trip.
	Client client.Client
}

// StampAppliedPeerUIDs SSA-patches Resource <resourceName>.Status
// .AppliedPeerUIDs with `uids`. Idempotent — SSA's map-merge means a
// repeat patch with the same map is a no-op at the apiserver level.
//
// nil / empty uids is rendered as an empty map (NOT a delete) so the
// satellite's reconcilePeers gate len(applied)==0 keeps its
// adoption-mode meaning across the field's lifetime.
func (s *AppliedPeerUIDsStamper) StampAppliedPeerUIDs(ctx context.Context, resourceName string, uids map[string]string) error {
	// Defensive copy: the caller hands us the satellite's local
	// expected-map; mutating it (e.g. via the apiserver's deserialiser)
	// could race the satellite's next reconcile.
	body := make(map[string]string, len(uids))
	maps.Copy(body, uids)

	apply := &blockstoriov1alpha1.Resource{
		TypeMeta: metav1.TypeMeta{
			Kind:       resourceKind,
			APIVersion: blockstoriov1alpha1.GroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{Name: resourceName},
		Status: blockstoriov1alpha1.ResourceStatus{
			AppliedPeerUIDs: body,
		},
	}

	// Intentionally NO ForceOwnership: AppliedPeerUIDs is OWNED by
	// this stamper alone (other Status writers don't touch it), so
	// the merge surface is one whole-map claim. If a future writer
	// shares the map we'd need to switch to listMap-style merging,
	// but for now this is the simplest correct shape.
	err := s.Client.Status().Patch(ctx, apply,
		client.Apply, //nolint:staticcheck // SA1019: applyconfiguration-gen output not yet available for our CRDs
		client.FieldOwner(appliedPeerUIDsFieldOwner))
	if err != nil {
		return errors.Wrapf(err, "ssa AppliedPeerUIDs on Resource %s", resourceName)
	}

	return nil
}
