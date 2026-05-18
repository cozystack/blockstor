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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// metadataCreatedFieldOwner is the SSA field-manager the satellite
// uses when it writes the MetadataCreated Status Condition. Distinct
// from the observer's owner (which writes DiskState / CurrentGi) and
// the volume-status owner (which writes DevicePath) so the apiserver
// merges all three writers' claims cleanly under listMapKey=type
// for Conditions and listMapKey=volumeNumber for Volumes.
const metadataCreatedFieldOwner = "blockstor-satellite-metadata-created"

// MetadataCreatedStamper implements
// satellite.MetadataCreatedStamper. SSA-patches a
// `MetadataCreated=True` Condition onto Resource.Status.Conditions
// after the satellite's `drbdmeta create-md` step succeeds.
//
// One instance per satellite — the agent wires this in after the
// controller-runtime manager is built (cached client lives there).
// Phase 11.3 Stage 1.
type MetadataCreatedStamper struct {
	// Client is the controller-runtime cached client. Reads + writes
	// flow through the same client the rest of the controllers use,
	// so the SSA patch lands on the same apiserver round-trip the
	// other Status writers (volume-status, observer) share.
	Client client.Client
}

// StampMetadataCreated SSA-patches a `MetadataCreated=True`
// Condition onto Resource <resourceName>.Status.Conditions.
// Idempotent — SSA's listMap merging on `type` means a repeat
// patch with the same fields is a no-op at the apiserver level
// (LastTransitionTime is preserved because the apiserver only
// updates it when the Condition's Status actually changes).
func (s *MetadataCreatedStamper) StampMetadataCreated(ctx context.Context, resourceName string) error {
	apply := &blockstoriov1alpha1.Resource{
		TypeMeta: metav1.TypeMeta{
			Kind:       resourceKind,
			APIVersion: blockstoriov1alpha1.GroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{Name: resourceName},
		Status: blockstoriov1alpha1.ResourceStatus{
			Conditions: []metav1.Condition{
				{
					Type:               blockstoriov1alpha1.ConditionMetadataCreated,
					Status:             metav1.ConditionTrue,
					Reason:             "CreateMdSucceeded",
					Message:            "drbdmeta create-md completed",
					LastTransitionTime: metav1.Now(),
				},
			},
		},
	}

	// Intentionally NO ForceOwnership: SSA's listMap merge on
	// `type` lets this writer own only the MetadataCreated entry.
	// Other Condition writers (future toggle-disk failure
	// Condition, etc.) keep their entries intact.
	err := s.Client.Status().Patch(ctx, apply,
		client.Apply, //nolint:staticcheck // SA1019: applyconfiguration-gen output not yet available for our CRDs
		client.FieldOwner(metadataCreatedFieldOwner))
	if err != nil {
		return errors.Wrapf(err, "ssa MetadataCreated Condition on Resource %s", resourceName)
	}

	return nil
}
