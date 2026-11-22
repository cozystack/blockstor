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

package placer

import (
	"context"
	"time"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// witnessHoldWindow bounds how long a placement keeps the tiebreaker
// reconciler from adding a witness when it never gets to say it is
// done, a controller that died in the middle of it say. A placement
// writes its replicas in well under a second each.
const witnessHoldWindow = time.Minute

// holdWitness keeps the RD's tiebreaker reconciler from adding a
// witness while this placement writes more than one diskful replica,
// and returns the function that lets it decide again.
//
// The placer writes the replicas one at a time. Between the second and
// the third of a three-replica placement the RD has two diskful
// replicas and no witness, the shape a witness is for, and the
// reconciler put one on the node the third replica is for. The
// placement then took that witness over (takeOverReplicaWrittenMeanwhile),
// but the first two replicas had already set it up as a diskless peer.
// When it came up diskful their kernel refused the handshake ("The peer
// is configured to be diskless but presents Inconsistent") and left the
// connection StandAlone. LINSTOR decides on the witness once the whole
// placement is written, in the same transaction, so a placement of an
// odd count never shows a witness at all; the RD annotation
// PlacementInProgressUntilAnnotation does that across the processes
// that place and the controller that decides.
//
// A placement of one replica passes through no other shape, and needs
// no hold. The hold is best effort: one that cannot be written leaves
// the take-over to settle the count, as before. Its removal names the
// stamp this placement wrote, so a placement still running elsewhere
// keeps its own.
func (p *Placer) holdWitness(ctx context.Context, rdName string, toPlace int) func() {
	if toPlace < 2 {
		return func() {}
	}

	stamp := time.Now().Add(witnessHoldWindow).UTC().Format(time.RFC3339Nano)

	err := p.store.ResourceDefinitions().PatchResourceDefinitionSpec(ctx, rdName, func(rd *apiv1.ResourceDefinition) error {
		if rd.Annotations == nil {
			rd.Annotations = map[string]string{}
		}

		rd.Annotations[apiv1.PlacementInProgressUntilAnnotation] = stamp

		return nil
	})
	if err != nil {
		return func() {}
	}

	return func() {
		_ = p.store.ResourceDefinitions().PatchResourceDefinitionSpec(context.WithoutCancel(ctx), rdName,
			func(rd *apiv1.ResourceDefinition) error {
				if rd.Annotations[apiv1.PlacementInProgressUntilAnnotation] == stamp {
					delete(rd.Annotations, apiv1.PlacementInProgressUntilAnnotation)
				}

				return nil
			})
	}
}
