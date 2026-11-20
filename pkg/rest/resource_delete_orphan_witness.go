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
	"slices"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// reapOrphanWitnesses deletes, inside the same `r d` request, every
// auto-managed TIE_BREAKER witness of rdName once no diskful replica
// is left for it to arbitrate between.
//
// Upstream LINSTOR runs its auto-tiebreaker helper inside the
// resource-delete transaction (CtrlRscDeleteApiCallHandler →
// CtrlRscAutoHelper.manage), so a witness that no longer has a reason
// to exist is flagged DELETE before the DELETE call returns. linstor-csi
// relies on that: after deleting every replica it lists the
// resources and deletes the resource definition only if each one
// carries DELETE, and it never retries once DeleteVolume has
// returned success. blockstor's witness lifecycle lives in the
// asynchronous RD reconciler instead, which can stamp a fresh witness
// while csi is still deleting replicas (dropping the attach-time
// diskless of a 2-diskful volume leaves the "2 diskful, no diskless"
// shape the reconciler fills) and reap it only seconds after the last
// diskful is gone. csi's final listing lands in between, sees a live
// TIE_BREAKER, and the definition is left behind empty.
//
// Only the "no diskful left" shape is handled here: a witness with no
// diskful peer is useless in every branch of the reconciler's
// shouldTieBreakerExist and of upstream's, so removing it now cannot
// fight the reconciler. Witness decisions with diskful replicas still
// present stay with the reconciler.
//
// Best-effort, like the sibling-bump that precedes it: the caller's
// delete already succeeded, and the reconciler reaps whatever this
// misses.
func (s *Server) reapOrphanWitnesses(ctx context.Context, rdName string) {
	replicas, err := s.Store.Resources().ListByDefinition(ctx, rdName)
	if err != nil {
		return
	}

	var witnesses []string

	for i := range replicas {
		rsc := &replicas[i]
		if slices.Contains(rsc.Flags, apiv1.ResourceFlagDelete) {
			continue
		}

		if !isDisklessReplica(rsc) {
			return
		}

		if slices.Contains(rsc.Flags, apiv1.ResourceFlagTieBreaker) {
			witnesses = append(witnesses, rsc.NodeName)
		}
	}

	for _, node := range witnesses {
		if s.Store.Resources().Delete(ctx, rdName, node) != nil {
			continue
		}

		s.waitForResourceDeletionVisible(ctx, rdName, node)
	}
}
