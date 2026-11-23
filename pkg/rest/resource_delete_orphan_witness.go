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
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// reapUnneededWitnesses deletes, inside the same `r d` request, every
// auto-managed TIE_BREAKER witness of rdName that the delete left
// without a tie to break: one or no voting diskful replica remains.
//
// Upstream LINSTOR runs its auto-tiebreaker helper inside the
// resource-delete transaction (CtrlRscDeleteApiCallHandler →
// CtrlRscAutoHelper.manage). Its shouldTieBreakerExist counts only
// replicas not flagged DELETE or INACTIVE, so once fewer than two
// diskful replicas remain the witness is flagged DELETE before the
// DELETE call returns. linstor-csi relies on that: after deleting
// every replica it lists the resources and deletes the resource
// definition only if each one carries DELETE, and it never retries
// once DeleteVolume has returned success. blockstor's witness
// lifecycle lives in the asynchronous RD reconciler instead, which
// reaps the witness only seconds later; csi's final listing lands in
// between, sees a live TIE_BREAKER, and the definition is left behind
// empty.
//
// The decision mirrors the reconciler's remove branch for these
// shapes, so the two never fight: two or more voting diskful replicas
// leave the witness to the reconciler, and with exactly one the
// witness stays while the operator asked to keep it
// (`r d --keep-tiebreaker`) or an INACTIVE replica is waiting for a
// backfill onto it (Bug 393). With no diskful left neither carve-out
// applies, as in keepExistingWitnessFor.
//
// The replicas are read past the informer cache: a diskful another
// apiserver replica has just created must not look absent, or the
// witness of a healthy two-diskful definition would be dropped.
//
// Best-effort, like the sibling-bump that follows it: the caller's
// delete already succeeded, and the reconciler reaps whatever this
// misses.
func (s *Server) reapUnneededWitnesses(ctx context.Context, rdName string) {
	logger := log.FromContext(ctx).V(1).WithValues("rd", rdName)

	replicas, err := store.LiveReplicas(ctx, s.Store, rdName)
	if err != nil {
		logger.Info("could not list replicas to reap an unneeded witness", "error", err.Error())

		return
	}

	witnesses, keep := unneededWitnesses(replicas)
	if len(witnesses) == 0 {
		return
	}

	if keep && s.keepTiebreakerActive(ctx, rdName) {
		return
	}

	for _, node := range witnesses {
		// DeleteIfTieBreaker, not Delete: a witness the placer is
		// promoting to a diskful backfill replica right now must
		// survive, exactly as in the reconciler's removeWitnesses.
		deleted, delErr := s.Store.Resources().DeleteIfTieBreaker(ctx, rdName, node)
		if delErr != nil {
			logger.Info("could not delete an unneeded witness", "node", node, "error", delErr.Error())

			continue
		}

		if deleted {
			s.waitForResourceDeletionVisible(ctx, rdName, node)
		}
	}
}

// unneededWitnesses returns the nodes of the TIE_BREAKER witnesses the
// replica set no longer needs, and whether exactly one voting diskful
// survives, in which case the keep-tiebreaker override still applies.
// Replicas flagged DELETE are already going and are not counted;
// INACTIVE ones do not vote either, but while one exists a witness is
// a backfill target and stays.
func unneededWitnesses(replicas []apiv1.Resource) ([]string, bool) {
	var (
		witnesses   []string
		diskful     int
		hasInactive bool
	)

	for i := range replicas {
		rsc := &replicas[i]

		switch {
		case slices.Contains(rsc.Flags, apiv1.ResourceFlagDelete):
		case slices.Contains(rsc.Flags, apiv1.ResourceFlagInactive):
			hasInactive = true
		case slices.Contains(rsc.Flags, apiv1.ResourceFlagTieBreaker):
			witnesses = append(witnesses, rsc.NodeName)
		case !isDisklessReplica(rsc):
			diskful++
		}
	}

	const tieDiskful = 2

	if diskful >= tieDiskful || (diskful == 1 && hasInactive) {
		return nil, false
	}

	return witnesses, diskful == 1
}

// keepTiebreakerActive reports whether the definition carries a
// keep-tiebreaker deadline still in the future, read past the cache
// because the same request may have just stamped it.
func (s *Server) keepTiebreakerActive(ctx context.Context, rdName string) bool {
	rd, err := s.Store.ResourceDefinitions().GetUncached(ctx, rdName)
	if err != nil {
		// Unknown: keeping is the side the reconciler can still fix.
		return true
	}

	deadline, err := time.Parse(time.RFC3339, rd.Annotations[KeepTiebreakerUntilAnnotation])
	if err != nil {
		return false
	}

	return time.Now().Before(deadline)
}
