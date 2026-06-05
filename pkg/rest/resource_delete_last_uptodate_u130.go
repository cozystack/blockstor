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
	"net/http"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// U130 — "LINSTOR shouldn't allow deleting the last active peer."
//
// Upstream user report (LINBIT/linstor-server, closed): start with 1
// diskful + 1 diskless(Primary/InUse), `r c` a SECOND diskful (which
// begins as SyncTarget/Inconsistent), then `r d` the ORIGINAL diskful
// before the resync finishes. The cluster wedges: the DELETING node
// stays Connecting, the SyncTarget is stranded (its only sync source is
// gone), and the Primary is left on a DISKLESS replica with no UpToDate
// backing storage anywhere.
//
// Guard: refuse an `r d` that would remove the LAST UpToDate diskful
// replica while another replica is still mid-sync (a SyncTarget /
// Inconsistent diskful exists that has not yet reached UpToDate). The
// SyncTarget cannot finish without its UpToDate source, so dropping the
// source is an unrecoverable data-availability loss.
//
// Scope is intentionally NARROW so it does not regress the prior
// campaign decision (bug-hunt #6): "deleting the last diskful is allowed
// like upstream" for the NO-SYNC-IN-FLIGHT case. The guard only fires
// when ALL of:
//   - the target replica is diskful (not diskless, not tiebreaker), and
//   - the target is currently the only UpToDate diskful replica, and
//   - at least one OTHER replica is a diskful mid-sync (SyncTarget /
//     Inconsistent — it would be stranded).
//
// When no peer is mid-sync the deletion proceeds (last legitimate
// diskful drop is allowed, matching upstream and bug-hunt #6).
//
// Override: an explicit `?force=true` bypasses the guard, mirroring
// upstream LINSTOR's force-flag escape hatch for operator-acknowledged
// destructive deletes.

// resourceMidSyncDeleteRefusal reports whether deleting `target`
// (already fetched) would strand a mid-sync peer per U130. It returns
// true when the guard MUST fire and the delete must be refused.
//
// `siblings` is the full replica set of the RD (including `target`).
func resourceMidSyncDeleteRefusal(target *apiv1.Resource, siblings []apiv1.Resource) bool {
	// A diskless / tiebreaker replica carries no UpToDate backing
	// storage; removing it never strands a SyncTarget's only source.
	if !resourceIsDiskful(target) {
		return false
	}

	if !resourceIsUpToDate(target) {
		// The target itself is not an UpToDate source — removing it
		// cannot remove "the last UpToDate source". Out of scope.
		return false
	}

	otherUpToDateDiskful := 0
	otherMidSyncDiskful := 0

	for i := range siblings {
		sib := &siblings[i]
		if sib.NodeName == target.NodeName {
			continue
		}

		if !resourceIsDiskful(sib) {
			continue
		}

		switch {
		case resourceIsUpToDate(sib):
			otherUpToDateDiskful++
		case resourceIsMidSync(sib):
			otherMidSyncDiskful++
		}
	}

	// Safe: another UpToDate diskful survives — the SyncTarget keeps a
	// valid source after the delete, or there is simply nothing in
	// flight to strand.
	if otherUpToDateDiskful > 0 {
		return false
	}

	// Refuse only when the target is the sole UpToDate source AND a
	// diskful peer is mid-sync that would be stranded by the delete.
	return otherMidSyncDiskful > 0
}

// resourceIsDiskful reports whether a replica carries real backing
// storage (not DISKLESS, not TIE_BREAKER).
func resourceIsDiskful(res *apiv1.Resource) bool {
	for _, f := range res.Flags {
		if f == apiv1.ResourceFlagDiskless || f == apiv1.ResourceFlagTieBreaker {
			return false
		}
	}

	return true
}

// resourceIsUpToDate reports whether any of the replica's volumes
// observed disk_state == "UpToDate". DRBD's per-volume disk state is
// the authoritative "this replica holds a complete, current copy"
// signal.
func resourceIsUpToDate(res *apiv1.Resource) bool {
	for i := range res.Volumes {
		if res.Volumes[i].State.DiskState == drbdDiskStateUpToDate {
			return true
		}
	}

	return false
}

// resourceIsMidSync reports whether the replica is a diskful copy still
// catching up: either its local disk_state is Inconsistent/SyncTarget,
// or it reports a SyncTarget replication-state toward any peer. Such a
// replica's ONLY source of truth is an UpToDate peer; losing that peer
// strands it.
func resourceIsMidSync(res *apiv1.Resource) bool {
	for i := range res.Volumes {
		vol := &res.Volumes[i]

		switch vol.State.DiskState {
		case drbdDiskStateInconsistent, drbdDiskStateSyncTarget:
			return true
		}

		for _, rep := range vol.State.ReplicationStates {
			if rep.ReplicationState == drbdReplStateSyncTarget {
				return true
			}
		}
	}

	return false
}

// DRBD-9 disk-state / replication-state tokens reported by `drbdsetup
// events2`. Raw strings (no shared constant exists in pkg/api/v1 — the
// state surface is free-form upstream).
const (
	drbdDiskStateUpToDate     = "UpToDate"
	drbdDiskStateInconsistent = "Inconsistent"
	drbdDiskStateSyncTarget   = "SyncTarget"
	drbdReplStateSyncTarget   = "SyncTarget"
)

// refuseLastUpToDateDiskfulMidSyncDelete is the U130 gate invoked from
// handleResourceDelete BEFORE the destructive store Delete. It lists
// the RD's replicas, evaluates resourceMidSyncDeleteRefusal, and on a
// positive verdict writes a structured 409 ApiCallRc refusal envelope.
//
// Returns true when the caller may proceed with the delete, false when
// the refusal has already been written.
//
// `force` short-circuits the guard (operator-acknowledged override).
func (s *Server) refuseLastUpToDateDiskfulMidSyncDelete(
	ctx context.Context, w http.ResponseWriter,
	rdName string, target *apiv1.Resource, force bool,
) bool {
	if force {
		return true
	}

	siblings, err := s.Store.Resources().ListByDefinition(ctx, rdName)
	if err != nil {
		// A list failure must not silently allow a destructive delete
		// the guard might otherwise have blocked. Surface the error.
		if errors.Is(err, store.ErrNotFound) {
			// RD vanished concurrently — let the downstream Delete's
			// own NotFound path produce the idempotent envelope.
			return true
		}

		writeStoreError(w, err)

		return false
	}

	if !resourceMidSyncDeleteRefusal(target, siblings) {
		return true
	}

	writeJSON(w, http.StatusConflict, []apiv1.APICallRc{{
		RetCode: apiCallRcError | apiCallRcFailInUse,
		Message: "Cannot delete the last UpToDate diskful replica of '" +
			rdName + "' on '" + target.NodeName +
			"' while another replica is still syncing.",
		Cause: "This is the only UpToDate copy and a peer replica is " +
			"mid-resync (SyncTarget/Inconsistent). Removing it would " +
			"strand the syncing peer with no source and leave the " +
			"resource with no UpToDate backing storage.",
		Correc: "Wait for the resync to finish (every diskful replica " +
			"UpToDate) and retry, or pass `?force=true` to override and " +
			"accept the data-availability risk.",
		ObjRefs: map[string]string{
			objRefRscDfn: rdName,
			objRefNode:   target.NodeName,
		},
	}})

	return false
}
