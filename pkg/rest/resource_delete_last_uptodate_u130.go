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
//
// This is a data-availability guard, so it FAILS SAFE: when it cannot
// positively confirm that another UpToDate copy would survive the
// delete, it refuses. The load-bearing safety property is "never drop
// the last source of a syncing peer" — a false refusal merely asks the
// operator to wait or pass `?force=true`, whereas a false allow strands
// the SyncTarget with no UpToDate source (unrecoverable data loss).
//
// BUG-045 hardening: the decision must NOT rest solely on the
// possibly-lagging CRD `diskState` projection. The satellite observer
// can leave a live source's `diskState` blank (a Secondary SyncSource
// whose local `device` frame's `disk:UpToDate` was not re-stamped),
// while the SyncSource replication-state — emitted on the reliably-
// delivered peer-device frame — is present. The guard therefore treats
// a SyncSource replication-state as kernel ground-truth evidence that a
// replica holds an UpToDate copy, and treats an unknown/empty diskState
// on a diskful replica conservatively rather than as "not a source".
func resourceMidSyncDeleteRefusal(target *apiv1.Resource, siblings []apiv1.Resource) bool {
	// A diskless / tiebreaker replica carries no UpToDate backing
	// storage; removing it never strands a SyncTarget's only source.
	if !resourceIsDiskful(target) {
		return false
	}

	// The target must be a plausible last source. A diskful replica
	// that the kernel confirms is UpToDate or SyncSource clearly is.
	// A diskful replica whose diskState is empty/unknown is ALSO treated
	// as a possible source here (fail-safe): the BUG-045 race leaves a
	// real source's diskState blank, and concluding "not a source" on an
	// unknown projection is exactly the false-allow that strands the
	// SyncTarget. Only a target the kernel positively reports as NOT a
	// complete copy (Inconsistent / SyncTarget / Diskless / Outdated /
	// Failed) is out of scope.
	if !resourceIsPossibleUpToDateSource(target) {
		return false
	}

	otherConfirmedSources := 0
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
		case resourceIsConfirmedUpToDateSource(sib):
			// Kernel-confirmed surviving source: UpToDate diskState or a
			// SyncSource replication-state. The SyncTarget keeps a valid
			// source after the delete.
			otherConfirmedSources++
		case resourceIsMidSync(sib):
			otherMidSyncDiskful++
		}
	}

	// Safe: a kernel-confirmed UpToDate/SyncSource diskful survives —
	// the SyncTarget keeps a valid source after the delete, or there is
	// simply nothing in flight to strand.
	if otherConfirmedSources > 0 {
		return false
	}

	// Refuse when the target is the sole surviving source AND a diskful
	// peer is mid-sync that would be stranded by the delete.
	return otherMidSyncDiskful > 0
}

// resourceIsConfirmedUpToDateSource reports whether the kernel
// positively confirms the replica holds a complete, current copy that
// can serve a resync: its local disk_state is UpToDate, OR it reports a
// SyncSource replication-state toward a peer. The SyncSource signal is
// kernel ground truth — DRBD only lets a node feed a peer's resync from
// an UpToDate local disk — so it stands in for a lagging/blank diskState
// projection (BUG-045).
func resourceIsConfirmedUpToDateSource(res *apiv1.Resource) bool {
	return resourceIsUpToDate(res) || resourceIsSyncSource(res)
}

// resourceIsPossibleUpToDateSource reports whether the replica might be
// the last UpToDate source and so must keep the guard armed. It is the
// fail-safe widening of resourceIsConfirmedUpToDateSource: in addition
// to kernel-confirmed sources it also returns true for a diskful replica
// whose diskState is empty/unknown (the BUG-045 unstamped-projection
// case). Only a replica the kernel positively reports as NOT a complete
// copy is excluded.
func resourceIsPossibleUpToDateSource(res *apiv1.Resource) bool {
	if resourceIsConfirmedUpToDateSource(res) {
		return true
	}

	// An empty/unknown diskState on a diskful replica is treated
	// conservatively as "might be a source": refusing in doubt is the
	// fail-safe choice for a last-copy delete. A replica the kernel
	// positively reports mid-sync or not-a-copy is excluded by
	// resourceHasKnownDiskState being true with a non-UpToDate value.
	return !resourceHasKnownDiskState(res)
}

// resourceHasKnownDiskState reports whether any of the replica's volumes
// carries a non-empty kernel-observed disk_state. False means the
// observer has not (yet) projected a disk_state for the replica — the
// BUG-045 unstamped-projection window — which the last-copy guard treats
// conservatively.
func resourceHasKnownDiskState(res *apiv1.Resource) bool {
	for i := range res.Volumes {
		if res.Volumes[i].State.DiskState != "" {
			return true
		}
	}

	return false
}

// resourceIsSyncSource reports whether the replica reports a SyncSource
// replication-state toward any peer on any volume. A SyncSource is, by
// DRBD invariant, holding an UpToDate local copy (it is actively feeding
// a peer's resync), so this is authoritative evidence the replica is a
// valid source even when the diskState projection lags or is blank.
func resourceIsSyncSource(res *apiv1.Resource) bool {
	for i := range res.Volumes {
		for _, rep := range res.Volumes[i].State.ReplicationStates {
			if rep.ReplicationState == drbdReplStateSyncSource {
				return true
			}
		}
	}

	return false
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
	drbdReplStateSyncSource   = "SyncSource"
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
