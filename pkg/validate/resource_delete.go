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

package validate

import (
	"errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// ErrLastSyncSourceDelete refuses removing the only replica a mid-sync peer
// can resync from.
var ErrLastSyncSourceDelete = errors.New(
	"refusing to delete the last complete copy while a peer is still syncing from it")

// DRBD state strings this guard reasons about.
const (
	diskStateUpToDate     = "UpToDate"
	diskStateInconsistent = "Inconsistent"
	diskStateSyncTarget   = "SyncTarget"
	replStateSyncTarget   = "SyncTarget"
	replStateSyncSource   = "SyncSource"
)

// MidSyncDeleteRefusal reports whether deleting target would strand a peer
// that is still resyncing from it. Upstream calls this U130: "LINSTOR
// shouldn't allow deleting the last active peer."
//
// The shape: one diskful replica plus a diskless Primary, a second diskful is
// added and starts as SyncTarget/Inconsistent, and the original diskful is
// deleted before the resync finishes. The SyncTarget is left with no source,
// the deleting node stays Connecting, and the Primary sits on a diskless
// replica with no complete copy anywhere in the cluster.
//
// The judgement is deliberately asymmetric. A replica the kernel positively
// reports as not-a-complete-copy is out of scope, but a diskful replica whose
// disk state has not been projected yet counts as a possible last source:
// concluding "not a source" from an unknown projection is precisely the
// false-allow that strands the peer.
func MidSyncDeleteRefusal(target *apiv1.Resource, siblings []apiv1.Resource) bool {
	if !ResourceIsDiskful(target) {
		return false
	}

	if !resourceIsPossibleUpToDateSource(target) {
		return false
	}

	confirmedSources := 0
	midSyncPeers := 0

	for i := range siblings {
		sib := &siblings[i]
		if sib.NodeName == target.NodeName || !ResourceIsDiskful(sib) {
			continue
		}

		switch {
		case resourceIsConfirmedUpToDateSource(sib):
			confirmedSources++
		case resourceIsMidSync(sib):
			midSyncPeers++
		}
	}

	// A kernel-confirmed complete copy survives the delete, so any peer
	// still syncing keeps a source.
	if confirmedSources > 0 {
		return false
	}

	return midSyncPeers > 0
}

// ResourceIsDiskful reports whether a replica carries real backing storage
// (not DISKLESS, not TIE_BREAKER).
func ResourceIsDiskful(res *apiv1.Resource) bool {
	for _, f := range res.Flags {
		if f == apiv1.ResourceFlagDiskless || f == apiv1.ResourceFlagTieBreaker {
			return false
		}
	}

	return true
}

// resourceIsConfirmedUpToDateSource reports whether the kernel positively
// confirms the replica holds a complete copy that can serve a resync: its
// disk state is UpToDate, or it reports SyncSource toward a peer. The second
// signal is ground truth — DRBD only feeds a peer's resync from an UpToDate
// local disk — so it stands in for a lagging or blank disk-state projection.
func resourceIsConfirmedUpToDateSource(res *apiv1.Resource) bool {
	return resourceIsUpToDate(res) || resourceIsSyncSource(res)
}

// resourceIsPossibleUpToDateSource widens the above with the fail-safe case:
// a diskful replica whose disk state has not been projected yet might still
// be the last source, so the guard stays armed for it.
func resourceIsPossibleUpToDateSource(res *apiv1.Resource) bool {
	if resourceIsConfirmedUpToDateSource(res) {
		return true
	}

	return !resourceHasKnownDiskState(res)
}

func resourceHasKnownDiskState(res *apiv1.Resource) bool {
	for i := range res.Volumes {
		if res.Volumes[i].State.DiskState != "" {
			return true
		}
	}

	return false
}

func resourceIsSyncSource(res *apiv1.Resource) bool {
	for i := range res.Volumes {
		for _, rep := range res.Volumes[i].State.ReplicationStates {
			if rep.ReplicationState == replStateSyncSource {
				return true
			}
		}
	}

	return false
}

func resourceIsUpToDate(res *apiv1.Resource) bool {
	for i := range res.Volumes {
		if res.Volumes[i].State.DiskState == diskStateUpToDate {
			return true
		}
	}

	return false
}

// resourceIsMidSync reports whether the replica is a diskful copy still
// catching up. Such a replica's only source of truth is a complete peer;
// losing that peer strands it.
func resourceIsMidSync(res *apiv1.Resource) bool {
	for i := range res.Volumes {
		vol := &res.Volumes[i]

		switch vol.State.DiskState {
		case diskStateInconsistent, diskStateSyncTarget:
			return true
		}

		for _, rep := range vol.State.ReplicationStates {
			if rep.ReplicationState == replStateSyncTarget {
				return true
			}
		}
	}

	return false
}
