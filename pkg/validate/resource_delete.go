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
	"slices"
	"strings"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/naming"
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
		if naming.Same(sib.NodeName, target.NodeName) || !ResourceIsDiskful(sib) {
			continue
		}

		// A peer already on its way out is not stranded by losing its
		// source: nothing will ever read what it was syncing.
		if slices.Contains(sib.Flags, apiv1.ResourceFlagDelete) {
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

// DefinitionBeingDeleted reports whether the whole definition is being
// taken apart, as opposed to one replica of a volume that stays. replicas
// are the definition's replicas as they stand.
//
// MidSyncDeleteRefusal protects the data of a volume that outlives the
// delete. LINSTOR has no such check on any delete path; blockstor keeps it
// for a single replica dropped from a live volume, and nowhere else. When
// every replica is going, a peer left Inconsistent is about to go too, and
// refusing the source only strands the delete: the volume can then never
// be removed at all.
//
// Two shapes say the definition is going. The DELETE flag is LINSTOR's own
// mark for `rd d`, and it settles the question. linstor-csi's DeleteVolume
// never sends `rd d` first: it sets BalanceResourcesEnabled=False on the
// definition, waits until no replica is InUse, then deletes the replicas
// one by one and the volume definitions after them; the definition itself
// goes only when no snapshot still needs it (pkg/client/linstor.go,
// Delete). That property write is the only notice the replica deletes get
// that they are one volume teardown.
//
// The same property is what an operator sets to place replicas by hand,
// and a hand placement is exactly where U130 happens: a second diskful
// added next to a diskless Primary and the original dropped before the
// sync finishes. So the property alone counts only while nothing is
// InUse, which is the state DeleteVolume waits for and a volume with a
// Primary is never in.
func DefinitionBeingDeleted(rd *apiv1.ResourceDefinition, replicas []apiv1.Resource) bool {
	if rd == nil {
		return false
	}

	if slices.Contains(rd.Flags, apiv1.ResourceFlagDelete) {
		return true
	}

	if !strings.EqualFold(rd.Props[apiv1.PropBalanceResourcesEnabled], "false") {
		return false
	}

	return !slices.ContainsFunc(replicas, func(res apiv1.Resource) bool {
		return res.State.InUse != nil && *res.State.InUse
	})
}

// SortForDeletion orders replicas the way they are safe to delete one at a
// time: the diskful replicas that are not complete copies yet first, every
// other replica after them in its original order.
//
// It matters where the mid-sync guard runs between two deletes, which is
// `r d` naming several nodes: naming a source before the replica syncing
// from it would have the guard refuse the source over a peer the same
// command was about to delete. Once the incomplete replica is deleted it
// carries DELETE, and MidSyncDeleteRefusal no longer counts it. It does not
// order the teardown itself: a delete only stamps the replica, and the
// satellites take DRBD down in parallel.
func SortForDeletion(replicas []apiv1.Resource) {
	slices.SortStableFunc(replicas, func(a, b apiv1.Resource) int {
		return deletionRank(&a) - deletionRank(&b)
	})
}

func deletionRank(res *apiv1.Resource) int {
	if ResourceIsDiskful(res) && resourceIsMidSync(res) {
		return 0
	}

	return 1
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
