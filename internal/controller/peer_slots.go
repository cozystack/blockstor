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

package controller

import (
	"context"
	"slices"
	"strings"

	cerrors "github.com/cockroachdb/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/naming"
	"github.com/cozystack/blockstor/pkg/store"
)

// errAwaitingPeerSlots holds back the identities of a new replica of a
// definition that records no peer-slot count while none of its replicas
// with a disk has told the count its metadata holds. Stamped with a
// guess, the replica would keep the guess for good; the report of a
// sibling re-runs its reconcile (enqueueSiblings).
var errAwaitingPeerSlots = cerrors.New("awaiting the peer slots of the definition's replicas")

// knownPeerSlots is what the replicas of a definition tell of their
// peer slots: the least count among those with a disk that recorded or
// reported one, and whether a replica with a disk that predates the
// record has not told its count yet. A new replica not stamped yet tells
// nothing and holds nothing up: it gets what the others tell.
type knownPeerSlots struct {
	least  int32
	untold bool
}

// replicaKnownPeerSlots is the count a replica is known to have: its
// recorded one, else the one its satellite read from its superblock.
func replicaKnownPeerSlots(res *blockstoriov1alpha1.Resource) *int32 {
	if res.Spec.PeerSlots != nil {
		return res.Spec.PeerSlots
	}

	return res.Status.MetadataPeerSlots
}

// replicaKeepsDisk reports a replica with a disk: neither DISKLESS nor a
// TIE_BREAKER witness.
func replicaKeepsDisk(res *blockstoriov1alpha1.Resource) bool {
	return !slices.Contains(res.Spec.Flags, apiv1.ResourceFlagDiskless) &&
		!slices.Contains(res.Spec.Flags, apiv1.ResourceFlagTieBreaker)
}

// definitionKnownPeerSlots collects what the replicas of rdName with a
// disk, other than skip, tell of their peer slots, from the cache.
func (r *ResourceReconciler) definitionKnownPeerSlots(ctx context.Context, rdName, skip string) (knownPeerSlots, error) {
	replicas, err := r.cachedResources(ctx)
	if err != nil {
		return knownPeerSlots{}, cerrors.Wrapf(err, "list the replicas of %q for their peer slots", rdName)
	}

	var known knownPeerSlots

	for i := range replicas {
		res := &replicas[i]
		if res.Name == skip || !naming.Same(res.Spec.ResourceDefinitionName, rdName) || !replicaKeepsDisk(res) {
			continue
		}

		slots := replicaKnownPeerSlots(res)
		if slots == nil {
			known.untold = known.untold || res.PeerSlotsPredateRecord()

			continue
		}

		if known.least == 0 || *slots < known.least {
			known.least = *slots
		}
	}

	return known, nil
}

// peerSlotsToStamp is the count stamped on target, which records none,
// nil to leave it unrecorded for now.
//
// A replica that predates the record keeps the count its metadata was
// created with, which only its superblock tells: blockstor created it
// with 15, LINSTOR with its definition's count, 7 by default, and a
// replica adopted by an older linstor-migrate carries no count. Its
// satellite reads the superblock (Status.MetadataPeerSlots); until then
// it is left unrecorded and read as 15, as before. One without a disk
// has no superblock and gets its definition's count once that is known.
//
// A new replica gets its definition's count, as LINSTOR copies it to
// every replica it creates. A definition that records none gets the
// least count its replicas with a disk tell, the count LINSTOR created
// an adopted definition's replicas with; while a replica that predates
// the record has not told its count and none told one, the new replica
// waits (errAwaitingPeerSlots) rather than be stamped 15 for good. A
// definition with no such replica keeps reading as 15, the count
// blockstor gave every replica before it recorded counts.
func (r *ResourceReconciler) peerSlotsToStamp(
	ctx context.Context, target *blockstoriov1alpha1.Resource, rd *blockstoriov1alpha1.ResourceDefinition, predatesRecord bool,
) (*int32, error) {
	if rd == nil {
		return nil, nil //nolint:nilnil // no definition to decide from yet
	}

	if predatesRecord && replicaKeepsDisk(target) {
		return copyI32(target.Status.MetadataPeerSlots), nil
	}

	restored, err := r.restoredReplicaPeerSlots(ctx, target, rd)
	if err != nil || restored != nil {
		return restored, err
	}

	if rd.Spec.PeerSlots != nil {
		return copyI32(rd.Spec.PeerSlots), nil
	}

	known, err := r.definitionKnownPeerSlots(ctx, rd.Name, target.Name)
	if err != nil {
		return nil, err
	}

	switch {
	case known.least > 0:
		return &known.least, nil
	case predatesRecord:
		return nil, nil //nolint:nilnil // a witness waits for its definition's count
	case known.untold && replicaKeepsDisk(target):
		return nil, cerrors.Wrapf(errAwaitingPeerSlots, "replica %q", target.Name)
	case known.untold:
		// A new diskless replica is not held back: it creates no
		// metadata, and is stamped once the count is known.
		return nil, nil //nolint:nilnil // stamped once the count is known
	default:
		v := drbd.EffectivePeerSlots(nil)

		return &v, nil
	}
}

// restoredReplicaPeerSlots is the count of a new diskful replica of a
// definition restored or cloned from a snapshot, nil when it gets its
// definition's: the count of the source's replica on the same node,
// whose snapshot the replica's volume is cloned from.
//
// The clone carries that replica's superblock, and the metadata probe
// keeps its layout with no more slots than the replica's own count. The
// restored definition takes the source definition's current count,
// which differs from the replica's once the source was modified after
// the replica was created, as replicas keep theirs. Stamped with the
// definition's count, the replica can record more slots than the
// metadata the probe keeps: a grow sized for the record leaves the
// device larger than the definition, and the placement check counts
// slots the metadata does not have. LINSTOR gives the
// restored replica the snapshot replica's count (RscDrbdLayerHelper,
// v1.33.2).
//
// A replica's count never changes, so the live source replica's count
// is the one its snapshot holds; the superblock's own read comes first,
// as it is the truth for a replica an older satellite created. With no
// such replica left, the replica gets its definition's count.
func (r *ResourceReconciler) restoredReplicaPeerSlots(
	ctx context.Context, target *blockstoriov1alpha1.Resource, rd *blockstoriov1alpha1.ResourceDefinition,
) (*int32, error) {
	source, _, restored := strings.Cut(rd.Spec.Props[store.RestoreFromSnapshotProp], ":")
	if !restored || source == "" || !replicaKeepsDisk(target) {
		return nil, nil //nolint:nilnil // not a restored replica with a disk
	}

	replicas, err := r.cachedResources(ctx)
	if err != nil {
		return nil, cerrors.Wrapf(err, "list the replicas of %q for the peer slots of its restore", source)
	}

	for i := range replicas {
		res := &replicas[i]
		if !naming.Same(res.Spec.ResourceDefinitionName, source) || !naming.Same(res.Spec.NodeName, target.Spec.NodeName) ||
			!replicaKeepsDisk(res) {
			continue
		}

		if slots := res.Status.MetadataPeerSlots; slots != nil {
			return copyI32(slots), nil
		}

		return copyI32(res.Spec.PeerSlots), nil
	}

	return nil, nil //nolint:nilnil // no source replica left on the node
}

// recordDefinitionPeerSlots records on a definition that records no
// peer-slot count the least count its replicas with a disk hold, once
// every one of them that predates the record has reported its own. That is the count
// LINSTOR gave the definition of a replica set adopted by a
// linstor-migrate older than the count, and 15 for one blockstor
// created before it recorded counts; a replica created later gets it.
func (r *ResourceReconciler) recordDefinitionPeerSlots(ctx context.Context, rdName string) error {
	var rd blockstoriov1alpha1.ResourceDefinition

	err := r.Get(ctx, client.ObjectKey{Name: naming.Key(rdName)}, &rd)
	if err != nil {
		return cerrors.Wrapf(client.IgnoreNotFound(err), "read %q for its peer slots", rdName)
	}

	if rd.Spec.PeerSlots != nil {
		return nil
	}

	known, err := r.definitionKnownPeerSlots(ctx, rdName, "")
	if err != nil || known.untold || known.least == 0 {
		return err
	}

	base := rd.DeepCopy()
	rd.Spec.PeerSlots = &known.least

	err = r.Patch(ctx, &rd, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}))
	if err != nil {
		return cerrors.Wrapf(client.IgnoreNotFound(err), "record the peer slots of %q", rdName)
	}

	return nil
}
