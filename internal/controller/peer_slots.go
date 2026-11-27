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
	"time"

	cerrors "github.com/cockroachdb/errors"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

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

// awaitingPeerSlotsError is errAwaitingPeerSlots naming the replicas
// whose count is still missing, so the wait can say what it waits for.
type awaitingPeerSlotsError struct {
	untold []string
}

func (e *awaitingPeerSlotsError) Error() string {
	return errAwaitingPeerSlots.Error() + ": " + strings.Join(e.untold, ", ")
}

func (e *awaitingPeerSlotsError) Is(target error) bool {
	return target == errAwaitingPeerSlots //nolint:errorlint // identity of the sentinel
}

// knownPeerSlots is what the replicas of a definition tell of their
// peer slots: the least count among those with a disk that recorded or
// reported one, and whether a replica with a disk that predates the
// record has not told its count yet. A new replica not stamped yet tells
// nothing and holds nothing up: it gets what the others tell.
type knownPeerSlots struct {
	least  int32
	untold []string
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
			if res.PeerSlotsPredateRecord() {
				known.untold = append(known.untold, res.Name)
			}

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
	case len(known.untold) > 0 && replicaKeepsDisk(target):
		return nil, &awaitingPeerSlotsError{untold: known.untold}
	case len(known.untold) > 0:
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
	if err != nil || len(known.untold) > 0 || known.least == 0 {
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

// awaitingPeerSlotsReason is the reason of the Warning Event and of
// the PeerSlotsKnown condition a new replica gets while it waits for the
// peer slots of its definition's replicas.
const awaitingPeerSlotsReason = "AwaitingPeerSlots"

// awaitingPeerSlotsLimit is how long a new replica waits for the peer
// slots of its definition's replicas before its status says so
// (ConditionPeerSlotsKnown). Its satellite says only that its node id
// is not allocated, after a minute, which does not say why.
const awaitingPeerSlotsLimit = 5 * time.Minute

// peerSlotsFieldOwner is the SSA field manager of the PeerSlotsKnown
// condition, so the controller owns that entry alone and the
// satellite's conditions on the same list stay as they are.
const peerSlotsFieldOwner = "blockstor-controller-peer-slots"

// awaitPeerSlots reports a new replica held back by errAwaitingPeerSlots
// and returns when to look again. Every pass logs and raises a Warning
// Event naming the replicas whose count is missing. Past
// awaitingPeerSlotsLimit the replica's status carries
// PeerSlotsKnown=False with the same message, and the pass is requeued
// at the limit so that happens even when nothing else changes.
//
// The wait itself goes on: stamped with a guess, the replica would keep
// the guess for good, and its metadata would be created with it.
func (r *ResourceReconciler) awaitPeerSlots(
	ctx context.Context, target *blockstoriov1alpha1.Resource, untold []string,
) (ctrl.Result, error) {
	message := "no DRBD node id until the peer slots of " + strings.Join(untold, ", ") +
		" are known: their satellites read them from the superblock, which needs the satellite on this " +
		"version, its node online and dump-md to succeed"

	waited := time.Duration(0)
	if !target.CreationTimestamp.IsZero() {
		waited = time.Since(target.CreationTimestamp.Time)
	}

	logger := logf.FromContext(ctx).WithValues("resource", target.Name, "untold", untold, "waited", waited.String())
	logger.Info("a new replica waits for the peer slots its definition's replicas hold")

	if r.Recorder != nil {
		r.Recorder.Eventf(target, nil, corev1.EventTypeWarning, awaitingPeerSlotsReason, "AllocateNodeID", message)
	}

	if waited < awaitingPeerSlotsLimit {
		return ctrl.Result{RequeueAfter: awaitingPeerSlotsLimit - waited}, nil
	}

	logger.Error(errAwaitingPeerSlots, "a new replica waited past the limit for the peer slots of its definition's replicas",
		"limit", awaitingPeerSlotsLimit.String())

	err := r.applyPeerSlotsKnown(ctx, target, metav1.ConditionFalse, awaitingPeerSlotsReason,
		message+"; waiting since "+target.CreationTimestamp.UTC().Format(time.RFC3339))
	if err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: awaitingPeerSlotsLimit}, nil
}

// settlePeerSlotsKnown turns the PeerSlotsKnown condition of a replica
// that waited past the limit True once it has its count.
func (r *ResourceReconciler) settlePeerSlotsKnown(ctx context.Context, target *blockstoriov1alpha1.Resource) error {
	if !apimeta.IsStatusConditionFalse(target.Status.Conditions, blockstoriov1alpha1.ConditionPeerSlotsKnown) {
		return nil
	}

	return r.applyPeerSlotsKnown(ctx, target, metav1.ConditionTrue, "PeerSlotsRecorded",
		"the replica has its peer-slot count")
}

func (r *ResourceReconciler) applyPeerSlotsKnown(
	ctx context.Context, target *blockstoriov1alpha1.Resource, status metav1.ConditionStatus, reason, message string,
) error {
	apply := &blockstoriov1alpha1.Resource{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Resource",
			APIVersion: blockstoriov1alpha1.GroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{Name: target.Name},
		Status: blockstoriov1alpha1.ResourceStatus{
			Conditions: []metav1.Condition{{
				Type:               blockstoriov1alpha1.ConditionPeerSlotsKnown,
				Status:             status,
				Reason:             reason,
				Message:            message,
				LastTransitionTime: metav1.Now(),
			}},
		},
	}

	err := r.Status().Patch(ctx, apply,
		client.Apply, //nolint:staticcheck // SA1019: applyconfiguration-gen output not yet available for our CRDs
		client.FieldOwner(peerSlotsFieldOwner))
	if err != nil {
		return cerrors.Wrapf(client.IgnoreNotFound(err), "set the %s condition of %q",
			blockstoriov1alpha1.ConditionPeerSlotsKnown, target.Name)
	}

	return nil
}
