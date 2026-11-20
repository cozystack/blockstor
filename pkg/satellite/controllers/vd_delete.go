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

package controllers

import (
	"context"
	"slices"

	"github.com/cockroachdb/errors"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/naming"
	"github.com/cozystack/blockstor/pkg/satellite/intent"
)

// deletedVolumesFieldOwner is the SSA field manager of
// Status.DeletedVolumes. It is the field's only writer, so an entry it stops
// applying is gone.
const deletedVolumesFieldOwner = "blockstor-satellite-vd-delete"

// deleteFlaggedVolumes is this replica's part of a volume definition delete.
// It runs after an apply that went through, which left every volume
// definition flagged DELETE out of DRBD's configuration, deletes the volume
// of each one this replica has not confirmed yet, and then confirms it in
// Status.DeletedVolumes under the definition's deleteID. That flag is the
// only thing that deletes a volume of a replica that stays: a volume that is
// merely missing from the definition is never deleted here.
//
// Confirmations whose definition is no longer flagged, or flagged under
// another deleteID, are dropped in the same write, so an old one cannot
// later stand for a new volume of the same number.
func (r *ResourceReconciler) deleteFlaggedVolumes(ctx context.Context, res *blockstoriov1alpha1.Resource, rd *blockstoriov1alpha1.ResourceDefinition) error {
	keep, todo := r.planVolumeDeletes(res, rd)

	if len(todo) > 0 {
		req, err := r.deleteVolumesRequest(ctx, res, rd, todo)
		if err != nil {
			return err
		}

		err = r.Config.Apply.DeleteVolumes(ctx, req)
		if err != nil {
			return errors.Wrap(err, "delete the volumes of volume definitions flagged DELETE")
		}

		for _, vd := range todo {
			keep = append(keep, blockstoriov1alpha1.DeletedVolumeStatus{VolumeNumber: vd.VolumeNumber, DeleteID: vd.DeleteID})
		}
	}

	byNumber := func(a, b blockstoriov1alpha1.DeletedVolumeStatus) int { return int(a.VolumeNumber - b.VolumeNumber) }

	slices.SortFunc(keep, byNumber)

	current := slices.Clone(res.Status.DeletedVolumes)
	slices.SortFunc(current, byNumber)

	if slices.Equal(keep, current) {
		return nil
	}

	return r.applyDeletedVolumes(ctx, res, keep)
}

// recheckQuorumAfterFailedApply has DRBD recheck the resource's quorum
// when an apply failed after its `drbdadm adjust` had already removed the
// minor of a volume flagged DELETE. DeleteVolumes, which does that recheck
// otherwise, waits for an apply that goes through, and until then the
// remaining volumes of a Primary would stay suspended or failing their I/O.
func (r *ResourceReconciler) recheckQuorumAfterFailedApply(
	ctx context.Context,
	res *blockstoriov1alpha1.Resource,
	rd *blockstoriov1alpha1.ResourceDefinition,
	logger logr.Logger,
) {
	if !rdNeedsDRBD(rd) {
		return
	}

	_, todo := r.planVolumeDeletes(res, rd)

	req := &intent.DeleteVolumesRequest{Name: naming.Key(res.Spec.ResourceDefinitionName)}

	for _, vd := range todo {
		if vd.DRBDMinor != nil {
			req.Volumes = append(req.Volumes, intent.VolumeToDelete{VolumeNumber: vd.VolumeNumber, Minor: vd.DRBDMinor})
		}
	}

	if len(req.Volumes) == 0 {
		return
	}

	err := r.Config.Apply.RecheckQuorumAfterVolumeRemoval(ctx, req)
	if err == nil {
		return
	}

	logger.Info("quorum recheck after a volume removal failed", "error", err.Error())

	if r.Config.Recorder != nil {
		r.Config.Recorder.Eventf(res, nil, corev1.EventTypeWarning, volumeDeleteFailedReason, "DeleteVolume",
			"volume definition delete is waiting for this replica: %s", err.Error())
	}
}

// planVolumeDeletes splits the volume definitions flagged DELETE into the
// confirmations this replica keeps and the volumes it still has to delete.
func (r *ResourceReconciler) planVolumeDeletes(
	res *blockstoriov1alpha1.Resource, rd *blockstoriov1alpha1.ResourceDefinition,
) ([]blockstoriov1alpha1.DeletedVolumeStatus, []*blockstoriov1alpha1.ResourceDefinitionVolume) {
	var (
		keep []blockstoriov1alpha1.DeletedVolumeStatus
		todo []*blockstoriov1alpha1.ResourceDefinitionVolume
	)

	for i := range rd.Spec.VolumeDefinitions {
		vd := &rd.Spec.VolumeDefinitions[i]
		if !vd.Deleting() {
			continue
		}

		confirmed := slices.ContainsFunc(res.Status.DeletedVolumes, func(d blockstoriov1alpha1.DeletedVolumeStatus) bool {
			return d.VolumeNumber == vd.VolumeNumber && d.DeleteID == vd.DeleteID
		})
		if confirmed {
			keep = append(keep, blockstoriov1alpha1.DeletedVolumeStatus{VolumeNumber: vd.VolumeNumber, DeleteID: vd.DeleteID})
		} else {
			todo = append(todo, vd)
		}
	}

	return keep, todo
}

// deleteVolumesRequest builds the satellite's request for the volumes, with
// the ZFS rename suffix recorded first, as a replica delete does.
func (r *ResourceReconciler) deleteVolumesRequest(
	ctx context.Context,
	res *blockstoriov1alpha1.Resource,
	rd *blockstoriov1alpha1.ResourceDefinition,
	todo []*blockstoriov1alpha1.ResourceDefinitionVolume,
) (*intent.DeleteVolumesRequest, error) {
	req := &intent.DeleteVolumesRequest{Name: naming.Key(res.Spec.ResourceDefinitionName)}

	withDRBD := rdNeedsDRBD(rd)

	for _, vd := range todo {
		vol := intent.VolumeToDelete{VolumeNumber: vd.VolumeNumber, StoragePool: volumeDeletePool(res, rd, vd)}
		if withDRBD {
			vol.Minor = vd.DRBDMinor
		}

		if vol.StoragePool != "" && req.DeletedSuffix == "" {
			suffix, err := r.recordRenameSuffixFor(ctx, res, vol.StoragePool)
			if err != nil {
				return nil, err
			}

			req.DeletedSuffix = suffix
		}

		req.Volumes = append(req.Volumes, vol)
	}

	return req, nil
}

// volumeDeletePool is the pool the volume was created in, resolved the way
// the dispatcher resolves it for the create: the volume definition's own
// pool first, then the replica's, then the definition's. A diskless replica
// has no storage for the volume.
func volumeDeletePool(res *blockstoriov1alpha1.Resource, rd *blockstoriov1alpha1.ResourceDefinition, vd *blockstoriov1alpha1.ResourceDefinitionVolume) string {
	if slices.Contains(res.Spec.Flags, apiv1.ResourceFlagDiskless) {
		return ""
	}

	for _, pool := range []string{vd.Props["StorPoolName"], resolveDeleteStoragePool(res), rd.Spec.Props["StorPoolName"]} {
		if pool != "" {
			return pool
		}
	}

	return ""
}

// applyDeletedVolumes writes the confirmations as this replica's whole
// Status.DeletedVolumes.
func (r *ResourceReconciler) applyDeletedVolumes(ctx context.Context, res *blockstoriov1alpha1.Resource, deleted []blockstoriov1alpha1.DeletedVolumeStatus) error {
	apply := &blockstoriov1alpha1.Resource{
		TypeMeta: metav1.TypeMeta{
			Kind:       resourceKind,
			APIVersion: blockstoriov1alpha1.GroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{Name: res.Name},
		Status:     blockstoriov1alpha1.ResourceStatus{DeletedVolumes: deleted},
	}

	err := r.Status().Patch(ctx, apply,
		client.Apply, //nolint:staticcheck // SA1019: applyconfiguration-gen output not yet available for our CRDs
		client.FieldOwner(deletedVolumesFieldOwner), client.ForceOwnership)
	if err != nil {
		return errors.Wrap(err, "ssa Status.DeletedVolumes")
	}

	res.Status.DeletedVolumes = deleted

	return nil
}
