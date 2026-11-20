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

package satellite

import (
	"context"

	"github.com/cockroachdb/errors"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/naming"
	"github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
)

// DeleteVolumes deletes volumes of a replica that stays: the ones whose
// volume definition is flagged DELETE, after an apply that already left them
// out of DRBD's configuration. Each volume goes the way a replica delete
// takes all of them, top down: out of the kernel, then the LUKS mapper, then
// the storage, through the same provider delete, so ZFS renames a zvol that
// still has snapshots instead of destroying it.
//
// It stops at the first volume it could not delete and reports it; nothing
// after a failed step runs, since deleting storage DRBD still holds is the
// one thing this must never try. Every step is idempotent, so the retry
// starts over.
//
// Once the volumes are out of DRBD, DRBD rechecks the resource's quorum, so
// the replica that removes a volume last does not keep the volumes that
// stay suspended, or failing their I/O under on-no-quorum io-error.
func (r *Reconciler) DeleteVolumes(ctx context.Context, req *intent.DeleteVolumesRequest) error {
	err := r.removeVolumeMinors(ctx, req)
	if err != nil {
		return err
	}

	for _, vol := range req.Volumes {
		if r.cfg.Cryptsetup != nil {
			// Best effort, as in DeleteResource: a mapper that was never
			// opened is the common case, and one that is still open makes
			// the storage delete below fail on its own.
			_ = r.cfg.Cryptsetup.Close(ctx, luksMapperName(req.Name, vol.VolumeNumber))
		}

		if vol.StoragePool == "" {
			continue
		}

		provider, ok := r.provider(vol.StoragePool)
		if !ok {
			return errors.Errorf("storage pool %q is not registered on this satellite; volume %s/%d is not deleted yet",
				vol.StoragePool, req.Name, vol.VolumeNumber)
		}

		target := storage.Volume{ResourceName: req.Name, VolumeNumber: vol.VolumeNumber}

		if suffixed, ok := provider.(storage.SuffixedDeleter); ok {
			err = suffixed.DeleteVolumeWithSuffix(ctx, target, req.DeletedSuffix)
		} else {
			err = provider.DeleteVolume(ctx, target)
		}

		if err != nil {
			return errors.Wrapf(err, "delete volume %s/%d", req.Name, vol.VolumeNumber)
		}
	}

	return nil
}

// removeVolumeMinors takes every requested volume out of DRBD, then has
// DRBD recheck the resource's quorum, which a volume's removal can leave
// stale.
func (r *Reconciler) removeVolumeMinors(ctx context.Context, req *intent.DeleteVolumesRequest) error {
	withDRBD := false

	for _, vol := range req.Volumes {
		err := r.removeVolumeMinor(ctx, req.Name, vol)
		if err != nil {
			return err
		}

		withDRBD = withDRBD || vol.Minor != nil
	}

	if r.cfg.Adm == nil || !withDRBD {
		return nil
	}

	return r.recheckQuorum(ctx, req.Name)
}

// RecheckQuorumAfterVolumeRemoval rechecks the resource's quorum when a
// requested volume's minor is already out of the kernel, for an apply that
// failed after its `drbdadm adjust` removed the minor: DeleteVolumes runs
// only after an apply that went through, and the remaining volumes must not
// stay held for as long as the apply keeps failing.
func (r *Reconciler) RecheckQuorumAfterVolumeRemoval(ctx context.Context, req *intent.DeleteVolumesRequest) error {
	if r.cfg.Adm == nil {
		return nil
	}

	for _, vol := range req.Volumes {
		if vol.Minor == nil {
			continue
		}

		owner, err := r.cfg.Adm.ResourceOwningMinor(ctx, *vol.Minor)
		if err != nil {
			return errors.Wrapf(err, "find the owner of DRBD minor %d", *vol.Minor)
		}

		if !naming.Same(owner, req.Name) {
			return r.recheckQuorum(ctx, req.Name)
		}
	}

	return nil
}

// recheckQuorum has DRBD recompute the quorum verdict a removed volume left
// behind (see drbd.Adm.RecheckQuorumAfterVolumeRemoval).
func (r *Reconciler) recheckQuorum(ctx context.Context, name string) error {
	done, err := r.cfg.Adm.RecheckQuorumAfterVolumeRemoval(ctx, name)
	if err != nil {
		return errors.Wrapf(err, "recheck the quorum of %s after its volumes were removed", name)
	}

	if done == drbd.QuorumRecheckRan {
		log.FromContext(ctx).Info("had DRBD recompute quorum after a volume removal", "resource", name)
	}

	return nil
}

// removeVolumeMinor makes sure the volume's DRBD device is gone before its
// storage is touched. After a re-render without the volume `drbdadm adjust`
// has removed it already; when the volume was the resource's last one no
// .res is rendered and nothing has, so it is removed here. A minor the kernel
// gives to another resource is not this volume's any more and is left alone.
func (r *Reconciler) removeVolumeMinor(ctx context.Context, name string, vol intent.VolumeToDelete) error {
	if r.cfg.Adm == nil || vol.Minor == nil {
		return nil
	}

	owner, err := r.cfg.Adm.ResourceOwningMinor(ctx, *vol.Minor)
	if err != nil {
		return errors.Wrapf(err, "find the owner of DRBD minor %d", *vol.Minor)
	}

	if owner == "" || !naming.Same(owner, name) {
		return nil
	}

	err = r.cfg.Adm.RemoveMinor(ctx, *vol.Minor)
	if err != nil {
		return errors.Wrapf(err, "remove volume %s/%d from DRBD", name, vol.VolumeNumber)
	}

	return nil
}
