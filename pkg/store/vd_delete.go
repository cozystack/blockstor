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

package store

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"

	"github.com/google/uuid"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// A volume definition delete runs the way LINSTOR runs it: the definition is
// flagged DELETE, every replica's satellite deletes its own volume, and the
// definition leaves the resource definition only once every replica has
// confirmed. MarkVolumeDefinitionDeleted is the first step, the one an
// operator's `vd d` takes; FinishVolumeDefinitionDelete is the last, which
// the REST door tries at once and the controller retries until it holds.

// ErrVolumeDefinitionNotDeleting is returned by FinishVolumeDefinitionDelete
// for a volume definition nobody asked to delete. Removing it would be a
// delete nobody requested.
var ErrVolumeDefinitionNotDeleting = errors.New("volume definition is not flagged DELETE")

// MarkVolumeDefinitionDeleted flags the volume definition DELETE and gives
// this delete its deleteID, the value the replicas' confirmations must
// carry. A definition that is flagged already keeps its deleteID, so a
// repeated `vd d` joins the delete in flight instead of starting another one
// whose confirmations the first one's replicas would then have to repeat.
//
// It returns the definition as stored and whether this call set the flag.
func MarkVolumeDefinitionDeleted(ctx context.Context, st Store, rdName string, vn int32) (apiv1.VolumeDefinition, bool, error) {
	var (
		out    apiv1.VolumeDefinition
		marked bool
	)

	err := st.VolumeDefinitions().PatchVolumeDefinitionSpec(ctx, rdName, vn, func(vd *apiv1.VolumeDefinition) error {
		marked = false

		if !vd.Deleting() {
			vd.Flags = append(slices.Clone(vd.Flags), apiv1.VolumeDefinitionFlagDelete)
			vd.DeleteID = uuid.NewString()
			marked = true
		}

		out = *vd

		return nil
	})
	if err != nil {
		return apiv1.VolumeDefinition{}, false, fmt.Errorf("flag volume definition %s/%d DELETE: %w", rdName, vn, err)
	}

	return out, marked, nil
}

// VolumeDefinitionDeleteProgress is where a volume definition delete stands.
type VolumeDefinitionDeleteProgress struct {
	// Removed is true once the definition is gone from its resource
	// definition: every replica confirmed, or it was gone already.
	Removed bool

	// Confirmed names the nodes whose replica has deleted its volume.
	Confirmed []string

	// Pending names the nodes whose replica has not confirmed yet. A
	// replica that is itself being deleted stays here until it is gone:
	// its own delete takes the volume with it, and only its disappearance
	// says that happened.
	Pending []string
}

// FinishVolumeDefinitionDelete removes the flagged volume definition from
// its resource definition once every replica of the definition confirmed it
// deleted its volume under the definition's deleteID, and otherwise reports
// which replicas it is waiting for. A replica whose satellite is down, or
// that never confirms, holds the definition: LINSTOR removes it anyway and
// leaves that node's volume on disk with nothing that would ever delete it.
// `node lost` is the way to let a node go for good, and it takes the
// node's replicas with it.
//
// The definition and its replicas are read past the cache when the store
// can: a replica the cache has not caught up with may hold the volume, and
// removing the definition without its confirmation would leave that volume
// behind.
func FinishVolumeDefinitionDelete(ctx context.Context, st Store, rdName string, vn int32) (VolumeDefinitionDeleteProgress, error) {
	var progress VolumeDefinitionDeleteProgress

	vds, err := LiveVolumes(ctx, st, rdName)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return progress, fmt.Errorf("list volume definitions of %s: %w", rdName, err)
	}

	idx := slices.IndexFunc(vds, func(vd apiv1.VolumeDefinition) bool { return vd.VolumeNumber == vn })
	if idx < 0 {
		progress.Removed = true

		return progress, nil
	}

	vd := vds[idx]
	if !vd.Deleting() {
		return progress, fmt.Errorf("%s/%d: %w", rdName, vn, ErrVolumeDefinitionNotDeleting)
	}

	replicas, err := LiveReplicas(ctx, st, rdName)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return progress, fmt.Errorf("list replicas of %s: %w", rdName, err)
	}

	for i := range replicas {
		if replicas[i].ConfirmedVolumeDelete(vn, vd.DeleteID) {
			progress.Confirmed = append(progress.Confirmed, replicas[i].NodeName)
		} else {
			progress.Pending = append(progress.Pending, replicas[i].NodeName)
		}
	}

	sort.Strings(progress.Confirmed)
	sort.Strings(progress.Pending)

	if len(progress.Pending) > 0 {
		return progress, nil
	}

	// Gone, or replaced by a volume created under the number since: either
	// way the definition this delete was for is no longer there.
	err = st.VolumeDefinitions().RemoveDeleted(ctx, rdName, vn, vd.DeleteID)
	if err != nil && !errors.Is(err, ErrNotFound) && !errors.Is(err, ErrVolumeDefinitionNotDeleting) {
		return progress, fmt.Errorf("remove volume definition %s/%d: %w", rdName, vn, err)
	}

	progress.Removed = true

	return progress, nil
}

// keepDeleteMark carries a delete in flight across a write built from a
// value that never saw it, as the Kubernetes store does: the DELETE flag
// is never cleared and the deleteID never swapped.
func keepDeleteMark(prev, next *apiv1.VolumeDefinition) {
	if !prev.Deleting() {
		return
	}

	if !next.Deleting() {
		next.Flags = append(slices.Clone(next.Flags), apiv1.VolumeDefinitionFlagDelete)
	}

	next.DeleteID = prev.DeleteID
}
