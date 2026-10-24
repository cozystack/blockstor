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
	"strings"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// ErrRestoreSourceInUse refuses deleting a snapshot that a restore which is
// not finished yet still reads from.
var ErrRestoreSourceInUse = errors.New("a restore from this snapshot is not finished")

// ReleaseRestoreSource gets snapshot srcRD:snapName ready to be deleted.
//
// A restored or cloned definition keeps its restore marker after the restore
// is done, and while the marker is there every new replica is brought up from
// that snapshot and placed on its nodes. Deleting the snapshot under it left
// the definition looking fine until it needed a replica (node replacement,
// `rd ap`, a toggle back to diskful), which then came up blank and never
// reached UpToDate.
//
// So a delete first ends the dependency: every definition whose marker names
// the snapshot and whose restore is finished (RestoreMarkerClearRefusal, the
// rule an operator's own delete of the marker passes) has the marker taken
// off, and its later replicas sync from their peers. A definition whose
// restore is still running keeps it, and the delete is refused with
// ErrRestoreSourceInUse; that is retryable, and lasts as long as the restore.
// A definition being deleted is not waited for.
func ReleaseRestoreSource(ctx context.Context, st Store, srcRD, snapName string) error {
	marker := srcRD + ":" + snapName

	rds, err := st.ResourceDefinitions().List(ctx)
	if err != nil {
		return fmt.Errorf("list the definitions restored from %s: %w", marker, err)
	}

	for i := range rds {
		if !strings.EqualFold(rds[i].Props[RestoreFromSnapshotProp], marker) {
			continue
		}

		if slices.Contains(rds[i].Flags, apiv1.ResourceFlagDelete) {
			continue
		}

		err = releaseRestoreMarker(ctx, st, rds[i].Name, marker)
		if err != nil {
			return err
		}
	}

	return nil
}

func releaseRestoreMarker(ctx context.Context, st Store, rdName, marker string) error {
	refusal := RestoreMarkerClearRefusal(ctx, st, rdName)
	if errors.Is(refusal, ErrRestoreUnfinished) {
		return fmt.Errorf("%w: %q is still being restored from it (%w)", ErrRestoreSourceInUse, rdName, refusal)
	}

	if errors.Is(refusal, ErrNotFound) {
		return nil
	}

	if refusal != nil {
		return refusal
	}

	err := st.ResourceDefinitions().PatchResourceDefinitionSpec(ctx, rdName, func(rd *apiv1.ResourceDefinition) error {
		// Re-checked on the object the patch writes: a marker rewritten in
		// between is somebody else's restore.
		if strings.EqualFold(rd.Props[RestoreFromSnapshotProp], marker) {
			delete(rd.Props, RestoreFromSnapshotProp)
		}

		return nil
	})
	if errors.Is(err, ErrNotFound) {
		return nil
	}

	if err != nil {
		return fmt.Errorf("take the restore marker off %q: %w", rdName, err)
	}

	return nil
}

// ErrReplicasWithoutDefinition refuses a restore or clone into a name that
// has no definition but already has diskless replicas under it.
var ErrReplicasWithoutDefinition = errors.New("replicas already exist under that name with no definition over them")

// RefuseReplicasWithoutDefinition refuses materialising a new definition over
// diskless replicas that are already there with none over them: an
// operator's diskless replica left by a cascade that did not finish, or
// applied by hand. Both restore doors, REST and CLI, run it before they
// create the definition.
//
// Created over them, the definition adopted them, the stamp on their node
// failed because a replica with no data is not one a restore can use, and the
// rollback of the definition it had created cascaded over every replica under
// the name, taking those with it although the request had not made them.
// Refusing before anything is written leaves them where they were and names
// them.
//
// A replica that holds data is not refused here: the stamp takes it as
// placed, and it is the restore's to keep or roll back. Nor is the
// controller's tie-breaker witness, which the stamp promotes into the replica
// the restore asked for. A replica already being deleted is no obstacle
// either: it is going whatever the restore does.
//
// Read past the cache on both sides: a definition the cache has not seen yet
// would make its own replicas read as orphans. The answer wraps
// ErrAlreadyExists, so the doors answer it with 409.
func RefuseReplicasWithoutDefinition(ctx context.Context, st Store, rdName string) error {
	_, err := st.ResourceDefinitions().GetUncached(ctx, rdName)
	if err == nil {
		return nil
	}

	if !errors.Is(err, ErrNotFound) {
		return fmt.Errorf("read %q to check for replicas without it: %w", rdName, err)
	}

	replicas, err := LiveReplicas(ctx, st, rdName)
	if err != nil {
		return fmt.Errorf("list the replicas under %q: %w", rdName, err)
	}

	var orphans []string

	for i := range replicas {
		flags := replicas[i].Flags
		if slices.Contains(flags, apiv1.ResourceFlagDelete) || slices.Contains(flags, apiv1.ResourceFlagTieBreaker) {
			continue
		}

		if slices.Contains(flags, apiv1.ResourceFlagDiskless) {
			orphans = append(orphans, replicas[i].NodeName)
		}
	}

	if len(orphans) == 0 {
		return nil
	}

	return fmt.Errorf("%w: %q has diskless replicas on %s but no resource definition: %w; "+
		"delete those Resource objects, then retry", ErrAlreadyExists, rdName, strings.Join(orphans, ", "),
		ErrReplicasWithoutDefinition)
}
