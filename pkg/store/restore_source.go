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

// RestoreSourceInUse reports, without changing anything, whether a
// definition whose restore is not finished reads from srcRD:snapName. A
// caller that cannot judge the restore (no store) treats any marker naming the
// snapshot as in use.
func RestoreSourceInUse(rds []apiv1.ResourceDefinition, srcRD, snapName string) (string, bool) {
	marker := srcRD + ":" + snapName

	for i := range rds {
		if strings.EqualFold(rds[i].Props[RestoreFromSnapshotProp], marker) &&
			!slices.Contains(rds[i].Flags, apiv1.ResourceFlagDelete) {
			return rds[i].Name, true
		}
	}

	return "", false
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
