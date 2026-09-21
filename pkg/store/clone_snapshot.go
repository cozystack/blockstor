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
	"strings"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// RestoreFromSnapshotProp is the marker a restored or cloned definition
// carries, `<source definition>:<snapshot>`. The satellite reads it to restore
// from the snapshot instead of creating a blank volume, and the resume paths
// read it to recognise their own leftover.
const RestoreFromSnapshotProp = "BlockstorRestoreFromSnapshot"

// CloneSnapshotOwnerProp marks a snapshot the clone path took for itself. Its
// value is the name of the clone the snapshot was taken for.
//
// The marker alone cannot say whether a snapshot is the clone's own: a restore
// writes the same marker, pointing at an operator's snapshot, and an operator
// can name a snapshot anything, `clone-<target>` included. Only a prop this
// server wrote when it took the snapshot separates the two, so a snapshot
// without it is somebody's data and is never reaped.
const CloneSnapshotOwnerProp = "Blockstor/CloneSnapshotOf"

// ClonedSnapshotRef names the internal snapshot a clone left on its source,
// and the clone it was taken for.
type ClonedSnapshotRef struct {
	Source   string
	Snapshot string
	Clone    string
}

// OwnedCloneSnapshot answers which internal snapshot a definition was cloned
// from, read before the definition is deleted since its props are the only
// record of it. The zero value means there is none to reap: the definition is
// not a clone, the snapshot is gone, or the snapshot does not carry the owner
// prop naming this definition.
func OwnedCloneSnapshot(ctx context.Context, st Store, rdName string) ClonedSnapshotRef {
	rd, err := st.ResourceDefinitions().Get(ctx, rdName)
	if err != nil {
		return ClonedSnapshotRef{}
	}

	source, snapName, found := strings.Cut(rd.Props[RestoreFromSnapshotProp], ":")
	if !found || source == "" || snapName == "" {
		return ClonedSnapshotRef{}
	}

	snap, err := st.Snapshots().Get(ctx, source, snapName)
	if err != nil || !strings.EqualFold(snap.Props[CloneSnapshotOwnerProp], rdName) {
		return ClonedSnapshotRef{}
	}

	return ClonedSnapshotRef{Source: source, Snapshot: snapName, Clone: rdName}
}

// ErrCloneSnapshotInUse reports an internal clone snapshot another definition
// was restored from, which is therefore kept.
var ErrCloneSnapshotInUse = errors.New("internal clone snapshot is still a restore source")

// ReapClonedSnapshot drops the internal snapshot once the clone it was taken
// for is gone, which is what lets the source be deleted again: both doors
// refuse to delete a definition that still has snapshots.
//
// The snapshot is visible in `s l`, so it can have been used as the source of
// a restore, and that definition keeps reading it through its own marker: the
// placer pins new replicas to the snapshot's nodes and the satellite restores
// a replica from it by name. Such a snapshot is kept, and the error says why.
func ReapClonedSnapshot(ctx context.Context, st Store, ref ClonedSnapshotRef) error {
	if ref.Source == "" || ref.Snapshot == "" {
		return nil
	}

	definitions, err := st.ResourceDefinitions().List(ctx)
	if err != nil {
		return fmt.Errorf("list definitions restored from %s/%s: %w", ref.Source, ref.Snapshot, err)
	}

	marker := ref.Source + ":" + ref.Snapshot

	for i := range definitions {
		// The clone itself carries the marker too, and a cache that has not
		// seen its delete yet still lists it.
		if strings.EqualFold(definitions[i].Name, ref.Clone) {
			continue
		}

		if strings.EqualFold(definitions[i].Props[RestoreFromSnapshotProp], marker) {
			return fmt.Errorf("%w: %s was restored from %s", ErrCloneSnapshotInUse, definitions[i].Name, marker)
		}
	}

	err = st.Snapshots().Delete(ctx, ref.Source, ref.Snapshot)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return fmt.Errorf("delete internal clone snapshot %s/%s: %w", ref.Source, ref.Snapshot, err)
	}

	return nil
}

// OrphanedCloneSnapshots names the snapshots in the list that a clone took for
// itself and whose clone no longer exists. A delete that reaped nothing, or a
// reap that failed, leaves these behind, and they are what keeps the source
// from being deleted, so a refusal over them should name them.
func OrphanedCloneSnapshots(ctx context.Context, st Store, snaps []apiv1.Snapshot) []string {
	var orphans []string

	for i := range snaps {
		owner := snaps[i].Props[CloneSnapshotOwnerProp]
		if owner == "" {
			continue
		}

		_, err := st.ResourceDefinitions().Get(ctx, owner)
		if errors.Is(err, ErrNotFound) {
			orphans = append(orphans, snaps[i].Name)
		}
	}

	return orphans
}
