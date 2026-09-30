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
	"sort"
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
//
// That question is asked of the API server, not of a cache: a restore that has
// not reached the informer yet is exactly the dependent a cached scan misses,
// and the snapshot would go from under it. When the answer cannot be had, the
// snapshot is kept; the refusal a later delete of the source meets names it.
func ReapClonedSnapshot(ctx context.Context, st Store, ref ClonedSnapshotRef) error {
	if ref.Source == "" || ref.Snapshot == "" {
		return nil
	}

	dependents, err := restoredFrom(ctx, st, ref.Source, ref.Snapshot, ref.Clone)
	if err != nil {
		return err
	}

	if len(dependents) > 0 {
		return fmt.Errorf("%w: %s was restored from %s:%s",
			ErrCloneSnapshotInUse, dependents[0], ref.Source, ref.Snapshot)
	}

	err = st.Snapshots().Delete(ctx, ref.Source, ref.Snapshot)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return fmt.Errorf("delete internal clone snapshot %s/%s: %w", ref.Source, ref.Snapshot, err)
	}

	return nil
}

// restoredFrom names the definitions, other than the clone itself, whose
// marker says they were restored from source:snapshot. The clone is skipped
// because it carries the marker too, and it may still be listed right after
// its own delete.
func restoredFrom(ctx context.Context, st Store, source, snapshot, clone string) ([]string, error) {
	definitions, err := st.ResourceDefinitions().ListUncached(ctx)
	if err != nil {
		return nil, fmt.Errorf("list definitions restored from %s/%s: %w", source, snapshot, err)
	}

	marker := source + ":" + snapshot

	var names []string

	for i := range definitions {
		if clone != "" && strings.EqualFold(definitions[i].Name, clone) {
			continue
		}

		if strings.EqualFold(definitions[i].Props[RestoreFromSnapshotProp], marker) {
			names = append(names, definitions[i].Name)
		}
	}

	return names, nil
}

// LeftCloneSnapshots is what the snapshots a source still carries say about
// clones that are gone.
type LeftCloneSnapshots struct {
	// Deletable were taken by a clone that no longer exists, and no
	// definition was restored from them.
	Deletable []string
	// Unowned look like a clone's internal snapshot, `clone-<name>` for a
	// definition that no longer exists, but carry no owner prop: a version
	// before the prop existed took them, or an operator named one that way.
	// Nothing here reaps them; they are named so the operator knows why the
	// source is still refused.
	Unowned []string
	// InUse were taken by a clone that is gone but still have a definition
	// restored from them, keyed by snapshot.
	InUse map[string]string
}

// Empty reports whether there is nothing to tell the operator.
func (l LeftCloneSnapshots) Empty() bool {
	return len(l.Deletable) == 0 && len(l.Unowned) == 0 && len(l.InUse) == 0
}

// CloneSnapshotsLeftBehind sorts a source's snapshots that outlived the clone
// they belong to. A delete that reaped nothing, a reap that failed, and a reap
// that kept a snapshot another definition was restored from all leave one,
// and a repeated delete of the clone cannot revisit them: the clone is gone.
// The refusal a delete of the source meets is the one place that still sees
// them, so it names them, and never calls a snapshot deletable while a
// definition still restores from it.
func CloneSnapshotsLeftBehind(ctx context.Context, st Store, snaps []apiv1.Snapshot) (LeftCloneSnapshots, error) {
	out := LeftCloneSnapshots{InUse: map[string]string{}}

	for i := range snaps {
		owner := snaps[i].Props[CloneSnapshotOwnerProp]
		legacy := owner == ""

		if legacy {
			var found bool

			owner, found = strings.CutPrefix(snaps[i].Name, "clone-")
			if !found || owner == "" {
				continue
			}
		}

		_, err := st.ResourceDefinitions().Get(ctx, owner)
		if !errors.Is(err, ErrNotFound) {
			continue
		}

		dependents, err := restoredFrom(ctx, st, snaps[i].ResourceName, snaps[i].Name, owner)
		if err != nil {
			return LeftCloneSnapshots{}, err
		}

		switch {
		case len(dependents) > 0:
			out.InUse[snaps[i].Name] = dependents[0]
		case legacy:
			out.Unowned = append(out.Unowned, snaps[i].Name)
		default:
			out.Deletable = append(out.Deletable, snaps[i].Name)
		}
	}

	return out, nil
}

// Explain words the report for the refusal both delete doors give, so the two
// cannot drift. The correction deletes only what is safe to delete, and names
// the definition that has to go first for a snapshot still restored from.
func (l LeftCloneSnapshots) Explain(source string) (string, string) {
	var causes, corrections []string

	if len(l.Deletable) > 0 {
		causes = append(causes, "internal clone snapshot(s) "+strings.Join(l.Deletable, ", ")+
			" outlived the clone they were taken for, and nothing was restored from them")
		corrections = append(corrections, "delete "+strings.Join(l.Deletable, ", ")+
			" with `linstor s d "+source+" <snapshot>`")
	}

	if len(l.Unowned) > 0 {
		causes = append(causes, "snapshot(s) "+strings.Join(l.Unowned, ", ")+
			" look like a clone's internal snapshot for a clone that no longer exists, "+
			"but were not stamped as one, so they are never reaped")
		corrections = append(corrections, "delete "+strings.Join(l.Unowned, ", ")+
			" by hand if nothing of yours depends on them")
	}

	kept := make([]string, 0, len(l.InUse))
	for snapshot := range l.InUse {
		kept = append(kept, snapshot)
	}

	sort.Strings(kept)

	for _, snapshot := range kept {
		causes = append(causes, "internal clone snapshot "+snapshot+" is kept because "+
			l.InUse[snapshot]+" was restored from it")
		corrections = append(corrections, "delete "+l.InUse[snapshot]+" first; "+snapshot+
			" can go once nothing restores from it")
	}

	return strings.Join(causes, "; "), strings.Join(corrections, "; ") + ", then delete " + source + " again"
}
