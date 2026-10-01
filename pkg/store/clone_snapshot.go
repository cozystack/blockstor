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

// CloneSnapshotReapingProp marks an internal clone snapshot the reap has
// started on. A restore that created its definition from the snapshot reads
// it back past the cache and withdraws when it finds this prop or no snapshot
// at all; see ReapClonedSnapshot for why both sides are needed.
const CloneSnapshotReapingProp = "Blockstor/CloneSnapshotReaping"

// ErrCloneSnapshotInUse reports an internal clone snapshot another definition
// was restored from, which is therefore kept.
var ErrCloneSnapshotInUse = errors.New("internal clone snapshot is still a restore source")

// ErrRestoreSourceWithdrawn reports a snapshot that was deleted, or is being
// reaped, while a definition was being restored from it.
var ErrRestoreSourceWithdrawn = errors.New("snapshot was withdrawn while a definition was restored from it")

// ReapClonedSnapshot drops the internal snapshot once the clone it was taken
// for is gone, which is what lets the source be deleted again: both doors
// refuse to delete a definition that still has snapshots.
//
// The snapshot is visible in `s l`, so it can have been used as the source of
// a restore, and that definition keeps reading it through its own marker: the
// placer pins new replicas to the snapshot's nodes and the satellite restores
// a replica from it by name. Such a snapshot is kept, and the error says why.
//
// A dependent appears in two steps, the snapshot read and the definition
// create, so no list taken here alone can see one that is between them. The
// reap therefore marks the snapshot first, then asks the API server for
// dependents, and only then deletes; a restore creates its definition first,
// then reads the snapshot back past the cache and withdraws on the mark or on
// a snapshot that is gone (RestoreSourceWithdrawn). Whichever of the list and
// the create comes second sees the other: a create before the list is a
// dependent the list finds, and a create after it is followed by a read that
// finds the mark.
//
// When the dependents cannot be listed the snapshot is kept and the mark
// taken off again; the refusal a later delete of the source meets names it.
func ReapClonedSnapshot(ctx context.Context, st Store, ref ClonedSnapshotRef) error {
	if ref.Source == "" || ref.Snapshot == "" {
		return nil
	}

	err := setReapingMark(ctx, st, ref.Source, ref.Snapshot, ref.Clone)
	if errors.Is(err, ErrNotFound) {
		return nil
	}

	if err != nil {
		return fmt.Errorf("mark internal clone snapshot %s/%s for reaping: %w", ref.Source, ref.Snapshot, err)
	}

	definitions, err := st.ResourceDefinitions().ListUncached(ctx)
	if err != nil {
		return keepCloneSnapshot(ctx, st, ref,
			fmt.Errorf("list definitions restored from %s/%s: %w", ref.Source, ref.Snapshot, err))
	}

	dependents := restoredFrom(definitions, ref.Source, ref.Snapshot, ref.Clone)
	if len(dependents) > 0 {
		return keepCloneSnapshot(ctx, st, ref, fmt.Errorf("%w: %s was restored from %s:%s",
			ErrCloneSnapshotInUse, dependents[0], ref.Source, ref.Snapshot))
	}

	err = st.Snapshots().Delete(ctx, ref.Source, ref.Snapshot)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return keepCloneSnapshot(ctx, st, ref,
			fmt.Errorf("delete internal clone snapshot %s/%s: %w", ref.Source, ref.Snapshot, err))
	}

	return nil
}

// keepCloneSnapshot takes the reaping mark back off a snapshot the reap
// decided to keep, so restores from it are not refused, and returns why it
// was kept.
func keepCloneSnapshot(ctx context.Context, st Store, ref ClonedSnapshotRef, why error) error {
	err := setReapingMark(ctx, st, ref.Source, ref.Snapshot, "")
	if err != nil && !errors.Is(err, ErrNotFound) {
		return fmt.Errorf("%w; the reaping mark stayed on it, so restores from it are refused "+
			"until `linstor s sp %s %s %s` clears it: %w",
			why, ref.Source, ref.Snapshot, CloneSnapshotReapingProp, err)
	}

	return why
}

// setReapingMark writes the reaping mark naming clone, or removes it when
// clone is empty.
func setReapingMark(ctx context.Context, st Store, source, snapshot, clone string) error {
	snap, err := st.Snapshots().Get(ctx, source, snapshot)
	if err != nil {
		return err //nolint:wrapcheck // callers wrap with the snapshot they name
	}

	if clone == "" {
		if _, marked := snap.Props[CloneSnapshotReapingProp]; !marked {
			return nil
		}

		delete(snap.Props, CloneSnapshotReapingProp)
	} else {
		if snap.Props == nil {
			snap.Props = map[string]string{}
		}

		snap.Props[CloneSnapshotReapingProp] = clone
	}

	return st.Snapshots().Update(ctx, &snap) //nolint:wrapcheck // callers wrap with the snapshot they name
}

// RestoreSourceWithdrawn is the restore's half of the protocol described on
// ReapClonedSnapshot: called after the restored definition was created, it
// reads the snapshot back past the cache and reports ErrRestoreSourceWithdrawn
// when it is gone or a reap has started on it. Any other error means the
// answer could not be had, and the caller withdraws as well.
func RestoreSourceWithdrawn(ctx context.Context, st Store, source, snapshot string) error {
	snaps, err := st.Snapshots().ListByDefinitionUncached(ctx, source)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return fmt.Errorf("read back snapshot %s/%s: %w", source, snapshot, err)
	}

	for i := range snaps {
		if !strings.EqualFold(snaps[i].Name, snapshot) {
			continue
		}

		if _, reaping := snaps[i].Props[CloneSnapshotReapingProp]; reaping {
			return fmt.Errorf("%w: %s/%s is being deleted", ErrRestoreSourceWithdrawn, source, snapshot)
		}

		return nil
	}

	return fmt.Errorf("%w: %s/%s was deleted", ErrRestoreSourceWithdrawn, source, snapshot)
}

// restoredFrom names the definitions, other than the clone itself, whose
// marker says they were restored from source:snapshot. The clone is skipped
// because it carries the marker too, and it may still be listed right after
// its own delete.
func restoredFrom(definitions []apiv1.ResourceDefinition, source, snapshot, clone string) []string {
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

	return names
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
//
// On an error the snapshots sorted before it are returned with it.
func CloneSnapshotsLeftBehind(ctx context.Context, st Store, snaps []apiv1.Snapshot) (LeftCloneSnapshots, error) {
	out := LeftCloneSnapshots{InUse: map[string]string{}}

	var definitions []apiv1.ResourceDefinition

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

		// One list answers every snapshot, taken only once one needs it.
		if definitions == nil {
			definitions, err = st.ResourceDefinitions().ListUncached(ctx)
			if err != nil {
				return out, fmt.Errorf("list definitions restored from %s: %w", snaps[i].ResourceName, err)
			}
		}

		dependents := restoredFrom(definitions, snaps[i].ResourceName, snaps[i].Name, owner)

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
// cannot drift. Every command it prints names the snapshot it acts on, and the
// steps come in the order they have to run: what can go now, then each
// definition that holds a snapshot and that snapshot after it, then the source.
func (l LeftCloneSnapshots) Explain(source string) (string, string) {
	size := len(l.Deletable) + len(l.Unowned) + len(l.InUse)
	causes := make([]string, 0, size)
	steps := make([]string, 0, size+1)

	deleteCmd := func(snapshot string) string {
		return "`linstor s d " + source + " " + snapshot + "`"
	}

	for _, snapshot := range l.Deletable {
		causes = append(causes, "internal clone snapshot "+snapshot+
			" outlived the clone it was taken for, and nothing was restored from it")
		steps = append(steps, deleteCmd(snapshot))
	}

	for _, snapshot := range l.Unowned {
		causes = append(causes, "snapshot "+snapshot+" looks like the internal snapshot of a clone "+
			"that no longer exists, but was not stamped as one, so it is never reaped")
		steps = append(steps, "if nothing of yours depends on "+snapshot+", "+deleteCmd(snapshot))
	}

	kept := make([]string, 0, len(l.InUse))
	for snapshot := range l.InUse {
		kept = append(kept, snapshot)
	}

	sort.Strings(kept)

	for _, snapshot := range kept {
		causes = append(causes, "internal clone snapshot "+snapshot+" is kept because "+
			l.InUse[snapshot]+" was restored from it")
		steps = append(steps, "delete "+l.InUse[snapshot]+" if it is no longer needed, then "+deleteCmd(snapshot))
	}

	steps = append(steps, "then delete "+source+" again")

	return strings.Join(causes, "; "), strings.Join(steps, "; ")
}
