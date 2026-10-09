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
	"strings"
	"time"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

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
// started on, as `<clone>@<RFC 3339 time>`. A restore that created its
// definition from the snapshot reads it back past the cache and withdraws when
// it finds a live mark or no snapshot at all; see ReapClonedSnapshot for why
// both sides are needed.
const CloneSnapshotReapingProp = "Blockstor/CloneSnapshotReaping"

// cloneSnapshotReapBudget bounds a reap from its first write to its last: no
// call the reap makes, the delete included, can land after it.
const cloneSnapshotReapBudget = 30 * time.Second

// cloneSnapshotReapingMarkTTL is how long a reaping mark stays live. It covers
// the reap up to its delete: a mark older than this belongs to a reap that can
// no longer issue one, since cloneSnapshotReapBudget is far shorter, so it is
// ignored, and a mark left by a reap that died, or whose clean-up failed,
// cannot refuse restores from a snapshot the reap never deleted for longer
// than this. A delete that was issued is a different state and outlives any
// budget while a satellite finalizer holds the snapshot; that one is read off
// the snapshot's DELETE flag, not off the mark. The gap between the two
// budgets also absorbs clock skew between the replica that wrote the mark and
// the one that reads it.
const cloneSnapshotReapingMarkTTL = 5 * time.Minute

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
// then reads the snapshot back past the cache and withdraws on a live mark or
// on a snapshot that is gone (RestoreSourceWithdrawn). Whichever of the list
// and the create comes second sees the other: a create before the list is a
// dependent the list finds, and a create after it is followed by a read that
// finds the mark.
//
// The reap runs on a context the caller cannot cancel, bounded by
// cloneSnapshotReapBudget: the delete has already been reported, and a reap
// cut between its mark and its clean-up would leave the mark behind. A mark
// that is left anyway, by a process that died or a clean-up that failed,
// expires (cloneSnapshotReapingMarkTTL).
//
// When the dependents cannot be listed the snapshot is kept and the mark
// taken off again; the refusal a later delete of the source meets names it.
func ReapClonedSnapshot(ctx context.Context, st Store, ref ClonedSnapshotRef) error {
	if ref.Source == "" || ref.Snapshot == "" {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cloneSnapshotReapBudget)
	defer cancel()

	err := setReapingMark(ctx, st, ref.Source, ref.Snapshot, ref.Clone+"@"+time.Now().UTC().Format(time.RFC3339))
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

	dependents := restoredFrom(definitions, ref.Source, ref.Snapshot)
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
			"for up to %s: %w", why, cloneSnapshotReapingMarkTTL, err)
	}

	return why
}

// setReapingMark writes mark as the reaping mark, or removes the mark when it
// is empty. The snapshot is read past the cache: the mark this reap wrote one
// round trip earlier may not have reached the cache yet, and a removal decided
// on that read would find nothing to remove.
func setReapingMark(ctx context.Context, st Store, source, snapshot, mark string) error {
	snap, err := uncachedSnapshot(ctx, st, source, snapshot)
	if err != nil {
		return err
	}

	if mark == "" {
		if _, marked := snap.Props[CloneSnapshotReapingProp]; !marked {
			return nil
		}

		delete(snap.Props, CloneSnapshotReapingProp)
	} else {
		if snap.Props == nil {
			snap.Props = map[string]string{}
		}

		snap.Props[CloneSnapshotReapingProp] = mark
	}

	return st.Snapshots().Update(ctx, &snap) //nolint:wrapcheck // callers wrap with the snapshot they name
}

// uncachedSnapshot reads one snapshot from the API server.
func uncachedSnapshot(ctx context.Context, st Store, source, snapshot string) (apiv1.Snapshot, error) {
	snaps, err := st.Snapshots().ListByDefinitionUncached(ctx, source)
	if err != nil {
		return apiv1.Snapshot{}, fmt.Errorf("read snapshot %s/%s: %w", source, snapshot, err)
	}

	for i := range snaps {
		if strings.EqualFold(snaps[i].Name, snapshot) {
			return snaps[i], nil
		}
	}

	return apiv1.Snapshot{}, fmt.Errorf("snapshot %s/%s: %w", source, snapshot, ErrNotFound)
}

// reapingMarkIsLive reports whether a reaping mark belongs to a reap that can
// still delete the snapshot. A mark whose time cannot be read is not.
func reapingMarkIsLive(mark string, now time.Time) bool {
	at := strings.LastIndex(mark, "@")
	if at < 0 {
		return false
	}

	written, err := time.Parse(time.RFC3339, mark[at+1:])
	if err != nil {
		return false
	}

	return now.Sub(written) < cloneSnapshotReapingMarkTTL
}

// RestoreSourceWithdrawn is the restore's half of the protocol described on
// ReapClonedSnapshot: called after the restored definition was created, it
// reads the snapshot back past the cache and reports ErrRestoreSourceWithdrawn
// when it is gone or a reap that is still live has started on it. Any other
// error means the answer could not be had, and the caller withdraws as well.
func RestoreSourceWithdrawn(ctx context.Context, st Store, source, snapshot string) error {
	snap, err := uncachedSnapshot(ctx, st, source, snapshot)
	if errors.Is(err, ErrNotFound) {
		return fmt.Errorf("%w: %s/%s was deleted", ErrRestoreSourceWithdrawn, source, snapshot)
	}

	if err != nil {
		return err
	}

	if slices.Contains(snap.Flags, apiv1.SnapshotFlagDelete) ||
		reapingMarkIsLive(snap.Props[CloneSnapshotReapingProp], time.Now()) {
		return fmt.Errorf("%w: %s/%s is being deleted", ErrRestoreSourceWithdrawn, source, snapshot)
	}

	return nil
}

// restoredFrom names the definitions whose marker says they were restored
// from source:snapshot. The clone the snapshot was taken for is not skipped by
// name: by the time the reap lists, the clone is gone from the API server, as
// a definition has no finalizer to linger behind, and a definition that has
// taken its name since is a dependent like any other.
func restoredFrom(definitions []apiv1.ResourceDefinition, source, snapshot string) []string {
	marker := source + ":" + snapshot

	var names []string

	for i := range definitions {
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
				return LeftCloneSnapshots{}, fmt.Errorf("list definitions restored from %s: %w", snaps[i].ResourceName, err)
			}
		}

		dependents := restoredFrom(definitions, snaps[i].ResourceName, snaps[i].Name)

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
// steps come in the order they have to run: what can go now, then what has to
// wait, then the source. A snapshot a definition still restores from gets no
// command at all: the snapshot delete door does not check for dependents, and
// a pasteable delete next to "kept because" is one an operator runs.
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
		steps = append(steps, "keep "+snapshot+" while "+l.InUse[snapshot]+" exists, since "+
			l.InUse[snapshot]+" still reads it; once "+l.InUse[snapshot]+
			" is deleted, the next delete of "+source+" names it again")
	}

	steps = append(steps, "then delete "+source+" again")

	return strings.Join(causes, "; "), strings.Join(steps, "; ")
}
