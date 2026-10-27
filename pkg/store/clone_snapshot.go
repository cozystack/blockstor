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
	"crypto/rand"
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
// prop naming this definition. An error means it could not be told: the
// caller deletes nothing on it, since the definition's props are the only
// record and a delete would lose the snapshot to nobody.
func OwnedCloneSnapshot(ctx context.Context, st Store, rdName string) (ClonedSnapshotRef, error) {
	rd, err := st.ResourceDefinitions().Get(ctx, rdName)
	if errors.Is(err, ErrNotFound) {
		return ClonedSnapshotRef{}, nil
	}

	if err != nil {
		return ClonedSnapshotRef{}, fmt.Errorf("read %s to find the snapshot it was cloned from: %w", rdName, err)
	}

	source, snapName, found := strings.Cut(rd.Props[RestoreFromSnapshotProp], ":")
	if !found || source == "" || snapName == "" {
		return ClonedSnapshotRef{}, nil
	}

	// Past the cache: a clone deleted right after it was created can be
	// served by a replica whose informer has not seen the snapshot yet, and
	// a cached miss would skip the reap with nothing to say so.
	snap, err := uncachedSnapshot(ctx, st, source, snapName)
	if errors.Is(err, ErrNotFound) {
		return ClonedSnapshotRef{}, nil
	}

	if err != nil {
		return ClonedSnapshotRef{}, fmt.Errorf("check whether %s owns %s/%s: %w", rdName, source, snapName, err)
	}

	if !strings.EqualFold(snap.Props[CloneSnapshotOwnerProp], rdName) {
		return ClonedSnapshotRef{}, nil
	}

	return ClonedSnapshotRef{Source: source, Snapshot: snapName, Clone: rdName}, nil
}

// CloneSnapshotReapingProp marks an internal clone snapshot the reap has
// started on, as `<clone>#<token>@<RFC 3339 time>`. The token is random per
// reap, so two reaps of one clone started in the same second, which the CLI
// can run since it reaps after a delete that found nothing, never take each
// other's mark for their own. A restore that created its
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

// ErrRestoreSourceReaping is the withdrawal a live reaping mark alone causes:
// no delete was issued yet, and the reap may still keep the snapshot, so the
// same restore retried once the reap is over can go through.
var ErrRestoreSourceReaping = errors.New("a reap of the snapshot is under way; retry once it finishes")

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
// cloneSnapshotReapBudget: it runs after the definition is deleted and
// before the delete is answered, and a reap cut between its mark and its
// clean-up, by a caller that went away, would leave the mark behind. A mark
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

	mark := newReapingMark(ref.Clone, time.Now())

	err := setReapingMark(ctx, st, ref.Source, ref.Snapshot, mark)
	if errors.Is(err, ErrNotFound) || errors.Is(err, errAnotherReap) {
		return nil
	}

	if err != nil {
		return fmt.Errorf("mark internal clone snapshot %s/%s for reaping: %w", ref.Source, ref.Snapshot, err)
	}

	definitions, err := st.ResourceDefinitions().ListUncached(ctx)
	if err != nil {
		return keepCloneSnapshot(ctx, st, ref, mark,
			fmt.Errorf("list definitions restored from %s/%s: %w", ref.Source, ref.Snapshot, err))
	}

	dependents := restoredFrom(definitions, ref.Source, ref.Snapshot)
	if len(dependents) > 0 {
		return keepCloneSnapshot(ctx, st, ref, mark, fmt.Errorf("%w: %s was restored from %s:%s",
			ErrCloneSnapshotInUse, dependents[0], ref.Source, ref.Snapshot))
	}

	err = st.Snapshots().Delete(ctx, ref.Source, ref.Snapshot)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return keepCloneSnapshot(ctx, st, ref, mark,
			fmt.Errorf("delete internal clone snapshot %s/%s: %w", ref.Source, ref.Snapshot, err))
	}

	return nil
}

// newReapingMark is the mark one reap writes: the clone, a token no other
// reap shares, and the time the TTL is read from.
func newReapingMark(clone string, now time.Time) string {
	return clone + "#" + rand.Text() + "@" + now.UTC().Format(time.RFC3339)
}

// keepCloneSnapshot takes the reaping mark back off a snapshot the reap
// decided to keep, so restores from it are not refused, and returns why it
// was kept. Only the mark this reap wrote is taken off: one another reap
// wrote since belongs to that reap.
func keepCloneSnapshot(ctx context.Context, st Store, ref ClonedSnapshotRef, mark string, why error) error {
	err := dropReapingMark(ctx, st, ref.Source, ref.Snapshot, mark)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return fmt.Errorf("%w; the reaping mark stayed on it, so restores from it are refused "+
			"for up to %s: %w", why, cloneSnapshotReapingMarkTTL, err)
	}

	return why
}

// errAnotherReap reports a live mark another reap of the snapshot wrote.
// That reap owns the decision about the snapshot: it lists dependents and
// deletes or keeps it, so this one stands down rather than write over its
// mark, which would leave the other reap with no mark once this one kept
// the snapshot and took its own off.
var errAnotherReap = errors.New("another reap of the snapshot is under way")

// setReapingMark writes mark as the reaping mark, unless another reap's
// live mark is on the snapshot. The decision and the write are one patch,
// so a mark written in between is seen, not overwritten.
func setReapingMark(ctx context.Context, st Store, source, snapshot, mark string) error {
	//nolint:wrapcheck // callers wrap with the snapshot they name
	return st.Snapshots().PatchSnapshotProps(ctx, source, snapshot, func(props map[string]string) error {
		current := props[CloneSnapshotReapingProp]
		if current != "" && current != mark && reapingMarkIsLive(current, time.Now()) {
			return errAnotherReap
		}

		props[CloneSnapshotReapingProp] = mark

		return nil
	})
}

// errMarkNotMine stops a removal that finds a mark other than its own.
var errMarkNotMine = errors.New("the reaping mark is not this reap's")

// dropReapingMark removes the reaping mark when it is still mark, deciding
// and writing in one patch.
func dropReapingMark(ctx context.Context, st Store, source, snapshot, mark string) error {
	err := st.Snapshots().PatchSnapshotProps(ctx, source, snapshot, func(props map[string]string) error {
		if props[CloneSnapshotReapingProp] != mark {
			return errMarkNotMine
		}

		delete(props, CloneSnapshotReapingProp)

		return nil
	})
	if errors.Is(err, errMarkNotMine) {
		return nil
	}

	return err //nolint:wrapcheck // callers wrap with the snapshot they name
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

	if slices.Contains(snap.Flags, apiv1.SnapshotFlagDelete) {
		return fmt.Errorf("%w: %s/%s is being deleted", ErrRestoreSourceWithdrawn, source, snapshot)
	}

	if reapingMarkIsLive(snap.Props[CloneSnapshotReapingProp], time.Now()) {
		return fmt.Errorf("%w: %s/%s: %w", ErrRestoreSourceWithdrawn, source, snapshot, ErrRestoreSourceReaping)
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
	// Deleting carry the DELETE flag: a delete was issued and a satellite
	// finalizer still holds them. They go on their own.
	Deleting []string
}

// Empty reports whether there is nothing to tell the operator.
func (l *LeftCloneSnapshots) Empty() bool {
	return len(l.Deletable) == 0 && len(l.Unowned) == 0 && len(l.InUse) == 0 && len(l.Deleting) == 0
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

		// A delete already issued needs nothing from the operator but
		// time; a command to delete it again would only repeat it.
		if slices.Contains(snaps[i].Flags, apiv1.SnapshotFlagDelete) {
			out.Deleting = append(out.Deleting, snaps[i].Name)

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
func (l *LeftCloneSnapshots) Explain(source string) (string, string) {
	size := len(l.Deletable) + len(l.Unowned) + len(l.InUse) + len(l.Deleting)
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

	for _, snapshot := range l.Deleting {
		causes = append(causes, "internal clone snapshot "+snapshot+" is being deleted")
		steps = append(steps, "wait for "+snapshot+" to go, which needs no command; retry shortly")
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

// DropCloneSnapshotMarks removes the clone-snapshot owner and reaping props
// from a definition's props. They belong to the snapshot, not to a definition
// restored from it: copied onward, every snapshot later taken of that
// definition would inherit a claim of ownership it was never given.
func DropCloneSnapshotMarks(props map[string]string) {
	delete(props, CloneSnapshotOwnerProp)
	delete(props, CloneSnapshotReapingProp)
}
