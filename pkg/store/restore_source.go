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
	"strings"
	"time"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// ErrRestoreSourceInUse refuses deleting a snapshot that a restore which is
// not finished yet still reads from.
var ErrRestoreSourceInUse = errors.New("a restore from this snapshot is not finished")

// ErrRestoreSourceWithdrawn reports a snapshot that was deleted, or that a
// delete has started on, while a definition was being restored from it.
var ErrRestoreSourceWithdrawn = errors.New("snapshot was withdrawn while a definition was restored from it")

// ErrRestoreSourceDeleting is the withdrawal a live deleting mark alone
// causes: the delete may still keep the snapshot, for a restore it found
// unfinished, so the same restore retried once the delete is over can go
// through.
var ErrRestoreSourceDeleting = errors.New("a delete of the snapshot is under way; retry once it finishes")

// SnapshotDeletingProp marks a snapshot a delete has started on, as
// `<token>@<RFC 3339 time>`. The token is random per delete, so a delete
// only ever takes its own mark back off. See DeleteSnapshot.
const SnapshotDeletingProp = "Blockstor/SnapshotDeleting"

// snapshotDeleteBudget bounds a delete from its mark to its last write: no
// call it makes can land after it.
const snapshotDeleteBudget = 30 * time.Second

// snapshotDeletingMarkTTL is how long a deleting mark refuses restores. A
// mark older than this belongs to a delete that can no longer issue one,
// since snapshotDeleteBudget is far shorter, so a mark a dead process left
// on a snapshot it never deleted stops refusing restores from it. The gap
// between the two also absorbs clock skew between the replicas that write
// and read the mark.
const snapshotDeletingMarkTTL = 5 * time.Minute

// DeleteSnapshot deletes snapshot srcRD:snapName for an operator-facing door,
// the REST delete, the CLI and the auto-snapshot prune, once no restore needs
// it. A snapshot that does not exist answers ErrNotFound, before any
// definition is looked at: there is nothing to hold.
//
// A restored or cloned definition keeps its restore marker after the restore
// is done, and while the marker is there every new replica is brought up from
// that snapshot and placed on its nodes. Deleting the snapshot under it left
// the definition looking fine until it needed a replica (node replacement,
// `rd ap`, a toggle back to diskful), which then came up blank and never
// reached UpToDate.
//
// So the delete first ends the dependency: every definition whose marker
// names the snapshot and whose restore is finished (RestoreMarkerClearRefusal,
// the rule an operator's own delete of the marker passes) has the marker taken
// off, and its later replicas sync from their peers. A definition whose
// restore is still running keeps it, and the delete is refused with
// ErrRestoreSourceInUse; that is retryable, and lasts as long as the restore.
// A definition being deleted is not waited for.
//
// A restore makes its dependent in two steps, the snapshot read and the
// definition create, so no list taken here alone can see one that is between
// them. The delete therefore marks the snapshot first, then lists the
// definitions past the cache, and only then deletes; a restore creates its
// definition first, then reads the snapshot back past the cache and withdraws
// on a live mark or on a snapshot that is gone (RestoreSourceWithdrawn).
// Whichever of the list and the create comes second sees the other. The mark
// stays on a snapshot whose delete was issued, and is taken back off one the
// delete keeps.
//
// The delete runs on a context the caller cannot cancel, bounded by
// snapshotDeleteBudget, so a caller that goes away between the mark and its
// clean-up does not leave the mark behind; one a dead process leaves expires
// (snapshotDeletingMarkTTL).
func DeleteSnapshot(ctx context.Context, st Store, srcRD, snapName string) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), snapshotDeleteBudget)
	defer cancel()

	snap, err := uncachedSnapshot(ctx, st, srcRD, snapName)
	if err != nil {
		return err
	}

	mark := rand.Text() + "@" + time.Now().UTC().Format(time.RFC3339)

	err = setDeletingMark(ctx, st, &snap, mark)
	if err != nil {
		return fmt.Errorf("mark snapshot %s of %s as being deleted: %w", snapName, srcRD, err)
	}

	// The stored names from here on: LINSTOR folds name case, and a marker
	// is written from the stored snapshot.
	srcRD, snapName = snap.ResourceName, snap.Name

	err = releaseRestoreSource(ctx, st, srcRD, snapName)
	if err != nil {
		return keepSnapshot(ctx, st, srcRD, snapName, mark, err)
	}

	err = st.Snapshots().Delete(ctx, srcRD, snapName)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return keepSnapshot(ctx, st, srcRD, snapName, mark,
			fmt.Errorf("delete snapshot %s of %s: %w", snapName, srcRD, err))
	}

	return nil
}

// releaseRestoreSource ends the dependency of every definition restored from
// srcRD:snapName, or refuses with ErrRestoreSourceInUse; see DeleteSnapshot.
// The definitions are listed past the cache: one created a moment ago is
// exactly the one a cache misses.
func releaseRestoreSource(ctx context.Context, st Store, srcRD, snapName string) error {
	marker := srcRD + ":" + snapName

	rds, err := st.ResourceDefinitions().ListUncached(ctx)
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

// RestoreSourceWithdrawn is the restore's half of the protocol DeleteSnapshot
// describes: called once the restored definition carries its marker, it reads
// the snapshot back past the cache and reports ErrRestoreSourceWithdrawn when
// it is gone, wrapping ErrNotFound, or when a delete that is still live has
// started on it, wrapping ErrRestoreSourceDeleting. Any other error means the
// answer could not be had, and the caller withdraws as well.
func RestoreSourceWithdrawn(ctx context.Context, st Store, srcRD, snapName string) error {
	snap, err := uncachedSnapshot(ctx, st, srcRD, snapName)
	if errors.Is(err, ErrNotFound) {
		return fmt.Errorf("%w: snapshot %s of %s was deleted: %w", ErrRestoreSourceWithdrawn, snapName, srcRD, err)
	}

	if err != nil {
		return err
	}

	if deletingMarkIsLive(snap.Props[SnapshotDeletingProp], time.Now()) {
		return fmt.Errorf("%w: snapshot %s of %s: %w", ErrRestoreSourceWithdrawn, snapName, srcRD,
			ErrRestoreSourceDeleting)
	}

	return nil
}

// keepSnapshot takes this delete's mark back off a snapshot it decided to
// keep, so restores from it are not refused, and returns why it was kept.
func keepSnapshot(ctx context.Context, st Store, srcRD, snapName, mark string, why error) error {
	err := dropDeletingMark(ctx, st, srcRD, snapName, mark)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return fmt.Errorf("%w; the deleting mark stayed on it, so restores from it are refused "+
			"for up to %s: %w", why, snapshotDeletingMarkTTL, err)
	}

	return why
}

// setDeletingMark writes mark as the deleting mark on snap, read past the
// cache by the caller.
func setDeletingMark(ctx context.Context, st Store, snap *apiv1.Snapshot, mark string) error {
	if snap.Props == nil {
		snap.Props = map[string]string{}
	}

	snap.Props[SnapshotDeletingProp] = mark

	return st.Snapshots().Update(ctx, snap) //nolint:wrapcheck // the caller wraps with the snapshot it names
}

// dropDeletingMark removes the deleting mark when it is still mark. The
// snapshot is read past the cache: the mark written one round trip earlier
// may not have reached the cache yet.
func dropDeletingMark(ctx context.Context, st Store, srcRD, snapName, mark string) error {
	snap, err := uncachedSnapshot(ctx, st, srcRD, snapName)
	if err != nil {
		return err
	}

	if snap.Props[SnapshotDeletingProp] != mark {
		return nil
	}

	delete(snap.Props, SnapshotDeletingProp)

	// A snapshot that had no props before the mark gets none back: a restore
	// falls back to the source's props only for a snapshot without any.
	if len(snap.Props) == 0 {
		snap.Props = nil
	}

	return st.Snapshots().Update(ctx, &snap) //nolint:wrapcheck // the caller wraps with the snapshot it names
}

// uncachedSnapshot reads one snapshot from the API server, both names
// compared the way LINSTOR compares names. The scoped read compares the
// definition name verbatim, so it asks under the spelling the definition is
// stored with; a snapshot whose definition is gone is asked for as named.
func uncachedSnapshot(ctx context.Context, st Store, srcRD, snapName string) (apiv1.Snapshot, error) {
	rd, err := st.ResourceDefinitions().GetUncached(ctx, srcRD)
	if err == nil {
		srcRD = rd.Name
	} else if !errors.Is(err, ErrNotFound) {
		return apiv1.Snapshot{}, fmt.Errorf("read %q to find snapshot %s on it: %w", srcRD, snapName, err)
	}

	snaps, err := st.Snapshots().ListByDefinitionUncached(ctx, srcRD)
	if err != nil {
		return apiv1.Snapshot{}, fmt.Errorf("read snapshot %s of %s: %w", snapName, srcRD, err)
	}

	for i := range snaps {
		if strings.EqualFold(snaps[i].Name, snapName) {
			return snaps[i], nil
		}
	}

	return apiv1.Snapshot{}, fmt.Errorf("snapshot %s of %s: %w", snapName, srcRD, ErrNotFound)
}

// deletingMarkIsLive reports whether a deleting mark belongs to a delete that
// can still issue one. A mark whose time cannot be read is not.
func deletingMarkIsLive(mark string, now time.Time) bool {
	at := strings.LastIndex(mark, "@")
	if at < 0 {
		return false
	}

	written, err := time.Parse(time.RFC3339, mark[at+1:])
	if err != nil {
		return false
	}

	return now.Sub(written) < snapshotDeletingMarkTTL
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
