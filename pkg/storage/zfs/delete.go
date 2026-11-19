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

package zfs

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/cozystack/blockstor/pkg/storage"
)

// Deletion follows LINSTOR's ZFS provider (ZfsProvider.delete, canDelete and
// deleteCascading in linstor-server), so a pool carries one scheme whichever
// of the two wrote it, and datasets LINSTOR left marked before a migration
// are finished by the same code that finishes blockstor's own.
//
// Nothing is ever destroyed recursively. A zvol that still has snapshots, or a
// snapshot that still has clones, cannot be destroyed without taking those
// with it, so it is marked for deletion and renamed out of the way instead.
// The marked dataset is destroyed later, by the cascade, when the delete of
// its last snapshot or clone empties it. A dataset nobody marked is never
// destroyed by the cascade.
const (
	// deletedPrefix is what a marked dataset's name starts with after the
	// rename: `_deleted_<rd>_<vol>_<time>` for a zvol and
	// `<zvol>@_deleted_<snap>_<time>` for a snapshot.
	deletedPrefix = "_deleted_"

	// markedForDeletionProp is the ZFS user property LINSTOR sets on a
	// dataset whose deletion was requested but had to wait. The name is
	// LINSTOR's, so a dataset either of them marked is finished by both.
	markedForDeletionProp = "linstor:marked_for_deletion"

	// legacyDeletedMarker is the suffix older blockstor releases put on a
	// snapshot whose delete was blocked by a clone. Such a snapshot was
	// renamed because its delete was requested, so it counts as marked.
	legacyDeletedMarker = "__DELETED__"
)

// errAmbiguousSnapshot refuses a snapshot delete that matches more than one
// renamed zvol: destroying the wrong one would delete data nobody asked for.
var errAmbiguousSnapshot = errors.New("snapshot found under more than one deleted zvol")

// DeleteVolume deletes the zvol of vol. A zvol without snapshots is destroyed,
// and the snapshot it was cloned from goes with it when that snapshot was
// itself marked for deletion and this was its last clone. A zvol with
// snapshots is marked and renamed instead, and keeps them all. Missing is a
// no-op; a probe that fails is an error, never "missing", because the caller
// reads a nil return as "the storage is gone".
func (p *Provider) DeleteVolume(ctx context.Context, vol storage.Volume) error {
	dataset := p.volumeDataset(vol)

	present, err := p.datasetPresent(ctx, dataset)
	if err != nil || !present {
		return err
	}

	return p.deleteVolumeDataset(ctx, dataset)
}

// DeleteSnapshot deletes the snapshot wherever it lives now: under the zvol of
// its resource, or under the renamed zvol a replica delete left it on. A
// snapshot without clones is destroyed, and the zvol it belongs to goes with
// it when that zvol was marked for deletion and this was its last snapshot. A
// snapshot with clones is marked and renamed instead.
func (p *Provider) DeleteSnapshot(ctx context.Context, snap storage.Snapshot) error {
	dataset, err := p.locateSnapshot(ctx, snap)
	if err != nil || dataset == "" {
		return err
	}

	return p.deleteSnapshotDataset(ctx, dataset)
}

func (p *Provider) deleteVolumeDataset(ctx context.Context, dataset string) error {
	snapshots, err := p.remainingSnapshots(ctx, dataset)
	if err != nil {
		return err
	}

	if len(snapshots) > 0 {
		return p.markAndRename(ctx, dataset, p.deletedVolumeName(dataset))
	}

	origin, err := p.originOf(ctx, dataset)
	if err != nil {
		return err
	}

	err = p.destroy(ctx, dataset)
	if err != nil {
		return err
	}

	return p.cascadeToOrigin(ctx, origin)
}

func (p *Provider) deleteSnapshotDataset(ctx context.Context, dataset string) error {
	clones, err := p.clonesOf(ctx, dataset)
	if err != nil {
		return err
	}

	if len(clones) > 0 {
		return p.markAndRename(ctx, dataset, deletedSnapshotName(dataset, p.renameTime()))
	}

	err = p.destroy(ctx, dataset)
	if err != nil {
		return err
	}

	return p.cascadeToBase(ctx, baseOf(dataset))
}

// remainingSnapshots lists the snapshots of the zvol that keep it alive. A
// snapshot already marked for deletion whose last clone is gone is destroyed
// on the way: its delete was requested before, and leaving it would keep the
// zvol for nothing.
func (p *Provider) remainingSnapshots(ctx context.Context, dataset string) ([]string, error) {
	snapshots, err := p.snapshotsOf(ctx, dataset)
	if err != nil {
		return nil, err
	}

	remaining := make([]string, 0, len(snapshots))

	for _, snapshot := range snapshots {
		gone, err := p.destroyIfReleased(ctx, snapshot)
		if err != nil {
			return nil, err
		}

		if !gone {
			remaining = append(remaining, snapshot)
		}
	}

	return remaining, nil
}

// cascadeToOrigin is the step after a zvol is destroyed: the snapshot it was
// cloned from may have been waiting only for it.
func (p *Provider) cascadeToOrigin(ctx context.Context, origin string) error {
	if origin == "" {
		return nil
	}

	gone, err := p.destroyIfReleased(ctx, origin)
	if err != nil || !gone {
		return err
	}

	return p.cascadeToBase(ctx, baseOf(origin))
}

// cascadeToBase is the step after a snapshot is destroyed: the zvol it
// belonged to may have been waiting only for it.
func (p *Provider) cascadeToBase(ctx context.Context, dataset string) error {
	present, err := p.datasetPresent(ctx, dataset)
	if err != nil || !present {
		return err
	}

	marked, err := p.markedForDeletion(ctx, dataset)
	if err != nil || !marked {
		return err
	}

	snapshots, err := p.snapshotsOf(ctx, dataset)
	if err != nil || len(snapshots) > 0 {
		return err
	}

	origin, err := p.originOf(ctx, dataset)
	if err != nil {
		return err
	}

	err = p.destroy(ctx, dataset)
	if err != nil {
		return err
	}

	return p.cascadeToOrigin(ctx, origin)
}

// destroyIfReleased destroys a snapshot that was marked for deletion and has
// no clones left, and reports whether it did. Anything else is kept.
func (p *Provider) destroyIfReleased(ctx context.Context, snapshot string) (bool, error) {
	present, err := p.datasetPresent(ctx, snapshot)
	if err != nil || !present {
		return false, err
	}

	marked, err := p.markedForDeletion(ctx, snapshot)
	if err != nil || !marked {
		return false, err
	}

	clones, err := p.clonesOf(ctx, snapshot)
	if err != nil || len(clones) > 0 {
		return false, err
	}

	return true, p.destroy(ctx, snapshot)
}

// markAndRename marks the dataset first and renames it second, as LINSTOR
// does. A crash in between leaves a marked dataset under its old name, which
// the retried delete marks and renames again.
func (p *Provider) markAndRename(ctx context.Context, dataset, renamed string) error {
	_, err := p.exec.Run(ctx, "zfs", "set", markedForDeletionProp+"=true", dataset)
	if err != nil {
		return errors.Wrapf(err, "zfs set %s on %s", markedForDeletionProp, dataset)
	}

	_, err = p.exec.Run(ctx, "zfs", "rename", dataset, renamed)
	if err != nil {
		return errors.Wrapf(err, "zfs rename %s to %s", dataset, renamed)
	}

	return nil
}

// destroy is the only way this file removes a dataset, and it never recurses:
// a snapshot or clone that appeared after the checks makes ZFS refuse rather
// than take it along.
func (p *Provider) destroy(ctx context.Context, dataset string) error {
	_, err := p.exec.Run(ctx, "zfs", "destroy", dataset)
	if err != nil {
		return errors.Wrapf(err, "zfs destroy %s", dataset)
	}

	return nil
}

// locateSnapshot finds the snapshot under its resource's zvol, or under the
// zvol a replica delete renamed it with. "" means it exists nowhere.
func (p *Provider) locateSnapshot(ctx context.Context, snap storage.Snapshot) (string, error) {
	live := p.snapshotDataset(snap)

	present, err := p.datasetPresent(ctx, live)
	if err != nil || present {
		return live, err
	}

	renamed, err := p.deletedVolumesOf(ctx, live[:strings.LastIndex(live, "@")])
	if err != nil {
		return "", err
	}

	var found []string

	for _, zvol := range renamed {
		candidate := zvol + "@" + snap.SnapshotName

		present, err = p.datasetPresent(ctx, candidate)
		if err != nil {
			return "", err
		}

		if present {
			found = append(found, candidate)
		}
	}

	switch len(found) {
	case 0:
		return "", nil
	case 1:
		return found[0], nil
	default:
		return "", errors.Wrapf(errAmbiguousSnapshot, "%s: %s", snap.SnapshotName, strings.Join(found, ", "))
	}
}

// deletedVolumesOf lists the zvols a delete of dataset renamed:
// `<pool>/_deleted_<name>_<suffix>`, the shape both LINSTOR and blockstor give
// them.
func (p *Provider) deletedVolumesOf(ctx context.Context, dataset string) ([]string, error) {
	out, err := p.exec.Run(ctx, "zfs", "list", "-H", "-o", "name", "-t", "volume", "-d", "1", p.cfg.Pool)
	if err != nil {
		return nil, errors.Wrapf(err, "zfs list volumes of %s", p.cfg.Pool)
	}

	prefix := p.cfg.Pool + "/" + deletedPrefix + strings.TrimPrefix(dataset, p.cfg.Pool+"/") + "_"

	var renamed []string

	for line := range strings.SplitSeq(string(out), "\n") {
		name := strings.TrimSpace(line)

		suffix, ok := strings.CutPrefix(name, prefix)
		if ok && suffix != "" && !strings.ContainsAny(suffix, "/@") {
			renamed = append(renamed, name)
		}
	}

	return renamed, nil
}

// snapshotsOf lists the snapshots taken directly of the zvol.
func (p *Provider) snapshotsOf(ctx context.Context, dataset string) ([]string, error) {
	out, err := p.exec.Run(ctx, "zfs", "list", "-H", "-o", "name", "-t", "snapshot", "-d", "1", dataset)
	if err != nil {
		return nil, errors.Wrapf(err, "zfs list snapshots of %s", dataset)
	}

	var snapshots []string

	for line := range strings.SplitSeq(string(out), "\n") {
		if name := strings.TrimSpace(line); strings.HasPrefix(name, dataset+"@") {
			snapshots = append(snapshots, name)
		}
	}

	return snapshots, nil
}

// clonesOf lists the datasets cloned from the snapshot.
func (p *Provider) clonesOf(ctx context.Context, snapshot string) ([]string, error) {
	value, err := p.property(ctx, snapshot, "clones")
	if err != nil || value == "" {
		return nil, err
	}

	return strings.Split(value, ","), nil
}

// originOf is the snapshot the zvol was cloned from, "" for none.
func (p *Provider) originOf(ctx context.Context, dataset string) (string, error) {
	return p.property(ctx, dataset, "origin")
}

// markedForDeletion reports whether a delete of the dataset was requested and
// had to wait: LINSTOR's property, or the name an older blockstor gave such a
// snapshot.
func (p *Provider) markedForDeletion(ctx context.Context, dataset string) (bool, error) {
	if at := strings.LastIndex(dataset, "@"); at >= 0 && strings.Contains(dataset[at+1:], legacyDeletedMarker) {
		return true, nil
	}

	value, err := p.property(ctx, dataset, markedForDeletionProp)
	if err != nil {
		return false, err
	}

	return value == "true", nil
}

// property reads one property, with ZFS's "-" for "none" folded into "".
func (p *Provider) property(ctx context.Context, dataset, name string) (string, error) {
	ctx, cancel := withProbeTimeout(ctx)
	defer cancel()

	out, err := p.exec.Run(ctx, "zfs", "get", "-H", "-o", "value", name, dataset)
	if err != nil {
		return "", errors.Wrapf(err, "zfs get %s %s", name, dataset)
	}

	value := strings.TrimSpace(string(out))
	if value == "-" {
		return "", nil
	}

	return value, nil
}

// datasetPresent is datasetExists for the paths that delete: only ZFS saying
// the dataset does not exist counts as absent. A probe that times out or
// fails otherwise is an error, because a delete that took it for "absent"
// would report storage gone that is still there.
func (p *Provider) datasetPresent(ctx context.Context, dataset string) (bool, error) {
	ctx, cancel := withProbeTimeout(ctx)
	defer cancel()

	out, err := p.exec.Run(ctx, "zfs", "list", "-H", "-o", "name", dataset)
	if err != nil {
		if strings.Contains(err.Error(), "dataset does not exist") {
			return false, nil
		}

		return false, errors.Wrapf(err, "zfs list %s", dataset)
	}

	return strings.TrimSpace(string(out)) != "", nil
}

// deletedVolumeName is where a zvol with snapshots goes: LINSTOR's
// `_deleted_<name>_<time>`, at the zvol's own level of the pool.
func (p *Provider) deletedVolumeName(dataset string) string {
	parent, name := dataset[:strings.LastIndex(dataset, "/")], dataset[strings.LastIndex(dataset, "/")+1:]

	return parent + "/" + deletedPrefix + name + "_" + p.renameTime()
}

// deletedSnapshotName is where a snapshot with clones goes: LINSTOR's
// `<zvol>@_deleted_<snap>_<time>`.
func deletedSnapshotName(snapshot, stamp string) string {
	at := strings.LastIndex(snapshot, "@")

	return snapshot[:at] + "@" + deletedPrefix + snapshot[at+1:] + "_" + stamp
}

// baseOf is the zvol a snapshot belongs to.
func baseOf(snapshot string) string {
	return snapshot[:strings.LastIndex(snapshot, "@")]
}

// renameTime is LINSTOR's rename stamp, yyyy-MM-dd'T'HH-mm-ss-SSS, which
// keeps two deletes of the same name from colliding.
func (p *Provider) renameTime() string {
	now := p.now()

	return now.Format("2006-01-02T15-04-05") + fmt.Sprintf("-%03d", now.Nanosecond()/int(time.Millisecond))
}
