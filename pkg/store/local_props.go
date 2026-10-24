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
	"errors"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// RestoreFromSnapshotProp is the marker a restored or cloned definition
// carries, `<source definition>:<snapshot>`. The satellite reads it to restore
// from the snapshot instead of creating a blank volume, and the resume paths
// read it to recognise their own leftover.
const RestoreFromSnapshotProp = "BlockstorRestoreFromSnapshot"

// RollbackAbandonedProp marks a resource definition whose compensation started
// and did not report finishing, with the step it stopped at. The replay gate of
// the clone path refuses over it.
const RollbackAbandonedProp = "BlockstorRollbackAbandoned"

// RollbackInProgress is the value RollbackAbandonedProp carries while a
// rollback is running, before it reports the step it stopped at.
const RollbackInProgress = "in-progress"

// RollbackSinceProp records when the in-progress mark was written, in RFC
// 3339. A rollback that is still running holds the mark for less than its
// budget; one whose process was killed between writing the mark and taking it
// off leaves it for good, and every retry over it answered 409 until somebody
// cleared it by hand. With the time on it, a retry takes over a mark older
// than StaleRollbackAfter (TakeOverStaleRollback).
const RollbackSinceProp = "Blockstor/RollbackSince"

// StaleRollbackAfter is how long an in-progress mark may stand before a retry
// takes it over. A rollback runs on a detached budget of about 21 seconds and
// every write it makes after that lands on an expired context, so a mark
// older than this has nobody behind it. The margin covers clock skew between
// the API server replicas that wrote and read it.
const StaleRollbackAfter = 2 * time.Minute

// MarkRollback writes the rollback mark into props: the in-progress mark with
// the time it was written, or a step, which stands until an operator acts and
// carries no time.
func MarkRollback(props map[string]string, step string, now time.Time) map[string]string {
	if props == nil {
		props = map[string]string{}
	}

	props[RollbackAbandonedProp] = step

	if step == RollbackInProgress {
		props[RollbackSinceProp] = now.UTC().Format(time.RFC3339)
	} else {
		delete(props, RollbackSinceProp)
	}

	return props
}

// UnmarkRollback removes the rollback mark and its time.
func UnmarkRollback(props map[string]string) {
	delete(props, RollbackAbandonedProp)
	delete(props, RollbackSinceProp)
}

// rollbackMarkStale reports whether props carry an in-progress mark written
// more than StaleRollbackAfter before now. A mark with no readable time is
// never stale: it is taken off by hand, as before.
func rollbackMarkStale(props map[string]string, now time.Time) bool {
	if props[RollbackAbandonedProp] != RollbackInProgress {
		return false
	}

	since, err := time.Parse(time.RFC3339, props[RollbackSinceProp])
	if err != nil {
		return false
	}

	return now.Sub(since) > StaleRollbackAfter
}

// RestoreAdoptedProp marks a definition that a retry adopted as the leftover of
// an earlier attempt at the same clone or restore, before it wrote anything
// into it. The attempt that created the definition reads it before rolling
// back, and leaves the definition to the retry that answered for it.
//
// It stays on the definition once the adopting request has answered. That
// request cannot know whether the attempt it adopted from is still running,
// and a rollback that started after the mark came off would read no adoption
// and delete what the retry answered for, which is the race the mark closes.
// Kept, it says only that a retry answered for the definition, which stays
// true; it never travels to a snapshot or a definition copied from this one
// (TravellingProps), and it goes with the definition.
const RestoreAdoptedProp = "Blockstor/RestoreAdopted"

// RestoreVolumesProp records, on a restored or cloned definition, the volumes
// the snapshot it was restored from held, as `<number>=<size KiB>` pairs.
//
// The snapshot is the reference a retry judges a leftover against, and it does
// not always outlive the leftover: an operator can delete it, and a clone's
// internal one is reaped. Judged without it, a definition with one volume and
// one live replica read as finished whatever the snapshot held, so a clone
// that had restored only some of its volumes before the snapshot went was
// reported complete. The record is written with the marker, and it is the
// reference whenever the caller does not hand the snapshot in, whether or not
// the snapshot still exists: every rollback's spare-or-delete judgement
// (ReadsAsFinished) reads it. A leftover written before the record existed
// carries none and is judged as before, on its own volumes alone.
const RestoreVolumesProp = "Blockstor/RestoreVolumes"

// WithRestoreMarker stamps a definition being restored from snap with the
// marker the satellite restores from, and the record of the volumes snap held
// beside it, so every door writes the two together. It returns the props,
// allocated when nil.
func WithRestoreMarker(props map[string]string, snap *apiv1.Snapshot) map[string]string {
	if props == nil {
		props = map[string]string{}
	}

	props[RestoreFromSnapshotProp] = snap.ResourceName + ":" + snap.Name
	props[RestoreVolumesProp] = EncodeRestoreVolumes(snap.VolumeDefinitions)

	return props
}

// EncodeRestoreVolumes writes the RestoreVolumesProp value for a snapshot's
// volumes.
func EncodeRestoreVolumes(vols []apiv1.SnapshotVolumeDef) string {
	parts := make([]string, 0, len(vols))
	for _, vol := range vols {
		parts = append(parts, strconv.FormatInt(int64(vol.VolumeNumber), 10)+"="+strconv.FormatInt(vol.SizeKib, 10))
	}

	return strings.Join(parts, ",")
}

// RecordedRestoreVolumes reads the RestoreVolumesProp back. ok is false when
// the definition carries no record, or one that does not parse, which is
// judged as no record at all.
func RecordedRestoreVolumes(props map[string]string) ([]apiv1.SnapshotVolumeDef, bool) {
	raw, present := props[RestoreVolumesProp]
	if !present {
		return nil, false
	}

	if raw == "" {
		return []apiv1.SnapshotVolumeDef{}, true
	}

	pairs := strings.Split(raw, ",")
	out := make([]apiv1.SnapshotVolumeDef, 0, len(pairs))

	for _, pair := range pairs {
		number, size, found := strings.Cut(pair, "=")
		if !found {
			return nil, false
		}

		n, err := strconv.ParseInt(number, 10, 32)
		if err != nil {
			return nil, false
		}

		kib, err := strconv.ParseInt(size, 10, 64)
		if err != nil {
			return nil, false
		}

		out = append(out, apiv1.SnapshotVolumeDef{VolumeNumber: int32(n), SizeKib: kib})
	}

	return out, true
}

// TravellingProps copies a props bag for use on another object, leaving out
// the props that describe the object they were read from and nothing derived
// from it. Nil stays nil.
//
// A definition's props are copied forward wholesale: onto every snapshot taken
// of it, and from a snapshot, or from the source when the snapshot has none,
// onto every definition restored or cloned from it. A mark that records
// something about THIS definition, read on a copy, says it about a definition
// it was never true of: a healthy clone of a marked source was refused on the
// replay linstor-csi sends, and the correction said to delete it.
//
// The restore marker is the other member of the class: it records where THIS
// definition's data came from, and the dispatcher, the placer and autoplace
// all read it off whatever definition carries it. A volume-less clone of a
// restored definition kept the source's marker, so the first volume added to
// it later was restored from somebody else's snapshot, on that snapshot's
// nodes. Every restore door stamps its own marker after the copy, and nothing
// reads the marker off a snapshot.
func TravellingProps(props map[string]string) map[string]string {
	out := maps.Clone(props)
	delete(out, RollbackAbandonedProp)
	delete(out, RollbackSinceProp)
	delete(out, RestoreAdoptedProp)
	delete(out, RestoreFromSnapshotProp)
	delete(out, RestoreVolumesProp)

	return out
}

// serverOwnedDefinitionProps are the props blockstor writes onto a definition
// and reads back to decide what that definition is. A caller's prop edit that
// rewrites or removes one of them changes what blockstor believes about the
// definition without blockstor having done anything.
var serverOwnedDefinitionProps = []string{ //nolint:gochecknoglobals // a fixed list, read only
	RestoreFromSnapshotProp,
	RollbackAbandonedProp,
	RollbackSinceProp,
	RestoreAdoptedProp,
	RestoreVolumesProp,
}

// The steps a rollback gives up at before it touches anything: it found a
// snapshot on the definition, or could not read whether there is one, and a
// snapshot is somebody's data it does not destroy. The definition they mark
// is whole.
const (
	RollbackStepSnapshots     = "snapshots"
	RollbackStepReadSnapshots = "read-snapshots"
)

// The first steps that take a definition apart, one per door: the REST
// rollback starts by reaping replicas, the CLI one by deleting the definition.
// A rollback writes one of them before it runs it, so a mark that names
// either may stand over a definition already part torn.
const (
	RollbackStepReapReplicas     = "reap-replicas"
	RollbackStepDeleteDefinition = "delete-definition"
)

// ErrRollbackStepMarkKept refuses an operator's delete of a rollback mark that
// names a step a rollback gave up at after it had started taking the
// definition apart: clearing the mark hands the remains to a retry that
// resumes over them. A mark over a definition left whole (in progress, or a
// rollback that stopped at its snapshots) is the operator's to clear.
var ErrRollbackStepMarkKept = errors.New("the rollback mark names the step a rollback gave up at " +
	"after it had started taking the definition apart; delete the definition instead")

// RollbackMarkClearRefusal judges an operator's edit by the props before and
// after it: a delete of RollbackAbandonedProp is allowed only when it held a
// mark over a definition left whole. See ServerOwnedPropEditByOperator.
func RollbackMarkClearRefusal(before, after map[string]string) error {
	switch before[RollbackAbandonedProp] {
	case "", RollbackInProgress, RollbackStepSnapshots, RollbackStepReadSnapshots:
		return nil
	}

	if _, kept := after[RollbackAbandonedProp]; kept {
		return nil
	}

	return ErrRollbackStepMarkKept
}

// ServerOwnedPropEditByOperator is ServerOwnedPropEdit for an operator's own
// edit of a definition (`rd modify`, `rd set-property`), which may delete three
// of the marks by name.
//
// RollbackAbandonedProp, while it marks a definition left whole (see
// RollbackMarkClearRefusal). A rollback that had to leave the definition
// whole and then could not take its mark back off leaves one that every retry
// refuses, and deleting the definition is no way out when it holds a volume a
// caller was told exists.
//
// RestoreAdoptedProp. A claim whose patch landed and whose answer and
// read-back both failed leaves the mark with no request answering for it, and
// every later rollback yields on it; where the clone or restore can never
// finish, nothing else takes it off. Deleting it hands the definition back to
// the rollback of whichever attempt is still running, so it is the operator's
// to do once none is.
//
// RestoreFromSnapshotProp, once the restore it marks is finished (see
// RestoreMarkerClearRefusal). The marker keeps pinning the definition's
// placement to the snapshot's backend and nodes long after the data is
// restored, and deleting it is the only way to lift that pin.
//
// Setting any of these marks, or any edit of the others, stays refused. The clone
// door keeps the strict check: its edits are re-applied by every replay, where
// deleting a mark would clear one a running request still relies on.
//
// An override of a mark to an empty value is the upstream spelling of the
// same delete (`set-property KEY ""`), and is read as one.
func ServerOwnedPropEditByOperator(overrideProps map[string]string, deleteProps, deleteNamespaces []string) string {
	clearable := func(k string) bool {
		return k == RollbackAbandonedProp || k == RollbackSinceProp || k == RestoreAdoptedProp || k == RestoreFromSnapshotProp
	}
	kept := slices.DeleteFunc(slices.Clone(deleteProps), clearable)

	overrides := maps.Clone(overrideProps)
	maps.DeleteFunc(overrides, func(k, v string) bool { return clearable(k) && v == "" })

	return ServerOwnedPropEdit(overrides, kept, deleteNamespaces)
}

// ServerOwnedPropEdit names the first server-owned definition prop a set of
// prop edits would rewrite or remove, or "" when it touches none: a key set or
// deleted by name, or a deleted namespace the key lives in.
func ServerOwnedPropEdit(overrideProps map[string]string, deleteProps, deleteNamespaces []string) string {
	for _, key := range serverOwnedDefinitionProps {
		if _, set := overrideProps[key]; set {
			return key
		}

		if slices.Contains(deleteProps, key) {
			return key
		}

		for _, ns := range deleteNamespaces {
			if NamespaceCovers(ns, key) {
				return key
			}
		}
	}

	return ""
}

// NamespaceCovers reports whether deleting namespace ns removes key. It is the
// one reading of a `delete_namespaces` entry, shared by the deleter and every
// guard in front of it: a guard that spelled the namespace another way would
// let through a delete it means to refuse, and then refuse putting the key
// back. A namespace covers the keys below it and not a key spelled as it, as
// upstream's PropsContainer.removeNamespace does; trailing slashes are the
// same namespace, and one that is nothing but slashes names none.
func NamespaceCovers(ns, key string) bool {
	ns = strings.TrimRight(ns, "/")

	return ns != "" && strings.HasPrefix(key, ns+"/")
}
