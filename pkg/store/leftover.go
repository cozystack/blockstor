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

// LeftoverProgress is where a target carrying a clone's or a restore's marker
// stands. Every door that resumes one asks it: the REST clone and its status
// poll, the REST restore and the CLI restore. They have to get the same
// answer, or one door finishes what its twin refuses.
type LeftoverProgress int

const (
	// LeftoverUnfinished is not done and not broken: no volumes yet, some of
	// the snapshot's volumes still missing, or no replica holding them. The
	// resume finishes it.
	LeftoverUnfinished LeftoverProgress = iota
	// LeftoverFinished holds every volume and, where one is needed, at least
	// one replica.
	LeftoverFinished
	// LeftoverForeign carries a volume the restore would not have written.
	LeftoverForeign
	// LeftoverTearingDown still lists replicas, but every one of them is
	// already accepted for deletion. Neither finished nor safe to resume: the
	// deletion is reaping what completing it would write.
	LeftoverTearingDown
)

// AssessLeftover is the one judgement of a marker-bearing leftover. targetVDs
// may be empty. snap is the point-in-time the leftover was restored from, or
// nil once that is gone. A clone needs a replica to hold its data; a restore
// needs one only when the request placed replicas at all, since a bare restore
// leaves an empty shell by design.
//
// The replicas are read first, because a tear-down decides the answer whatever
// the volumes say. A leftover the resume would write into, one with no volumes
// or missing one the snapshot recorded, is refused while every replica under
// the name is stamped for deletion: hydrating into it races the tear-down
// reaping what it writes. A leftover that would be answered finished is refused
// the same way when it needs a replica to be finished, since none of its
// replicas holds the data any more. A finished bare restore needs none, so its
// replay stays a replay.
//
// Where the replicas are is not part of the question. One replica with a disk
// holds the data, and which nodes carry it afterwards is placement, which ordinary
// operations change: a resume that topped up every node the request named
// re-stamped a replica on a node the operator had emptied, restored from the
// point-in-time beside a replica that had moved on with live writes.
//
// A volume number the snapshot never recorded is judged by what else the target
// holds. Beside a missing snapshot volume it is somebody else's: the resume
// would hydrate around it and report complete a definition carrying a volume
// nothing restored. Where a replica is needed and there is none, the same.
// Beside every snapshot volume it is the target's own, added after the copy
// finished the way a volume is expanded: on a clone or a placed restore once it
// also has a replica, and on a bare restore, which places nothing, by those
// volumes alone. Refusing it would tell the operator to delete a working
// definition.
func AssessLeftover(
	ctx context.Context, st Store, targetName string,
	targetVDs []apiv1.VolumeDefinition, snap *apiv1.Snapshot, needReplica bool,
) (LeftoverProgress, error) {
	count, err := CountReplicas(ctx, st, targetName)
	if err != nil {
		return LeftoverUnfinished, err
	}

	tearingDown := count.Total > 0 && count.Live == 0

	unfinished := LeftoverUnfinished
	if tearingDown {
		unfinished = LeftoverTearingDown
	}

	if len(targetVDs) == 0 {
		return unfinished, nil
	}

	snap, err = referenceShape(ctx, st, targetName, snap)
	if err != nil {
		return LeftoverUnfinished, err
	}

	var shape leftoverShape

	if snap != nil {
		shape = leftoverAgainstSnapshot(snap, targetVDs)

		switch {
		case shape.smaller, shape.missing && shape.extra:
			return LeftoverForeign, nil
		case shape.missing:
			return unfinished, nil
		}
	}

	switch {
	case !needReplica:
		return LeftoverFinished, nil
	case count.Holding > 0:
		return LeftoverFinished, nil
	case tearingDown:
		return LeftoverTearingDown, nil
	case shape.extra:
		return LeftoverForeign, nil
	default:
		return LeftoverUnfinished, nil
	}
}

// referenceShape is what a leftover is judged against: the snapshot while it
// exists, and once it is gone the volumes the definition recorded from it.
func referenceShape(ctx context.Context, st Store, targetName string, snap *apiv1.Snapshot) (*apiv1.Snapshot, error) {
	if snap != nil {
		return snap, nil
	}

	return recordedSnapshotShape(ctx, st, targetName)
}

// recordedSnapshotShape stands in for a snapshot that is gone with the volumes
// the definition recorded when it was restored from it (RestoreVolumesProp),
// so a leftover is still judged against what the snapshot held. nil means no
// record: the leftover is judged on its own volumes, as before the record
// existed.
func recordedSnapshotShape(ctx context.Context, st Store, targetName string) (*apiv1.Snapshot, error) {
	rd, err := st.ResourceDefinitions().GetUncached(ctx, targetName)
	if errors.Is(err, ErrNotFound) {
		return nil, nil //nolint:nilnil // no definition, no record
	}

	if err != nil {
		return nil, fmt.Errorf("read %q for its recorded volumes: %w", targetName, err)
	}

	vols, ok := RecordedRestoreVolumes(rd.Props)
	if !ok {
		return nil, nil //nolint:nilnil // no record: judged as before
	}

	return &apiv1.Snapshot{VolumeDefinitions: vols}, nil
}

// CountReplicas counts the replicas under a name: how many hold the data, how
// many are not stamped for deletion, and all of them. A replica stamped for
// deletion does not hold the data: its satellite finalizer may keep it listed
// for as long as the owning node is down, and a replay answered 201 over it
// binds a volume to a definition that is going away. Nor does a diskless or
// tie-breaker one, which keeps a definition from reading as torn down and
// never makes it finished: answered 201 over, it reports a volume restored
// with no copy of the data anywhere.
//
// The list is the API server's where the store has a direct reader: every
// caller decides from it whether a leftover is finished, torn down or still to
// be completed, and a lagging informer answers all three wrong, a replica
// whose delete it has not seen yet counted live, one just stamped not at all.
func CountReplicas(ctx context.Context, st Store, rdName string) (ReplicaCount, error) {
	replicas, err := LiveReplicas(ctx, st, rdName)
	if err != nil {
		return ReplicaCount{}, fmt.Errorf("list the replicas of %q: %w", rdName, err)
	}

	count := ReplicaCount{Total: len(replicas)}

	for i := range replicas {
		if slices.Contains(replicas[i].Flags, apiv1.ResourceFlagDelete) {
			continue
		}

		count.Live++

		if HoldsData(&replicas[i]) {
			count.Holding++
		}
	}

	return count, nil
}

// ReplicaCount is what CountReplicas found under a definition.
type ReplicaCount struct {
	// Holding is the replicas that hold the data: not stamped for deletion,
	// and neither diskless nor a tie-breaker.
	Holding int
	// Live is the replicas not stamped for deletion.
	Live int
	// Total is every replica listed.
	Total int
}

// HoldsData reports whether a replica has a disk of its own. A diskless or
// tie-breaker replica reads its peers' data over the network and has none.
func HoldsData(res *apiv1.Resource) bool {
	return !slices.Contains(res.Flags, apiv1.ResourceFlagDiskless) &&
		!slices.Contains(res.Flags, apiv1.ResourceFlagTieBreaker)
}

// MissingReplicas names the nodes in wanted that hold no replica of rdName
// with a disk, read past the cache. A rollback that leaves a definition because it
// already reads as finished uses it to say which of the replicas the request
// asked for never landed: a finished definition is not re-placed by a retry,
// so the caller has to place them.
func MissingReplicas(ctx context.Context, st Store, rdName string, wanted []string) ([]string, error) {
	replicas, err := LiveReplicas(ctx, st, rdName)
	if err != nil {
		return nil, fmt.Errorf("list the replicas of %q: %w", rdName, err)
	}

	var missing []string

	for _, node := range wanted {
		placed := slices.ContainsFunc(replicas, func(r apiv1.Resource) bool {
			return strings.EqualFold(r.NodeName, node) && !slices.Contains(r.Flags, apiv1.ResourceFlagDelete) &&
				HoldsData(&r)
		})
		if !placed {
			missing = append(missing, node)
		}
	}

	return missing, nil
}

// leftoverShape is how a target's volumes compare with the snapshot's.
type leftoverShape struct {
	// missing: a volume the snapshot recorded is not on the target yet. A
	// multi-volume restore whose first attempt died between two
	// VolumeDefinitions().Create calls has this.
	missing bool
	// smaller: a volume is on the target at less than the snapshot captured.
	smaller bool
	// extra: the target holds a volume number the snapshot never recorded.
	extra bool
}

// leftoverAgainstSnapshot compares a leftover target's volumes with the
// snapshot it would be restored from.
//
// Comparing volume COUNTS here would refuse the partial: hydration tolerates a
// volume already present so it can be finished.
//
// Larger than captured is still the target's own: a finished clone may be
// expanded like any volume, and an ordinary ControllerExpandVolume on it must
// not turn every later replay into a refusal whose correction deletes a clone
// holding data. Hydration never writes a smaller volume and nothing shrinks
// one, so smaller is the shape that is somebody else's.
func leftoverAgainstSnapshot(snap *apiv1.Snapshot, targetVDs []apiv1.VolumeDefinition) leftoverShape {
	var shape leftoverShape

	captured := make(map[int32]int64, len(snap.VolumeDefinitions))
	for _, vol := range snap.VolumeDefinitions {
		captured[vol.VolumeNumber] = vol.SizeKib
	}

	present := make(map[int32]struct{}, len(targetVDs))

	for i := range targetVDs {
		present[targetVDs[i].VolumeNumber] = struct{}{}

		was, ok := captured[targetVDs[i].VolumeNumber]

		switch {
		case !ok:
			shape.extra = true
		case targetVDs[i].SizeKib < was:
			shape.smaller = true
		}
	}

	for number := range captured {
		if _, ok := present[number]; !ok {
			shape.missing = true
		}
	}

	return shape
}
