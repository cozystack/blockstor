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

package rest

import (
	"context"
	"slices"
	"time"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// SnapshotTakeWait is how long a snapshot create holds its answer for the
// take to settle. The binaries set it on every rest.Server they build.
//
// LINSTOR answers a snapshot create only once the take is over: the REST
// call maps the whole create flux to its response, and that flux suspends
// IO, takes the snapshot, resumes and commits SUCCESSFUL before it ends.
// linstor-csi relies on that. Right after the create it reads the snapshot
// view through golinstor's resource cache, which keeps the answer for its
// 30s timeout and is dropped only by a snapshot write, and every CSI retry
// of the same create reads that cached copy. Answered before the take, the
// copy said "not ready" and the VolumeSnapshot stayed unready for the whole
// cache timeout although the snapshot was SUCCESSFUL two seconds in.
//
// The bound sits past the controller's suspend deadline, after which the
// controller records the snapshot FAILED, so a take that never settles
// still answers, as before, once the wait runs out.
const SnapshotTakeWait = 150 * time.Second

// snapshotTakePollInterval is how often the wait reads the snapshot back.
// The read is served by the informer cache, so it costs no API round trip.
const snapshotTakePollInterval = 100 * time.Millisecond

// snapshotRef names one snapshot a create wrote.
type snapshotRef struct {
	rd   string
	name string
}

// waitSnapshotsSettled returns once every snapshot in refs is settled, the
// request ends, or SnapshotTakeWait on the server runs out. A zero wait
// returns at once, which is what a Server built without it (the unit tests)
// gets.
func (s *Server) waitSnapshotsSettled(ctx context.Context, refs []snapshotRef) {
	if s.SnapshotTakeWait <= 0 || len(refs) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(ctx, s.SnapshotTakeWait)
	defer cancel()

	ticker := time.NewTicker(snapshotTakePollInterval)
	defer ticker.Stop()

	pending := slices.Clone(refs)
	seen := make(map[snapshotRef]bool, len(refs))

	for {
		pending = slices.DeleteFunc(pending, func(ref snapshotRef) bool {
			return s.snapshotSettled(ctx, ref, seen)
		})

		if len(pending) == 0 {
			return
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// snapshotSettled reports whether the take of ref is over: SUCCESSFUL, a
// failure flag, or a delete. A snapshot the read cannot find yet is pending
// until it has been seen once, since the cache may trail the create; gone
// after that, it was deleted. A failing read leaves it pending.
func (s *Server) snapshotSettled(ctx context.Context, ref snapshotRef, seen map[snapshotRef]bool) bool {
	snap, err := s.Store.Snapshots().Get(ctx, ref.rd, ref.name)
	if err != nil {
		return errors.Is(err, store.ErrNotFound) && seen[ref]
	}

	seen[ref] = true

	return slices.ContainsFunc(snap.Flags, isSettledSnapshotFlag)
}

// isSettledSnapshotFlag reports whether a served flag ends a take.
func isSettledSnapshotFlag(flag string) bool {
	switch flag {
	case apiv1.SnapshotFlagSuccessful,
		apiv1.SnapshotFlagFailedDeployment,
		apiv1.SnapshotFlagFailedDisconnect,
		apiv1.SnapshotFlagDelete,
		snapshotFlagFailed:
		return true
	}

	return false
}

// snapshotFlagFailed is the controller's terminal failure flag, the CRD's
// SnapshotStatusFlagFailed, served as stored.
const snapshotFlagFailed = "FAILED"
