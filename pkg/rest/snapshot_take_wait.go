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
	"net/http"
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

// snapshotUnseenConfirmPolls is how many reads in a row may miss a snapshot
// the cache never showed before the wait asks the API server whether it is
// there at all. The cache trails a create by far less, so only a snapshot
// deleted before the cache caught up pays for that read.
const snapshotUnseenConfirmPolls = 10

// snapshotRef names one snapshot a create wrote.
type snapshotRef struct {
	rd   string
	name string
}

// takeEnd is how the take of one snapshot ended, as far as the wait saw.
type takeEnd int

const (
	// takeOpen is a take the wait did not see end: no wait, the bound
	// ran out, or the request ended first.
	takeOpen takeEnd = iota
	takeSucceeded
	takeFailed
	takeDeleted
)

// takeOutcome is how the take of one snapshot ended and, for a failed
// one, the flag and the abort reason the controller recorded.
type takeOutcome struct {
	end    takeEnd
	flag   string
	reason string
}

// waitSnapshotsSettled returns how each take in refs ended, once every one
// is settled, the request ends, or SnapshotTakeWait on the server runs out.
// A ref missing from the result is still open. A zero wait returns at once,
// which is what a Server built without it (the unit tests) gets.
func (s *Server) waitSnapshotsSettled(ctx context.Context, refs []snapshotRef) map[snapshotRef]takeOutcome {
	if s.SnapshotTakeWait <= 0 || len(refs) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, s.SnapshotTakeWait)
	defer cancel()

	ticker := time.NewTicker(snapshotTakePollInterval)
	defer ticker.Stop()

	pending := slices.Clone(refs)
	seen := make(map[snapshotRef]int, len(refs))
	outcomes := make(map[snapshotRef]takeOutcome, len(refs))

	for {
		pending = slices.DeleteFunc(pending, func(ref snapshotRef) bool {
			outcome := s.snapshotTakeOutcome(ctx, ref, seen)
			if outcome.end == takeOpen {
				return false
			}

			outcomes[ref] = outcome

			return true
		})

		if len(pending) == 0 {
			return outcomes
		}

		select {
		case <-ctx.Done():
			return outcomes
		case <-ticker.C:
		}
	}
}

// snapshotTakeOutcome reads how the take of ref ended: SUCCESSFUL, a
// failure flag, or a delete. A snapshot the read cannot find yet is open
// until it has been seen once, since the cache may trail the create; gone
// after that, it was deleted. One the cache keeps missing without ever
// showing it is looked up on the API server, where the create is already
// visible: a delete that landed before the cache caught up is otherwise
// waited on until the bound. A failing read leaves it open.
//
// seen counts each ref's misses before its first sighting, and goes
// negative once the ref has been seen.
func (s *Server) snapshotTakeOutcome(ctx context.Context, ref snapshotRef, seen map[snapshotRef]int) takeOutcome {
	snap, err := s.Store.Snapshots().Get(ctx, ref.rd, ref.name)
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			return takeOutcome{end: takeOpen}
		}

		if seen[ref] < 0 {
			return takeOutcome{end: takeDeleted}
		}

		seen[ref]++
		if seen[ref]%snapshotUnseenConfirmPolls != 0 {
			return takeOutcome{end: takeOpen}
		}

		_, err = store.UncachedSnapshot(ctx, s.Store, ref.rd, ref.name)
		if errors.Is(err, store.ErrNotFound) {
			return takeOutcome{end: takeDeleted}
		}

		return takeOutcome{end: takeOpen}
	}

	seen[ref] = -1

	// SUCCESSFUL first: a take that committed is over even if a failure
	// marker trails it. The served flags drop SUCCESSFUL once a delete
	// started, so a deleting snapshot lands on DELETE below.
	if slices.Contains(snap.Flags, apiv1.SnapshotFlagSuccessful) {
		return takeOutcome{end: takeSucceeded}
	}

	if i := slices.IndexFunc(snap.Flags, isFailedSnapshotFlag); i >= 0 {
		return takeOutcome{end: takeFailed, flag: snap.Flags[i], reason: snap.AbortReason}
	}

	if slices.Contains(snap.Flags, apiv1.SnapshotFlagDelete) {
		return takeOutcome{end: takeDeleted}
	}

	return takeOutcome{end: takeOpen}
}

// takeFailureRcs is the error a create answers for each take in refs that
// failed or was deleted under it. LINSTOR's create flux ends with the
// abort's error, which its REST layer appends to the responses already
// collected and answers with 500; linstor-csi sees the error instead of a
// snapshot it believes taken.
func takeFailureRcs(refs []snapshotRef, outcomes map[snapshotRef]takeOutcome) []apiv1.APICallRc {
	var rcs []apiv1.APICallRc

	for _, ref := range refs {
		outcome := outcomes[ref]

		switch outcome.end {
		case takeFailed:
			message := "snapshot " + ref.rd + "/" + ref.name + " aborted (" + outcome.flag + ")"
			if outcome.reason != "" {
				message += ": " + outcome.reason
			}

			rcs = append(rcs, apiv1.APICallRc{RetCode: apiCallRcError | apiCallRcFailUnknownError, Message: message})
		case takeDeleted:
			rcs = append(rcs, apiv1.APICallRc{
				RetCode: apiCallRcError | apiCallRcFailUnknownError,
				Message: "snapshot " + ref.rd + "/" + ref.name + " was deleted before its take ended",
			})
		case takeOpen, takeSucceeded:
		}
	}

	return rcs
}

// writeSnapshotCreateAnswer answers a create with its rcs: 201, or 500 with
// the failure appended when a take failed, as LINSTOR's REST layer maps an
// error entry.
func writeSnapshotCreateAnswer(w http.ResponseWriter, rcs, failures []apiv1.APICallRc) {
	if len(failures) > 0 {
		writeJSON(w, http.StatusInternalServerError, append(rcs, failures...))

		return
	}

	writeJSON(w, http.StatusCreated, rcs)
}

// isFailedSnapshotFlag reports whether a served flag records a failed take.
func isFailedSnapshotFlag(flag string) bool {
	switch flag {
	case apiv1.SnapshotFlagFailedDeployment,
		apiv1.SnapshotFlagFailedDisconnect,
		snapshotFlagFailed:
		return true
	}

	return false
}

// snapshotFlagFailed is the controller's terminal failure flag, the CRD's
// SnapshotStatusFlagFailed, served as stored.
const snapshotFlagFailed = "FAILED"
