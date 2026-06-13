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
	"time"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// Bug 038 snapshot-readiness gate.
//
// The clone / snapshot-restore data plane materialises each restored
// replica with a NODE-LOCAL RestoreVolumeFromSnapshot (`zfs clone` of
// the source `@snap` on the same node). The REST handler stamps the
// restore-marked Resource CRDs synchronously, but the per-node `@snap`
// is created ASYNCHRONOUSLY by the satellite SnapshotReconciler. A
// replica that reconciles before its co-located `@snap` exists hits
// `RestoreVolumeFromSnapshot` → storage.ErrNotFound → a blank
// `CreateVolume`, which is TERMINAL: the ZFS provider's `datasetExists`
// short-circuit means a later clone is skipped forever, and the Bug 397
// blank-fallback guard then makes every replica a SyncTarget with no
// SyncSource → an all-empty deadlock (clone.sh "diskState empty both
// peers"; cross-node lanes "stuck Connecting / no devicePath").
//
// The fix gates placement on per-node snapshot readiness: the handler
// waits until every snapshot-holding node has a per-node entry with a
// non-zero CreateTimestamp before stamping the replicas. The
// SnapshotReconciler stamps that timestamp only AFTER `CreateSnapshot`
// returns (i.e. after the on-disk `@snap` exists — see
// pkg/satellite/controllers/snapshot.go handleTakeSnapshotPhase), so a
// non-zero timestamp is proof the local snapshot is materialised and a
// node-local restore can succeed.
const (
	// snapshotReadyTimeoutDefault bounds the synchronous wait. The
	// clone / CSI-clone callers already poll CloneStatus in a loop, so
	// a bounded block here matches the existing cache-retry idiom and
	// keeps the POST in the same multi-second envelope as a spawn.
	snapshotReadyTimeoutDefault = 90 * time.Second
	// snapshotReadyPollDefault is the inter-poll back-off. The
	// SnapshotReconciler materialises a small CoW snapshot in well
	// under a second per node; 500 ms keeps the steady-state latency
	// negligible once the snapshot lands.
	snapshotReadyPollDefault = 500 * time.Millisecond
)

// snapshotReadyTuning resolves the readiness-gate timeout + poll
// interval, falling back to the package defaults when the Server fields
// are left zero (the production path). Tests shrink them to exercise
// the wait loop without a real sleep.
func (s *Server) snapshotReadyTuning() (time.Duration, time.Duration) {
	timeout := s.SnapshotReadyTimeout
	if timeout <= 0 {
		timeout = snapshotReadyTimeoutDefault
	}

	poll := s.SnapshotReadyPoll
	if poll <= 0 {
		poll = snapshotReadyPollDefault
	}

	return timeout, poll
}

// waitSnapshotReadyOnNodes blocks until the snapshot `snapName` on
// `srcRD` reports a materialised on-disk copy on every node in `nodes`
// (per-node CreateTimestamp != 0), or the readiness timeout elapses.
//
// Returns the set of nodes that became ready. On timeout it returns the
// nodes that DID become ready (possibly a subset, possibly empty); the
// caller decides whether a partial / empty result is still actionable.
// This is deliberately best-effort and NEVER fails the restore: gating
// is the primary race fix, but the satellite-side blank-fallback
// requeue (materializeVolume) is the defense-in-depth backstop, so a
// timeout falling through to placement degrades to the pre-gate
// behaviour rather than wedging a legitimate restore.
//
// Nodes whose per-node snapshot status never materialises (legacy
// snapshot CRDs that predate per-node tracking, or a satellite that
// genuinely failed the snapshot) are simply not reported ready; the
// caller still places on the requested node set so a degenerate /
// legacy snapshot keeps the upstream "restore onto the snapshot nodes"
// contract.
func (s *Server) waitSnapshotReadyOnNodes(ctx context.Context, srcRD, snapName string, nodes []string) []string {
	if len(nodes) == 0 {
		return nil
	}

	timeout, poll := s.snapshotReadyTuning()
	deadline := time.Now().Add(timeout)

	want := make(map[string]struct{}, len(nodes))
	for _, n := range nodes {
		if n != "" {
			want[n] = struct{}{}
		}
	}

	for {
		snap, err := s.Store.Snapshots().Get(ctx, srcRD, snapName)
		if err == nil {
			ready := readySnapshotNodes(&snap, want)
			if len(ready) == len(want) {
				return ready
			}
		}

		if time.Now().After(deadline) {
			// Timed out — report whatever became ready (possibly
			// nothing). The caller proceeds to place anyway; the
			// satellite requeue covers the residual race.
			if err != nil {
				return nil
			}

			return readySnapshotNodes(&snap, want)
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(poll):
		}
	}
}

// readySnapshotNodes returns the members of `want` whose per-node
// snapshot entry carries a non-zero CreateTimestamp (the same
// materialised-on-disk proxy isSnapshotSuccessful keys on). Order is
// not significant.
func readySnapshotNodes(snap *apiv1.Snapshot, want map[string]struct{}) []string {
	ready := make([]string, 0, len(want))

	for i := range snap.Snapshots {
		entry := &snap.Snapshots[i]
		if entry.CreateTimestamp == 0 {
			continue
		}

		if _, ok := want[entry.NodeName]; ok {
			ready = append(ready, entry.NodeName)
		}
	}

	return ready
}
