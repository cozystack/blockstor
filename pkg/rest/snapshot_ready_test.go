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
	"sync"
	"testing"
	"time"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Bug 038 regression: the clone / restore data plane is a node-local
// RestoreVolumeFromSnapshot, but the per-node `@snap` materialises
// ASYNCHRONOUSLY via the satellite SnapshotReconciler. The REST handler
// MUST NOT stamp the restore-marked replicas before the snapshot
// reports a non-zero per-node CreateTimestamp on its nodes — a replica
// that reconciles before its co-located `@snap` exists poisons the
// dataset with a blank CreateVolume and the whole RD deadlocks
// all-empty (clone.sh "diskState empty both peers").
//
// These tests exercise the readiness gate (waitSnapshotReadyOnNodes)
// directly with a LAG STORE that withholds the per-node timestamp for
// the first few reads, then flips it on — modelling the
// SnapshotReconciler stamping it a beat after the restore POST lands.

// readyGateSnapshotStore wraps a SnapshotStore and rewrites the per-node
// CreateTimestamp on Get: the first `notReadyReads` reads report the
// snapshot with ZERO timestamps (the satellite hasn't taken it yet);
// every read after that reports `readyTimestamp` on each per-node
// entry (the snapshot has materialised). Reads are counted so a test
// can assert the gate actually polled past the not-ready window.
type readyGateSnapshotStore struct {
	store.SnapshotStore

	mu             sync.Mutex
	reads          int
	notReadyReads  int
	readyTimestamp int64
}

func (l *readyGateSnapshotStore) Get(ctx context.Context, rdName, snapName string) (apiv1.Snapshot, error) {
	snap, err := l.SnapshotStore.Get(ctx, rdName, snapName)
	if err != nil {
		return snap, err //nolint:wrapcheck // test seam mirrors the real store contract
	}

	l.mu.Lock()
	l.reads++
	ready := l.reads > l.notReadyReads
	l.mu.Unlock()

	for i := range snap.Snapshots {
		if ready {
			snap.Snapshots[i].CreateTimestamp = l.readyTimestamp
		} else {
			snap.Snapshots[i].CreateTimestamp = 0
		}
	}

	return snap, nil
}

func (l *readyGateSnapshotStore) readCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.reads
}

// lagStore embeds an in-memory store and swaps Snapshots() for the lag
// wrapper so the gate sees the not-ready-then-ready timeline.
type lagStore struct {
	store.Store

	snaps *readyGateSnapshotStore
}

func (s *lagStore) Snapshots() store.SnapshotStore { return s.snaps }

// seedReadyGateSnapshot stamps a 2-node snapshot whose per-node
// entries start with a ZERO CreateTimestamp (materialisation pending).
func seedReadyGateSnapshot(t *testing.T, st store.Store, nodes []string) {
	t.Helper()

	ctx := t.Context()

	perNode := make([]apiv1.SnapshotPerNode, 0, len(nodes))
	for _, n := range nodes {
		perNode = append(perNode, apiv1.SnapshotPerNode{
			SnapshotName:    "snap-1",
			NodeName:        n,
			CreateTimestamp: 0, // not yet materialised
		})
	}

	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name:         "snap-1",
		ResourceName: "src",
		Nodes:        nodes,
		Snapshots:    perNode,
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{
			{VolumeNumber: 0, SizeKib: 64},
		},
	}); err != nil {
		t.Fatalf("seed snapshot: %v", err)
	}
}

// TestWaitSnapshotReadyBlocksUntilPerNodeTimestamp pins the core race
// fix: the gate withholds its go-ahead until EVERY target node reports
// a non-zero per-node CreateTimestamp, then returns the full node set.
func TestWaitSnapshotReadyBlocksUntilPerNodeTimestamp(t *testing.T) {
	t.Parallel()

	mem := store.NewInMemory()
	seedReadyGateSnapshot(t, mem, []string{"n1", "n2"})

	lag := &lagStore{
		Store: mem,
		snaps: &readyGateSnapshotStore{
			SnapshotStore:  mem.Snapshots(),
			notReadyReads:  3, // first 3 reads report timestamp 0
			readyTimestamp: 1234567890,
		},
	}

	srv := &Server{
		Store:                lag,
		SnapshotReadyTimeout: 2 * time.Second,
		SnapshotReadyPoll:    2 * time.Millisecond,
	}

	ready := srv.waitSnapshotReadyOnNodes(t.Context(), "src", "snap-1", []string{"n1", "n2"})

	if len(ready) != 2 {
		t.Fatalf("ready nodes: got %v, want both n1+n2 once the per-node "+
			"timestamp lands", ready)
	}

	if got := lag.snaps.readCount(); got <= lag.snaps.notReadyReads {
		t.Errorf("gate read the snapshot %d times, want > %d "+
			"(it must poll PAST the not-ready window, not place on the "+
			"first read)", got, lag.snaps.notReadyReads)
	}
}

// TestWaitSnapshotReadyTimesOutDegradesToPlacement pins the
// best-effort contract: a snapshot whose per-node timestamp NEVER
// materialises (a wedged satellite / legacy CRD) must not wedge the
// restore forever — the gate times out and returns whatever was ready
// (here: nothing), and the caller proceeds to stamp anyway (the
// satellite-side blank-fallback requeue is the backstop).
func TestWaitSnapshotReadyTimesOutDegradesToPlacement(t *testing.T) {
	t.Parallel()

	mem := store.NewInMemory()
	seedReadyGateSnapshot(t, mem, []string{"n1", "n2"})

	lag := &lagStore{
		Store: mem,
		snaps: &readyGateSnapshotStore{
			SnapshotStore:  mem.Snapshots(),
			notReadyReads:  1 << 30, // never becomes ready
			readyTimestamp: 1234567890,
		},
	}

	srv := &Server{
		Store:                lag,
		SnapshotReadyTimeout: 30 * time.Millisecond,
		SnapshotReadyPoll:    5 * time.Millisecond,
	}

	start := time.Now()
	ready := srv.waitSnapshotReadyOnNodes(t.Context(), "src", "snap-1", []string{"n1", "n2"})
	elapsed := time.Since(start)

	if len(ready) != 0 {
		t.Errorf("ready nodes on a never-ready snapshot: got %v, want none", ready)
	}

	if elapsed < 25*time.Millisecond {
		t.Errorf("gate returned after %v, want it to honour the ~30ms timeout "+
			"(a too-early return means it isn't actually waiting)", elapsed)
	}

	if elapsed > 2*time.Second {
		t.Errorf("gate blocked %v on a never-ready snapshot, want a bounded "+
			"timeout (must not wedge the restore forever)", elapsed)
	}
}

// TestWaitSnapshotReadyEmptyNodeListIsNoop pins that the gate is a
// zero-cost no-op when there are no target nodes (a legacy snapshot
// with an empty node set stamps nothing downstream anyway).
func TestWaitSnapshotReadyEmptyNodeListIsNoop(t *testing.T) {
	t.Parallel()

	srv := &Server{Store: store.NewInMemory()}

	if ready := srv.waitSnapshotReadyOnNodes(t.Context(), "src", "snap-1", nil); ready != nil {
		t.Errorf("empty node list: got %v, want nil", ready)
	}
}
