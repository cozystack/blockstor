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
	"testing"
	"time"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// Two reaps of one clone started in the same second write different marks.
func TestReapingMarksOfOneSecondDiffer(t *testing.T) {
	t.Parallel()

	now := time.Now()
	if a, b := newReapingMark("dst", now), newReapingMark("dst", now); a == b {
		t.Fatalf("two reaps wrote the same mark %q", a)
	}

	if !reapingMarkIsLive(newReapingMark("dst", now), now) {
		t.Error("a fresh mark does not read as live")
	}
}

// A second reap does not write over a live mark of the first: that reap
// owns the decision, so the second one stands down and the first reap's
// mark stays for a restore to see.
func TestASecondReapLeavesTheFirstReapsMark(t *testing.T) {
	t.Parallel()

	st := NewInMemory()
	ctx := t.Context()
	seedCloneSnapshot(t, st)

	now := time.Now()
	first, second := newReapingMark("dst", now), newReapingMark("dst", now)

	if err := setReapingMark(ctx, st, "src", "clone-dst", first); err != nil {
		t.Fatalf("first mark: %v", err)
	}

	if err := setReapingMark(ctx, st, "src", "clone-dst", second); !errors.Is(err, errAnotherReap) {
		t.Fatalf("second mark = %v, want errAnotherReap", err)
	}

	if err := dropReapingMark(ctx, st, "src", "clone-dst", second); err != nil {
		t.Fatalf("second reap takes its mark off: %v", err)
	}

	if got := readMark(t, st); got != first {
		t.Fatalf("mark = %q, want the first reap's %q", got, first)
	}
}

// The order that let a reap delete a restore's source: reap A marks, reap B
// would have written over it and then kept the snapshot, taking off the
// only mark. B now stands down without touching it, so a restore that
// creates its definition next still sees A under way and withdraws.
func TestARestoreStillSeesTheFirstReapAfterASecondOneKeeps(t *testing.T) {
	t.Parallel()

	st := NewInMemory()
	ctx := t.Context()
	seedCloneSnapshot(t, st)

	if err := setReapingMark(ctx, st, "src", "clone-dst", newReapingMark("dst", time.Now())); err != nil {
		t.Fatalf("reap A marks: %v", err)
	}

	if err := ReapClonedSnapshot(ctx, st, ClonedSnapshotRef{Source: "src", Snapshot: "clone-dst", Clone: "dst"}); err != nil {
		t.Fatalf("reap B = %v, want it to stand down", err)
	}

	if _, err := st.Snapshots().Get(ctx, "src", "clone-dst"); err != nil {
		t.Fatalf("reap B deleted the snapshot reap A owns: %v", err)
	}

	if err := RestoreSourceWithdrawn(ctx, st, "src", "clone-dst"); err == nil {
		t.Fatal("a restore found no reap under way while reap A still has its delete to make")
	}
}

func seedCloneSnapshot(t *testing.T, st Store) {
	t.Helper()

	if err := st.Snapshots().Create(t.Context(), &apiv1.Snapshot{
		Name: "clone-dst", ResourceName: "src",
		Props: map[string]string{CloneSnapshotOwnerProp: "dst"},
	}); err != nil {
		t.Fatalf("seed the snapshot: %v", err)
	}
}

func readMark(t *testing.T, st Store) string {
	t.Helper()

	snap, err := st.Snapshots().Get(t.Context(), "src", "clone-dst")
	if err != nil {
		t.Fatalf("read back: %v", err)
	}

	return snap.Props[CloneSnapshotReapingProp]
}

// The clone path's marks never travel from a definition onto a snapshot an
// operator takes, so that snapshot cannot claim the clone path took it.
func TestCloneSnapshotMarksDoNotTravel(t *testing.T) {
	t.Parallel()

	got := TravellingProps(map[string]string{
		CloneSnapshotOwnerProp:   "dst",
		CloneSnapshotReapingProp: newReapingMark("dst", time.Now()),
		"Aux/keep":               "yes",
	})

	if len(got) != 1 || got["Aux/keep"] != "yes" {
		t.Errorf("travelling props %v, want only Aux/keep", got)
	}
}
