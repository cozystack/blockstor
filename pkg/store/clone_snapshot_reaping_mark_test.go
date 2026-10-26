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

// A reap that keeps the snapshot takes off only its own mark: one another
// reap wrote since stays, so a restore still sees that reap under way.
func TestKeepingAReapLeavesAnotherReapsMark(t *testing.T) {
	t.Parallel()

	st := NewInMemory()
	ctx := t.Context()

	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name: "clone-dst", ResourceName: "src",
		Props: map[string]string{CloneSnapshotOwnerProp: "dst"},
	}); err != nil {
		t.Fatalf("seed the snapshot: %v", err)
	}

	now := time.Now()
	first, second := newReapingMark("dst", now), newReapingMark("dst", now)

	if err := setReapingMark(ctx, st, "src", "clone-dst", first); err != nil {
		t.Fatalf("first mark: %v", err)
	}

	if err := setReapingMark(ctx, st, "src", "clone-dst", second); err != nil {
		t.Fatalf("second mark: %v", err)
	}

	if err := dropReapingMark(ctx, st, "src", "clone-dst", first); err != nil {
		t.Fatalf("first reap keeps the snapshot: %v", err)
	}

	snap, err := st.Snapshots().Get(ctx, "src", "clone-dst")
	if err != nil {
		t.Fatalf("read back: %v", err)
	}

	if got := snap.Props[CloneSnapshotReapingProp]; got != second {
		t.Fatalf("mark after the first reap kept the snapshot = %q, want the second reap's %q", got, second)
	}
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
