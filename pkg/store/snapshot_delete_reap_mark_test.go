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
	"testing"
	"time"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// racingSnaps runs inject once, right before the delete's write numbered at
// (from 1): the delete writes its mark, then takes it back off. A writer
// that comes in there comes after whatever read the delete decided that
// write on.
type racingSnaps struct {
	SnapshotStore

	at     int
	writes int
	inject func()
}

func (s *racingSnaps) Update(ctx context.Context, snap *apiv1.Snapshot) error {
	s.beforeWrite()

	return s.SnapshotStore.Update(ctx, snap) //nolint:wrapcheck // a pass-through
}

func (s *racingSnaps) PatchSnapshotProps(
	ctx context.Context, rd, snap string, mutate func(map[string]string) error,
) error {
	s.beforeWrite()

	return s.SnapshotStore.PatchSnapshotProps(ctx, rd, snap, mutate) //nolint:wrapcheck // a pass-through
}

func (s *racingSnaps) beforeWrite() {
	s.writes++
	if s.writes == s.at && s.inject != nil {
		s.inject()
	}
}

type racingStore struct {
	Store

	snaps *racingSnaps
}

func (s *racingStore) Snapshots() SnapshotStore { return s.snaps }

// A delete marks the snapshot as being deleted, then keeps it under an
// unfinished restore and takes its mark back off. A reap's mark written
// right before either write must survive it: it is what makes a restore from the snapshot stand
// down while the reap, which has already listed dependents, deletes it.
func TestDeleteKeepsAReapMarkWrittenInBetween(t *testing.T) {
	t.Parallel()

	for name, at := range map[string]int{"before the mark is set": 1, "before the mark is taken off": 2} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			inner := NewInMemory()
			seedRestoreSource(t, inner, "src", "snap")
			seedRestoredFrom(t, inner, "dst", "src:snap") // unfinished: the delete keeps the snapshot

			reap := newReapingMark("clone1", time.Now())
			st := &racingStore{Store: inner, snaps: &racingSnaps{SnapshotStore: inner.Snapshots(), at: at}}
			st.snaps.inject = func() {
				if err := setReapingMark(t.Context(), inner, "src", "snap", reap); err != nil {
					t.Errorf("write the reap mark: %v", err)
				}
			}

			_ = DeleteSnapshot(t.Context(), st, "src", "snap")

			snap, err := inner.Snapshots().Get(t.Context(), "src", "snap")
			if err != nil {
				t.Fatalf("the kept snapshot is gone: %v", err)
			}

			if snap.Props[CloneSnapshotReapingProp] != reap {
				t.Fatalf("reap mark lost: props=%v", snap.Props)
			}

			if mark, ok := snap.Props[SnapshotDeletingProp]; ok {
				t.Errorf("the kept snapshot still carries the deleting mark %q", mark)
			}
		})
	}
}

// Taking the deleting mark off a snapshot that had no props leaves it with
// none, so a restore from it still falls back to its source's props.
func TestDroppingTheDeletingMarkLeavesNoProps(t *testing.T) {
	t.Parallel()

	st := NewInMemory()
	seedRestoreSource(t, st, "src-bare", "snap-bare")
	seedRestoredFrom(t, st, "dst-bare", "src-bare:snap-bare")

	_ = DeleteSnapshot(t.Context(), st, "src-bare", "snap-bare")

	snap, err := st.Snapshots().Get(t.Context(), "src-bare", "snap-bare")
	if err != nil {
		t.Fatalf("the kept snapshot is gone: %v", err)
	}

	if snap.Props != nil {
		t.Errorf("props = %#v, want none", snap.Props)
	}
}

// Two deletes of one snapshot can overlap: one that keeps the snapshot
// takes off only its own deleting mark. Another delete's mark, written
// right before the drop, stays, so a restore reading back in that gap
// still stands down while the other delete goes on.
func TestDeleteTakesOffOnlyItsOwnDeletingMark(t *testing.T) {
	t.Parallel()

	inner := NewInMemory()
	seedRestoreSource(t, inner, "src-two", "snap-two")
	seedRestoredFrom(t, inner, "dst-two", "src-two:snap-two") // unfinished: the delete keeps the snapshot

	other := "other-delete@" + time.Now().UTC().Format(time.RFC3339)
	st := &racingStore{Store: inner, snaps: &racingSnaps{SnapshotStore: inner.Snapshots(), at: 2}}
	st.snaps.inject = func() {
		err := inner.Snapshots().PatchSnapshotProps(t.Context(), "src-two", "snap-two", func(props map[string]string) error {
			props[SnapshotDeletingProp] = other

			return nil
		})
		if err != nil {
			t.Errorf("write the other delete's mark: %v", err)
		}
	}

	_ = DeleteSnapshot(t.Context(), st, "src-two", "snap-two")

	snap, err := inner.Snapshots().Get(t.Context(), "src-two", "snap-two")
	if err != nil {
		t.Fatalf("the kept snapshot is gone: %v", err)
	}

	if got := snap.Props[SnapshotDeletingProp]; got != other {
		t.Fatalf("deleting mark = %q, want the other delete's %q", got, other)
	}
}
