// SPDX-License-Identifier: Apache-2.0

package store_test

import (
	"context"
	"errors"
	"maps"
	"strings"
	"testing"
	"time"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// cacheWithoutTheMark answers every cached snapshot read without the reaping
// mark, the way a cache that has not seen the reap's own write does. Only the
// uncached list carries it.
type cacheWithoutTheMark struct{ store.SnapshotStore }

func withoutTheMark(snap apiv1.Snapshot) apiv1.Snapshot {
	snap.Props = maps.Clone(snap.Props)
	delete(snap.Props, store.CloneSnapshotReapingProp)

	return snap
}

func (c cacheWithoutTheMark) Get(ctx context.Context, rdName, snapName string) (apiv1.Snapshot, error) {
	snap, err := c.SnapshotStore.Get(ctx, rdName, snapName)

	return withoutTheMark(snap), err
}

func (c cacheWithoutTheMark) ListByDefinition(ctx context.Context, rdName string) ([]apiv1.Snapshot, error) {
	snaps, err := c.SnapshotStore.ListByDefinition(ctx, rdName)
	for i := range snaps {
		snaps[i] = withoutTheMark(snaps[i])
	}

	return snaps, err //nolint:wrapcheck // pass-through test double
}

type cacheWithoutTheMarkStore struct{ store.Store }

func (c cacheWithoutTheMarkStore) Snapshots() store.SnapshotStore {
	return cacheWithoutTheMark{c.Store.Snapshots()}
}

func seedKeptCloneSnapshot(t *testing.T, st store.Store) {
	t.Helper()

	seedOwnedCloneSnapshot(t, st)

	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{
		Name:  "third",
		Props: map[string]string{store.RestoreFromSnapshotProp: "src:clone-dst"},
	}); err != nil {
		t.Fatalf("seed the restored definition: %v", err)
	}
}

// The mark came off through a cached read, which right after the reap's own
// write may not hold it yet, and a kept snapshot then refused every restore.
func TestReapClonedSnapshotTakesTheMarkOffPastTheCache(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	backend := store.NewInMemory()
	seedKeptCloneSnapshot(t, backend)

	err := store.ReapClonedSnapshot(ctx, cacheWithoutTheMarkStore{backend},
		store.ClonedSnapshotRef{Source: "src", Snapshot: "clone-dst", Clone: "dst"})
	if !errors.Is(err, store.ErrCloneSnapshotInUse) {
		t.Fatalf("reap = %v, want ErrCloneSnapshotInUse", err)
	}

	if err := store.RestoreSourceWithdrawn(ctx, backend, "src", "clone-dst"); err != nil {
		t.Errorf("a restore from the kept snapshot is refused: %v", err)
	}
}

// honoursCancel fails every call on a context that is done, which the
// in-memory store does not.
type honoursCancel struct{ store.SnapshotStore }

func (h honoursCancel) ListByDefinitionUncached(ctx context.Context, rdName string) ([]apiv1.Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err //nolint:wrapcheck // the context error is the point
	}

	return h.SnapshotStore.ListByDefinitionUncached(ctx, rdName) //nolint:wrapcheck // pass-through test double
}

func (h honoursCancel) Update(ctx context.Context, snap *apiv1.Snapshot) error {
	if err := ctx.Err(); err != nil {
		return err //nolint:wrapcheck // the context error is the point
	}

	return h.SnapshotStore.Update(ctx, snap) //nolint:wrapcheck // pass-through test double
}

type honoursCancelStore struct{ store.Store }

func (h honoursCancelStore) Snapshots() store.SnapshotStore {
	return honoursCancel{h.Store.Snapshots()}
}

// The delete has been reported by the time the reap runs, so a caller that
// hangs up, or a SIGTERM, must not cut it between its mark and its clean-up.
func TestReapClonedSnapshotOutlivesItsCaller(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedOwnedCloneSnapshot(t, backend)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := store.ReapClonedSnapshot(ctx, honoursCancelStore{backend},
		store.ClonedSnapshotRef{Source: "src", Snapshot: "clone-dst", Clone: "dst"})
	if err != nil {
		t.Fatalf("reap on a cancelled caller: %v", err)
	}

	if _, err := backend.Snapshots().Get(t.Context(), "src", "clone-dst"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("the reap stopped with its caller: %v", err)
	}
}

// A mark left by a reap that died, or whose clean-up failed, refused restores
// from a healthy snapshot for good. It expires, and one whose time cannot be
// read is not a live reap's.
func TestRestoreSourceWithdrawnIgnoresAMarkNoLiveReapCanOwn(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		mark      string
		withdrawn bool
	}{
		{name: "fresh", mark: "dst@" + time.Now().UTC().Format(time.RFC3339), withdrawn: true},
		{name: "expired", mark: "dst@" + time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)},
		{name: "unreadable", mark: "dst"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			backend := store.NewInMemory()

			if err := backend.Snapshots().Create(ctx, &apiv1.Snapshot{
				Name: "clone-dst", ResourceName: "src",
				Props: map[string]string{store.CloneSnapshotReapingProp: tc.mark},
			}); err != nil {
				t.Fatalf("seed the snapshot: %v", err)
			}

			err := store.RestoreSourceWithdrawn(ctx, backend, "src", "clone-dst")
			if got := errors.Is(err, store.ErrRestoreSourceWithdrawn); got != tc.withdrawn {
				t.Errorf("withdrawn = %v (%v), want %v", got, err, tc.withdrawn)
			}
		})
	}
}

// The steps come in the order they can run, and the source comes last.
func TestLeftCloneSnapshotsExplainOrdersTheSteps(t *testing.T) {
	t.Parallel()

	_, steps := store.LeftCloneSnapshots{
		Deletable: []string{"clone-a"},
		Unowned:   []string{"clone-b"},
		InUse:     map[string]string{"clone-c": "third"},
	}.Explain("src")

	last := -1

	for _, want := range []string{"s d src clone-a", "s d src clone-b", "keep clone-c", "then delete src again"} {
		at := strings.Index(steps, want)
		if at <= last {
			t.Fatalf("steps %q: %q out of order", steps, want)
		}

		last = at
	}
}

// A delete that was issued is held by the satellite finalizer until the
// on-disk delete succeeds, which outlives the reaping mark's lifetime. The
// restore reads that state off the snapshot, not off the mark's age.
func TestRestoreSourceWithdrawnOnASnapshotBeingDeleted(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	backend := store.NewInMemory()

	if err := backend.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name: "clone-dst", ResourceName: "src",
		Flags: []string{apiv1.SnapshotFlagDelete},
		Props: map[string]string{
			store.CloneSnapshotReapingProp: "dst@" + time.Now().Add(-time.Hour).UTC().Format(time.RFC3339),
		},
	}); err != nil {
		t.Fatalf("seed the snapshot: %v", err)
	}

	err := store.RestoreSourceWithdrawn(ctx, backend, "src", "clone-dst")
	if !errors.Is(err, store.ErrRestoreSourceWithdrawn) {
		t.Errorf("restore from a snapshot whose delete is in flight = %v, want withdrawn", err)
	}
}
