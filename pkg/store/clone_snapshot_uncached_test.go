// SPDX-License-Identifier: Apache-2.0

package store_test

import (
	"context"
	"errors"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// cacheMissesOne hides one definition from List, the way an informer that has
// not seen its create yet does, and answers ListUncached from the backend.
type cacheMissesOne struct {
	store.ResourceDefinitionStore

	hidden string
}

func (c cacheMissesOne) List(ctx context.Context) ([]apiv1.ResourceDefinition, error) {
	all, err := c.ResourceDefinitionStore.List(ctx)

	out := all[:0]
	for i := range all {
		if all[i].Name != c.hidden {
			out = append(out, all[i])
		}
	}

	return out, err //nolint:wrapcheck // pass-through test double
}

func (c cacheMissesOne) ListUncached(ctx context.Context) ([]apiv1.ResourceDefinition, error) {
	return c.ResourceDefinitionStore.List(ctx) //nolint:wrapcheck // pass-through test double
}

type cacheMissesOneStore struct {
	store.Store

	hidden string
}

func (c cacheMissesOneStore) ResourceDefinitions() store.ResourceDefinitionStore {
	return cacheMissesOne{ResourceDefinitionStore: c.Store.ResourceDefinitions(), hidden: c.hidden}
}

// Deleting the clone before the replica serving the delete has listed the
// definition restored from its snapshot took the point-in-time from under that
// definition. The question is asked of the API server.
func TestReapClonedSnapshotAsksPastTheCacheForDependents(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	backend := store.NewInMemory()

	if err := backend.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name: "clone-dst", ResourceName: "src",
		Props: map[string]string{store.CloneSnapshotOwnerProp: "dst"},
	}); err != nil {
		t.Fatalf("seed the snapshot: %v", err)
	}

	if err := backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:  "third",
		Props: map[string]string{store.RestoreFromSnapshotProp: "src:clone-dst"},
	}); err != nil {
		t.Fatalf("seed the restored definition: %v", err)
	}

	err := store.ReapClonedSnapshot(ctx, cacheMissesOneStore{Store: backend, hidden: "third"},
		store.ClonedSnapshotRef{Source: "src", Snapshot: "clone-dst", Clone: "dst"})
	if !errors.Is(err, store.ErrCloneSnapshotInUse) {
		t.Errorf("reap = %v, want ErrCloneSnapshotInUse", err)
	}

	if _, err := backend.Snapshots().Get(ctx, "src", "clone-dst"); err != nil {
		t.Errorf("the snapshot third restores from was reaped: %v", err)
	}
}

var errAPIServerUnavailable = errors.New("apiserver unavailable")

// markSeenAtList records whether the snapshot carried the reaping mark when
// the reap listed dependents, and optionally fails the list.
type markSeenAtList struct {
	store.ResourceDefinitionStore

	snaps   store.SnapshotStore
	seen    *bool
	listErr error
}

func (m markSeenAtList) ListUncached(ctx context.Context) ([]apiv1.ResourceDefinition, error) {
	snap, err := m.snaps.Get(ctx, "src", "clone-dst")
	if err == nil {
		_, *m.seen = snap.Props[store.CloneSnapshotReapingProp]
	}

	if m.listErr != nil {
		return nil, m.listErr
	}

	return m.ResourceDefinitionStore.ListUncached(ctx) //nolint:wrapcheck // pass-through test double
}

type markSeenAtListStore struct {
	store.Store

	seen    *bool
	listErr error
}

func (m markSeenAtListStore) ResourceDefinitions() store.ResourceDefinitionStore {
	return markSeenAtList{
		ResourceDefinitionStore: m.Store.ResourceDefinitions(),
		snaps:                   m.Snapshots(), seen: m.seen, listErr: m.listErr,
	}
}

func seedOwnedCloneSnapshot(t *testing.T, st store.Store) {
	t.Helper()

	if err := st.Snapshots().Create(t.Context(), &apiv1.Snapshot{
		Name: "clone-dst", ResourceName: "src",
		Props: map[string]string{store.CloneSnapshotOwnerProp: "dst"},
	}); err != nil {
		t.Fatalf("seed the snapshot: %v", err)
	}
}

// A list taken before the mark cannot see a restore created after it, and
// nothing on the restore's side would then tell it the snapshot is going.
func TestReapClonedSnapshotMarksBeforeItLists(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedOwnedCloneSnapshot(t, backend)

	var seen bool

	err := store.ReapClonedSnapshot(t.Context(), markSeenAtListStore{Store: backend, seen: &seen},
		store.ClonedSnapshotRef{Source: "src", Snapshot: "clone-dst", Clone: "dst"})
	if err != nil {
		t.Fatalf("reap: %v", err)
	}

	if !seen {
		t.Error("the reap listed dependents before it marked the snapshot")
	}
}

// A snapshot the reap keeps must take restores again, or the one it kept for
// a dependent is refused to the next.
func TestReapClonedSnapshotTakesTheMarkOffWhatItKeeps(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		listErr error
		want    error
	}{
		{name: "dependent", want: store.ErrCloneSnapshotInUse},
		{name: "list-failed", listErr: errAPIServerUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			backend := store.NewInMemory()
			seedOwnedCloneSnapshot(t, backend)

			if err := backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
				Name:  "third",
				Props: map[string]string{store.RestoreFromSnapshotProp: "src:clone-dst"},
			}); err != nil {
				t.Fatalf("seed the restored definition: %v", err)
			}

			var seen bool

			err := store.ReapClonedSnapshot(ctx,
				markSeenAtListStore{Store: backend, seen: &seen, listErr: tc.listErr},
				store.ClonedSnapshotRef{Source: "src", Snapshot: "clone-dst", Clone: "dst"})
			if err == nil || (tc.want != nil && !errors.Is(err, tc.want)) {
				t.Fatalf("reap = %v, want a refusal", err)
			}

			snap, err := backend.Snapshots().Get(ctx, "src", "clone-dst")
			if err != nil {
				t.Fatalf("the kept snapshot is gone: %v", err)
			}

			if mark, ok := snap.Props[store.CloneSnapshotReapingProp]; ok {
				t.Errorf("the kept snapshot still carries the reaping mark %q", mark)
			}
		})
	}
}

// snapshotsUnreadable fails every uncached snapshot listing.
type snapshotsUnreadable struct{ store.SnapshotStore }

func (snapshotsUnreadable) ListByDefinitionUncached(context.Context, string) ([]apiv1.Snapshot, error) {
	return nil, errAPIServerUnavailable
}

type snapshotsUnreadableStore struct{ store.Store }

func (s snapshotsUnreadableStore) Snapshots() store.SnapshotStore {
	return snapshotsUnreadable{SnapshotStore: s.Store.Snapshots()}
}

// A snapshot that could not be read is not a snapshot the clone does not
// own: the lookup says it could not tell, so the delete stops before it
// loses the only record of the snapshot. A definition that is no clone,
// and one already gone, still answer nothing to reap.
func TestOwnedCloneSnapshotReportsAReadFailure(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	backend := store.NewInMemory()

	if err := backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:  "dst",
		Props: map[string]string{store.RestoreFromSnapshotProp: "src:clone-dst"},
	}); err != nil {
		t.Fatalf("seed the clone: %v", err)
	}

	if err := backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "plain"}); err != nil {
		t.Fatalf("seed the plain definition: %v", err)
	}

	broken := snapshotsUnreadableStore{Store: backend}

	if _, err := store.OwnedCloneSnapshot(ctx, broken, "dst"); !errors.Is(err, errAPIServerUnavailable) {
		t.Errorf("lookup over an unreadable snapshot = %v, want the read error", err)
	}

	for _, name := range []string{"plain", "gone"} {
		if ref, err := store.OwnedCloneSnapshot(ctx, broken, name); err != nil || ref.Snapshot != "" {
			t.Errorf("lookup of %s = %+v, %v; want nothing to reap and no error", name, ref, err)
		}
	}
}

type definitionsUnreadable struct{ store.ResourceDefinitionStore }

func (definitionsUnreadable) Get(context.Context, string) (apiv1.ResourceDefinition, error) {
	return apiv1.ResourceDefinition{}, errAPIServerUnavailable
}

type definitionsUnreadableStore struct{ store.Store }

func (s definitionsUnreadableStore) ResourceDefinitions() store.ResourceDefinitionStore {
	return definitionsUnreadable{ResourceDefinitionStore: s.Store.ResourceDefinitions()}
}

// A definition that could not be read is not a definition that is no clone.
func TestOwnedCloneSnapshotReportsAnUnreadableDefinition(t *testing.T) {
	t.Parallel()

	if _, err := store.OwnedCloneSnapshot(t.Context(), definitionsUnreadableStore{Store: store.NewInMemory()}, "dst"); !errors.Is(err, errAPIServerUnavailable) {
		t.Errorf("lookup over an unreadable definition = %v, want the read error", err)
	}
}
