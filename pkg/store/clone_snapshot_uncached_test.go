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
