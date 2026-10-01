// SPDX-License-Identifier: Apache-2.0

package store_test

import (
	"context"
	"errors"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// laggingDefinitionList still lists a definition its store has deleted, the way
// the manager's cache does until the watch catches up.
type laggingDefinitionList struct {
	store.ResourceDefinitionStore

	ghost apiv1.ResourceDefinition
}

func (l laggingDefinitionList) List(ctx context.Context) ([]apiv1.ResourceDefinition, error) {
	out, err := l.ResourceDefinitionStore.List(ctx)

	return append(out, l.ghost), err
}

type laggingDefinitionStore struct {
	store.Store

	ghost apiv1.ResourceDefinition
}

func (l laggingDefinitionStore) ResourceDefinitions() store.ResourceDefinitionStore {
	return laggingDefinitionList{ResourceDefinitionStore: l.Store.ResourceDefinitions(), ghost: l.ghost}
}

// The clone itself carries the marker naming its snapshot. A cache that has not
// seen the clone's delete yet still lists it, and read as a dependent it would
// keep the snapshot every time.
func TestReapClonedSnapshotIgnoresTheCloneItReapsFor(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	backend := store.NewInMemory()

	if err := backend.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name: "clone-dst", ResourceName: "src",
		Props: map[string]string{store.CloneSnapshotOwnerProp: "dst"},
	}); err != nil {
		t.Fatalf("seed the snapshot: %v", err)
	}

	lagging := laggingDefinitionStore{Store: backend, ghost: apiv1.ResourceDefinition{
		Name:  "dst",
		Props: map[string]string{store.RestoreFromSnapshotProp: "src:clone-dst"},
	}}

	err := store.ReapClonedSnapshot(ctx, lagging, store.ClonedSnapshotRef{
		Source: "src", Snapshot: "clone-dst", Clone: "dst",
	})
	if err != nil {
		t.Fatalf("reap: %v", err)
	}

	if _, err := backend.Snapshots().Get(ctx, "src", "clone-dst"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("the snapshot was kept because the clone it belonged to was still listed: %v", err)
	}
}
