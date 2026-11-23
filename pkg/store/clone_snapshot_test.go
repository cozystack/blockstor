// SPDX-License-Identifier: Apache-2.0

package store_test

import (
	"errors"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// The clone was skipped by name, on the theory that it could still be listed
// after its own delete. It cannot: the list is the API server's, and a
// definition has no finalizer to linger behind. What the skip did let through
// is a different definition that has taken the clone's name since, restored
// from the same snapshot; the reap deleted the snapshot from under it.
func TestReapClonedSnapshotCountsADefinitionThatTookTheClonesName(t *testing.T) {
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
		Name:  "dst",
		Props: map[string]string{store.RestoreFromSnapshotProp: "src:clone-dst"},
	}); err != nil {
		t.Fatalf("seed the new definition under the clone's name: %v", err)
	}

	err := store.ReapClonedSnapshot(ctx, backend, store.ClonedSnapshotRef{
		Source: "src", Snapshot: "clone-dst", Clone: "dst",
	})
	if !errors.Is(err, store.ErrCloneSnapshotInUse) {
		t.Errorf("reap = %v, want ErrCloneSnapshotInUse", err)
	}

	if _, err := backend.Snapshots().Get(ctx, "src", "clone-dst"); err != nil {
		t.Errorf("the snapshot the new definition restores from was reaped: %v", err)
	}
}
