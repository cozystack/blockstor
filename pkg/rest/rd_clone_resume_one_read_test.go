// SPDX-License-Identifier: Apache-2.0

package rest

import (
	"context"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// snapshotGoneOnSecondListing deletes one snapshot just before the second
// live listing of its source and counts every snapshot create, so a request
// that reads the snapshot twice and retakes it in between is seen doing so.
type snapshotGoneOnSecondListing struct {
	store.Store

	source, snapshot string
	listings         *atomic.Int32
	creates          *atomic.Int32
}

type snapshotGoneOnSecondListingSnaps struct {
	store.SnapshotStore

	outer snapshotGoneOnSecondListing
}

func (s snapshotGoneOnSecondListing) Snapshots() store.SnapshotStore {
	return snapshotGoneOnSecondListingSnaps{SnapshotStore: s.Store.Snapshots(), outer: s}
}

func (s snapshotGoneOnSecondListingSnaps) ListByDefinitionUncached(ctx context.Context, rdName string) ([]apiv1.Snapshot, error) {
	if strings.EqualFold(rdName, s.outer.source) && s.outer.listings.Add(1) == 2 {
		_ = s.Delete(ctx, rdName, s.outer.snapshot)
	}

	return s.SnapshotStore.ListByDefinitionUncached(ctx, rdName) //nolint:wrapcheck // pass-through test double
}

func (s snapshotGoneOnSecondListingSnaps) Create(ctx context.Context, snap *apiv1.Snapshot) error {
	s.outer.creates.Add(1)

	return s.SnapshotStore.Create(ctx, snap) //nolint:wrapcheck // pass-through test double
}

// The resume judges the leftover against the snapshot it reads once, and the
// rest of the request goes by that read: a snapshot deleted between two reads
// was taken again from the source as it is now and finished a leftover that
// already held data restored from the earlier one.
func TestRDCloneResumeNeverRetakesASnapshotItAlreadyFound(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, backend, "src-once")

	if err := backend.VolumeDefinitions().Create(ctx, "src-once",
		&apiv1.VolumeDefinition{VolumeNumber: 1, SizeKib: 64 * 1024}); err != nil {
		t.Fatalf("give the source a second volume: %v", err)
	}

	plain, stopPlain := startServerWithStore(t, backend)

	if code := cloneOnce(t, plain, "src-once", "dst-once", nil); code != http.StatusCreated {
		stopPlain()
		t.Fatalf("first clone = %d, want 201", code)
	}

	stopPlain()

	// A first attempt that died between two volumes: one volume and its
	// replica restored from the snapshot, the second volume missing.
	if err := backend.VolumeDefinitions().Delete(ctx, "dst-once", 1); err != nil {
		t.Fatalf("leave the second volume missing: %v", err)
	}

	wrapped := snapshotGoneOnSecondListing{
		Store: backend, source: "src-once", snapshot: cloneSnapshotName("dst-once"),
		listings: &atomic.Int32{}, creates: &atomic.Int32{},
	}

	base, stop := startServerWithStore(t, wrapped)
	defer stop()

	_ = cloneOnce(t, base, "src-once", "dst-once", nil)

	if n := wrapped.creates.Load(); n != 0 {
		t.Errorf("the resume took %d new snapshot(s) of the source to finish a leftover restored from an earlier one", n)
	}
}

// The clone's internal snapshot is found whatever case a retry spells the
// target in: LINSTOR folds the names, and a retry that missed it would take a
// fresh snapshot of the source under a name that already exists.
func TestLiveCloneSnapshotFoldsTheNamesCase(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	backend := store.NewInMemory()

	if err := backend.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name: strings.ToUpper(cloneSnapshotName("dst-fold")), ResourceName: "src-fold",
	}); err != nil {
		t.Fatalf("seed the snapshot: %v", err)
	}

	if _, err := liveCloneSnapshot(ctx, backend, "src-fold", cloneSnapshotName("dst-fold")); err != nil {
		t.Errorf("the snapshot spelled in another case was not found: %v", err)
	}
}
