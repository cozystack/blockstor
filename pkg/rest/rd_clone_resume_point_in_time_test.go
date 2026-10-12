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

// snapshotGoneAfterFirstRead answers the first read of one snapshot from the
// backend and deletes it on every later one: the clone's internal snapshot
// reaped between the leftover judgement and the resume taking a snapshot.
type snapshotGoneAfterFirstRead struct {
	store.Store

	source, snapshot string
	reads            *atomic.Int32
}

type snapshotGoneAfterFirstReadSnaps struct {
	store.SnapshotStore

	outer snapshotGoneAfterFirstRead
}

func (s snapshotGoneAfterFirstRead) Snapshots() store.SnapshotStore {
	return snapshotGoneAfterFirstReadSnaps{SnapshotStore: s.Store.Snapshots(), outer: s}
}

func (s snapshotGoneAfterFirstReadSnaps) Get(ctx context.Context, rdName, snapName string) (apiv1.Snapshot, error) {
	if strings.EqualFold(rdName, s.outer.source) && strings.EqualFold(snapName, s.outer.snapshot) &&
		s.outer.reads.Add(1) > 1 {
		_ = s.Delete(ctx, rdName, snapName)
	}

	return s.SnapshotStore.Get(ctx, rdName, snapName) //nolint:wrapcheck // pass-through test double
}

// ListByDefinitionUncached counts as a read of the snapshot too: the resume
// asks the API server whether it is still there.
func (s snapshotGoneAfterFirstReadSnaps) ListByDefinitionUncached(ctx context.Context, rdName string) ([]apiv1.Snapshot, error) {
	if strings.EqualFold(rdName, s.outer.source) && s.outer.reads.Add(1) > 1 {
		_ = s.Delete(ctx, rdName, s.outer.snapshot)
	}

	return s.SnapshotStore.ListByDefinitionUncached(ctx, rdName) //nolint:wrapcheck // pass-through test double
}

// A leftover already restored from the clone's snapshot, a volume with a
// replica, was finished from a fresh snapshot of the source as it is now once
// the first one was gone: two moments of the source joined into one clone.
func TestRDCloneResumeRefusesToFinishFromALaterPointInTime(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, backend, "src-pit")

	if err := backend.VolumeDefinitions().Create(ctx, "src-pit",
		&apiv1.VolumeDefinition{VolumeNumber: 1, SizeKib: 64 * 1024}); err != nil {
		t.Fatalf("give the source a second volume: %v", err)
	}

	plain, stopPlain := startServerWithStore(t, backend)

	if code := cloneOnce(t, plain, "src-pit", "dst-pit", nil); code != http.StatusCreated {
		stopPlain()
		t.Fatalf("first clone = %d, want 201", code)
	}

	stopPlain()

	// What a first attempt that died between two volumes leaves: one volume
	// and its replica, restored from the snapshot, the second volume missing.
	if err := backend.VolumeDefinitions().Delete(ctx, "dst-pit", 1); err != nil {
		t.Fatalf("leave the second volume missing: %v", err)
	}

	base, stop := startServerWithStore(t, snapshotGoneAfterFirstRead{
		Store: backend, source: "src-pit", snapshot: cloneSnapshotName("dst-pit"), reads: &atomic.Int32{},
	})
	defer stop()

	if code := cloneOnce(t, base, "src-pit", "dst-pit", nil); code != http.StatusConflict {
		t.Errorf("resume over a restored leftover whose snapshot went = %d, want 409", code)
	}

	if _, err := backend.Snapshots().Get(ctx, "src-pit", cloneSnapshotName("dst-pit")); err == nil {
		t.Error("the resume took a new snapshot of the source to finish the leftover from")
	}

	if vds, _ := backend.VolumeDefinitions().List(ctx, "dst-pit"); len(vds) != 1 {
		t.Errorf("the refused resume wrote into the leftover: %d volume(s)", len(vds))
	}
}

// cacheMissesTheCloneSnapshot answers every cached read of one snapshot after
// the first NotFound, as an informer that dropped it on a relist and has not
// caught up does; the API server holds it throughout.
type cacheMissesTheCloneSnapshot struct {
	store.Store

	source, snapshot string
	reads            *atomic.Int32
}

type cacheMissesTheCloneSnapshotSnaps struct {
	store.SnapshotStore

	outer cacheMissesTheCloneSnapshot
}

func (c cacheMissesTheCloneSnapshot) Snapshots() store.SnapshotStore {
	return cacheMissesTheCloneSnapshotSnaps{SnapshotStore: c.Store.Snapshots(), outer: c}
}

func (c cacheMissesTheCloneSnapshotSnaps) Get(ctx context.Context, rdName, snapName string) (apiv1.Snapshot, error) {
	if strings.EqualFold(rdName, c.outer.source) && strings.EqualFold(snapName, c.outer.snapshot) &&
		c.outer.reads.Add(1) > 1 {
		return apiv1.Snapshot{}, store.ErrNotFound
	}

	return c.SnapshotStore.Get(ctx, rdName, snapName) //nolint:wrapcheck // pass-through test double
}

// The point-in-time gate refuses a clone and tells the operator to delete it,
// so it asks the API server: a cache that had not seen the snapshot refused a
// leftover whose snapshot was right there.
func TestRDCloneResumeAsksTheAPIServerWhetherItsSnapshotIsGone(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, backend, "src-pitc")

	if err := backend.VolumeDefinitions().Create(ctx, "src-pitc",
		&apiv1.VolumeDefinition{VolumeNumber: 1, SizeKib: 64 * 1024}); err != nil {
		t.Fatalf("give the source a second volume: %v", err)
	}

	plain, stopPlain := startServerWithStore(t, backend)

	if code := cloneOnce(t, plain, "src-pitc", "dst-pitc", nil); code != http.StatusCreated {
		stopPlain()
		t.Fatalf("first clone = %d, want 201", code)
	}

	stopPlain()

	if err := backend.VolumeDefinitions().Delete(ctx, "dst-pitc", 1); err != nil {
		t.Fatalf("leave the second volume missing: %v", err)
	}

	base, stop := startServerWithStore(t, cacheMissesTheCloneSnapshot{
		Store: backend, source: "src-pitc", snapshot: cloneSnapshotName("dst-pitc"), reads: &atomic.Int32{},
	})
	defer stop()

	if code := cloneOnce(t, base, "src-pitc", "dst-pitc", nil); code == http.StatusConflict {
		t.Error("resume over a leftover whose snapshot the cache had not seen = 409")
	}
}

// cacheKeepsTheCloneSnapshot answers cached reads of one snapshot with a copy
// taken before it was deleted, as an informer that has not seen the delete
// yet does; the API server no longer holds it.
type cacheKeepsTheCloneSnapshot struct {
	store.Store

	stale apiv1.Snapshot
}

type cacheKeepsTheCloneSnapshotSnaps struct {
	store.SnapshotStore

	outer cacheKeepsTheCloneSnapshot
}

func (c cacheKeepsTheCloneSnapshot) Snapshots() store.SnapshotStore {
	return cacheKeepsTheCloneSnapshotSnaps{SnapshotStore: c.Store.Snapshots(), outer: c}
}

func (c cacheKeepsTheCloneSnapshotSnaps) Get(ctx context.Context, rdName, snapName string) (apiv1.Snapshot, error) {
	if strings.EqualFold(rdName, c.outer.stale.ResourceName) && strings.EqualFold(snapName, c.outer.stale.Name) {
		return c.outer.stale, nil
	}

	return c.SnapshotStore.Get(ctx, rdName, snapName) //nolint:wrapcheck // pass-through test double
}

// The resume asks the API server whether the clone's snapshot is gone, and
// with no replica on the leftover finishes it from a new one. The snapshot
// was then looked up again through the cache, which had not seen the delete,
// and the leftover was finished from the snapshot the first read had just
// found gone: replicas restoring from nothing.
func TestRDCloneResumeRetakesTheSnapshotACacheStillHolds(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, backend, "src-stale")

	plain, stopPlain := startServerWithStore(t, backend)

	if code := cloneOnce(t, plain, "src-stale", "dst-stale", nil); code != http.StatusCreated {
		stopPlain()
		t.Fatalf("first clone = %d, want 201", code)
	}

	stopPlain()

	snapName := cloneSnapshotName("dst-stale")

	stale, err := backend.Snapshots().Get(ctx, "src-stale", snapName)
	if err != nil {
		t.Fatalf("read the clone's snapshot: %v", err)
	}

	replicas, err := backend.Resources().ListByDefinition(ctx, "dst-stale")
	if err != nil {
		t.Fatalf("list the clone's replicas: %v", err)
	}

	for i := range replicas {
		if err := backend.Resources().Delete(ctx, "dst-stale", replicas[i].NodeName); err != nil {
			t.Fatalf("leave the leftover without a replica: %v", err)
		}
	}

	if err := backend.Snapshots().Delete(ctx, "src-stale", snapName); err != nil {
		t.Fatalf("delete the clone's snapshot: %v", err)
	}

	base, stop := startServerWithStore(t, cacheKeepsTheCloneSnapshot{Store: backend, stale: stale})
	defer stop()

	if code := cloneOnce(t, base, "src-stale", "dst-stale", nil); code != http.StatusCreated {
		t.Fatalf("resume of a leftover with no replica and its snapshot gone = %d, want 201", code)
	}

	if _, err := liveCloneSnapshot(ctx, backend, "src-stale", snapName); err != nil {
		t.Errorf("the resume finished the leftover from a snapshot that is gone: %v", err)
	}
}

// A leftover with no replica whose snapshot is gone is finished from a new
// snapshot like a fresh clone, unless its volumes do not fit the source as it
// is now. Smaller than the source's, the retake wrote a snapshot of a moment
// the leftover was never restored from and failed hydrating over it with a
// bare already-exists; a volume the source no longer has was kept beside the
// retake, a volume nothing restored. Both are refused first, naming the way
// out, with nothing taken. A leftover grown past the source, or missing a
// volume the source has, still fits and is finished.
func TestRDCloneResumeRefusesARetakeTheLeftoverDoesNotFit(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		leftover  []apiv1.VolumeDefinition
		sourceKib int64
		srcVol1   bool
		fits      bool
	}{
		{name: "smaller", leftover: []apiv1.VolumeDefinition{{VolumeNumber: 0, SizeKib: 64 * 1024}}, sourceKib: 128 * 1024},
		{
			name:     "volume-the-source-lacks",
			leftover: []apiv1.VolumeDefinition{{VolumeNumber: 0, SizeKib: 64 * 1024}, {VolumeNumber: 1, SizeKib: 64 * 1024}},
		},
		{name: "grown", leftover: []apiv1.VolumeDefinition{{VolumeNumber: 0, SizeKib: 128 * 1024}}, fits: true},
		{
			name: "missing-a-volume", srcVol1: true, fits: true,
			leftover: []apiv1.VolumeDefinition{{VolumeNumber: 0, SizeKib: 64 * 1024}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := store.NewInMemory()
			ctx := t.Context()
			seedDeployedCloneSource(t, backend, "src-fit")

			if tc.srcVol1 {
				if err := backend.VolumeDefinitions().Create(ctx, "src-fit",
					&apiv1.VolumeDefinition{VolumeNumber: 1, SizeKib: 64 * 1024}); err != nil {
					t.Fatalf("give the source a second volume: %v", err)
				}
			}

			if tc.sourceKib > 0 {
				growSourceVolume(t, backend, "src-fit", tc.sourceKib)
			}

			if err := backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
				Name:  "dst-fit",
				Props: map[string]string{restoreFromSnapshotKey: restoreMarker("src-fit", cloneSnapshotName("dst-fit"))},
			}); err != nil {
				t.Fatalf("seed the leftover: %v", err)
			}

			for i := range tc.leftover {
				if err := backend.VolumeDefinitions().Create(ctx, "dst-fit", &tc.leftover[i]); err != nil {
					t.Fatalf("seed the leftover's volume: %v", err)
				}
			}

			base, stop := startServerWithStore(t, backend)
			defer stop()

			resp := postClone(t, base, "src-fit", map[string]any{"name": "dst-fit", "use_zfs_clone": true})
			rc := decodeCloneMessage(t, resp)
			_ = resp.Body.Close()

			_, snapErr := backend.Snapshots().Get(ctx, "src-fit", cloneSnapshotName("dst-fit"))

			if tc.fits {
				if resp.StatusCode != http.StatusCreated {
					t.Errorf("resume over a leftover that fits = %d %q, want 201", resp.StatusCode, rc.Message)
				}

				return
			}

			if resp.StatusCode != http.StatusConflict || !strings.Contains(rc.Correc, "delete 'dst-fit' and clone again") {
				t.Errorf("resume over a leftover the source no longer fits = %d %q / %q, want 409 naming the way out",
					resp.StatusCode, rc.Message, rc.Correc)
			}

			if snapErr == nil {
				t.Error("the refused resume took a new snapshot of the source")
			}
		})
	}
}

// A diskless replica holds none of the earlier moment's data, so a leftover
// whose only replica is diskless is finished from a new snapshot like one with
// no replica at all, rather than refused as holding data from a snapshot that
// is gone.
func TestRDCloneResumeRetakesOverALeftoverWithOnlyADisklessReplica(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-dlr")

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:  "dst-dlr",
		Props: map[string]string{restoreFromSnapshotKey: restoreMarker("src-dlr", cloneSnapshotName("dst-dlr"))},
	}); err != nil {
		t.Fatalf("seed the leftover: %v", err)
	}

	if err := st.VolumeDefinitions().Create(ctx, "dst-dlr",
		&apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 64 * 1024}); err != nil {
		t.Fatalf("seed the leftover's volume: %v", err)
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name: "dst-dlr", NodeName: "node-b", Flags: []string{apiv1.ResourceFlagDiskless},
	}); err != nil {
		t.Fatalf("seed the diskless replica: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	if code := cloneOnce(t, base, "src-dlr", "dst-dlr", nil); code != http.StatusCreated {
		t.Errorf("resume of a leftover whose only replica is diskless = %d, want 201", code)
	}
}
