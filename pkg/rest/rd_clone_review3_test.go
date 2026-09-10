// SPDX-License-Identifier: Apache-2.0

package rest

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// growSourceVolume resizes the source's volume 0, the routine operation that
// must not change what a finished clone's retry answers.
func growSourceVolume(t *testing.T, st store.Store, rdName string, sizeKib int64) {
	t.Helper()

	vd, err := st.VolumeDefinitions().Get(t.Context(), rdName, 0)
	if err != nil {
		t.Fatalf("read the source volume: %v", err)
	}

	vd.SizeKib = sizeKib

	if err := st.VolumeDefinitions().Update(t.Context(), rdName, &vd); err != nil {
		t.Fatalf("grow the source: %v", err)
	}
}

// A clone that COMPLETED is a copy of a point-in-time and owes the source
// nothing afterwards. linstor-csi replays CreateVolume whenever a response is
// lost or external-provisioner restarts, so the replay has to be the
// idempotent success CSI requires.
//
// Comparing the leftover snapshot against the live source made an ordinary
// resize poison every later retry, permanently: the internal snapshot is
// deterministic and outlives the clone by design, the marker survives, and so
// every repeat took the same refusal — one whose correction, "delete the
// snapshot", the operator cannot follow, because that snapshot is the origin
// of the clone they already have.
func TestRDCloneReplayOfAFinishedCloneSurvivesASourceResize(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-replay")

	base, stop := startServerWithStore(t, st)
	defer stop()

	body := map[string]any{"name": "dst-replay", "use_zfs_clone": true}

	first := postClone(t, base, "src-replay", body)
	_ = first.Body.Close()

	if first.StatusCode != http.StatusCreated {
		t.Fatalf("first clone: got %d, want 201", first.StatusCode)
	}

	// The clone is finished. Now the source grows, which has nothing to do
	// with the copy already taken.
	growSourceVolume(t, st, "src-replay", 128*1024)

	replay := postClone(t, base, "src-replay", body)
	_ = replay.Body.Close()

	if replay.StatusCode != http.StatusCreated {
		t.Fatalf("replay after a source resize: got %d, want 201 — a finished clone is a "+
			"copy of a point-in-time, and CSI replays this call whenever a response is lost",
			replay.StatusCode)
	}

	// And the replay left the finished clone alone, at the size it was taken.
	vd, err := st.VolumeDefinitions().Get(ctx, "dst-replay", 0)
	if err != nil {
		t.Fatalf("read the clone's volume: %v", err)
	}

	if vd.SizeKib != 64*1024 {
		t.Errorf("clone volume 0 = %d KiB, want the 65536 it was cloned at", vd.SizeKib)
	}
}

// A leftover target holding volumes that are NOT the ones this clone would
// restore is not a resume. Hydrating skips what is already there, so the retry
// would leave the old shape in place and report the clone complete — and the
// status poll compares volume counts, not sizes, so it would agree.
func TestRDCloneRefusesALeftoverTargetWithADifferentVolumeShape(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-shape3")

	// The snapshot an earlier attempt took, at the source's current size.
	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name:              cloneSnapshotName("dst-shape3"),
		ResourceName:      "src-shape3",
		Nodes:             []string{"node-a"},
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{{VolumeNumber: 0, SizeKib: 64 * 1024}},
	}); err != nil {
		t.Fatalf("seed the leftover snapshot: %v", err)
	}

	seedCloneLeftover(t, st, "src-shape3", "dst-shape3")

	// ... and a volume already under that name at the wrong size.
	if err := st.VolumeDefinitions().Create(ctx, "dst-shape3",
		&apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 8 * 1024}); err != nil {
		t.Fatalf("seed the stale volume: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := postClone(t, base, "src-shape3", map[string]any{
		"name":          "dst-shape3",
		"use_zfs_clone": true,
	})
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d, want 409 — that target holds a volume this clone would not "+
			"have written, and hydrating would skip it and report success", resp.StatusCode)
	}

	vd, err := st.VolumeDefinitions().Get(ctx, "dst-shape3", 0)
	if err != nil {
		t.Fatalf("read the target's volume: %v", err)
	}

	if vd.SizeKib != 8*1024 {
		t.Errorf("the refused clone rewrote the volume to %d KiB", vd.SizeKib)
	}
}

// The create branch refuses a source that would place no replicas (Bug 114).
// The reuse branch ran none of those checks, so a leftover snapshot recording
// no nodes placed nothing and the clone answered 201 over an empty shell.
func TestRDCloneRefusesALeftoverSnapshotWithNoNodes(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-nonodes")

	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name:              cloneSnapshotName("dst-nonodes"),
		ResourceName:      "src-nonodes",
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{{VolumeNumber: 0, SizeKib: 64 * 1024}},
	}); err != nil {
		t.Fatalf("seed the node-less snapshot: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := postClone(t, base, "src-nonodes", map[string]any{
		"name":          "dst-nonodes",
		"use_zfs_clone": true,
	})
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d, want 409 — a snapshot with no nodes places no replicas",
			resp.StatusCode)
	}

	replicas, err := st.Resources().ListByDefinition(ctx, "dst-nonodes")
	if err != nil {
		t.Fatalf("list the target's replicas: %v", err)
	}

	if len(replicas) != 0 {
		t.Errorf("the refused clone stamped %d replica(s)", len(replicas))
	}
}

// The staleness check's volume-count arm is load-bearing in the direction the
// size arm cannot see: when the source has LOST a volume, the per-volume walk
// covers only the source's remaining ones and finds every one of them in the
// snapshot, so without the count the stale snapshot passes.
func TestRDCloneRefusesToResumeWhenTheSourceLostAVolume(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-lostvol")

	// The snapshot covers two volumes; the source now has one.
	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name:         cloneSnapshotName("dst-lostvol"),
		ResourceName: "src-lostvol",
		Nodes:        []string{"node-a"},
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{
			{VolumeNumber: 0, SizeKib: 64 * 1024},
			{VolumeNumber: 1, SizeKib: 64 * 1024},
		},
	}); err != nil {
		t.Fatalf("seed the leftover snapshot: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := postClone(t, base, "src-lostvol", map[string]any{
		"name":          "dst-lostvol",
		"use_zfs_clone": true,
	})
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d, want 409 — the snapshot covers a volume the source no longer has",
			resp.StatusCode)
	}
}

// The third arm, and the one neither of its siblings can reach: the counts
// agree and every size that is compared agrees, because the volume the source
// now carries is not in the snapshot at all. A source that dropped one volume
// and gained another between the attempts lands exactly here — `vd d 0` then
// `vd c` renumbers rather than refills — and resuming would hydrate the
// target from a snapshot describing a volume the source no longer has, under
// a number it never had.
func TestRDCloneRefusesToResumeWhenTheSourceRenumberedItsVolume(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-renum")

	// The snapshot covers volume 0, at the size the source had it.
	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name:         cloneSnapshotName("dst-renum"),
		ResourceName: "src-renum",
		Nodes:        []string{"node-a"},
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{
			{VolumeNumber: 0, SizeKib: 64 * 1024},
		},
	}); err != nil {
		t.Fatalf("seed the leftover snapshot: %v", err)
	}

	// The source dropped volume 0 and took volume 1 in its place, so it
	// still has exactly one volume, of exactly the captured size.
	if err := st.VolumeDefinitions().Delete(ctx, "src-renum", 0); err != nil {
		t.Fatalf("drop the source volume: %v", err)
	}

	if err := st.VolumeDefinitions().Create(ctx, "src-renum", &apiv1.VolumeDefinition{
		VolumeNumber: 1,
		SizeKib:      64 * 1024,
	}); err != nil {
		t.Fatalf("add the replacement volume: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := postClone(t, base, "src-renum", map[string]any{
		"name":          "dst-renum",
		"use_zfs_clone": true,
	})
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d, want 409 — the snapshot does not cover the volume the source now has",
			resp.StatusCode)
	}

	vds, err := st.VolumeDefinitions().List(ctx, "dst-renum")
	if err != nil {
		t.Fatalf("list the target's volumes: %v", err)
	}

	if len(vds) != 0 {
		t.Errorf("the refused clone hydrated %d volume(s) from the stale snapshot", len(vds))
	}
}

// The volume-definition restore's collision guard has two halves: a pre-check
// that LISTs the target's volumes, and the hydrate that CREATEs them. The
// pre-check's own comment waves a request through when that list cannot be
// read, on the stated grounds that "the downstream hydrate Create still
// guards" — so a blanket AlreadyExists tolerance in the hydrate turns the
// endpoint into a 200 reporting a layout it never wrote.
//
// The same hole opens with no read error at all, since one call LISTs and the
// other CREATEs: a volume appearing between the two arrives at the hydrate.
func TestSnapshotRestoreVolumeDefinitionRefusesAVolumeAtADifferentSize(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx,
		&apiv1.ResourceDefinition{Name: "vd-src"}); err != nil {
		t.Fatalf("seed the source: %v", err)
	}

	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name:              "snap-vd",
		ResourceName:      "vd-src",
		Nodes:             []string{"n1"},
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{{VolumeNumber: 0, SizeKib: 64 * 1024}},
	}); err != nil {
		t.Fatalf("seed the snapshot: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx,
		&apiv1.ResourceDefinition{Name: "vd-dst"}); err != nil {
		t.Fatalf("seed the target: %v", err)
	}

	// A volume already under that number, recording a different size: not the
	// one this snapshot would write.
	if err := st.VolumeDefinitions().Create(ctx, "vd-dst",
		&apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 8 * 1024}); err != nil {
		t.Fatalf("seed the colliding volume: %v", err)
	}

	// The pre-check LISTs and the hydrate CREATEs, and the pre-check's own
	// comment waves a request through when that list cannot be read, on the
	// grounds that the hydrate still guards. Blinding the list is how this
	// test asks whether that is true — and it is also the no-error case, where
	// a volume simply appears between the two calls.
	base, stop := startServerWithStore(t, blindVolumeListStore{st})
	defer stop()

	body := []byte(`{"to_resource":"vd-dst"}`)

	resp := httpPost(t,
		base+"/v1/resource-definitions/vd-src/snapshot-restore-volume-definition/snap-vd", body)
	_ = resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		t.Fatalf("status = 200 — the operator was told the layout was restored, and it was not")
	}

	vd, err := st.VolumeDefinitions().Get(ctx, "vd-dst", 0)
	if err != nil {
		t.Fatalf("read the target's volume: %v", err)
	}

	if vd.SizeKib != 8*1024 {
		t.Errorf("volume 0 = %d KiB; the refused restore rewrote it", vd.SizeKib)
	}
}

// A definition stored without an explicit stack never said what its layers
// are; it does not have none. One client's attempt stamps the leftover with
// the default it resolved, another client retries without layer_list, and
// comparing the raw slices calls that a shape change — with the empty side
// rendered as nothing at all in the refusal.
func TestRDCloneResumesWhenTheLeftoverStackIsTheResolvedDefault(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-defstack")

	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name:              cloneSnapshotName("dst-defstack"),
		ResourceName:      "src-defstack",
		Nodes:             []string{"node-a"},
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{{VolumeNumber: 0, SizeKib: 64 * 1024}},
	}); err != nil {
		t.Fatalf("seed the leftover snapshot: %v", err)
	}

	// The leftover an earlier client left, carrying the stack it resolved.
	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:       "dst-defstack",
		LayerStack: apiv1.DefaultLayerStack(),
		Props: map[string]string{
			restoreFromSnapshotKey: restoreMarker("src-defstack", cloneSnapshotName("dst-defstack")),
		},
	}); err != nil {
		t.Fatalf("seed the leftover: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// The retry omits layer_list, so the shape it asks for is the source's,
	// which stores none.
	resp := postClone(t, base, "src-defstack", map[string]any{
		"name":          "dst-defstack",
		"use_zfs_clone": true,
	})
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want 201 — an unset stack means the default, so the leftover "+
			"and the request describe the same shape", resp.StatusCode)
	}

	vds, err := st.VolumeDefinitions().List(ctx, "dst-defstack")
	if err != nil {
		t.Fatalf("list the target's volumes: %v", err)
	}

	if len(vds) != 1 {
		t.Errorf("after the resume the target has %d volume(s), want 1", len(vds))
	}
}

// blindVolumeListStore hides the target's volumes from a List while leaving
// them where a Get or a Create will find them: the pre-check's read failing,
// and the window between the two calls, produce the same shape.
type blindVolumeListStore struct {
	store.Store
}

func (b blindVolumeListStore) VolumeDefinitions() store.VolumeDefinitionStore {
	return blindVolumeList{b.Store.VolumeDefinitions()}
}

type blindVolumeList struct {
	store.VolumeDefinitionStore
}

func (b blindVolumeList) List(context.Context, string) ([]apiv1.VolumeDefinition, error) {
	return nil, nil
}

// A replay of a finished clone still has to apply the request's property
// edits. The first attempt can fail in applyClonePropEdits AFTER the volumes
// are already there, so the retry arrives at a target that looks finished with
// the edits never applied — and answering 201 then drops them, which is the
// accept-and-drop this endpoint refuses external_name and volume_passphrases
// to avoid.
func TestRDCloneReplayStillAppliesPropEdits(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-props")

	base, stop := startServerWithStore(t, st)
	defer stop()

	// A finished clone, made without any edits.
	first := postClone(t, base, "src-props", map[string]any{
		"name":          "dst-props",
		"use_zfs_clone": true,
	})
	_ = first.Body.Close()

	if first.StatusCode != http.StatusCreated {
		t.Fatalf("first clone: got %d, want 201", first.StatusCode)
	}

	// The replay carries edits the first attempt never applied.
	replay := postClone(t, base, "src-props", map[string]any{
		"name":           "dst-props",
		"use_zfs_clone":  true,
		"override_props": map[string]string{"Aux/replay": "landed"},
	})
	_ = replay.Body.Close()

	if replay.StatusCode != http.StatusCreated {
		t.Fatalf("replay: got %d, want 201", replay.StatusCode)
	}

	got, err := st.ResourceDefinitions().Get(ctx, "dst-props")
	if err != nil {
		t.Fatalf("get the clone: %v", err)
	}

	if got.Props["Aux/replay"] != "landed" {
		t.Errorf("the replay reported success and dropped its override_props: %v", got.Props)
	}
}

// materializeRestoredRD creates the definition, hydrates the volumes and THEN
// stamps the replicas. A leftover read on volumes alone is called finished the
// instant hydrate returns — the Bug 354 empty shell, certified complete, with
// the clone-status poll agreeing because it compares volume counts. An attempt
// that dies in that window left a target every later retry answered 201 over.
func TestRDCloneResumesALeftoverThatWasHydratedButNeverPlaced(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-unplaced")

	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name:              cloneSnapshotName("dst-unplaced"),
		ResourceName:      "src-unplaced",
		Nodes:             []string{"node-a"},
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{{VolumeNumber: 0, SizeKib: 64 * 1024}},
	}); err != nil {
		t.Fatalf("seed the leftover snapshot: %v", err)
	}

	seedCloneLeftover(t, st, "src-unplaced", "dst-unplaced")

	// Hydrated, never placed: exactly where an attempt dies between the two.
	if err := st.VolumeDefinitions().Create(ctx, "dst-unplaced",
		&apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 64 * 1024}); err != nil {
		t.Fatalf("seed the hydrated volume: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := postClone(t, base, "src-unplaced", map[string]any{
		"name":          "dst-unplaced",
		"use_zfs_clone": true,
	})
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want 201", resp.StatusCode)
	}

	replicas, err := st.Resources().ListByDefinition(ctx, "dst-unplaced")
	if err != nil {
		t.Fatalf("list the target's replicas: %v", err)
	}

	if len(replicas) == 0 {
		t.Error("the retry reported the clone complete and placed nothing")
	}
}

// A replay of a finished clone is a copy of a point-in-time and owes the source
// nothing. The shape comparison derives what it compares against from the LIVE
// source, so an ordinary `rd modify --resource-group` on the source turned
// every later replay into a 409 — with a correction linstor-csi cannot follow,
// since it sends the same body every time.
func TestRDCloneReplayOfAFinishedCloneSurvivesASourceGroupMove(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-rgmove")

	base, stop := startServerWithStore(t, st)
	defer stop()

	first := postClone(t, base, "src-rgmove", map[string]any{
		"name":          "dst-rgmove",
		"use_zfs_clone": true,
	})
	_ = first.Body.Close()

	if first.StatusCode != http.StatusCreated {
		t.Fatalf("first clone = %d, want 201", first.StatusCode)
	}

	// The operator moves the SOURCE to another group. Nothing about the
	// finished clone changed.
	src, err := st.ResourceDefinitions().Get(ctx, "src-rgmove")
	if err != nil {
		t.Fatalf("read the source: %v", err)
	}

	src.ResourceGroupName = "grp-moved"
	if err := st.ResourceDefinitions().Update(ctx, &src); err != nil {
		t.Fatalf("move the source: %v", err)
	}

	replay := postClone(t, base, "src-rgmove", map[string]any{
		"name":          "dst-rgmove",
		"use_zfs_clone": true,
	})
	_ = replay.Body.Close()

	if replay.StatusCode != http.StatusCreated {
		t.Fatalf("replay = %d, want 201 — the clone was finished before the source moved",
			replay.StatusCode)
	}
}

// The internal snapshot is deletable: handleSnapshotDelete has no guard for one
// a clone depends on, and operators do remove stray clone-* snapshots because
// they block deleting the source. Asking the finished question after taking one
// meant a pure replay re-snapshotted the LIVE source, and once the source had
// grown that fresh snapshot diverged from the finished target and refused the
// replay from then on — with a correction that destroys a clone holding data.
func TestRDCloneReplayWithTheInternalSnapshotGoneTakesNoNewOne(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-nosnap")

	base, stop := startServerWithStore(t, st)
	defer stop()

	first := postClone(t, base, "src-nosnap", map[string]any{
		"name":          "dst-nosnap",
		"use_zfs_clone": true,
	})
	_ = first.Body.Close()

	if first.StatusCode != http.StatusCreated {
		t.Fatalf("first clone = %d, want 201", first.StatusCode)
	}

	if err := st.Snapshots().Delete(ctx, "src-nosnap", cloneSnapshotName("dst-nosnap")); err != nil {
		t.Fatalf("delete the internal snapshot: %v", err)
	}

	// The source grows, which is what used to poison the replay permanently.
	vd, err := st.VolumeDefinitions().Get(ctx, "src-nosnap", 0)
	if err != nil {
		t.Fatalf("read the source volume: %v", err)
	}

	vd.SizeKib = 128 * 1024
	if err := st.VolumeDefinitions().Update(ctx, "src-nosnap", &vd); err != nil {
		t.Fatalf("grow the source: %v", err)
	}

	for attempt := 1; attempt <= 2; attempt++ {
		replay := postClone(t, base, "src-nosnap", map[string]any{
			"name":          "dst-nosnap",
			"use_zfs_clone": true,
		})
		_ = replay.Body.Close()

		if replay.StatusCode != http.StatusCreated {
			t.Fatalf("replay %d = %d, want 201", attempt, replay.StatusCode)
		}
	}

	// And the replay took no snapshot of the live source: a pure replay must
	// not write, least of all an object claiming by its name to be this
	// clone's origin while recording a different point-in-time.
	if _, err := st.Snapshots().Get(ctx, "src-nosnap", cloneSnapshotName("dst-nosnap")); err == nil {
		t.Error("the replay re-snapshotted the live source")
	}
}

// hydrateVolumesFromSnapshot tolerates a volume already present at the matching
// size precisely so a partial hydration can be finished, and the restore
// endpoint sharing this data plane does resume the identical state. Comparing
// volume COUNTS classified a strict prefix as somebody else's definition, so a
// multi-volume clone whose first attempt died between two creates was stranded.
func TestRDCloneResumesAHalfHydratedMultiVolumeLeftover(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-mv")

	if err := st.VolumeDefinitions().Create(ctx, "src-mv",
		&apiv1.VolumeDefinition{VolumeNumber: 1, SizeKib: 32 * 1024}); err != nil {
		t.Fatalf("give the source a second volume: %v", err)
	}

	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name:         cloneSnapshotName("dst-mv"),
		ResourceName: "src-mv",
		Nodes:        []string{"node-a"},
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{
			{VolumeNumber: 0, SizeKib: 64 * 1024},
			{VolumeNumber: 1, SizeKib: 32 * 1024},
		},
	}); err != nil {
		t.Fatalf("seed the leftover snapshot: %v", err)
	}

	seedCloneLeftover(t, st, "src-mv", "dst-mv")

	// One of the two volumes landed before the first attempt died.
	if err := st.VolumeDefinitions().Create(ctx, "dst-mv",
		&apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 64 * 1024}); err != nil {
		t.Fatalf("seed the half hydration: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := postClone(t, base, "src-mv", map[string]any{
		"name":          "dst-mv",
		"use_zfs_clone": true,
	})
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want 201 — the leftover is a prefix of what this clone restores",
			resp.StatusCode)
	}

	vds, err := st.VolumeDefinitions().List(ctx, "dst-mv")
	if err != nil {
		t.Fatalf("list the target's volumes: %v", err)
	}

	if len(vds) != 2 {
		t.Errorf("target has %d volume(s) after the resume, want 2", len(vds))
	}
}

// One cluster state, two answers: the create branch refuses a clone whose
// snapshot would have to be taken on an offline node, and the reuse branch ran
// none of that. The snapshot's nodes are where the restore places the clone's
// replicas, so an offline one is the same problem whichever attempt took the
// snapshot — and the retry stamped a replica on a node whose satellite cannot
// act on it.
func TestRDCloneRefusesToResumeOntoAnOfflineNode(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-offline")

	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name:              cloneSnapshotName("dst-offline"),
		ResourceName:      "src-offline",
		Nodes:             []string{"node-a"},
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{{VolumeNumber: 0, SizeKib: 64 * 1024}},
	}); err != nil {
		t.Fatalf("seed the leftover snapshot: %v", err)
	}

	if err := st.Nodes().SetConnectionStatus(ctx, "node-a", apiv1.NodeTypeOffline); err != nil {
		t.Fatalf("take the node offline: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := postClone(t, base, "src-offline", map[string]any{
		"name":          "dst-offline",
		"use_zfs_clone": true,
	})
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 — the snapshot's node is offline, the same answer "+
			"the create branch gives", resp.StatusCode)
	}
}

// The modify body declares delete_namespaces and the merge dropped it, so
// `linstor rd delete-property <rd> --namespace <ns>` answered 200 and changed
// nothing.
func TestRDModifyHonoursDeleteNamespaces(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: "rd-ns",
		Props: map[string]string{
			"DrbdOptions":              "top",
			"DrbdOptions/Net/protocol": "C",
			"DrbdOptionsOther":         "keep-me",
		},
	}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(map[string]any{"delete_namespaces": []string{"DrbdOptions"}})

	resp := httpPut(t, base+"/v1/resource-definitions/rd-ns", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	got, err := st.ResourceDefinitions().Get(ctx, "rd-ns")
	if err != nil {
		t.Fatalf("read the RD back: %v", err)
	}

	for _, key := range []string{"DrbdOptions", "DrbdOptions/Net/protocol"} {
		if _, present := got.Props[key]; present {
			t.Errorf("prop %q survived delete_namespaces", key)
		}
	}

	// The neighbouring key that merely shares a prefix is not in the
	// namespace and must stay.
	if got.Props["DrbdOptionsOther"] != "keep-me" {
		t.Errorf("DrbdOptionsOther = %q, want keep-me", got.Props["DrbdOptionsOther"])
	}
}
