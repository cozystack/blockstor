// SPDX-License-Identifier: Apache-2.0

package rest

import (
	"context"
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
