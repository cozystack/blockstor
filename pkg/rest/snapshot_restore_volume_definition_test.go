// SPDX-License-Identifier: Apache-2.0

package rest

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// The volume-definition restore fetched the target definition and threw it
// away. Its siblings refuse a target carrying DELETE because finishing one
// races the tear-down reaping what it writes, and volumes hydrated here are
// precisely that.
func TestSnapshotRestoreVolumeDefinitionRefusesADyingTarget(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedRestoreSource(ctx, t, st)

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:  "pvc-vd-dying",
		Flags: []string{rdFlagDelete},
	}); err != nil {
		t.Fatalf("seed the dying target: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(map[string]string{"to_resource": "pvc-vd-dying"})

	resp := httpPost(t,
		base+"/v1/resource-definitions/pvc-src/snapshot-restore-volume-definition/snap-1", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d, want 409 — that definition is being deleted", resp.StatusCode)
	}

	if msg := decodeRCs(t, resp)[0].Message; !strings.Contains(msg, "being deleted") {
		t.Errorf("message = %q, want it to name the deletion", msg)
	}

	vds, err := st.VolumeDefinitions().List(ctx, "pvc-vd-dying")
	if err != nil {
		t.Fatalf("list the target's volumes: %v", err)
	}

	if len(vds) != 0 {
		t.Errorf("hydrated %d volume(s) into a definition being deleted", len(vds))
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

// The ownership term is what separates a restore's own expanded volume from a
// larger volume on a definition the volume-definition restore does not own.
// The existing size fixture seeds a smaller volume, so it never reached it.
func TestSnapshotRestoreVolumeDefinitionRefusesALargerVolumeItDidNotWrite(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "vdl-src"}); err != nil {
		t.Fatalf("seed the source: %v", err)
	}

	seedRestoreSnapshot(t, st, "vdl-src", "snap-vdl", []string{"n1"})

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "vdl-dst"}); err != nil {
		t.Fatalf("seed the target: %v", err)
	}

	if err := st.VolumeDefinitions().Create(ctx, "vdl-dst",
		&apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 256 * 1024}); err != nil {
		t.Fatalf("seed the larger volume: %v", err)
	}

	base, stop := startServerWithStore(t, blindVolumeListStore{st})
	defer stop()

	resp := httpPost(t, base+"/v1/resource-definitions/vdl-src/snapshot-restore-volume-definition/snap-vdl",
		[]byte(`{"to_resource":"vdl-dst"}`))
	_ = resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		t.Error("status = 200 over a larger volume the restore did not write")
	}
}
