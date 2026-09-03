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

// seedRestoreSource is a definition with one snapshot of it, the minimum a
// snapshot-restore needs.
// drbdRestoreMarkerForTest is the marker key materializeRestoredRD stamps.
const drbdRestoreMarkerForTest = "BlockstorRestoreFromSnapshot"

func seedRestoreSource(ctx context.Context, t *testing.T, st store.Store) {
	t.Helper()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-src"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name:         "snap-1",
		ResourceName: "pvc-src",
		Nodes:        []string{"n1", "n2"},
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{
			{VolumeNumber: 0, SizeKib: 1024 * 1024},
		},
	}); err != nil {
		t.Fatalf("seed snap: %v", err)
	}
}

// CSI requires CreateVolume to be idempotent: a repeat with the same name and
// parameters must succeed and return the volume that already exists.
// external-provisioner has no other way to make progress after a partial
// failure, so a second call that fails makes the first partial failure
// terminal for that volume name — the PVC stays Pending forever and the
// leftover definition has to be deleted by hand.
func TestSnapshotRestoreIsIdempotent(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()
	seedRestoreSource(ctx, t, st)

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(map[string]string{
		"to_resource":   "pvc-dst",
		"from_snapshot": "snap-1",
	})

	first := httpPost(t, base+"/v1/resource-definitions/pvc-src/snapshot-restore-resource", body)
	_ = first.Body.Close()

	if first.StatusCode != http.StatusCreated {
		t.Fatalf("first restore: got %d, want 201", first.StatusCode)
	}

	// The retry external-provisioner issues after a partial failure.
	second := httpPost(t, base+"/v1/resource-definitions/pvc-src/snapshot-restore-resource", body)
	defer func() { _ = second.Body.Close() }()

	if second.StatusCode != http.StatusCreated {
		t.Fatalf("retry: got %d, want 201 — a repeat of the same restore must succeed, "+
			"or the first partial failure is terminal for this volume name", second.StatusCode)
	}

	var rcs []apiv1.APICallRc
	if err := json.NewDecoder(second.Body).Decode(&rcs); err != nil {
		t.Fatalf("decode retry envelope: %v", err)
	}

	if len(rcs) == 0 {
		t.Fatal("the retry returned an empty envelope")
	}
}

// Idempotency stops at the restore's own leftover. A definition under that
// name which is NOT the one this restore would have produced is a genuine
// collision, and answering 201 would report a restore that never happened
// over somebody else's data.
func TestSnapshotRestoreStillRefusesAForeignName(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()
	seedRestoreSource(ctx, t, st)

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:  "pvc-dst",
		Props: map[string]string{"someone": "else"},
	}); err != nil {
		t.Fatalf("seed the occupant: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(map[string]string{
		"to_resource":   "pvc-dst",
		"from_snapshot": "snap-1",
	})

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-src/snapshot-restore-resource", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status: got %d, want 409 — that name holds a definition this restore "+
			"did not create", resp.StatusCode)
	}

	got, err := st.ResourceDefinitions().Get(ctx, "pvc-dst")
	if err != nil {
		t.Fatalf("get the occupant: %v", err)
	}

	if got.Props["someone"] != "else" {
		t.Error("the refused restore overwrote the definition that was already there")
	}
}

// The marker is stamped with the definition, BEFORE its volumes are hydrated
// and its replicas placed. So a definition carrying it says a restore
// started, not that one finished, and a retry that reads the marker as
// completion turns the terminal failure this endpoint used to have into a
// silent incomplete one — CSI sees the volume as ready and nothing ever
// finishes it.
//
// The leftover here is exactly that shape: the definition and the marker, no
// volumes. The retry has to complete it.
func TestSnapshotRestoreResumesAnIncompleteLeftover(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()
	seedRestoreSource(ctx, t, st)

	// What a first attempt leaves behind when it fails after creating the
	// definition and before hydrating the volumes.
	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:  "pvc-dst",
		Props: map[string]string{drbdRestoreMarkerForTest: "pvc-src:snap-1"},
	}); err != nil {
		t.Fatalf("seed the leftover: %v", err)
	}

	vds, err := st.VolumeDefinitions().List(ctx, "pvc-dst")
	if err != nil {
		t.Fatalf("list the leftover's volumes: %v", err)
	}

	if len(vds) != 0 {
		t.Fatalf("the leftover already has %d volume(s); the test is not modelling "+
			"an incomplete restore", len(vds))
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(map[string]string{
		"to_resource":   "pvc-dst",
		"from_snapshot": "snap-1",
	})

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-src/snapshot-restore-resource", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("retry over an incomplete leftover: got %d, want 201", resp.StatusCode)
	}

	// The point of the retry: the volumes the first attempt never wrote.
	vds, err = st.VolumeDefinitions().List(ctx, "pvc-dst")
	if err != nil {
		t.Fatalf("list volumes after the retry: %v", err)
	}

	if len(vds) != 1 {
		t.Errorf("after the retry the target has %d volume(s), want 1 — the retry "+
			"reported success over a definition it never finished", len(vds))
	}
}
