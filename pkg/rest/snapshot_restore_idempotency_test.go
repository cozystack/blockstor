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
