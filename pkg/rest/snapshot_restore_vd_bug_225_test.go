/*
Copyright 2026 Cozystack contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package rest

import (
	"encoding/json"
	"net/http"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Bug 225 (P2) — `linstor snapshot volume-definition-restore` failed
// because blockstor never wired
// `POST /v1/resource-definitions/{rd}/snapshot-restore-volume-definition/{snap}`.
// Upstream LINSTOR exposes the endpoint to copy a snapshot's
// VolumeDefinition layout onto a (typically pre-existing) target RD
// without spawning replicas — the operator-facing complement to
// `snapshot-restore-resource` which builds a brand-new RD.
//
// Wire contract mirrored from upstream Java
// `controller/.../SnapshotRestoreVolumeDefinition.java`:
//
//   - Body: `{"to_resource": "<targetRD>"}` (same `SnapshotRestore`
//     JSON shape as `snapshot-restore-resource`; the controller only
//     reads `to_resource`).
//   - Path: `/v1/resource-definitions/{srcRD}/snapshot-restore-volume-definition/{snap}`.
//   - Success: 200 + `[]ApiCallRc` envelope; the target RD now carries
//     a VolumeDefinition row for every volume the snapshot captured.
//
// Pre-fix the endpoint 404s — every test in this file asserts the
// post-fix wire shape AND that the resulting VolumeDefinitions are
// persisted on the target RD.

// TestBug225SnapshotRestoreVolumeDefinitionToExistingRD: the common
// case — a target RD already exists (created by the operator before
// the restore), the restore copies snapshot VDs onto it. Pre-fix
// `linstor snapshot volume-definition-restore <src> <snap> --to-resource <tgt>`
// died with 404; post-fix the target RD picks up the snapshot's
// VolumeDefinitions and is ready for `rd ap`.
func TestBug225SnapshotRestoreVolumeDefinitionToExistingRD(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "src-225"}); err != nil {
		t.Fatalf("seed source RD: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "tgt-225"}); err != nil {
		t.Fatalf("seed target RD: %v", err)
	}

	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name:         "snap-225",
		ResourceName: "src-225",
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{
			{VolumeNumber: 0, SizeKib: 1024 * 1024},
			{VolumeNumber: 1, SizeKib: 64 * 1024},
		},
	}); err != nil {
		t.Fatalf("seed snapshot: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(map[string]string{"to_resource": "tgt-225"})

	resp := httpPost(t,
		base+"/v1/resource-definitions/src-225/snapshot-restore-volume-definition/snap-225",
		body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	vds, err := st.VolumeDefinitions().List(ctx, "tgt-225")
	if err != nil {
		t.Fatalf("list target VDs: %v", err)
	}

	if len(vds) != 2 {
		t.Fatalf("hydrated VDs on target: got %d, want 2", len(vds))
	}

	wantSize := map[int32]int64{0: 1024 * 1024, 1: 64 * 1024}
	for _, vd := range vds {
		if got := vd.SizeKib; got != wantSize[vd.VolumeNumber] {
			t.Errorf("VD %d SizeKib: got %d, want %d", vd.VolumeNumber, got, wantSize[vd.VolumeNumber])
		}
	}
}

// TestBug225SnapshotRestoreVolumeDefinitionUnknownSnapshot: an
// unknown snapshot must 404 — same shape as the existing
// snapshot-restore-resource handler.
func TestBug225SnapshotRestoreVolumeDefinitionUnknownSnapshot(t *testing.T) {
	st := store.NewInMemory()

	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "src-225"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "tgt-225"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(map[string]string{"to_resource": "tgt-225"})

	resp := httpPost(t,
		base+"/v1/resource-definitions/src-225/snapshot-restore-volume-definition/ghost-snap",
		body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", resp.StatusCode)
	}
}

// TestBug225SnapshotRestoreVolumeDefinitionMissingToResource: empty
// `to_resource` must 400 — mirrors the snapshot-restore-resource
// handler's validation.
func TestBug225SnapshotRestoreVolumeDefinitionMissingToResource(t *testing.T) {
	st := store.NewInMemory()

	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "src-225"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Snapshots().Create(t.Context(), &apiv1.Snapshot{
		Name:         "snap-225",
		ResourceName: "src-225",
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{
			{VolumeNumber: 0, SizeKib: 64 * 1024},
		},
	}); err != nil {
		t.Fatalf("seed snapshot: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t,
		base+"/v1/resource-definitions/src-225/snapshot-restore-volume-definition/snap-225",
		[]byte(`{}`))
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400", resp.StatusCode)
	}
}
