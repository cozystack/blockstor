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

// TestVolumeDefinitionsCreateRoundTrip: create via REST envelope, list+get see it.
func TestVolumeDefinitionsCreateRoundTrip(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.VolumeDefinitionCreate{
		VolumeDefinition: apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 2048},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/volume-definitions", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201", resp.StatusCode)
	}

	listResp := httpGet(t, base+"/v1/resource-definitions/pvc-1/volume-definitions")
	defer func() { _ = listResp.Body.Close() }()

	var vds []apiv1.VolumeDefinition
	if jErr := json.NewDecoder(listResp.Body).Decode(&vds); jErr != nil {
		t.Fatalf("decode: %v", jErr)
	}

	if len(vds) != 1 || vds[0].SizeKib != 2048 {
		t.Errorf("got %+v, want one VD with SizeKib=2048", vds)
	}
}

// TestVolumeDefinitionsGetMissingRD: 404 when RD itself does not exist.
func TestVolumeDefinitionsGetMissingRD(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpGet(t, base+"/v1/resource-definitions/ghost/volume-definitions")
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", resp.StatusCode)
	}
}

// TestVolumeDefinitionsBadVolumeNumber: non-numeric `vn` → 400.
func TestVolumeDefinitionsBadVolumeNumber(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpGet(t, base+"/v1/resource-definitions/x/volume-definitions/notanum")
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400", resp.StatusCode)
	}
}

// TestVolumeDefinitionsWithoutStore: 503 across all VD paths.
func TestVolumeDefinitionsWithoutStore(t *testing.T) {
	base, stop := startServerCustom(t, &Server{Addr: pickFreeAddr(t), Store: nil})
	defer stop()

	resp := httpGet(t, base+"/v1/resource-definitions/x/volume-definitions")
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status: got %d, want 503", resp.StatusCode)
	}
}

// TestVolumeDefinitionsGet pins the per-VD GET happy path. The
// existing TestVolumeDefinitionsBadVolumeNumber covers the 400
// path; this one pins the canonical 200 with a deserialised
// VolumeDefinition body so a refactor that flipped the response
// shape to an envelope (`{"volume_definition":{...}}`) would be
// caught — golinstor's per-VD client decodes a bare VD.
func TestVolumeDefinitionsGet(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-getvd"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.VolumeDefinitions().Create(ctx, "pvc-getvd",
		&apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 1024 * 1024}); err != nil {
		t.Fatalf("seed VD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/resource-definitions/pvc-getvd/volume-definitions/0")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got apiv1.VolumeDefinition
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode bare VolumeDefinition: %v", err)
	}

	if got.VolumeNumber != 0 || got.SizeKib != 1024*1024 {
		t.Errorf("got %+v, want VolumeNumber=0 SizeKib=%d", got, 1024*1024)
	}
}

// TestVolumeDefinitionsGetMissingVD: GET on a vol_num that doesn't
// exist (RD exists, but no such volume) → 404. Pins the
// distinction from missing-RD (also 404 but for a different
// reason — operator log lines should differ).
func TestVolumeDefinitionsGetMissingVD(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/resource-definitions/pvc-1/volume-definitions/99")
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", resp.StatusCode)
	}
}

// TestVolumeDefinitionsUpdate is the CSI ControllerExpandVolume hot
// path: PUT /v1/resource-definitions/{rd}/volume-definitions/{vol}
// with a new SizeKib must round-trip into the store. The path-derived
// VolumeNumber must win over whatever the body declares so a typo on
// the body's volume_number can't accidentally resize a different vol.
func TestVolumeDefinitionsUpdate(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-grow"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.VolumeDefinitions().Create(ctx, "pvc-grow",
		&apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 1024 * 1024}); err != nil {
		t.Fatalf("seed VD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// Body's volume_number is intentionally wrong; the path's `0` must win.
	body, _ := json.Marshal(apiv1.VolumeDefinition{
		VolumeNumber: 99,
		SizeKib:      2 * 1024 * 1024,
	})

	resp := httpPut(t, base+"/v1/resource-definitions/pvc-grow/volume-definitions/0", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.VolumeDefinitions().Get(ctx, "pvc-grow", 0)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.SizeKib != 2*1024*1024 {
		t.Errorf("SizeKib after PUT: got %d, want %d", got.SizeKib, 2*1024*1024)
	}

	// Volume 99 must NOT have been silently created from the body.
	_, err = st.VolumeDefinitions().Get(ctx, "pvc-grow", 99)
	if err == nil {
		t.Errorf("PUT must not silently create vol-99 from body's volume_number")
	}
}

// TestVolumeDefinitionsUpdateMissing: PUT against a non-existent VD
// returns 404. Pins the missing-vol error path.
func TestVolumeDefinitionsUpdateMissing(t *testing.T) {
	st := store.NewInMemory()

	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.VolumeDefinition{SizeKib: 2 * 1024 * 1024})

	resp := httpPut(t, base+"/v1/resource-definitions/pvc-1/volume-definitions/0", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", resp.StatusCode)
	}
}

// TestVolumeDefinitionsUpdateBadVolumeNumber: non-numeric `vn` in
// the path → 400 (bad request) before we touch the store.
func TestVolumeDefinitionsUpdateBadVolumeNumber(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpPut(t,
		base+"/v1/resource-definitions/x/volume-definitions/notanum",
		[]byte(`{"size_kib":1024}`))
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400", resp.StatusCode)
	}
}

// TestVolumeDefinitionsDelete: DELETE removes the VD; subsequent
// GET returns 404. Pins the cleanup path the RD-delete reconciler
// drives.
func TestVolumeDefinitionsDelete(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.VolumeDefinitions().Create(ctx, "pvc-1",
		&apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 1024 * 1024}); err != nil {
		t.Fatalf("seed VD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t, base+"/v1/resource-definitions/pvc-1/volume-definitions/0")
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("DELETE status: got %d, want 204", resp.StatusCode)
	}

	_, err := st.VolumeDefinitions().Get(ctx, "pvc-1", 0)
	if err == nil {
		t.Errorf("VD still present after DELETE")
	}
}
