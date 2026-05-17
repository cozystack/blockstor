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

// Bug 220 + Bug 221 — the per-Resource Volume endpoints were entirely
// missing from blockstor's REST surface. Three routes are involved:
//
//   GET    /v1/resource-definitions/{rd}/resources/{node}/volumes
//   GET    /v1/resource-definitions/{rd}/resources/{node}/volumes/{vlmNr}
//   PUT    /v1/resource-definitions/{rd}/resources/{node}/volumes/{vlmNr}
//
// Upstream LINBIT linstor-server defines all three (controller's
// `Volumes.java`, @Path("v1/resource-definitions/{rscName}/resources/
// {nodeName}/volumes")). Pre-fix, python-linstor's `linstor volume
// set-property` and golinstor's `ResourceService.ModifyVolume /
// GetVolumes / GetVolume` 404'd against blockstor because the
// dispatcher had no handler registered. These tests pin the wire
// contract for the new handlers.
//
// Wire shapes (mirroring upstream):
//   - List GET returns `[]apiv1.Volume` (NOT a wrapping envelope).
//   - Single GET returns a bare `apiv1.Volume`.
//   - PUT body is the `GenericPropsModify` envelope (override_props /
//     delete_props / delete_namespaces); the merge lands on
//     `Resource.Spec.Volumes[i].Props` for the matching VolumeNumber.

// seedRDWithTwoResources seeds an RD plus two Resources, each carrying
// three Volume rows (VlmNr 0/1/2). Used as the canonical fixture for
// the list / get / put happy paths so each test starts from the same
// shape.
func seedRDWithTwoResourcesAndVolumes(t *testing.T, st store.Store) {
	t.Helper()

	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-220"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	for _, node := range []string{"n1", "n2"} {
		res := &apiv1.Resource{
			Name:     "pvc-220",
			NodeName: node,
			Volumes: []apiv1.Volume{
				{VolumeNumber: 0, StoragePool: "pool0", DevicePath: "/dev/drbd1000", AllocatedKib: 1024},
				{VolumeNumber: 1, StoragePool: "pool0", DevicePath: "/dev/drbd1001", AllocatedKib: 2048},
				{VolumeNumber: 2, StoragePool: "pool0", DevicePath: "/dev/drbd1002", AllocatedKib: 4096},
			},
		}
		if err := st.Resources().Create(ctx, res); err != nil {
			t.Fatalf("seed Resource on %s: %v", node, err)
		}
	}
}

// TestListVolumesPerResource pins the GET list happy path. Seed an RD
// with 2 Resources × 3 Volumes; GET on n1 must surface exactly its 3
// Volumes (not n2's) as a top-level JSON array — the upstream wire
// shape golinstor's `ResourceService.GetVolumes(rd, node)` decodes.
func TestListVolumesPerResource(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedRDWithTwoResourcesAndVolumes(t, st)

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/resource-definitions/pvc-220/resources/n1/volumes")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET list status: got %d, want 200", resp.StatusCode)
	}

	var vols []apiv1.Volume
	if err := json.NewDecoder(resp.Body).Decode(&vols); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(vols) != 3 {
		t.Fatalf("len: got %d, want 3 (Volumes are n1-local, n2's must not bleed in)", len(vols))
	}

	wantNums := map[int32]bool{0: true, 1: true, 2: true}

	for i := range vols {
		if !wantNums[vols[i].VolumeNumber] {
			t.Errorf("unexpected VolumeNumber %d in list", vols[i].VolumeNumber)
		}

		delete(wantNums, vols[i].VolumeNumber)
	}

	if len(wantNums) != 0 {
		t.Errorf("missing VolumeNumbers: %v", wantNums)
	}
}

// TestGetVolumeByNumberPerResource pins the GET single happy path. URL
// fixes (rd, node, vlmNr); response must be a bare `apiv1.Volume`, not
// a slice, with the matching VolumeNumber projected.
func TestGetVolumeByNumberPerResource(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedRDWithTwoResourcesAndVolumes(t, st)

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/resource-definitions/pvc-220/resources/n1/volumes/1")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET single status: got %d, want 200", resp.StatusCode)
	}

	var vol apiv1.Volume
	if err := json.NewDecoder(resp.Body).Decode(&vol); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if vol.VolumeNumber != 1 {
		t.Errorf("VolumeNumber: got %d, want 1", vol.VolumeNumber)
	}

	if vol.AllocatedKib != 2048 {
		t.Errorf("AllocatedKib: got %d, want 2048", vol.AllocatedKib)
	}
}

// TestGetVolumeByNumberPerResource_404 pins the GET single not-found
// path: a vlmNr that's not on the Resource MUST 404 (not 200-with-
// empty-object). Surfaces as the typed APICallRc envelope so the
// python CLI and golinstor unmarshal it cleanly.
func TestGetVolumeByNumberPerResource_404(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedRDWithTwoResourcesAndVolumes(t, st)

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/resource-definitions/pvc-220/resources/n1/volumes/99")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET missing vlmNr status: got %d, want 404", resp.StatusCode)
	}

	var rcs []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rcs); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}

	if len(rcs) == 0 {
		t.Fatalf("expected non-empty APICallRc envelope on 404, got empty body")
	}
}

// TestModifyVolumePerResource_SetProperty pins the PUT override_props
// half: a single Aux/* key is merged into the matching
// Resource.Spec.Volumes[i].Props bag without touching other Volumes or
// the Resource's top-level Props. Verified by a subsequent GET — the
// round-trip ensures both store-write AND read-side projection cover
// the per-Volume props map.
func TestModifyVolumePerResource_SetProperty(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedRDWithTwoResourcesAndVolumes(t, st)

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.GenericPropsModify{
		OverrideProps: map[string]string{
			"Aux/Region": "eu-west-1",
		},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPut(t, base+"/v1/resource-definitions/pvc-220/resources/n1/volumes/1", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status: got %d, want 200", resp.StatusCode)
	}

	// Subsequent GET must reflect the new prop on VlmNr=1 only.
	getResp := httpGet(t, base+"/v1/resource-definitions/pvc-220/resources/n1/volumes/1")
	defer func() { _ = getResp.Body.Close() }()

	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("follow-up GET status: got %d, want 200", getResp.StatusCode)
	}

	var vol apiv1.Volume
	if err := json.NewDecoder(getResp.Body).Decode(&vol); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if vol.Props["Aux/Region"] != "eu-west-1" {
		t.Errorf("Aux/Region on VlmNr=1: got %q (full props: %v), want %q", vol.Props["Aux/Region"], vol.Props, "eu-west-1")
	}

	// Sibling Volume (VlmNr=0) must not have inherited the prop —
	// per-Volume scope, not per-Resource.
	got, err := st.Resources().Get(t.Context(), "pvc-220", "n1")
	if err != nil {
		t.Fatalf("store Get: %v", err)
	}

	for i := range got.Volumes {
		if got.Volumes[i].VolumeNumber == 0 {
			if _, present := got.Volumes[i].Props["Aux/Region"]; present {
				t.Errorf("Aux/Region leaked onto sibling VlmNr=0: %v", got.Volumes[i].Props)
			}
		}
	}
}

// TestModifyVolumePerResource_DeleteProperty pins the PUT delete_props
// half: a key listed in DeleteProps is removed from the matching
// Volume's Props bag; sibling Volumes and the Resource's top-level
// Props are unaffected.
func TestModifyVolumePerResource_DeleteProperty(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()

	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-220d"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Resources().Create(t.Context(), &apiv1.Resource{
		Name:     "pvc-220d",
		NodeName: "n1",
		Volumes: []apiv1.Volume{
			{VolumeNumber: 0, Props: map[string]string{"keep-me": "stay"}},
			{VolumeNumber: 1, Props: map[string]string{
				"Aux/Region": "eu-west-1",
				"keep-me":    "stay",
			}},
		},
	}); err != nil {
		t.Fatalf("seed Resource: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.GenericPropsModify{
		DeleteProps: []string{"Aux/Region"},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPut(t, base+"/v1/resource-definitions/pvc-220d/resources/n1/volumes/1", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.Resources().Get(t.Context(), "pvc-220d", "n1")
	if err != nil {
		t.Fatalf("store Get: %v", err)
	}

	for i := range got.Volumes {
		if got.Volumes[i].VolumeNumber != 1 {
			continue
		}

		if _, present := got.Volumes[i].Props["Aux/Region"]; present {
			t.Errorf("Aux/Region still present on VlmNr=1 after delete: %v", got.Volumes[i].Props)
		}

		if got.Volumes[i].Props["keep-me"] != "stay" {
			t.Errorf("sibling key keep-me clobbered on VlmNr=1: %v", got.Volumes[i].Props)
		}
	}
}

// TestModifyVolumePerResource_404_BadVlmNr pins the PUT not-found
// path. A PUT against an existing (rd, node) Resource but a
// VolumeNumber that has no corresponding Volume row MUST 404 (not
// silently no-op) and return the typed envelope so callers can route
// the failure correctly.
func TestModifyVolumePerResource_404_BadVlmNr(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedRDWithTwoResourcesAndVolumes(t, st)

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.GenericPropsModify{
		OverrideProps: map[string]string{"Aux/Region": "eu-west-1"},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPut(t, base+"/v1/resource-definitions/pvc-220/resources/n1/volumes/99", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("PUT missing vlmNr status: got %d, want 404", resp.StatusCode)
	}

	var rcs []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rcs); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}

	if len(rcs) == 0 {
		t.Fatalf("expected non-empty APICallRc envelope on 404, got empty body")
	}
}
