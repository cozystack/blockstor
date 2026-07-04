// SPDX-License-Identifier: Apache-2.0

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
	"strings"
	"sync"
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
		// 32 MiB — above Bug 155's 4 MiB minimum, matches the rest
		// of the test suite's canonical size.
		VolumeDefinition: apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 32768},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/volume-definitions", body)
	defer func() { _ = resp.Body.Close() }()

	// Upstream LINSTOR returns 200 (not 201) for child-volume
	// creates under an existing parent RD. Mirrors that.
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	listResp := httpGet(t, base+"/v1/resource-definitions/pvc-1/volume-definitions")
	defer func() { _ = listResp.Body.Close() }()

	var vds []apiv1.VolumeDefinition
	if jErr := json.NewDecoder(listResp.Body).Decode(&vds); jErr != nil {
		t.Fatalf("decode: %v", jErr)
	}

	if len(vds) != 1 || vds[0].SizeKib != 32768 {
		t.Errorf("got %+v, want one VD with SizeKib=32768", vds)
	}
}

// TestVolumeDefinitionsConcurrentAutoNumberNoLostUpdate is the BUG-048
// (P1, availability) regression: two back-to-back number-less
// `linstor vd c <rd>` POSTs against an RD that already carries vol-0
// must BOTH succeed and land at DISTINCT VolumeNumbers (1 and 2) — none
// silently dropped.
//
// Pre-fix the auto-assign picked the smallest free number in a REST-side
// List BEFORE the store Create, so both requests read `[vol-0]`, both
// chose VlmNr=1, the loser was rejected FAIL_EXISTS_VLM_DFN, and the
// operator's second intended volume vanished (only vol-0 + vol-1 left).
// The store-side CreateAutoNumbered re-derives the hole inside its
// conflict-retry loop, so the loser lands at vol-2.
//
// This is the L1 half of the BUG-048 fix; the kernel-truth convergence
// half is tests/e2e/cli-matrix/multi-volume-late-vd-create.sh.
func TestVolumeDefinitionsConcurrentAutoNumberNoLostUpdate(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}
	// vol-0 already present — this is the "late vd-add" precondition.
	if err := st.VolumeDefinitions().Create(t.Context(), "pvc-1", &apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 32768}); err != nil {
		t.Fatalf("seed vol-0: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// Number-less create body — the auto-assign path the python CLI's
	// `linstor vd c <rd> <size>` takes: the wire body OMITS the
	// `volume_number` key entirely (a marshalled apiv1.VolumeDefinition
	// would emit `"volume_number":0` because the field has no omitempty,
	// which the handler reads as an EXPLICIT 0 — that is NOT what the
	// CLI sends, so the test crafts the raw omitted-key body directly).
	body := []byte(`{"volume_definition":{"size_kib":32768}}`)

	const concurrent = 2

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		statuses []int
	)

	for range concurrent {
		wg.Add(1)

		go func() {
			defer wg.Done()

			resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/volume-definitions", body)
			defer func() { _ = resp.Body.Close() }()

			mu.Lock()
			statuses = append(statuses, resp.StatusCode)
			mu.Unlock()
		}()
	}

	wg.Wait()

	for i, code := range statuses {
		if code != http.StatusOK {
			t.Errorf("concurrent vd c #%d: status %d, want 200 (no request may be lost/rejected)", i, code)
		}
	}

	listResp := httpGet(t, base+"/v1/resource-definitions/pvc-1/volume-definitions")
	defer func() { _ = listResp.Body.Close() }()

	var vds []apiv1.VolumeDefinition
	if jErr := json.NewDecoder(listResp.Body).Decode(&vds); jErr != nil {
		t.Fatalf("decode: %v", jErr)
	}

	// Exactly three VDs: vol-0 (seeded) + the two concurrent adds at 1, 2.
	if len(vds) != 3 {
		t.Fatalf("got %d VDs, want 3 (vol-0 + two concurrent adds); a lost-update would leave 2: %+v", len(vds), vds)
	}

	seen := map[int32]bool{}
	for i := range vds {
		if seen[vds[i].VolumeNumber] {
			t.Fatalf("duplicate VolumeNumber %d in %+v", vds[i].VolumeNumber, vds)
		}
		seen[vds[i].VolumeNumber] = true
	}

	for i := range 3 {
		want := int32(i)
		if !seen[want] {
			t.Errorf("missing VolumeNumber %d (set: %v) — a concurrent add was silently dropped", want, seen)
		}
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

	if resp.StatusCode != http.StatusOK {
		t.Errorf("DELETE status: got %d, want 200", resp.StatusCode)
	}

	_, err := st.VolumeDefinitions().Get(ctx, "pvc-1", 0)
	if err == nil {
		t.Errorf("VD still present after DELETE")
	}
}

// TestVolumeDefinitionsDeleteHappyPath: DELETE on a real VD → 204
// + the VD vanishes from a subsequent GET. Pins the success branch
// (delete handler was at 60%).
func TestVolumeDefinitionsDeleteHappyPath(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.VolumeDefinitions().Create(ctx, "pvc-1", &apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 1024}); err != nil {
		t.Fatalf("seed VD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t, base+"/v1/resource-definitions/pvc-1/volume-definitions/0")
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("delete status: got %d, want 200", resp.StatusCode)
	}

	getResp := httpGet(t, base+"/v1/resource-definitions/pvc-1/volume-definitions/0")
	_ = getResp.Body.Close()

	if getResp.StatusCode != http.StatusNotFound {
		t.Errorf("post-delete get: got %d, want 404", getResp.StatusCode)
	}
}

// TestVolumeDefinitionsDeleteBadVolumeNumber: non-numeric vn on the
// DELETE path → 400. parseVolNum is shared with GET but the handler
// branch is distinct; pin it explicitly.
func TestVolumeDefinitionsDeleteBadVolumeNumber(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpDelete(t, base+"/v1/resource-definitions/pvc-1/volume-definitions/notanum")
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400", resp.StatusCode)
	}
}

// TestVolumeDefinitionsDeleteMissingVD: DELETE on a non-existent
// vol_num folds into 200 + warn-mask ApiCallRc envelope (Bug 66).
// linstor-csi's ControllerExpand path re-issues vd-delete on retry;
// the previous 404 crashed the python CLI's XML decoder fallback.
func TestVolumeDefinitionsDeleteMissingVD(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t, base+"/v1/resource-definitions/pvc-1/volume-definitions/42")
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status: got %d, want 200", resp.StatusCode)
	}
}

// TestVolumeDefinitionsCreateBadJSON: malformed body → 400. The
// decoder error must surface as 4xx so golinstor doesn't loop.
func TestVolumeDefinitionsCreateBadJSON(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/volume-definitions", []byte("{not-json"))
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400", resp.StatusCode)
	}
}

// TestVolumeDefinitionsUpdateBadJSON: malformed body on PUT → 400.
func TestVolumeDefinitionsUpdateBadJSON(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	if err := st.VolumeDefinitions().Create(ctx, "pvc-1", &apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 1024}); err != nil {
		t.Fatalf("seed VD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPut(t, base+"/v1/resource-definitions/pvc-1/volume-definitions/0", []byte("{not-json"))
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400", resp.StatusCode)
	}
}

// TestVolumeDefinitionsUpdateGolinstorWireShape pins the exact wire
// payload that golinstor's `VolumeDefinitionService.ModifyVolumeDefinition`
// emits — a bare envelope with `size_kib` at the top level (no
// `volume_definition` wrapper). This is the CSI ControllerExpandVolume
// hot path: linstor-csi → golinstor → blockstor REST. If the server
// ever required the wrapper-envelope, every grow from kubernetes would
// silently no-op.
//
// Wire format reference (golinstor v0.58+):
//
//	type VolumeDefinitionModify struct {
//	    SizeKib uint64 `json:"size_kib,omitempty"`
//	    GenericPropsModify
//	    Flags []string `json:"flags,omitempty"`
//	}
func TestVolumeDefinitionsUpdateGolinstorWireShape(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-csi"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.VolumeDefinitions().Create(ctx, "pvc-csi",
		&apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 1024 * 1024}); err != nil {
		t.Fatalf("seed VD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// Raw JSON — bypass any apiv1 envelope to mirror what golinstor
	// puts on the wire. snake_case keys per the OpenAPI spec.
	resp := httpPut(t,
		base+"/v1/resource-definitions/pvc-csi/volume-definitions/0",
		[]byte(`{"size_kib":4194304}`))
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.VolumeDefinitions().Get(ctx, "pvc-csi", 0)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.SizeKib != 4194304 {
		t.Errorf("SizeKib after CSI-shape PUT: got %d, want %d", got.SizeKib, 4194304)
	}
}

// TestVolumeDefinitionsUpdateNoOp pins that a PUT with the same
// size as already stored round-trips with 200 and leaves the VD
// unchanged. csi-resizer occasionally re-applies the same target
// size (controller-resize retry after a transient error); a strict
// "size must change" guard would loop the resize controller.
func TestVolumeDefinitionsUpdateNoOp(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-noop"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	const sizeKib = 2 * 1024 * 1024
	if err := st.VolumeDefinitions().Create(ctx, "pvc-noop",
		&apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: sizeKib}); err != nil {
		t.Fatalf("seed VD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.VolumeDefinition{SizeKib: sizeKib})

	resp := httpPut(t, base+"/v1/resource-definitions/pvc-noop/volume-definitions/0", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.VolumeDefinitions().Get(ctx, "pvc-noop", 0)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.SizeKib != sizeKib {
		t.Errorf("no-op PUT changed SizeKib: got %d, want %d", got.SizeKib, sizeKib)
	}
}

// TestVolumeDefinitionsUpdateShrinkRejected pins scenario 4.W13: a
// PUT that reduces SizeKib without the `force=true` escape hatch must
// be rejected at the REST layer with a 4xx and an operator-actionable
// message. LINSTOR does NOT auto-shrink the backing FS — `lvreduce`
// after a spec-shrink without an in-FS `resize2fs -s` first truncates
// live data. Upstream LINSTOR's CtrlVlmDfnModifyApiCallHandler raises
// FAIL_INVLD_VLM_SIZE on the same input ("Deployed volumes can only
// grow in size, not shrink"); blockstor matches the wire shape but
// makes the message UG9-actionable so the operator knows the exact
// out-of-band sequence (shrink FS first, then resize device).
//
// Pre-4.W13 behaviour was "accept shrink + emit warning entry"; the
// audit log warning was easy to miss under burst PUTs from
// csi-resizer retries. Strict rejection matches upstream and stops
// the foot-gun from firing in the first place.
func TestVolumeDefinitionsUpdateShrinkRejected(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-shrink"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	const initialKib = 4 * 1024 * 1024
	if err := st.VolumeDefinitions().Create(ctx, "pvc-shrink",
		&apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: initialKib}); err != nil {
		t.Fatalf("seed VD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// Halve the size — explicit shrink without force.
	const shrunkKib = initialKib / 2

	body, _ := json.Marshal(apiv1.VolumeDefinition{SizeKib: shrunkKib})

	resp := httpPut(t, base+"/v1/resource-definitions/pvc-shrink/volume-definitions/0", body)
	defer func() { _ = resp.Body.Close() }()

	// 400 Bad Request — not 409, not 422. golinstor's `client.ApiCallError`
	// classifies any 4xx as a client-fixable error and surfaces the
	// envelope message in `linstor`'s exit-code-1 path. 400 is the
	// closest match for "the caller's input is semantically invalid
	// for the current resource state" — matching upstream LINSTOR's
	// CtrlVlmDfnModifyApiCallHandler shrink rejection.
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("shrink status: got %d, want 400 (scenario 4.W13: strict rejection without force)", resp.StatusCode)
	}

	var rcs []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rcs); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}

	if len(rcs) != 1 {
		t.Fatalf("envelope entries: got %d, want 1 (single error entry); got=%+v", len(rcs), rcs)
	}

	// MASK_ERROR + FAIL_INVLD_VLM_SIZE (upstream's `206 | MASK_ERROR`).
	// Keeps the audit-log grep alongside upstream LINSTOR's traffic.
	if rcs[0].RetCode&apiCallRcError == 0 {
		t.Errorf("ret_code: got %#x, want MASK_ERROR bit set", rcs[0].RetCode)
	}

	if rcs[0].RetCode&apiCallRcFailInvldVlmSize == 0 {
		t.Errorf("ret_code: got %#x, want FAIL_INVLD_VLM_SIZE (206) sub-code", rcs[0].RetCode)
	}

	// Message must carry the operator-actionable guidance: filesystem
	// shrink first, then device resize, and the explicit "LINSTOR does
	// NOT auto-shrink" disclaimer that scenario 4.W13 pins. Without
	// these markers an audit-log grep would miss the data-loss-risk
	// class entirely.
	for _, want := range []string{
		"filesystem shrink-then-resize required",
		"LINSTOR does NOT auto-shrink",
	} {
		if !strings.Contains(rcs[0].Message, want) {
			t.Errorf("message missing actionable marker %q: %q", want, rcs[0].Message)
		}
	}

	// Both the source and target KiB must appear so the operator can
	// sanity-check the magnitude of the rejected shrink in the audit log.
	if !strings.Contains(rcs[0].Message, "4194304") {
		t.Errorf("message missing source size %d KiB: %q", initialKib, rcs[0].Message)
	}

	if !strings.Contains(rcs[0].Message, "2097152") {
		t.Errorf("message missing target size %d KiB: %q", shrunkKib, rcs[0].Message)
	}

	// Stored VD must be untouched — the rejection happens before the
	// merge writes to the store. A partial update would leave the
	// controller spec mismatched against the satellite reality.
	got, err := st.VolumeDefinitions().Get(ctx, "pvc-shrink", 0)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.SizeKib != initialKib {
		t.Errorf("rejected shrink mutated the store: SizeKib got %d, want %d (untouched)",
			got.SizeKib, initialKib)
	}
}

// TestVolumeDefinitionsUpdateShrinkWithForceAccepted pins the escape
// hatch: a PUT with `?force=true` (or `"force": true` in the body)
// MUST accept the shrink. Operators who have already shrunk the FS
// out-of-band (e.g. resize2fs -s <new-size>; umount; …) need a way
// to bring the LINSTOR spec back into sync with the now-smaller FS.
// Matches scenario 4.W13's "force flag required" alternative.
//
// The accepted-with-force path still emits the warning advisory
// (existing wire contract from Bug 38) so the operator's audit log
// retains the data-loss-risk breadcrumb.
func TestVolumeDefinitionsUpdateShrinkWithForceAccepted(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-force"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	const initialKib = 4 * 1024 * 1024
	if err := st.VolumeDefinitions().Create(ctx, "pvc-force",
		&apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: initialKib}); err != nil {
		t.Fatalf("seed VD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	const shrunkKib = initialKib / 2

	body, _ := json.Marshal(map[string]any{
		"size_kib": shrunkKib,
		"force":    true,
	})

	resp := httpPut(t, base+"/v1/resource-definitions/pvc-force/volume-definitions/0", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("shrink-with-force status: got %d, want 200 (force=true is the W13 escape hatch)",
			resp.StatusCode)
	}

	var rcs []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rcs); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}

	if len(rcs) != 2 {
		t.Fatalf("envelope entries: got %d, want 2 (success + shrink advisory); got=%+v", len(rcs), rcs)
	}

	if rcs[0].RetCode&maskInfo == 0 {
		t.Errorf("entry 0 ret_code = %x, want maskInfo bit set", rcs[0].RetCode)
	}

	if rcs[1].RetCode&maskWarn == 0 {
		t.Errorf("entry 1 ret_code = %x, want warn-mask bit set (shrink advisory)", rcs[1].RetCode)
	}

	got, err := st.VolumeDefinitions().Get(ctx, "pvc-force", 0)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.SizeKib != shrunkKib {
		t.Errorf("SizeKib after force-shrink: got %d, want %d", got.SizeKib, shrunkKib)
	}
}

// TestVolumeDefinitionsUpdateShrinkWithForceQueryAccepted pins the
// `?force=true` query-string variant of the W13 escape hatch. The
// query knob exists so a `curl -X PUT … ?force=true` works without
// re-shaping the JSON body — useful for ad-hoc operator scripts that
// can't easily inject a field into a golinstor-shaped payload.
func TestVolumeDefinitionsUpdateShrinkWithForceQueryAccepted(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-fq"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	const initialKib = 10240
	if err := st.VolumeDefinitions().Create(ctx, "pvc-fq",
		&apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: initialKib}); err != nil {
		t.Fatalf("seed VD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	const shrunkKib = 5120

	body, _ := json.Marshal(apiv1.VolumeDefinition{SizeKib: shrunkKib})

	resp := httpPut(t,
		base+"/v1/resource-definitions/pvc-fq/volume-definitions/0?force=true", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("force-query status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.VolumeDefinitions().Get(ctx, "pvc-fq", 0)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.SizeKib != shrunkKib {
		t.Errorf("SizeKib after ?force=true: got %d, want %d", got.SizeKib, shrunkKib)
	}
}

// TestVolumeDefinitionsUpdateLargeSizeKibRoundTrip pins that a large
// (multi-TiB) `size_kib` survives the JSON round-trip without
// truncation. The wire field is int64 on our side and uint64 in
// golinstor; a regression that decoded into int32 would clamp anything
// above ~2 TiB. The guard uses DRBD's 16 TiB per-device ceiling
// (maxVolumeDefinitionSizeKib, Bug 155) — the largest accepted size and
// still 8× above the int32 clamp point, so it exercises the >int32 wire
// path while remaining a size the resize bounds gate accepts. Petabyte-
// scale sizes are refused on BOTH create and resize (see
// TestBug155VDCreateRefusesAbsurdSize and the round-4 resize-bounds
// regressions); this test previously used 1 PiB, which pinned the exact
// create/resize bounds asymmetry the round-4 gate closed.
func TestVolumeDefinitionsUpdateLargeSizeKibRoundTrip(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-pib"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.VolumeDefinitions().Create(ctx, "pvc-pib",
		&apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 1024}); err != nil {
		t.Fatalf("seed VD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	const largeInBoundsKib = maxVolumeDefinitionSizeKib // 16 TiB, the largest accepted size (~8× int32 max)

	body, _ := json.Marshal(apiv1.VolumeDefinition{SizeKib: largeInBoundsKib})

	resp := httpPut(t, base+"/v1/resource-definitions/pvc-pib/volume-definitions/0", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.VolumeDefinitions().Get(ctx, "pvc-pib", 0)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.SizeKib != largeInBoundsKib {
		t.Errorf("SizeKib after large PUT: got %d, want %d (truncation?)", got.SizeKib, largeInBoundsKib)
	}
}

// TestVolumeDefinitionsUpdateGetRoundTrip exercises the
// CSI-after-grow flow: a PUT that bumps SizeKib must be readable
// through GET in the same wire shape (`size_kib` snake_case at the
// top level, not wrapped). golinstor's `GetVolumeDefinition` decodes
// a bare `VolumeDefinition`; a refactor that wrapped the response
// envelope would break the controller's post-grow refresh.
func TestVolumeDefinitionsUpdateGetRoundTrip(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-rt"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.VolumeDefinitions().Create(ctx, "pvc-rt",
		&apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 1024 * 1024}); err != nil {
		t.Fatalf("seed VD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	const grownKib = 8 * 1024 * 1024

	body, _ := json.Marshal(apiv1.VolumeDefinition{SizeKib: grownKib})

	putResp := httpPut(t, base+"/v1/resource-definitions/pvc-rt/volume-definitions/0", body)
	_ = putResp.Body.Close()

	if putResp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status: got %d, want 200", putResp.StatusCode)
	}

	getResp := httpGet(t, base+"/v1/resource-definitions/pvc-rt/volume-definitions/0")
	defer func() { _ = getResp.Body.Close() }()

	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("GET status: got %d, want 200", getResp.StatusCode)
	}

	var got apiv1.VolumeDefinition
	if err := json.NewDecoder(getResp.Body).Decode(&got); err != nil {
		t.Fatalf("decode GET response: %v", err)
	}

	if got.SizeKib != grownKib {
		t.Errorf("GET after PUT saw stale size: got %d, want %d", got.SizeKib, grownKib)
	}

	if got.VolumeNumber != 0 {
		t.Errorf("VolumeNumber drifted across round-trip: got %d, want 0", got.VolumeNumber)
	}
}

// TestVolumeDefinitionsUpdateMergeSemanticsPreservesSizeKib pins the
// audit-4.6 fix for the VD-PUT merge regression: a PUT body without
// `size_kib` (e.g. a props-only modify from `linstor vd set-property`,
// or an older golinstor that only sends override_props) must NOT
// silently zero the stored SizeKib. The satellite reconciler's grow branch is
// `vol.GetSizeKib() > status.UsableKib`, so the immediate on-disk
// volume stays intact either way — but a zeroed SizeKib makes
// `linstor vd l` report 0 KiB and the NEXT legitimate grow becomes
// a no-op (UsableKib > 0 ≥ new SizeKib).
//
// Previously the handler did a wholesale Decode(&apiv1.VolumeDefinition)
// + Update, which collapsed SizeKib to 0 whenever the body omitted
// the field. Fixed by switching to a modify envelope with optional
// SizeKib pointer and merging into the fetched existing VD.
func TestVolumeDefinitionsUpdateMergeSemanticsPreservesSizeKib(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-keep"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	const initialKib = 1024 * 1024
	if err := st.VolumeDefinitions().Create(ctx, "pvc-keep",
		&apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: initialKib}); err != nil {
		t.Fatalf("seed VD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// Empty JSON object — golinstor's props-only modify would look
	// like this if SizeKib were unset (it has `omitempty`).
	resp := httpPut(t, base+"/v1/resource-definitions/pvc-keep/volume-definitions/0", []byte(`{}`))
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.VolumeDefinitions().Get(ctx, "pvc-keep", 0)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.SizeKib != initialKib {
		t.Errorf("SizeKib after empty-body PUT: got %d, want %d (merge must preserve)", got.SizeKib, initialKib)
	}
}

// TestVolumeDefinitionsUpdateOverridePropsMergesWithExisting pins
// the props-merge half of Bug-36's fix: an `override_props` map in
// the PUT body must overlay onto the existing Props (preserving
// untouched keys), not replace the whole map. Upstream LINSTOR's
// `linstor vd set-property foo=bar` issues a modify with
// override_props={foo:bar} and expects every other key to survive.
func TestVolumeDefinitionsUpdateOverridePropsMergesWithExisting(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-props"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.VolumeDefinitions().Create(ctx, "pvc-props", &apiv1.VolumeDefinition{
		VolumeNumber: 0,
		SizeKib:      1024 * 1024,
		Props: map[string]string{
			"DrbdOptions/Net/protocol": "C",
			"existing-key":             "existing-value",
		},
	}); err != nil {
		t.Fatalf("seed VD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// Send only override_props — no size_kib, no flags.
	resp := httpPut(t,
		base+"/v1/resource-definitions/pvc-props/volume-definitions/0",
		[]byte(`{"override_props":{"new-key":"new-value","existing-key":"updated-value"}}`))
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.VolumeDefinitions().Get(ctx, "pvc-props", 0)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.SizeKib != 1024*1024 {
		t.Errorf("SizeKib drifted across props-only PUT: got %d, want %d", got.SizeKib, 1024*1024)
	}

	if got.Props["DrbdOptions/Net/protocol"] != "C" {
		t.Errorf("untouched prop lost: got %q, want %q", got.Props["DrbdOptions/Net/protocol"], "C")
	}

	if got.Props["existing-key"] != "updated-value" {
		t.Errorf("override prop not applied: got %q, want %q", got.Props["existing-key"], "updated-value")
	}

	if got.Props["new-key"] != "new-value" {
		t.Errorf("new prop missing: got %q, want %q", got.Props["new-key"], "new-value")
	}
}

// TestVolumeDefinitionsUpdateDeletePropsRemovesKey pins the
// delete-side of the props-merge: a `delete_props` list in the body
// drops the named keys from the existing Props map without
// touching others. Driven by `linstor vd set-property foo=` (empty
// value), which golinstor translates into delete_props=[foo].
func TestVolumeDefinitionsUpdateDeletePropsRemovesKey(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-del"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.VolumeDefinitions().Create(ctx, "pvc-del", &apiv1.VolumeDefinition{
		VolumeNumber: 0,
		SizeKib:      1024 * 1024,
		Props: map[string]string{
			"keep-me":   "stay",
			"remove-me": "go",
		},
	}); err != nil {
		t.Fatalf("seed VD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPut(t,
		base+"/v1/resource-definitions/pvc-del/volume-definitions/0",
		[]byte(`{"delete_props":["remove-me"]}`))
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.VolumeDefinitions().Get(ctx, "pvc-del", 0)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.SizeKib != 1024*1024 {
		t.Errorf("SizeKib drifted across delete-props PUT: got %d, want %d", got.SizeKib, 1024*1024)
	}

	if _, found := got.Props["remove-me"]; found {
		t.Errorf("delete_props did not drop key: Props=%v", got.Props)
	}

	if got.Props["keep-me"] != "stay" {
		t.Errorf("delete_props collateral damage: got Props=%v, want keep-me=stay", got.Props)
	}
}

// TestVDUpdateShrinkWithForceEmitsAdvisoryWarning pins the W13
// force-shrink path retains Bug 38's audit-log breadcrumb: when the
// operator opts into the shrink (`"force": true` in the body), the
// envelope still carries the warn-mask advisory entry so the data-
// loss risk is visible in the audit log alongside the success line.
//
// Wire contract: HTTP 200, two-entry `[]ApiCallRc`. Entry 0 is the
// success line. Entry 1 carries the warn-mask bit, the from/to KiB
// values, and the literal token "shrinking" so the Python CLI's
// message-print loop emits it.
func TestVDUpdateShrinkWithForceEmitsAdvisoryWarning(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-shrink-warn"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	const initialKib = 10240
	if err := st.VolumeDefinitions().Create(ctx, "pvc-shrink-warn",
		&apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: initialKib}); err != nil {
		t.Fatalf("seed VD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	const shrunkKib = 5120

	body, _ := json.Marshal(map[string]any{
		"size_kib": shrunkKib,
		"force":    true,
	})

	resp := httpPut(t, base+"/v1/resource-definitions/pvc-shrink-warn/volume-definitions/0", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var rcs []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rcs); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}

	if len(rcs) != 2 {
		t.Fatalf("envelope entries: got %d, want 2 (success + shrink warning); got=%+v", len(rcs), rcs)
	}

	// Entry 0: the canonical success line. golinstor and the Python
	// CLI both dereference replies[0]; flipping the order would break
	// every existing caller.
	if rcs[0].RetCode&maskInfo == 0 {
		t.Errorf("entry 0 ret_code = %x, want maskInfo bit set", rcs[0].RetCode)
	}

	// Entry 1: the shrink advisory. Must carry the warn-mask bit so
	// the contract normalizer classifies it into the <warn> bucket
	// and so the Python CLI prints it as a warning, not as plain info.
	if rcs[1].RetCode&maskWarn == 0 {
		t.Errorf("entry 1 ret_code = %x, want warn-mask bit set", rcs[1].RetCode)
	}

	// Message must mention "shrinking" so an operator grepping the
	// API audit log can find shrink events without decoding ret_code.
	if !strings.Contains(rcs[1].Message, "shrinking") {
		t.Errorf("entry 1 message missing 'shrinking': %q", rcs[1].Message)
	}

	// Both the source and target KiB values must appear so the
	// operator can sanity-check the magnitude of the shrink.
	if !strings.Contains(rcs[1].Message, "10240") {
		t.Errorf("entry 1 message missing source size 10240 KiB: %q", rcs[1].Message)
	}

	if !strings.Contains(rcs[1].Message, "5120") {
		t.Errorf("entry 1 message missing target size 5120 KiB: %q", rcs[1].Message)
	}
}

// TestVDUpdateGrowNoWarning pins the negative case: a SizeKib that
// grows (or equals) the current size must NOT trigger the shrink
// advisory. CSI-resizer's ControllerExpandVolume drives thousands
// of these per hour in a busy cluster; emitting a spurious warning
// would flood the audit log and train operators to ignore the entry.
func TestVDUpdateGrowNoWarning(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-grow-warn"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	const initialKib = 10240
	if err := st.VolumeDefinitions().Create(ctx, "pvc-grow-warn",
		&apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: initialKib}); err != nil {
		t.Fatalf("seed VD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	const grownKib = 20480

	body, _ := json.Marshal(apiv1.VolumeDefinition{SizeKib: grownKib})

	resp := httpPut(t, base+"/v1/resource-definitions/pvc-grow-warn/volume-definitions/0", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var rcs []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rcs); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}

	if len(rcs) != 1 {
		t.Fatalf("envelope entries on grow: got %d, want 1 (success only); got=%+v", len(rcs), rcs)
	}

	if rcs[0].RetCode&maskWarn != 0 {
		t.Errorf("grow path leaked warn-mask bit: ret_code=%x", rcs[0].RetCode)
	}
}

// TestVDUpdateGrowWithUnhealthyPeerDefersNotRejects is the U204/U388
// (P2) regression: a `vd set-size` grow MUST succeed and be stamped on
// every replica EVEN WHEN a peer replica is unhealthy (Inconsistent /
// disconnected). Upstream LINSTOR reports could wedge the volume by
// stalling the grow on the unreachable peer; blockstor deliberately has
// NO peer-health gate on the resize REST path — a DRBD grow with a
// disconnected peer is safe (the reachable peers extend immediately and
// the unhealthy peer resyncs the grown region on reconnect). The resize
// is "deferred" per-replica, never rejected.
//
// This pins the wire-observable half: the PUT returns 200 with a
// success-only envelope (no FAIL_*, no health-related rejection) and the
// resize-pending breadcrumb is stamped on BOTH the healthy and the
// unhealthy replica, so the satellite picks the grow up on each as soon
// as it can reconcile. The live network-partition + reconnect-resync
// half lives in the L6 cell
// tests/e2e/cli-matrix/vd-resize-peer-disconnected-defers.sh.
func TestVDUpdateGrowWithUnhealthyPeerDefersNotRejects(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	const rd = "pvc-unhealthy-peer-grow"
	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rd}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	const initialKib = 1048576 // 1 GiB
	if err := st.VolumeDefinitions().Create(ctx, rd,
		&apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: initialKib}); err != nil {
		t.Fatalf("seed VD: %v", err)
	}

	// Two diskful replicas. n1 is healthy (UpToDate); n2 is UNHEALTHY —
	// its Status reports the volume Inconsistent, the shape a
	// mid-resync or freshly-reconnected peer carries. A naive
	// peer-health gate would reject the grow because not every replica
	// is UpToDate.
	if err := st.Resources().Create(ctx, &apiv1.Resource{Name: rd, NodeName: "n1"}); err != nil {
		t.Fatalf("seed Resource n1: %v", err)
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{Name: rd, NodeName: "n2"}); err != nil {
		t.Fatalf("seed Resource n2: %v", err)
	}

	// Stamp n2 as unhealthy via the Status subresource.
	if err := st.Resources().SetState(ctx, rd, "n2", apiv1.ResourceState{},
		[]apiv1.VolumeObservation{
			{VolumeNumber: 0, State: apiv1.VolumeState{DiskState: "Inconsistent"}},
		}); err != nil {
		t.Fatalf("seed unhealthy status on n2: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	const grownKib = 2097152 // 2 GiB

	body, _ := json.Marshal(apiv1.VolumeDefinition{SizeKib: grownKib})
	resp := httpPut(t, base+"/v1/resource-definitions/"+rd+"/volume-definitions/0", body)
	defer func() { _ = resp.Body.Close() }()

	// 1. The grow must NOT be rejected on peer health.
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("U204/U388: grow with an unhealthy peer returned %d, want 200 "+
			"(blockstor must defer the unhealthy peer, not reject the resize)", resp.StatusCode)
	}

	var rcs []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rcs); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}

	if len(rcs) != 1 {
		t.Fatalf("U204/U388: grow envelope has %d entries, want 1 (success only); got=%+v", len(rcs), rcs)
	}

	if rcs[0].RetCode&apiCallRcError != 0 {
		t.Errorf("U204/U388: grow with unhealthy peer leaked error-mask bit: ret_code=%x", rcs[0].RetCode)
	}

	// 2. The committed VD size must be the grown size.
	vd, err := st.VolumeDefinitions().Get(ctx, rd, 0)
	if err != nil {
		t.Fatalf("Get VD: %v", err)
	}

	if vd.SizeKib != grownKib {
		t.Errorf("U204/U388: VD SizeKib=%d after grow, want %d", vd.SizeKib, grownKib)
	}

	// 3. The resize-pending breadcrumb must be stamped on BOTH replicas
	// — the deferred grow is queued per-replica regardless of health, so
	// the unhealthy n2 picks it up as soon as it can reconcile.
	wantKey := resizePendingAnnotationPrefix + "0"
	for _, node := range []string{"n1", "n2"} {
		rsc, err := st.Resources().Get(ctx, rd, node)
		if err != nil {
			t.Fatalf("Get Resource %s: %v", node, err)
		}

		got := rsc.Annotations[wantKey]
		if got != "2097152" {
			t.Errorf("U204/U388: resize-pending stamp on %s = %q, want %q "+
				"(grow must be deferred to every replica, including the unhealthy one)",
				node, got, "2097152")
		}
	}
}

// TestVDUpdateNoSizeChangeNoWarning pins the "size-omitted" and
// "same-size" cases: a props-only modify, or a no-op resize that
// re-applies the current SizeKib (csi-resizer retry path), must
// NOT emit the shrink advisory. Same-size is upstream's
// WARN_VLMDFN_RESIZE_SAME_SIZE territory — explicitly NOT a shrink
// warning. Without this guard, csi-resizer's idempotent retry on
// every reconcile pass would synthesise a stream of spurious shrink
// warnings whenever a resize raced a controller restart.
func TestVDUpdateNoSizeChangeNoWarning(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-nosize"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	const initialKib = 10240
	if err := st.VolumeDefinitions().Create(ctx, "pvc-nosize",
		&apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: initialKib}); err != nil {
		t.Fatalf("seed VD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// Case A: size_kib omitted entirely (props-only modify).
	respA := httpPut(t,
		base+"/v1/resource-definitions/pvc-nosize/volume-definitions/0",
		[]byte(`{}`))

	if respA.StatusCode != http.StatusOK {
		_ = respA.Body.Close()
		t.Fatalf("case A status: got %d, want 200", respA.StatusCode)
	}

	var rcsA []apiv1.APICallRc
	if err := json.NewDecoder(respA.Body).Decode(&rcsA); err != nil {
		_ = respA.Body.Close()
		t.Fatalf("case A decode envelope: %v", err)
	}

	_ = respA.Body.Close()

	if len(rcsA) != 1 {
		t.Fatalf("case A: omitted size_kib produced %d entries, want 1; got=%+v", len(rcsA), rcsA)
	}

	// Case B: size_kib equal to the current value (csi-resizer retry).
	body, _ := json.Marshal(apiv1.VolumeDefinition{SizeKib: initialKib})

	respB := httpPut(t, base+"/v1/resource-definitions/pvc-nosize/volume-definitions/0", body)
	if respB.StatusCode != http.StatusOK {
		_ = respB.Body.Close()
		t.Fatalf("case B status: got %d, want 200", respB.StatusCode)
	}

	var rcsB []apiv1.APICallRc
	if err := json.NewDecoder(respB.Body).Decode(&rcsB); err != nil {
		_ = respB.Body.Close()
		t.Fatalf("case B decode envelope: %v", err)
	}

	_ = respB.Body.Close()

	if len(rcsB) != 1 {
		t.Fatalf("case B: same-size size_kib produced %d entries, want 1; got=%+v", len(rcsB), rcsB)
	}

	if rcsB[0].RetCode&maskWarn != 0 {
		t.Errorf("case B leaked warn-mask bit on no-op: ret_code=%x", rcsB[0].RetCode)
	}
}

// TestVDDeleteUnknownReturns200Warning pins the Bug 66 idempotence
// contract for `DELETE /v1/resource-definitions/{rd}/volume-
// definitions/{vn}`. Two NotFound shapes are covered:
//
//   - sub-test "missing-vd": parent RD exists, vlmNr is absent
//   - sub-test "missing-rd": parent RD itself is absent
//
// Both must fold into the same 200 + WARN + "already absent" envelope
// so linstor-csi's expand/shrink retry loops stay exit-0 on no-op
// replays and the python CLI's XML decoder fallback isn't tripped.
func TestVDDeleteUnknownReturns200Warning(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		seed func(t *testing.T, st store.Store)
		path string
	}{
		{
			name: "missing-vd",
			seed: func(t *testing.T, st store.Store) {
				t.Helper()

				if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-vd"}); err != nil {
					t.Fatalf("seed RD: %v", err)
				}
			},
			path: "/v1/resource-definitions/pvc-vd/volume-definitions/42",
		},
		{
			name: "missing-rd",
			seed: func(_ *testing.T, _ store.Store) {},
			path: "/v1/resource-definitions/ghost-rd/volume-definitions/0",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			st := store.NewInMemory()
			tc.seed(t, st)

			base, stop := startServerWithStore(t, st)
			defer stop()

			resp := httpDelete(t, base+tc.path)
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status: got %d, want 200", resp.StatusCode)
			}

			var rc []apiv1.APICallRc
			if err := json.NewDecoder(resp.Body).Decode(&rc); err != nil {
				t.Fatalf("decode ApiCallRc envelope: %v", err)
			}

			if len(rc) == 0 {
				t.Fatalf("ApiCallRc envelope: got empty, want one entry")
			}

			if rc[0].RetCode&maskWarn == 0 {
				t.Errorf("ret_code: got %#x, want WARN bit (%#x) set", rc[0].RetCode, maskWarn)
			}

			if !strings.Contains(rc[0].Message, "already absent") {
				t.Errorf("message: got %q, want 'already absent' marker", rc[0].Message)
			}
		})
	}
}
