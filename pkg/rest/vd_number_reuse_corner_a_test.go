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
	"context"
	"encoding/json"
	"net/http"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Corner-case campaign item A2b/A3 — volume-number allocation semantics.
//
// The upstream LINSTOR controller (CtrlVlmDfnCrtApiCallHandler) assigns
// the SMALLEST FREE volume number on an auto-assign `vd c` — it does NOT
// append max+1. Two operator-observable consequences fall out of that
// rule and were verified identical against the upstream Java oracle on
// the shared stand (piraeus-server v1.33.2):
//
//   A2b — reuse after delete: rd with vols 0+1, `vd d 0`, then a plain
//         `vd c` lands at the freed 0 (NOT 2). Affects the rendered
//         `.res` volume{} block ordering and the device minor.
//
//   A3  — explicit-then-plain: `vd c --vlmnr 5` first, then a plain
//         `vd c` lands at 0 (the smallest free), NOT 6.
//
// Bug 191's TestBug191VDCreateAutoAssignFillsGaps already pins the
// pre-seeded-gap variant; these two cases drive the allocation through
// the live DELETE + explicit-create handler paths so a future change to
// autoAssignVolumeNumber (e.g. a regression to max+1) is caught at the
// operator-observable wire boundary, not just at the unit helper.

// TestCornerA2bVolumeNumberReusedAfterDelete drives the exact A2b
// operator sequence end to end through the REST handlers: create VD 0
// and VD 1, DELETE VD 0, then POST a plain `vd c` (no volume_number).
// The freed VlmNr=0 MUST be re-assigned — matching upstream's
// smallest-free rule — not VlmNr=2 (max+1).
func TestCornerA2bVolumeNumberReusedAfterDelete(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	const rdName = "pvc-corner-a2b-reuse"

	err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rdName})
	if err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	vdURL := base + "/v1/resource-definitions/" + rdName + "/volume-definitions"

	// Seed vols 0 and 1 via the auto-assign create path (no
	// volume_number key) so the allocator picks 0 then 1.
	for i := range 2 {
		resp := httpPost(t, vdURL, []byte(`{"volume_definition":{"size_kib":32768}}`))
		_ = resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("seed vd #%d POST status: got %d, want 200", i, resp.StatusCode)
		}
	}

	assertVDNumbers(t, ctx, st, rdName, []int32{0, 1})

	// Delete VD 0 — freeing the lowest number.
	delResp := httpDelete(t, vdURL+"/0")
	_ = delResp.Body.Close()

	if delResp.StatusCode != http.StatusOK {
		t.Fatalf("vd d 0 status: got %d, want 200", delResp.StatusCode)
	}

	assertVDNumbers(t, ctx, st, rdName, []int32{1})

	// Plain `vd c` (no volume_number) — MUST reuse the freed 0, not
	// append 2.
	reuseResp := httpPost(t, vdURL, []byte(`{"volume_definition":{"size_kib":32768}}`))
	_ = reuseResp.Body.Close()

	if reuseResp.StatusCode != http.StatusOK {
		t.Fatalf("reuse vd c POST status: got %d, want 200", reuseResp.StatusCode)
	}

	// The defining assertion: the re-added volume took the freed 0.
	assertVDNumbers(t, ctx, st, rdName, []int32{0, 1})
}

// TestCornerA3ExplicitThenPlainGetsSmallestFree drives the A3 sequence:
// `vd c --vlmnr 5` first (explicit), then a plain `vd c`. The plain
// create MUST land at 0 (smallest free), NOT 6 (max+1). Pins that the
// explicit-create path does not poison the auto-assign allocator into a
// high-water-mark mode.
func TestCornerA3ExplicitThenPlainGetsSmallestFree(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	const rdName = "pvc-corner-a3-explicit"

	err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rdName})
	if err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	vdURL := base + "/v1/resource-definitions/" + rdName + "/volume-definitions"

	// First create with an explicit high volume_number.
	explicitBody, _ := json.Marshal(apiv1.VolumeDefinitionCreate{
		VolumeDefinition: apiv1.VolumeDefinition{VolumeNumber: 5, SizeKib: 32 * 1024},
	})

	explicitResp := httpPost(t, vdURL, explicitBody)
	_ = explicitResp.Body.Close()

	if explicitResp.StatusCode != http.StatusOK {
		t.Fatalf("explicit vd c --vlmnr 5 status: got %d, want 200", explicitResp.StatusCode)
	}

	assertVDNumbers(t, ctx, st, rdName, []int32{5})

	// Plain `vd c` — smallest free is 0, NOT 6.
	plainResp := httpPost(t, vdURL, []byte(`{"volume_definition":{"size_kib":32768}}`))
	_ = plainResp.Body.Close()

	if plainResp.StatusCode != http.StatusOK {
		t.Fatalf("plain vd c status: got %d, want 200", plainResp.StatusCode)
	}

	// 0 and 5 present; 6 absent — the smallest-free rule held.
	assertVDNumbers(t, ctx, st, rdName, []int32{0, 5})
}

// assertVDNumbers fails the test unless the VD set under rd is EXACTLY
// the want list (order-independent). Centralised so the corner-case
// allocation assertions share one diagnostic shape.
func assertVDNumbers(t *testing.T, ctx context.Context, st store.Store, rd string, want []int32) {
	t.Helper()

	vds, err := st.VolumeDefinitions().List(ctx, rd)
	if err != nil {
		t.Fatalf("VD list: %v", err)
	}

	have := map[int32]bool{}
	for i := range vds {
		have[vds[i].VolumeNumber] = true
	}

	if len(have) != len(want) {
		t.Fatalf("VD count: got %v, want %v", vdNums(vds), want)
	}

	for _, w := range want {
		if !have[w] {
			t.Fatalf("missing VlmNr=%d; have=%v want=%v", w, vdNums(vds), want)
		}
	}
}
