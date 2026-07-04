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
	"bytes"
	"encoding/json"
	"net/http"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Bug 434 (P2 correctness+availability): the RD/RG CREATE paths validate
// the layer stack (validateLayerStack: allowlist {DRBD,LUKS,STORAGE},
// DRBD first, STORAGE terminal), but two sibling paths bypassed it:
//
//  1. `rg modify` (handleRGUpdate) gated only place_count — an invalid
//     select_filter.layer_stack was merged and persisted unvalidated.
//  2. handleRDCreate ran validateRDCreateBody (which validates an
//     EXPLICIT layer_list) BEFORE inheritLayerStackFromRG, so an RD
//     created against such an RG inherited the invalid stack unchecked.
//
// Net: an invalid, unmaterialisable layer stack the DIRECT create path
// refuses (400) reached a persisted RD spec via the rg-modify → inherit
// chain. Same asymmetry class as the resize-bounds bypass (create
// validates; a sibling modify/inherit path bypasses).
//
// Fix: (1) validate the layer stack in the rg-update wire gate, and
// (2) defense-in-depth re-validate the resolved (possibly inherited)
// stack in handleRDCreate after the RG inherit.
//
// These FAIL on the pre-fix tree (the invalid stack is accepted /
// inherited) and PASS with the two gates.

// bug434InvalidStack is STORAGE-before-DRBD: STORAGE must be terminal and
// DRBD must be first, so this is refused by the create path.
var bug434InvalidStack = []string{"STORAGE", "DRBD"} //nolint:gochecknoglobals // shared test fixture

// TestBug434RGUpdateRefusesInvalidLayerStack — FAIL-on-bug (Fix 1).
//
// `rg modify` with an invalid select_filter.layer_stack must be rejected
// with the same 400 the create path returns, and the stored RG must keep
// its valid stack (the gate runs BEFORE PatchResourceGroup).
func TestBug434RGUpdateRefusesInvalidLayerStack(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name:         "rg-b434",
		SelectFilter: apiv1.AutoSelectFilter{LayerStack: []string{"DRBD", "STORAGE"}},
	}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(map[string]any{
		"select_filter": map[string]any{"layer_stack": bug434InvalidStack},
	})

	req, _ := http.NewRequestWithContext(ctx, http.MethodPut, base+"/v1/resource-groups/rg-b434", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		got, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 400 (Bug 434: rg modify must refuse an invalid layer_stack). Body: %s",
			resp.StatusCode, got)
	}

	// The rejected update must not reach the store: the RG keeps its
	// valid stack.
	stored, err := st.ResourceGroups().Get(ctx, "rg-b434")
	if err != nil {
		t.Fatalf("re-fetch RG: %v", err)
	}

	if got := stored.SelectFilter.LayerStack; len(got) != 2 || got[0] != "DRBD" || got[1] != "STORAGE" {
		t.Errorf("stored LayerStack changed after rejection: got %v, want [DRBD STORAGE]", got)
	}
}

// TestBug434RGUpdateValidLayerStackAccepted pins the healthy path: a
// valid re-order still lands.
func TestBug434RGUpdateValidLayerStackAccepted(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name:         "rg-b434-ok",
		SelectFilter: apiv1.AutoSelectFilter{LayerStack: []string{"DRBD", "STORAGE"}},
	}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(map[string]any{
		"select_filter": map[string]any{"layer_stack": []string{"DRBD", "LUKS", "STORAGE"}},
	})

	req, _ := http.NewRequestWithContext(ctx, http.MethodPut, base+"/v1/resource-groups/rg-b434-ok", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		got, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 200 (valid layer_stack must be accepted). Body: %s",
			resp.StatusCode, got)
	}
}

// TestBug434RGUpdateAbsentLayerStackUntouched pins "field absent = leave
// alone": a PUT that doesn't mention layer_stack must NOT fire the gate.
func TestBug434RGUpdateAbsentLayerStackUntouched(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name:         "rg-b434-absent",
		SelectFilter: apiv1.AutoSelectFilter{LayerStack: []string{"DRBD", "STORAGE"}},
	}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(map[string]any{"description": "operator note"})

	req, _ := http.NewRequestWithContext(ctx, http.MethodPut, base+"/v1/resource-groups/rg-b434-absent", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		got, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 200 (absent layer_stack must NOT fire the gate). Body: %s",
			resp.StatusCode, got)
	}
}

// TestBug434RDCreateRefusesInheritedInvalidLayerStack — FAIL-on-bug (Fix 2).
//
// Even if an RG already carries an invalid layer stack (seeded directly
// here to model an RG that predates the rg-modify gate, or was persisted
// by any other path), an RD created against it — inheriting the stack
// with no explicit layer_list — must be refused, and no RD may persist.
// handleRDCreate validates the EXPLICIT body before the RG inherit, so
// without the post-inherit re-validation the invalid stack reaches the RD.
func TestBug434RDCreateRefusesInheritedInvalidLayerStack(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	// Seed an RG that ALREADY holds the invalid stack (store Create does
	// not validate — validation lives in the REST handlers).
	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name:         "rg-b434-bad",
		SelectFilter: apiv1.AutoSelectFilter{LayerStack: bug434InvalidStack},
	}); err != nil {
		t.Fatalf("seed bad RG: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(map[string]any{
		"resource_definition": map[string]any{
			"name":                "rd-b434-inherit",
			"resource_group_name": "rg-b434-bad",
		},
	})

	resp := httpPost(t, base+"/v1/resource-definitions", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		got, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 400 (Bug 434: RD must not inherit an invalid layer stack). Body: %s",
			resp.StatusCode, got)
	}

	// No RD may persist with the invalid inherited stack.
	if _, err := st.ResourceDefinitions().Get(ctx, "rd-b434-inherit"); err == nil {
		t.Errorf("RD rd-b434-inherit persisted after rejection — must not (invalid inherited layer stack)")
	}
}
