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
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Bug 367 / 361 (bug-hunt v6, 2026-06-02): POST /v1/resource-groups
// with select_filter.place_count=-3 returned HTTP 201 and silently
// produced a corrupted RG; `rg spawn-resources` against it later
// produced an RD with zero replicas. PUT /v1/resource-groups/<rg>
// with place_count=-5 went one worse: HTTP 200 with "rebalance
// scheduled" — the scheduler kicked off against a negative target.
//
// Fix: validateRGSelectFilterPlaceCount enforces [0, 1_000_000] on
// both POST (handleRGCreate, eager) and PUT (handleRGUpdate, on the
// patch body). place_count=0 stays accepted: upstream linstor-client
// documents `--place-count 0` as "remove all replicas", so the
// scale-to-zero workflow must keep working. Negative is always
// silent corruption — no semantic.

// TestBug367RGCreateRefusesNegativePlaceCount pins the canonical
// POST reproducer.
func TestBug367RGCreateRefusesNegativePlaceCount(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(map[string]any{
		"name": "rg-neg",
		"select_filter": map[string]any{
			"place_count": -3,
		},
	})

	resp := httpPost(t, base+"/v1/resource-groups", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		got, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 400 (Bug 367 negative place_count on POST). Body: %s",
			resp.StatusCode, got)
	}

	got, _ := readAllBody(resp)
	if !strings.Contains(string(got), "place_count -3 is negative") {
		t.Errorf("envelope missing offending value: %s", got)
	}

	// No phantom RG must land.
	if _, err := st.ResourceGroups().Get(t.Context(), "rg-neg"); err == nil {
		t.Errorf("RG rg-neg persisted after rejection — must not")
	}
}

// TestBug367RGCreateRefusesIntMinPlaceCount pins the boundary —
// even INT32_MIN must not slip through (fuzz protection).
func TestBug367RGCreateRefusesIntMinPlaceCount(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(map[string]any{
		"name": "rg-intmin",
		"select_filter": map[string]any{
			"place_count": -2147483648,
		},
	})

	resp := httpPost(t, base+"/v1/resource-groups", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		got, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 400. Body: %s", resp.StatusCode, got)
	}
}

// TestBug367RGCreateRefusesAbsurdPlaceCount pins the sanity ceiling
// (1_000_000) — fuzzed INT32_MAX must not minted an RG the scheduler
// would later iterate against.
func TestBug367RGCreateRefusesAbsurdPlaceCount(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(map[string]any{
		"name": "rg-huge",
		"select_filter": map[string]any{
			"place_count": 2147483647,
		},
	})

	resp := httpPost(t, base+"/v1/resource-groups", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		got, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 400. Body: %s", resp.StatusCode, got)
	}

	got, _ := readAllBody(resp)
	if !strings.Contains(string(got), "sanity ceiling") {
		t.Errorf("envelope missing sanity-ceiling hint: %s", got)
	}
}

// TestBug367RGCreateZeroPlaceCountStillAccepted pins the
// upstream-LINSTOR-compat behaviour: `--place-count 0` is the
// documented way to "remove all replicas", so 0 must be accepted on
// both POST and PUT.
func TestBug367RGCreateZeroPlaceCountStillAccepted(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(map[string]any{
		"name": "rg-zero",
		"select_filter": map[string]any{
			"place_count": 0,
		},
	})

	resp := httpPost(t, base+"/v1/resource-groups", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		got, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 201 (place_count=0 must be accepted). Body: %s",
			resp.StatusCode, got)
	}
}

// TestBug367RGCreatePositivePlaceCountStillAccepted pins the
// healthy-path: a normal --place-count 3 must still succeed.
func TestBug367RGCreatePositivePlaceCountStillAccepted(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(map[string]any{
		"name": "rg-three",
		"select_filter": map[string]any{
			"place_count":  3,
			"storage_pool": "zfs-thin",
		},
	})

	resp := httpPost(t, base+"/v1/resource-groups", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		got, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 201. Body: %s", resp.StatusCode, got)
	}
}

// TestBug367RGUpdateRefusesNegativePlaceCount pins the PUT
// reproducer — the scarier of the two because the pre-fix path
// scheduled a rebalance against a negative target on a real RG.
func TestBug367RGUpdateRefusesNegativePlaceCount(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	// Seed a healthy RG.
	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "rg-put",
		SelectFilter: apiv1.AutoSelectFilter{
			PlaceCount:  3,
			StoragePool: "zfs-thin",
		},
	}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(map[string]any{
		"select_filter": map[string]any{
			"place_count": -5,
		},
	})

	req, _ := http.NewRequestWithContext(ctx, http.MethodPut, base+"/v1/resource-groups/rg-put", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		got, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 400 (Bug 367 negative place_count on PUT). Body: %s",
			resp.StatusCode, got)
	}

	got, _ := readAllBody(resp)
	if !strings.Contains(string(got), "place_count -5 is negative") {
		t.Errorf("envelope missing offending value: %s", got)
	}

	// Crucially: persisted PlaceCount must be UNCHANGED. The PUT
	// path runs the gate BEFORE PatchResourceGroup, so a rejected
	// update must not even reach the store.
	stored, err := st.ResourceGroups().Get(ctx, "rg-put")
	if err != nil {
		t.Fatalf("re-fetch RG: %v", err)
	}

	if got, want := stored.SelectFilter.PlaceCount, apiv1.LaxInt32(3); got != want {
		t.Errorf("stored PlaceCount: got %d, want %d (must not persist after rejection)", got, want)
	}
}

// TestBug367RGUpdateZeroPlaceCountStillAccepted pins the
// upstream-LINSTOR-compat behaviour on PUT: scale-to-zero stays
// allowed (the existing rebalance hook already turns it into a
// resource-deletion campaign — that's the intended workflow per
// upstream linstor-client docs).
func TestBug367RGUpdateZeroPlaceCountStillAccepted(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name:         "rg-toscale-down",
		SelectFilter: apiv1.AutoSelectFilter{PlaceCount: 3},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(map[string]any{
		"select_filter": map[string]any{
			"place_count": 0,
		},
	})

	req, _ := http.NewRequestWithContext(ctx, http.MethodPut, base+"/v1/resource-groups/rg-toscale-down", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		got, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 200 (place_count=0 must be accepted on PUT). Body: %s",
			resp.StatusCode, got)
	}
}

// TestBug367RGUpdateAbsentPlaceCountUntouched pins the "field
// absent = leave alone" invariant: a PUT body that doesn't mention
// place_count must NOT fire the gate (otherwise every cli `rg
// modify --description foo` would suddenly require place_count to
// be repeated).
func TestBug367RGUpdateAbsentPlaceCountUntouched(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name:         "rg-untouched",
		SelectFilter: apiv1.AutoSelectFilter{PlaceCount: 3},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(map[string]any{
		"description": "operator note",
	})

	req, _ := http.NewRequestWithContext(ctx, http.MethodPut, base+"/v1/resource-groups/rg-untouched", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		got, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 200 (absent place_count must NOT fire the gate). Body: %s",
			resp.StatusCode, got)
	}

	stored, err := st.ResourceGroups().Get(ctx, "rg-untouched")
	if err != nil {
		t.Fatalf("re-fetch: %v", err)
	}

	if got, want := stored.SelectFilter.PlaceCount, apiv1.LaxInt32(3); got != want {
		t.Errorf("PlaceCount changed after no-op patch: got %d, want %d", got, want)
	}
}
