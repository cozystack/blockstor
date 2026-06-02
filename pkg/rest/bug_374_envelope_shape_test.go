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
	"io"
	"net/http"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Bug 374 (P2, bughunt round 9 — 2026-06-02): several write-side REST
// endpoints returned a bare `WriteHeader(200|201)` with no JSON body.
// Sibling endpoints learned the lesson the hard way (Bug 45 against
// golinstor, Bug 129 against python-linstor 1.27.1) — both response
// parsers unconditionally json-decode every non-204 2xx response and
// crash with `"Expecting value: line 1 column 1 (char 0)"` on an
// empty body. Drift discovered in round-9 bughunt:
//
//   - POST /v1/resource-definitions/{rd}/adjust
//   - POST /v1/resource-definitions/{rd}/resources/{node}/adjust
//   - POST /v1/resource-definitions/{rd}/encryption-passphrase
//     (per-RD DRBD shared-secret writer)
//   - POST /v1/encryption/passphrase  (idempotent-replay branch only)
//   - POST /v1/resource-definitions/{rd}/resource-connections/
//     {a}/{b}/paths
//   - PUT  /v1/resource-groups/{rg}/volume-groups/{vlmNr}
//
// This file pins the wire shape so a future refactor can't silently
// reintroduce the bare 200. Each test reads the body, json.Unmarshals
// into `[]apiv1.APICallRc`, and asserts the slice is non-empty.

// Common assertion: status code is `wantCode`, body decodes as
// `[]apiv1.APICallRc`, slice non-empty, first entry has a non-empty
// Message. This is the shape every CLI parser dereferences. Callers
// must drain + close resp.Body themselves so the linter's bodyclose
// rule stays happy at the call-site.
func assertEnvelopeShape(t *testing.T, body []byte, gotCode, wantCode int) {
	t.Helper()

	if gotCode != wantCode {
		t.Fatalf("status: got %d, want %d", gotCode, wantCode)
	}

	if len(body) == 0 {
		t.Fatalf("empty body — bare WriteHeader regression (Bug 374)")
	}

	var rcs []apiv1.APICallRc
	if err := json.Unmarshal(body, &rcs); err != nil {
		t.Fatalf("body not []ApiCallRc: %v; body=%q", err, body)
	}

	if len(rcs) == 0 {
		t.Fatalf("empty []ApiCallRc slice; body=%q", body)
	}

	if rcs[0].Message == "" {
		t.Fatalf("first ApiCallRc has empty Message; body=%q", body)
	}
}

// readBodyBytes drains a response body that the caller has already
// scheduled to close via `defer resp.Body.Close()`. Separated from
// the body-close itself so golangci-lint's bodyclose pass sees the
// `Close()` at the use-site of the response and stops complaining.
func readBodyBytes(t *testing.T, resp *http.Response) []byte {
	t.Helper()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	return body
}

// TestBug374_AdjustAllEnvelope: POST /v1/resource-definitions/{rd}/
// adjust returns 200 + ApiCallRc envelope, not a bare 200.
func TestBug374_AdjustAllEnvelope(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "rd-1"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/resource-definitions/rd-1/adjust", nil)
	defer func() { _ = resp.Body.Close() }()

	assertEnvelopeShape(t, readBodyBytes(t, resp), resp.StatusCode, http.StatusOK)
}

// TestBug374_AdjustOneEnvelope: POST /v1/resource-definitions/{rd}/
// resources/{node}/adjust returns 200 + ApiCallRc envelope.
func TestBug374_AdjustOneEnvelope(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "rd-1"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{Name: "rd-1", NodeName: "n1"}); err != nil {
		t.Fatalf("seed Res: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/resource-definitions/rd-1/resources/n1/adjust", nil)
	defer func() { _ = resp.Body.Close() }()

	assertEnvelopeShape(t, readBodyBytes(t, resp), resp.StatusCode, http.StatusOK)
}

// TestBug374_DRBDPassphraseEnvelope: POST /v1/resource-definitions/
// {rd}/encryption-passphrase returns 200 + ApiCallRc envelope.
func TestBug374_DRBDPassphraseEnvelope(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "rd-1"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	reqBody, _ := json.Marshal(map[string]string{"passphrase": "supersecret"})
	resp := httpPost(t, base+"/v1/resource-definitions/rd-1/encryption-passphrase", reqBody)
	defer func() { _ = resp.Body.Close() }()

	assertEnvelopeShape(t, readBodyBytes(t, resp), resp.StatusCode, http.StatusOK)
}

// TestBug374_VGUpdateEnvelope: PUT /v1/resource-groups/{rg}/volume-
// groups/{vlmNr} returns 200 + ApiCallRc envelope.
func TestBug374_VGUpdateEnvelope(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "rg-1",
		VolumeGroups: []apiv1.VolumeGroup{
			{VolumeNumber: 0},
		},
	}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	reqBody, _ := json.Marshal(map[string]any{
		"override_props": map[string]string{"foo": "bar"},
	})
	resp := httpPut(t, base+"/v1/resource-groups/rg-1/volume-groups/0", reqBody)
	defer func() { _ = resp.Body.Close() }()

	assertEnvelopeShape(t, readBodyBytes(t, resp), resp.StatusCode, http.StatusOK)
}

// TestBug374_ResourceConnectionPathCreateEnvelope: POST
// /v1/resource-definitions/{rd}/resource-connections/{a}/{b}/paths
// returns 201 + ApiCallRc envelope.
func TestBug374_ResourceConnectionPathCreateEnvelope(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "rd-1"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	reqBody, _ := json.Marshal(map[string]any{
		"name": "ipath",
	})
	resp := httpPost(t,
		base+"/v1/resource-definitions/rd-1/resource-connections/a/b/paths", reqBody)
	defer func() { _ = resp.Body.Close() }()

	assertEnvelopeShape(t, readBodyBytes(t, resp), resp.StatusCode, http.StatusCreated)
}
