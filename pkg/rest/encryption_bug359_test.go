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

// Bug 359 — PATCH /v1/encryption/passphrase (enter) happy-path used
// to call `WriteHeader(200)` with NO body. python-linstor / golinstor
// unconditionally json-decode every non-204 2xx response, so
// `linstor encryption enter-passphrase` crashed with:
//
//     Unable to parse REST json data: Expecting value: line 1 column
//     1 (char 0)
//
// This is the exact failure mode Bug 129 already fixed on the POST
// (create) sibling; the PATCH (enter) handler was the last write-side
// encryption endpoint still emitting a bare status with no envelope,
// despite the Bug 129 comment claiming otherwise. These tests pin the
// happy-path 200 to a non-empty `[]APICallRc` envelope.

package rest

import (
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/cozystack/blockstor/pkg/store"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// TestBug359EncryptionEnterReturnsEnvelope pins that PATCH
// /v1/encryption/passphrase on the happy path (correct proof-of-
// knowledge) returns 200 OK with a non-empty `[]APICallRc` envelope
// carrying MASK_INFO + a non-empty message, matching the create/modify
// siblings.
func TestBug359EncryptionEnterReturnsEnvelope(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	// Establish the cluster passphrase first (POST/create).
	createBody, _ := json.Marshal(map[string]string{"new_passphrase": "secret"})
	createResp := httpPost(t, base+"/v1/encryption/passphrase", createBody)
	_ = createResp.Body.Close()
	if createResp.StatusCode != http.StatusCreated {
		t.Fatalf("create status: got %d, want 201", createResp.StatusCode)
	}

	// Enter with the correct passphrase (PATCH/enter).
	enterBody, _ := json.Marshal(map[string]string{"passphrase": "secret"})
	resp := httpPatch(t, base+"/v1/encryption/passphrase", enterBody)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("enter status: got %d, want 200", resp.StatusCode)
	}

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	if len(raw) == 0 {
		t.Fatalf("response body empty; python-linstor will crash json-decoding it (Bug 359)")
	}

	var rcs []apiv1.APICallRc
	if err := json.Unmarshal(raw, &rcs); err != nil {
		t.Fatalf("decode []APICallRc envelope: %v (body=%q)", err, string(raw))
	}

	if len(rcs) == 0 {
		t.Fatalf("envelope []APICallRc empty; want at least one entry (body=%q)", string(raw))
	}

	if rcs[0].RetCode&maskInfo == 0 {
		t.Errorf("ret_code: got %#x, want MASK_INFO bit (%#x) set", rcs[0].RetCode, maskInfo)
	}

	if rcs[0].Message == "" {
		t.Errorf("envelope entry has empty message; operator-visible CLI line would render blank")
	}
}

// TestBug359PythonCLIDoesNotCrashOnEnter mirrors python-linstor`s
// response.json() on the enter happy path: read the entire body, then
// json.loads it into a list. Empty/non-JSON → CLI traceback.
func TestBug359PythonCLIDoesNotCrashOnEnter(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	createBody, _ := json.Marshal(map[string]string{"new_passphrase": "secret"})
	createResp := httpPost(t, base+"/v1/encryption/passphrase", createBody)
	_ = createResp.Body.Close()

	enterBody, _ := json.Marshal(map[string]string{"passphrase": "secret"})
	resp := httpPatch(t, base+"/v1/encryption/passphrase", enterBody)
	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	if len(raw) == 0 {
		t.Fatalf("body empty; python-linstor json.loads would raise (Bug 359)")
	}

	var parsed any
	if err := json.Unmarshal(raw, &parsed); err != nil {
		t.Fatalf("json.Unmarshal (python json.loads equivalent): %v (body=%q)", err, string(raw))
	}

	asList, ok := parsed.([]any)
	if !ok {
		t.Fatalf("body decoded as %T; python-linstor iterates a list of ApiCallRc (body=%q)", parsed, string(raw))
	}

	if len(asList) == 0 {
		t.Fatalf("decoded list empty; python-linstor would print nothing (body=%q)", string(raw))
	}
}
