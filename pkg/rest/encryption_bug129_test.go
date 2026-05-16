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

// Bug 129 — regression of Bug 110: POST /v1/encryption/passphrase
// happy-path used to write `WriteHeader(201)` with NO body. python-
// linstor 1.27.1 unconditionally calls `response.json()` on every
// non-204 2xx response, so an empty body produces:
//
//     Unable to parse REST json data: Expecting value: line 1 column
//     1 (char 0)
//
// and the CLI prints a traceback. Bug 110 already fixed the same
// shape on PATCH (Enter) and PUT (Modify); the create handler was
// the only sibling left without an envelope.
//
// These tests pin the wire-shape contract on the happy-path 201:
// the response body MUST decode as a non-empty `[]APICallRc`
// envelope so python-linstor renders a clean "created" line.

package rest

import (
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/cozystack/blockstor/pkg/store"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// TestBug129EncryptionCreateReturnsEnvelope pins that POST
// /v1/encryption/passphrase on the happy path (fresh cluster, no
// passphrase yet) returns 201 Created with a non-empty
// `[]APICallRc` envelope carrying MASK_INFO + a "created"-style
// message. Mirrors the shape Bug 110 already established for the
// PATCH/PUT siblings (handlePassphraseEnter / handlePassphraseModify).
func TestBug129EncryptionCreateReturnsEnvelope(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	body, _ := json.Marshal(map[string]string{"new_passphrase": "secret"})

	resp := httpPost(t, base+"/v1/encryption/passphrase", body)

	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201", resp.StatusCode)
	}

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	if len(raw) == 0 {
		t.Fatalf("response body empty; python-linstor will crash json-decoding it (Bug 129)")
	}

	var rcs []apiv1.APICallRc

	if err := json.Unmarshal(raw, &rcs); err != nil {
		t.Fatalf("decode []APICallRc envelope: %v (body=%q)", err, string(raw))
	}

	if len(rcs) == 0 {
		t.Fatalf("envelope []APICallRc empty; want at least one entry (body=%q)", string(raw))
	}

	// MASK_INFO bit must be set so python-linstor renders the line
	// as an informational success, not as a warning/error. The
	// PATCH/PUT siblings already do this; matching the convention
	// keeps the CLI output uniform across create/enter/modify.
	if rcs[0].RetCode&maskInfo == 0 {
		t.Errorf("ret_code: got %#x, want MASK_INFO bit (%#x) set", rcs[0].RetCode, maskInfo)
	}

	if rcs[0].Message == "" {
		t.Errorf("envelope entry has empty message; operator-visible CLI line would render blank")
	}
}

// TestBug129PythonCLIDoesNotCrashOnCreate mirrors what python-linstor's
// `response.json()` does on every non-204 2xx: read the entire body,
// then `json.loads(body)`. If the body is empty or non-JSON, the CLI
// surfaces a traceback. This test feeds the response body through the
// same shape of decode (`json.Unmarshal` into `any`) to confirm a real
// JSON document comes out.
func TestBug129PythonCLIDoesNotCrashOnCreate(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	body, _ := json.Marshal(map[string]string{"new_passphrase": "secret"})

	resp := httpPost(t, base+"/v1/encryption/passphrase", body)

	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	// python-linstor's failure mode: empty bytes → json.loads
	// raises ValueError "Expecting value: line 1 column 1 (char 0)".
	if len(raw) == 0 {
		t.Fatalf("body empty; python-linstor json.loads would raise (Bug 129)")
	}

	// python-linstor on this endpoint expects a list (it iterates
	// the parsed payload to render one CLI line per ApiCallRc).
	// Decode into `any` first to mirror json.loads, then assert
	// the document is a list.
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
