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

// Bug 165 — asymmetric field-name handling on /v1/encryption/passphrase.
// POST (create) ONLY accepts `{"new_passphrase":"…"}`; PATCH (enter)
// accepts BOTH `{"new_passphrase":"…"}` AND `{"passphrase":"…"}` via
// passphraseRequest.proofOfKnowledge. Operators driving the apiserver
// with `--curl` or hand-rolled wire-shape scripts hit a 400 on POST
// with `passphrase`, then succeed on PATCH with the same body — the
// asymmetry is confusing and undocumented.
//
// Upstream LINSTOR's canonical create field is `new_passphrase`;
// `passphrase` is the alias the W13 CLI shape uses on enter-passphrase.
// We accept both consistently on POST + PATCH + PUT so the wire surface
// is symmetric across all three encryption verbs.
//
// These tests pin:
//   - POST with the alias `{"passphrase":"…"}` → 201 (parity with PATCH).
//   - POST with canonical `{"new_passphrase":"…"}` → 201 (regression guard).
//   - PATCH/PUT continue to accept both field names (contract guard,
//     so a future "tighten POST → simplify the struct" refactor can't
//     silently break the dual-key Bug 110/W13 wire surface).
//   - POST with empty body `{}` → 400 + standard `[]APICallRc` envelope.

package rest

import (
	"encoding/json"
	"io"
	"net/http"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"

	"github.com/cozystack/blockstor/pkg/store"
)

// TestBug165PostAcceptsPassphraseAlias pins that POST with the alias
// `{"passphrase":"…"}` (no `new_passphrase`) creates the cluster
// passphrase, matching PATCH's dual-key behaviour. Before the fix this
// returned 400 ("new_passphrase is required") because handlePassphraseCreate
// only looked at NewPassphrase.
func TestBug165PostAcceptsPassphraseAlias(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	body, _ := json.Marshal(map[string]string{"passphrase": "alias-only"})

	resp := httpPost(t, base+"/v1/encryption/passphrase", body)

	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("POST {\"passphrase\":\"…\"}: got %d, want 201 (Bug 165, body=%q)",
			resp.StatusCode, string(raw))
	}

	// Bug 129 envelope contract still holds: the body must decode as
	// a non-empty []APICallRc so python-linstor renders the CLI line.
	var rcs []apiv1.APICallRc

	if err := json.NewDecoder(resp.Body).Decode(&rcs); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}

	if len(rcs) == 0 || rcs[0].RetCode&maskInfo == 0 {
		t.Errorf("envelope rcs=%+v; want one entry with MASK_INFO set", rcs)
	}

	// The aliased value MUST become the stored passphrase: a follow-up
	// PATCH with the same alias unlocks the controller.
	enterBody, _ := json.Marshal(map[string]string{"passphrase": "alias-only"})
	enterResp := httpPatch(t, base+"/v1/encryption/passphrase", enterBody)
	_ = enterResp.Body.Close()

	if enterResp.StatusCode != http.StatusOK {
		t.Errorf("post-alias PATCH unlock: got %d, want 200", enterResp.StatusCode)
	}
}

// TestBug165PostAcceptsNewPassphrase is a regression guard: POST with
// the canonical `{"new_passphrase":"…"}` field MUST keep returning 201.
// A naive "rename NewPassphrase → Passphrase" fix would break this.
func TestBug165PostAcceptsNewPassphrase(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	body, _ := json.Marshal(map[string]string{"new_passphrase": "canonical"})

	resp := httpPost(t, base+"/v1/encryption/passphrase", body)

	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("POST {\"new_passphrase\":\"…\"}: got %d, want 201 (body=%q)",
			resp.StatusCode, string(raw))
	}
}

// TestBug165PatchAcceptsBothFields pins the existing dual-key
// behaviour on PATCH (enter-passphrase) as a contract guard so a
// future refactor can't quietly drop one of the field names.
func TestBug165PatchAcceptsBothFields(t *testing.T) {
	t.Run("new_passphrase", func(t *testing.T) {
		base, stop := startServerWithStore(t, store.NewInMemory())
		defer stop()

		createBody, _ := json.Marshal(map[string]string{"new_passphrase": "seed"})
		createResp := httpPost(t, base+"/v1/encryption/passphrase", createBody)
		_ = createResp.Body.Close()

		if createResp.StatusCode != http.StatusCreated {
			t.Fatalf("seed create: got %d, want 201", createResp.StatusCode)
		}

		enterBody, _ := json.Marshal(map[string]string{"new_passphrase": "seed"})
		enterResp := httpPatch(t, base+"/v1/encryption/passphrase", enterBody)
		_ = enterResp.Body.Close()

		if enterResp.StatusCode != http.StatusOK {
			t.Errorf("PATCH new_passphrase: got %d, want 200", enterResp.StatusCode)
		}
	})

	t.Run("passphrase", func(t *testing.T) {
		base, stop := startServerWithStore(t, store.NewInMemory())
		defer stop()

		createBody, _ := json.Marshal(map[string]string{"new_passphrase": "seed"})
		createResp := httpPost(t, base+"/v1/encryption/passphrase", createBody)
		_ = createResp.Body.Close()

		if createResp.StatusCode != http.StatusCreated {
			t.Fatalf("seed create: got %d, want 201", createResp.StatusCode)
		}

		enterBody, _ := json.Marshal(map[string]string{"passphrase": "seed"})
		enterResp := httpPatch(t, base+"/v1/encryption/passphrase", enterBody)
		_ = enterResp.Body.Close()

		if enterResp.StatusCode != http.StatusOK {
			t.Errorf("PATCH passphrase: got %d, want 200", enterResp.StatusCode)
		}
	})
}

// TestBug165PostRefusesEmptyPassphrase pins that POST with no
// passphrase field at all (or both empty) still returns 400 + the
// standard `[]APICallRc` error envelope. Without this guard, an empty
// alias-acceptance path would silently stamp the empty string as the
// cluster passphrase.
func TestBug165PostRefusesEmptyPassphrase(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	body := []byte(`{}`)

	resp := httpPost(t, base+"/v1/encryption/passphrase", body)

	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("POST {}: got %d, want 400 (body=%q)", resp.StatusCode, string(raw))
	}

	// Standard error envelope: []APICallRc with at least one entry and
	// a non-empty message so python-linstor renders the operator line.
	var rcs []apiv1.APICallRc

	if err := json.NewDecoder(resp.Body).Decode(&rcs); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}

	if len(rcs) == 0 || rcs[0].Message == "" {
		t.Errorf("error envelope rcs=%+v; want non-empty []APICallRc with a message", rcs)
	}
}
