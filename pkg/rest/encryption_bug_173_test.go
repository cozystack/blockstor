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

// Bug 173 — `PATCH /v1/encryption/passphrase` rejects the canonical
// upstream wire shape.
//
// The upstream LINSTOR OpenAPI spec defines `PassPhraseEnter` as
// `type: string` — a bare JSON string body, not an object. The
// upstream Java handler decodes `objectMapper.readValue(jsonData,
// String.class)`, and golinstor's `EncryptionService.Enter()` posts
// the raw password verbatim with no envelope. The generated
// `pkg/api/openapi/types.gen.go` correctly declares
// `type PassPhraseEnter = string` (line 1310) — that is the source-of-
// truth shape per the upstream spec.
//
// `handlePassphraseEnter` decodes into the wrapped `passphraseRequest`
// object (Bug 165 dual-key shape). Strict-OpenAPI clients
// (golinstor 0.55+, terraform-provider-linstor, hand-rolled
// `curl -d '"…"'` scripts) send a bare string per the spec and hit
// 400 with `wrong JSON shape: …has the wrong type`.
//
// Fix path B (chosen): extend the handler to accept BOTH the
// canonical bare-string body (matches spec + upstream + golinstor) AND
// the wrapped object body (preserves Bug 165 backward compat).
// Backward-compatible by construction — every existing caller keeps
// working.
//
// Path A (rewriting the OpenAPI spec to be an object) was rejected
// because (1) the upstream spec is canonical and `types.gen.go` is
// auto-generated from it, (2) upstream LINSTOR's Java handler decodes
// a bare string, and (3) golinstor's wire client sends a bare string
// — so the spec is correct and the handler is the diverging side.
//
// Tests pinned here:
//
//   - TestBug173PassphraseEnterAcceptsObjectShape: PATCH with the Bug
//     165 wrapped objects (`{"passphrase":"…"}`,
//     `{"new_passphrase":"…"}`) → 200. Regression guard so the bare-
//     string acceptance doesn't accidentally drop the object form.
//   - TestBug173PassphraseEnterAcceptsBareString: PATCH with a bare
//     JSON string `"…"` → 200. Matches the spec, upstream Java
//     handler, and golinstor's wire format. This is the test that
//     fails on HEAD (567e5c982).
//   - TestBug173OpenAPISpecMatchesHandler: parse
//     `pkg/api/openapi/types.gen.go`, assert `PassPhraseEnter` is
//     `string` (not an object struct). Combined with the bare-string
//     handler test above, the spec and the handler now line up.
//   - TestBug173PassphraseEnterRejectsEmptyBareString: PATCH with `""`
//     (empty bare string) → 401 (no proof-of-knowledge), parity with
//     the wrapped `{"passphrase":""}` rejection path.
//   - TestBug173PassphraseEnterRejectsMalformedString: PATCH with
//     non-JSON garbage or wrong-shape (e.g. `["x"]`) → 400 with the
//     standard envelope. Pins that the bare-string path doesn't
//     swallow malformed bodies.

package rest

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"net/http"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"

	"github.com/cozystack/blockstor/pkg/store"
)

// seedPassphrase POSTs a master passphrase so the PATCH path under
// test has something to validate the proof-of-knowledge against.
// Centralised here so the bug-173 tests don't drift from the Bug 165
// envelope-shape invariants when seeding state.
func seedPassphrase(t *testing.T, base, value string) {
	t.Helper()

	body, _ := json.Marshal(map[string]string{"new_passphrase": value})

	resp := httpPost(t, base+"/v1/encryption/passphrase", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("seed POST: got %d, want 201 (body=%q)", resp.StatusCode, string(raw))
	}
}

// TestBug173PassphraseEnterAcceptsObjectShape is a regression guard
// for the Bug 165 dual-key wrapped wire surface. After the Bug 173
// bare-string acceptance is added, both `{"passphrase":"…"}` and
// `{"new_passphrase":"…"}` MUST keep returning 200 — a naive "decode
// as string only" refactor would silently break every existing
// `--curl` / golinstor caller that sends the object form.
func TestBug173PassphraseEnterAcceptsObjectShape(t *testing.T) {
	t.Run("passphrase", func(t *testing.T) {
		base, stop := startServerWithStore(t, store.NewInMemory())
		defer stop()

		seedPassphrase(t, base, "seed-obj-passphrase")

		body, _ := json.Marshal(map[string]string{"passphrase": "seed-obj-passphrase"})

		resp := httpPatch(t, base+"/v1/encryption/passphrase", body)
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusOK {
			raw, _ := io.ReadAll(resp.Body)
			t.Fatalf("PATCH {\"passphrase\":\"…\"}: got %d, want 200 (body=%q)",
				resp.StatusCode, string(raw))
		}
	})

	t.Run("new_passphrase", func(t *testing.T) {
		base, stop := startServerWithStore(t, store.NewInMemory())
		defer stop()

		seedPassphrase(t, base, "seed-obj-newpass")

		body, _ := json.Marshal(map[string]string{"new_passphrase": "seed-obj-newpass"})

		resp := httpPatch(t, base+"/v1/encryption/passphrase", body)
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusOK {
			raw, _ := io.ReadAll(resp.Body)
			t.Fatalf("PATCH {\"new_passphrase\":\"…\"}: got %d, want 200 (body=%q)",
				resp.StatusCode, string(raw))
		}
	})
}

// TestBug173PassphraseEnterAcceptsBareString pins the spec-aligned
// canonical wire shape. The upstream `PassPhraseEnter` schema is
// `type: string`, the upstream Java handler decodes
// `readValue(jsonData, String.class)`, and golinstor's
// `EncryptionService.Enter()` sends the password as the raw body
// with no envelope. Pre-fix this returned 400 with `wrong JSON
// shape: …has the wrong type` because the handler decoded into the
// `passphraseRequest` struct.
func TestBug173PassphraseEnterAcceptsBareString(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	seedPassphrase(t, base, "seed-bare")

	// A bare JSON string body — exactly what golinstor.Enter() and a
	// `curl -d '"seed-bare"'` invocation put on the wire.
	body, _ := json.Marshal("seed-bare")

	resp := httpPatch(t, base+"/v1/encryption/passphrase", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("PATCH \"seed-bare\": got %d, want 200 (Bug 173, body=%q)",
			resp.StatusCode, string(raw))
	}
}

// TestBug173PassphraseEnterRejectsEmptyBareString pins that a bare
// empty-string body `""` returns the same 401 the wrapped
// `{"passphrase":""}` form returns. An empty proof-of-knowledge MUST
// never unlock the controller — a regression that treated "" as a
// match (e.g. comparing empty string to empty unconfigured value)
// would silently grant LUKS provisioning to any unauthenticated
// caller after a Secret-loss event.
func TestBug173PassphraseEnterRejectsEmptyBareString(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	seedPassphrase(t, base, "seed-empty-guard")

	body, _ := json.Marshal("")

	resp := httpPatch(t, base+"/v1/encryption/passphrase", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusUnauthorized {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("PATCH \"\": got %d, want 401 (Bug 173 empty-PoK guard, body=%q)",
			resp.StatusCode, string(raw))
	}
}

// TestBug173PassphraseEnterRejectsMalformedString pins that the
// bare-string acceptance doesn't swallow non-string JSON bodies. A
// caller that sends `["x"]` or `123` is sending the wrong shape and
// MUST get a 400 with the standard envelope; otherwise a typo'd
// client could silently coerce a number into a "passphrase" the
// caller never intended.
func TestBug173PassphraseEnterRejectsMalformedString(t *testing.T) {
	// `null` is intentionally excluded — Go's `json.NewDecoder` treats
	// `null` as a no-op for any target (string or struct), so the
	// downstream auth path runs with a zero-value proof-of-knowledge
	// and surfaces 401 "passphrase mismatch" rather than 400. That's
	// std-lib-defined behaviour, not a Bug 173 contract, and pinning
	// it here would over-constrain the implementation.
	cases := []struct {
		name string
		body []byte
	}{
		{"json-array", []byte(`["x"]`)},
		{"json-number", []byte(`42`)},
		{"json-boolean", []byte(`true`)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			base, stop := startServerWithStore(t, store.NewInMemory())
			defer stop()

			seedPassphrase(t, base, "seed-malformed")

			resp := httpPatch(t, base+"/v1/encryption/passphrase", tc.body)
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != http.StatusBadRequest {
				raw, _ := io.ReadAll(resp.Body)
				t.Fatalf("PATCH %s: got %d, want 400 (body=%q)",
					tc.name, resp.StatusCode, string(raw))
			}

			var rcs []apiv1.APICallRc

			if err := json.NewDecoder(resp.Body).Decode(&rcs); err != nil {
				t.Fatalf("decode envelope: %v", err)
			}

			if len(rcs) == 0 || rcs[0].Message == "" {
				t.Errorf("error envelope rcs=%+v; want non-empty []APICallRc with a message", rcs)
			}
		})
	}
}

// TestBug173OpenAPISpecMatchesHandler statically asserts that the
// generated `pkg/api/openapi/types.gen.go` declares `PassPhraseEnter`
// as a bare `string` (the upstream canonical shape) — NOT an object
// struct. Combined with the bare-string handler test above, this is
// the contract that pins spec ↔ handler agreement so a future
// "let's generate types from a stale schema" run can't quietly
// invert the wire shape.
//
// We parse the file with `go/parser` and walk the AST rather than
// grepping the source — a comment or string literal that happens to
// say `PassPhraseEnter = string` MUST NOT satisfy this test, only an
// actual type alias declaration in the type system can.
func TestBug173OpenAPISpecMatchesHandler(t *testing.T) {
	const typesPath = "../api/openapi/types.gen.go"

	fset := token.NewFileSet()

	f, err := parser.ParseFile(fset, typesPath, nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse %s: %v", typesPath, err)
	}

	var (
		found bool
		shape string
	)

	ast.Inspect(f, func(n ast.Node) bool {
		ts, ok := n.(*ast.TypeSpec)
		if !ok {
			return true
		}

		if ts.Name == nil || ts.Name.Name != "PassPhraseEnter" {
			return true
		}

		found = true

		// A bare `type X = string` alias has Assign != 0 and Type ==
		// *ast.Ident with Name == "string". A `type X struct{…}` is
		// *ast.StructType. We want the alias.
		ident, ok := ts.Type.(*ast.Ident)
		if !ok {
			shape = "non-ident (likely struct or pointer); the openapi spec defines PassPhraseEnter as `type: string` and the handler now accepts a bare string body — regenerate types.gen.go from third_party/linstor-openapi/rest_v1_openapi.yaml"

			return false
		}

		shape = ident.Name

		return false
	})

	if !found {
		t.Fatalf("PassPhraseEnter type declaration not found in %s; "+
			"types.gen.go drifted from third_party/linstor-openapi/rest_v1_openapi.yaml",
			typesPath)
	}

	if shape != "string" {
		t.Fatalf("PassPhraseEnter shape mismatch: got %q, want \"string\" (upstream spec)\n"+
			"the openapi spec is the source of truth; the handler must accept this shape", shape)
	}
}
