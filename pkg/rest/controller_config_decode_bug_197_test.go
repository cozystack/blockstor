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
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/store"
)

// Bug 197 (P1) — PUT /v1/controller/config bypasses the Bug 158/161
// decode gate.
//
// `handlePutControllerConfig` (Bug 159) used a bare
// `json.NewDecoder(r.Body).Decode(&body)` + raw `writeError(400,
// err.Error())` instead of the canonical `decodeJSON` helper. That
// produces three wire-confirmed misses against the typed-envelope
// invariant Bug 158/161 already locked in for every other handler:
//
//   1. Unknown top-level fields silently slip past — a typo'd
//      `level`/`loglevel` would no-op while the wire still says
//      "log level set to DEBUG", so operators can't trust the gesture.
//   2. Empty body leaks the raw std-lib `EOF` string on the wire
//      (Bug 158 canonicalised this to "request body is empty").
//   3. Malformed JSON leaks the std-lib's
//      `invalid character '...' looking for beginning of object key
//      string` text on the wire (Bug 158 canonicalised this to
//      "request body is not valid JSON").
//
// The fix routes the PUT body through `decodeJSON`, identical to
// every other body-consuming handler in pkg/rest/.

// TestBug197ControllerConfigRejectsUnknownField pins the Bug 161
// invariant for the PUT /v1/controller/config handler: an unknown
// top-level field must be refused with 400 + typed envelope and the
// runtime level must NOT be flipped. Pre-fix the handler accepted
// `{"log_level":"DEBUG","unknown_field":"x"}` with 200 + "log level
// set to DEBUG" envelope — operators saw success on a typo.
func TestBug197ControllerConfigRejectsUnknownField(t *testing.T) {
	restore := withRuntimeLogLevel(t)
	defer restore()

	runtimeLogLevel.Set(slog.LevelInfo)

	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	body := []byte(`{"log_level":"DEBUG","unknown_field":"x"}`)

	resp := httpPut(t, base+"/v1/controller/config", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		got := mustReadBody(t, resp)
		t.Fatalf("status: got %d, want 400 (Bug 197 unknown field). Body: %s", resp.StatusCode, got)
	}

	got := mustReadBody(t, resp)
	rc := mustDecodeEnvelope(t, got)
	assertErrorEnvelope(t, rc, got)

	// The envelope must name the offending field so operators can
	// self-correct after typing the wrong key.
	if !strings.Contains(string(got), "unknown_field") {
		t.Errorf("envelope must cite the unknown field %q: %s", "unknown_field", got)
	}

	// Runtime level must be untouched on rejection — a body with an
	// unknown field is a typed-envelope failure, not a stealth flip.
	if level := runtimeLogLevel.Level(); level != slog.LevelInfo {
		t.Errorf("runtime level mutated by rejected PUT: got %v, want INFO", level)
	}
}

// TestBug197ControllerConfigRejectsEmptyBody pins the Bug 158
// invariant for the PUT /v1/controller/config handler: an empty body
// must NOT leak the raw std-lib `EOF` string on the wire. Pre-fix the
// handler wrote `[{"message":"EOF"}]` — python-linstor surfaces this
// as `ERROR: EOF` with no hint that the body was missing.
func TestBug197ControllerConfigRejectsEmptyBody(t *testing.T) {
	restore := withRuntimeLogLevel(t)
	defer restore()

	runtimeLogLevel.Set(slog.LevelInfo)

	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpPut(t, base+"/v1/controller/config", nil)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		got := mustReadBody(t, resp)
		t.Fatalf("status: got %d, want 400 (Bug 197 empty body). Body: %s", resp.StatusCode, got)
	}

	got := mustReadBody(t, resp)

	// Raw "EOF" must not appear on the wire.
	if strings.TrimSpace(string(got)) == `"EOF"` || string(got) == "EOF" {
		t.Fatalf("body leaks raw decoder EOF: %s", got)
	}

	for _, leak := range []string{"EOF", "json:", "json.", "unmarshal"} {
		if strings.Contains(string(got), leak) {
			t.Errorf("body leaks decoder impl detail %q: %s", leak, got)
		}
	}

	rc := mustDecodeEnvelope(t, got)
	assertErrorEnvelope(t, rc, got)
}

// TestBug197ControllerConfigRejectsGarbage pins the Bug 158
// invariant for the PUT /v1/controller/config handler: a malformed
// JSON body must NOT leak the std-lib's `invalid character '...'`
// text on the wire. Pre-fix `garbage` produced `[{"message":"invalid
// character 'g' looking for beginning of value"}]`.
func TestBug197ControllerConfigRejectsGarbage(t *testing.T) {
	restore := withRuntimeLogLevel(t)
	defer restore()

	runtimeLogLevel.Set(slog.LevelInfo)

	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpPut(t, base+"/v1/controller/config", []byte("garbage"))
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		got := mustReadBody(t, resp)
		t.Fatalf("status: got %d, want 400 (Bug 197 garbage body). Body: %s", resp.StatusCode, got)
	}

	got := mustReadBody(t, resp)

	for _, leak := range []string{"invalid character", "literal null", "json:"} {
		if strings.Contains(string(got), leak) {
			t.Errorf("body leaks raw decoder string %q: %s", leak, got)
		}
	}

	rc := mustDecodeEnvelope(t, got)
	assertErrorEnvelope(t, rc, got)
}

// TestBug197ControllerConfigAcceptsValidNestedShape is the Bug 159
// regression guard: the nested upstream wire shape
// `{"log":{"level":"DEBUG"}}` must still be accepted post-fix. The
// flip from a bare decoder to `decodeJSON` armed
// `DisallowUnknownFields` on the body, so all known fields on the
// `putControllerConfigBody` / `putControllerConfigBodyLogSubtree`
// types must keep working — otherwise the canonical CLI gesture
// `linstor c set-log-level DEBUG` regresses to 400.
func TestBug197ControllerConfigAcceptsValidNestedShape(t *testing.T) {
	restore := withRuntimeLogLevel(t)
	defer restore()

	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	// Cover every nested key + the flat `log_level` so a regression
	// in the embedded type tags (a renamed json tag, a missing field)
	// fails this test rather than silently breaking one shape.
	cases := []struct {
		name string
		body string
	}{
		{"flat-log_level", `{"log_level":"DEBUG"}`},
		{"nested-level", `{"log":{"level":"DEBUG"}}`},
		{"nested-level_linstor", `{"log":{"level_linstor":"DEBUG"}}`},
		{"nested-level_global", `{"log":{"level_global":"DEBUG"}}`},
		{"nested-level_linstor_global", `{"log":{"level_linstor_global":"DEBUG"}}`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runtimeLogLevel.Set(slog.LevelInfo)

			resp := httpPut(t, base+"/v1/controller/config", []byte(tc.body))
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != http.StatusOK {
				got := mustReadBody(t, resp)
				t.Fatalf("status: got %d, want 200 (Bug 159 regression on %s). Body: %s",
					resp.StatusCode, tc.name, got)
			}

			if got := runtimeLogLevel.Level(); got != slog.LevelDebug {
				t.Errorf("runtime level for %s: got %v, want DEBUG", tc.name, got)
			}
		})
	}
}
