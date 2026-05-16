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
	"log/slog"
	"net/http"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Bug 159: `linstor c set-log-level DEBUG` ends up calling
// `PUT /v1/controller/config` with body `{"log":{"level":"DEBUG"}}`.
// Before the fix the apiserver only wired GET on that path, so the
// CLI got a 405 (typed via Bug 109's envelope but still unusable).
// These tests pin the PUT handler: accepted levels flip the runtime
// slog level, unknown levels are rejected with a typed envelope, and
// the pre-existing GET is unaffected.

// TestBug159SetLogLevelAcceptsKnownLevels: a PUT with the simple
// flat shape `{"log_level":"DEBUG"}` (operator-friendly, accepted
// for direct curl usage) returns 200 + LINSTOR envelope and flips
// the runtime level.
func TestBug159SetLogLevelAcceptsKnownLevels(t *testing.T) {
	restore := withRuntimeLogLevel(t)
	defer restore()

	runtimeLogLevel.Set(slog.LevelInfo)

	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	body := []byte(`{"log_level":"DEBUG"}`)

	resp := httpPut(t, base+"/v1/controller/config", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status: got %d, want 200", resp.StatusCode)
	}

	var rc []apiv1.APICallRc

	err := json.NewDecoder(resp.Body).Decode(&rc)
	if err != nil {
		t.Fatalf("decode envelope: %v", err)
	}

	if len(rc) == 0 {
		t.Fatalf("empty envelope — Python CLI crashes on replies[0]")
	}

	if rc[0].RetCode < 0 {
		t.Errorf("ret_code = %d, want >=0 (success marker)", rc[0].RetCode)
	}

	if rc[0].Message == "" {
		t.Errorf("empty message — operator log will be unreadable")
	}

	if got := runtimeLogLevel.Level(); got != slog.LevelDebug {
		t.Errorf("runtimeLogLevel: got %v, want DEBUG", got)
	}
}

// TestBug159SetLogLevelAcceptsAllUpstreamLevels exhausts the
// upstream-allowed set (TRACE/DEBUG/INFO/WARN/ERROR) so a new
// level slipping into the openapi enum forces an intentional update
// here rather than silently breaking the CLI for that level.
func TestBug159SetLogLevelAcceptsAllUpstreamLevels(t *testing.T) {
	cases := []struct {
		level string
		want  slog.Level
	}{
		{"TRACE", slog.LevelDebug - 4},
		{"DEBUG", slog.LevelDebug},
		{"INFO", slog.LevelInfo},
		{"WARN", slog.LevelWarn},
		{"ERROR", slog.LevelError},
	}

	for _, tc := range cases {
		t.Run(tc.level, func(t *testing.T) {
			restore := withRuntimeLogLevel(t)
			defer restore()

			runtimeLogLevel.Set(slog.LevelInfo)

			base, stop := startServerWithStore(t, store.NewInMemory())
			defer stop()

			body := []byte(`{"log_level":"` + tc.level + `"}`)

			resp := httpPut(t, base+"/v1/controller/config", body)
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != http.StatusOK {
				t.Fatalf("PUT %s status: got %d, want 200", tc.level, resp.StatusCode)
			}

			var rc []apiv1.APICallRc
			if err := json.NewDecoder(resp.Body).Decode(&rc); err != nil {
				t.Fatalf("decode envelope: %v", err)
			}

			if len(rc) == 0 || rc[0].RetCode < 0 {
				t.Fatalf("envelope shape wrong: %+v", rc)
			}

			if got := runtimeLogLevel.Level(); got != tc.want {
				t.Errorf("runtimeLogLevel for %s: got %v, want %v", tc.level, got, tc.want)
			}
		})
	}
}

// TestBug159SetLogLevelAcceptsUpstreamNestedShape pins the actual
// wire shape python-linstor 1.27.1 sends: `{"log":{"level":"DEBUG"}}`
// (linstorapi.py:3164-3168). Without this support the CLI gesture
// `linstor c set-log-level DEBUG` stays broken even after the flat
// shape works.
func TestBug159SetLogLevelAcceptsUpstreamNestedShape(t *testing.T) {
	restore := withRuntimeLogLevel(t)
	defer restore()

	runtimeLogLevel.Set(slog.LevelInfo)

	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	// All four nested keys upstream supports (level, level_linstor,
	// level_global, level_linstor_global) must flip the level —
	// python-linstor picks one based on (glob, library) flags.
	for _, key := range []string{"level", "level_linstor", "level_global", "level_linstor_global"} {
		t.Run(key, func(t *testing.T) {
			runtimeLogLevel.Set(slog.LevelInfo)

			body := []byte(`{"log":{"` + key + `":"DEBUG"}}`)

			resp := httpPut(t, base+"/v1/controller/config", body)
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != http.StatusOK {
				t.Fatalf("PUT %s status: got %d, want 200", key, resp.StatusCode)
			}

			if got := runtimeLogLevel.Level(); got != slog.LevelDebug {
				t.Errorf("runtimeLogLevel for nested %s: got %v, want DEBUG", key, got)
			}
		})
	}
}

// TestBug159SetLogLevelRefusesUnknownLevel: a bogus level returns
// 400 + typed envelope listing the accepted set so the operator
// can correct their input.
func TestBug159SetLogLevelRefusesUnknownLevel(t *testing.T) {
	restore := withRuntimeLogLevel(t)
	defer restore()

	runtimeLogLevel.Set(slog.LevelInfo)

	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	body := []byte(`{"log_level":"BOGUS"}`)

	resp := httpPut(t, base+"/v1/controller/config", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("PUT status: got %d, want 400", resp.StatusCode)
	}

	var rc []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rc); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}

	if len(rc) == 0 {
		t.Fatalf("empty envelope — Python CLI crashes on replies[0]")
	}

	if rc[0].RetCode >= 0 {
		t.Errorf("ret_code = %d, want <0 (error marker)", rc[0].RetCode)
	}

	if rc[0].Message == "" {
		t.Errorf("empty message — operator log will be unreadable")
	}

	// Error message must list the accepted set so the operator
	// can self-correct without reading source. The CLI surfaces
	// this string verbatim to the operator.
	for _, level := range []string{"TRACE", "DEBUG", "INFO", "WARN", "ERROR"} {
		if !strings.Contains(rc[0].Message, level) {
			t.Errorf("error message missing %q: %q", level, rc[0].Message)
		}
	}

	// Runtime level must be untouched on rejection — a bogus PUT
	// is no-op, not a stealth muzzle.
	if got := runtimeLogLevel.Level(); got != slog.LevelInfo {
		t.Errorf("runtime level mutated by rejected PUT: got %v, want INFO", got)
	}
}

// TestBug159GetLogLevelStillWorks: regression guard — the GET
// handler that returned `{}` before this fix must still do so,
// otherwise golinstor's `Controller.GetConfig()` breaks.
func TestBug159GetLogLevelStillWorks(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpGet(t, base+"/v1/controller/config")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET status: got %d, want 200", resp.StatusCode)
	}

	// golinstor decodes the response into ControllerConfig — any
	// valid JSON object satisfies the decoder. Pin the wire shape
	// is at least a JSON object (not an array or scalar) so the
	// decoder doesn't crash on a regression that changed the type.
	var got map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
}
