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
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// runtimeLogLevelGuard serialises tests that mutate the package-
// global runtimeLogLevel + slog.Default. Parallel runs would race
// otherwise — every test in this file flips global state.
var runtimeLogLevelGuard sync.Mutex //nolint:gochecknoglobals // test-only serialisation

// withRuntimeLogLevel snapshots the runtime log level + slog.Default
// before a test, returns a restore func, and acquires the guard
// mutex so concurrent tests in the file don't trample each other.
// Pattern matches the rest of pkg/rest's "save/restore globals"
// helpers.
func withRuntimeLogLevel(t *testing.T) func() {
	t.Helper()
	runtimeLogLevelGuard.Lock()

	prevLevel := runtimeLogLevel.Level()
	prevDefault := slog.Default()

	return func() {
		runtimeLogLevel.Set(prevLevel)
		slog.SetDefault(prevDefault)
		runtimeLogLevelGuard.Unlock()
	}
}

// TestParseLogLevelKnownValues pins the LINSTOR-CLI vocabulary →
// slog.Level mapping. Scenario 7.W06 names TRACE / DEBUG / INFO /
// WARN / ERROR as the supported set; if upstream grows a new level
// this test fails first and forces the mapping to be intentional.
func TestParseLogLevelKnownValues(t *testing.T) {
	cases := []struct {
		in   string
		want slog.Level
	}{
		{"TRACE", slog.LevelDebug - 4},
		{"DEBUG", slog.LevelDebug},
		{"INFO", slog.LevelInfo},
		{"WARN", slog.LevelWarn},
		{"WARNING", slog.LevelWarn},
		{"ERROR", slog.LevelError},
		{"debug", slog.LevelDebug},  // case-insensitive
		{"  INFO ", slog.LevelInfo}, // whitespace-tolerant
	}
	for _, tc := range cases {
		got, ok := parseLogLevel(tc.in)
		if !ok {
			t.Errorf("parseLogLevel(%q): ok=false, want true", tc.in)

			continue
		}

		if got != tc.want {
			t.Errorf("parseLogLevel(%q): got %v, want %v", tc.in, got, tc.want)
		}
	}
}

// TestParseLogLevelUnknownRejected guards the unknown-string path so
// the runtime flip stays a no-op for garbage input. Strict
// validation lives in the CLI; the REST layer just refuses to flip
// when it doesn't recognise the value.
func TestParseLogLevelUnknownRejected(t *testing.T) {
	for _, in := range []string{"", "VERBOSE", "fatal", "12", "NULL"} {
		if _, ok := parseLogLevel(in); ok {
			t.Errorf("parseLogLevel(%q): ok=true, want false", in)
		}
	}
}

// TestSetLogLevelFlipsRuntimeLevel is the scenario-7.W06 acceptance
// pin: a POST on /v1/controller/properties with LogLevel=DEBUG
// flips the package-global slog.LevelVar AND every subsequent log
// line through slog.Default() honours the new level — no pod
// restart, no re-binding of a handler. The capture buffer is the
// "kubectl logs deploy/blockstor-apiserver" equivalent the scenario
// spec calls out.
func TestSetLogLevelFlipsRuntimeLevel(t *testing.T) {
	restore := withRuntimeLogLevel(t)
	defer restore()

	// Pin the starting level to INFO so the DEBUG capture below
	// can't pass by accident (e.g. if some other test left the
	// global at DEBUG).
	runtimeLogLevel.Set(slog.LevelInfo)

	var buf bytes.Buffer

	// Bind slog.Default to runtimeLogLevel — this is the same
	// wiring cmd/apiserver does at startup, replayed in-test so
	// the flip is observable.
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: runtimeLogLevel})))

	// Sanity: DEBUG-level emit before the flip is suppressed by
	// the INFO floor. Without this assertion a buggy handler
	// (e.g. one that hard-codes LevelDebug) would silently make
	// the post-flip check pass.
	slog.Debug("pre-flip-debug")

	if strings.Contains(buf.String(), "pre-flip-debug") {
		t.Fatalf("DEBUG line emitted before flip: %q", buf.String())
	}

	// The actual PUT (POST in our contract — see controller_props.go
	// for why upstream calls this a PUT even though the wire verb is
	// POST). Scenario 7.W06 acceptance step.
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	body, _ := json.Marshal(apiv1.GenericPropsModify{
		OverrideProps: map[string]string{PropLogLevel: "DEBUG"},
	})

	resp := httpPost(t, base+"/v1/controller/properties", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("POST status: got %d, want 201", resp.StatusCode)
	}

	// LevelVar should now report DEBUG. This is the
	// no-pod-restart pin: a future regression that lost the
	// LevelVar.Set() call would leave this at INFO.
	if got := runtimeLogLevel.Level(); got != slog.LevelDebug {
		t.Fatalf("runtimeLogLevel: got %v, want %v", got, slog.LevelDebug)
	}

	// Post-flip emit must land in the capture buffer — proves the
	// already-installed slog.Default picks up the new level
	// without a re-Set. This is the "kubectl logs reflects the
	// new level immediately" half of the scenario.
	buf.Reset()
	slog.Debug("post-flip-debug")

	if !strings.Contains(buf.String(), "post-flip-debug") {
		t.Fatalf("DEBUG line missing after flip: %q", buf.String())
	}

	// And the property itself must round-trip through GET — an
	// operator running `controller list-properties` should see
	// the value they just set, otherwise the audit trail breaks.
	getResp := httpGet(t, base+"/v1/controller/properties")
	defer func() { _ = getResp.Body.Close() }()

	var got map[string]string

	err := json.NewDecoder(getResp.Body).Decode(&got)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if got[PropLogLevel] != "DEBUG" {
		t.Errorf("GET props[%s]: got %q, want DEBUG", PropLogLevel, got[PropLogLevel])
	}
}

// TestSetLogLevelTraceIsNoisierThanDebug pins the TRACE → slog
// mapping by behaviour, not by integer value: a handler floored at
// TRACE must emit both DEBUG and the synthetic TRACE level, while
// the same handler floored at DEBUG must emit DEBUG but suppress
// anything below it. This is what upstream LINSTOR's "TRACE <
// DEBUG" ordering means for an operator who runs `set-log-level
// TRACE` to chase a particularly noisy bug.
func TestSetLogLevelTraceIsNoisierThanDebug(t *testing.T) {
	restore := withRuntimeLogLevel(t)
	defer restore()

	trace, ok := parseLogLevel("TRACE")
	if !ok {
		t.Fatalf("parseLogLevel(TRACE): ok=false")
	}

	if trace >= slog.LevelDebug {
		t.Fatalf("TRACE (%v) must be strictly below DEBUG (%v)", trace, slog.LevelDebug)
	}

	var buf bytes.Buffer

	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: runtimeLogLevel})))

	// Flip to DEBUG: a TRACE-level emit should be suppressed.
	runtimeLogLevel.Set(slog.LevelDebug)
	slog.Default().Log(t.Context(), trace, "trace-while-debug")

	if strings.Contains(buf.String(), "trace-while-debug") {
		t.Fatalf("TRACE leaked through DEBUG floor: %q", buf.String())
	}

	// Flip to TRACE: the same emit should now land.
	buf.Reset()
	runtimeLogLevel.Set(trace)
	slog.Default().Log(t.Context(), trace, "trace-while-trace")

	if !strings.Contains(buf.String(), "trace-while-trace") {
		t.Fatalf("TRACE missing after flip to TRACE: %q", buf.String())
	}
}

// TestSetLogLevelUnknownValueDoesNotMutateRuntime pins the
// fail-soft semantic: a PUT with LogLevel=BOGUS persists the prop
// (so the operator sees what they typed and can correct it) but
// MUST NOT flip the slog level — otherwise a typo silently muzzles
// or spams the apiserver logs.
func TestSetLogLevelUnknownValueDoesNotMutateRuntime(t *testing.T) {
	restore := withRuntimeLogLevel(t)
	defer restore()

	runtimeLogLevel.Set(slog.LevelInfo)

	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	body, _ := json.Marshal(apiv1.GenericPropsModify{
		OverrideProps: map[string]string{PropLogLevel: "BOGUS"},
	})

	resp := httpPost(t, base+"/v1/controller/properties", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("POST status: got %d, want 201", resp.StatusCode)
	}

	if got := runtimeLogLevel.Level(); got != slog.LevelInfo {
		t.Errorf("runtime level mutated by unknown value: got %v, want INFO", got)
	}
}

// TestSetLogLevelAbsentPropDoesNotMutateRuntime pins the
// non-interference invariant: writing some OTHER controller prop
// (DefaultDebugSslConnector, an Autoplacer weight, ...) MUST NOT
// touch the slog level. Every prop write goes through
// applyRuntimeLogLevel, so a regression that "always reset to
// INFO" would silently undo a previous `set-log-level DEBUG`.
func TestSetLogLevelAbsentPropDoesNotMutateRuntime(t *testing.T) {
	restore := withRuntimeLogLevel(t)
	defer restore()

	runtimeLogLevel.Set(slog.LevelDebug)

	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	body, _ := json.Marshal(apiv1.GenericPropsModify{
		OverrideProps: map[string]string{"DefaultDebugSslConnector": "DebugSslConnector"},
	})

	resp := httpPost(t, base+"/v1/controller/properties", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("POST status: got %d, want 201", resp.StatusCode)
	}

	if got := runtimeLogLevel.Level(); got != slog.LevelDebug {
		t.Errorf("runtime level clobbered by unrelated prop write: got %v, want DEBUG", got)
	}
}

// TestRuntimeLogLevelAccessor pins the cmd/apiserver wiring contract:
// the exported accessor must return the same LevelVar the REST
// handlers mutate, otherwise the binary's slog handler would watch
// an orphan variable and the runtime flip would be invisible.
func TestRuntimeLogLevelAccessor(t *testing.T) {
	restore := withRuntimeLogLevel(t)
	defer restore()

	got := RuntimeLogLevel()
	if got != runtimeLogLevel {
		t.Fatalf("RuntimeLogLevel() returned a different *slog.LevelVar than the package global")
	}

	// Mutating via the accessor must be observable on the package
	// global — same pointer, but pin it anyway so an accidental
	// "return a copy" regression fails this test.
	got.Set(slog.LevelError)

	if runtimeLogLevel.Level() != slog.LevelError {
		t.Errorf("Set via accessor did not reach package global: got %v", runtimeLogLevel.Level())
	}
}
