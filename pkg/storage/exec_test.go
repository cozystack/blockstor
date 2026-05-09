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

package storage_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/storage"
)

// TestFakeExecRecordsCalls: every Run invocation appears in Calls in order.
func TestFakeExecRecordsCalls(t *testing.T) {
	fx := storage.NewFakeExec()

	_, _ = fx.Run(t.Context(), "lvs")
	_, _ = fx.Run(t.Context(), "lvcreate", "-T", "vg/pool", "-V", "1G", "-n", "vol")

	if len(fx.Calls) != 2 {
		t.Fatalf("Calls len: got %d, want 2", len(fx.Calls))
	}

	if fx.Calls[1].Name != "lvcreate" {
		t.Errorf("Calls[1].Name: got %q, want lvcreate", fx.Calls[1].Name)
	}

	want := []string{"-T", "vg/pool", "-V", "1G", "-n", "vol"}
	if strings.Join(fx.Calls[1].Args, ",") != strings.Join(want, ",") {
		t.Errorf("Calls[1].Args: got %v, want %v", fx.Calls[1].Args, want)
	}
}

// TestFakeExecCannedResponse: Expect returns the registered stdout/error.
func TestFakeExecCannedResponse(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("lvs --noheadings -o lv_name vg", storage.FakeResponse{
		Stdout: []byte("vol1\nvol2\n"),
	})

	out, err := fx.Run(t.Context(), "lvs", "--noheadings", "-o", "lv_name", "vg")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if !strings.Contains(string(out), "vol1") {
		t.Errorf("stdout: got %q, want to contain vol1", out)
	}
}

// errFakeVGMissing is a static sentinel; err113 forbids ad-hoc errors.New
// in tests so we declare it once here.
var errFakeVGMissing = errors.New("volume group \"vg\" not found")

// TestFakeExecCannedError: Expect can return an error, callers must
// surface it.
func TestFakeExecCannedError(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("vgs vg", storage.FakeResponse{Err: errFakeVGMissing})

	_, err := fx.Run(t.Context(), "vgs", "vg")
	if !errors.Is(err, errFakeVGMissing) {
		t.Errorf("Run err: got %v, want %v", err, errFakeVGMissing)
	}
}

// TestFakeExecEmptyDefault: no Expect → empty stdout, nil error.
func TestFakeExecEmptyDefault(t *testing.T) {
	fx := storage.NewFakeExec()

	out, err := fx.Run(t.Context(), "anything")
	if err != nil {
		t.Errorf("default err: got %v, want nil", err)
	}

	if len(out) != 0 {
		t.Errorf("default stdout: got %q, want empty", out)
	}
}

// TestFakeExecCommandLinesJoinsArgs: convenience accessor returns joined
// command lines for ContainsAll-style assertions.
func TestFakeExecCommandLinesJoinsArgs(t *testing.T) {
	fx := storage.NewFakeExec()

	_, _ = fx.Run(t.Context(), "lvremove", "-f", "vg/vol")
	_, _ = fx.Run(t.Context(), "vgs")

	got := fx.CommandLines()
	if len(got) != 2 {
		t.Fatalf("len: got %d", len(got))
	}

	if got[0] != "lvremove -f vg/vol" {
		t.Errorf("[0]: got %q", got[0])
	}

	if got[1] != "vgs" {
		t.Errorf("[1]: got %q", got[1])
	}
}

// TestFakeExecReset clears recorded calls but keeps Responses.
func TestFakeExecReset(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("lvs", storage.FakeResponse{Stdout: []byte("ok")})

	_, _ = fx.Run(t.Context(), "lvs")

	fx.Reset()

	if len(fx.Calls) != 0 {
		t.Errorf("Calls: got %d, want 0 after Reset", len(fx.Calls))
	}

	out, _ := fx.Run(t.Context(), "lvs")
	if string(out) != "ok" {
		t.Errorf("Responses lost across Reset: got %q", out)
	}
}

// TestRealExecCapturesStdout: RealExec captures the child process's
// stdout into the returned byte slice. Pins the production exec
// path's primary contract — every storage provider parses stdout
// directly (lvs, zfs list, vgs).
func TestRealExecCapturesStdout(t *testing.T) {
	// `printf` is part of POSIX shell builtins on darwin/linux; the
	// test stays portable without bringing in test-fixture binaries.
	out, err := storage.RealExec{}.Run(t.Context(), "printf", "%s", "hello-stdout")
	if err != nil {
		t.Fatalf("RealExec.Run: %v", err)
	}

	if string(out) != "hello-stdout" {
		t.Errorf("stdout: got %q, want %q", string(out), "hello-stdout")
	}
}

// TestRealExecFoldsStderrIntoError: when the child fails, the
// returned error must include both the original exec error AND the
// stderr text — providers surface that combined message verbatim,
// so a regression that dropped stderr would leave operators
// debugging blind.
func TestRealExecFoldsStderrIntoError(t *testing.T) {
	// `false` exits non-zero with no output; use `sh -c` to also
	// emit a stderr line so we can assert it appears in the error.
	_, err := storage.RealExec{}.Run(t.Context(), "sh", "-c", "echo FAIL-MARKER >&2; exit 7")
	if err == nil {
		t.Fatalf("expected non-nil error on exit 7")
	}

	if !strings.Contains(err.Error(), "FAIL-MARKER") {
		t.Errorf("stderr swallowed: error %q must contain FAIL-MARKER", err.Error())
	}
}

// TestRealExecHonoursContextCancel: a cancelled context kills the
// child process. Pins the timeout / shutdown contract — without it,
// a hung lvs / zfs would block the satellite reconciler indefinitely.
func TestRealExecHonoursContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel() // already cancelled before the call

	_, err := storage.RealExec{}.Run(ctx, "sh", "-c", "sleep 60")
	if err == nil {
		t.Errorf("expected error on cancelled context")
	}
}
