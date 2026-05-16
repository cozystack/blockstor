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

package luks_test

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/luks"
	"github.com/cozystack/blockstor/pkg/storage"
)

var errFakeNotLuks = errors.New("cryptsetup: not a LUKS device")

// TestFormatRunsLuksFormat: Format on a fresh device runs
// `cryptsetup luksFormat ...` with the keyfile piped via stdin.
func TestFormatRunsLuksFormat(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("cryptsetup isLuks /dev/sda",
		storage.FakeResponse{Err: errFakeNotLuks})

	c := luks.NewCryptsetup(fx)

	err := c.Format(t.Context(), "/dev/sda", []byte("supersecret"))
	if err != nil {
		t.Fatalf("Format: %v", err)
	}

	for _, line := range fx.CommandLines() {
		if strings.Contains(line, "luksFormat") &&
			strings.Contains(line, "/dev/sda") {
			return
		}
	}

	t.Errorf("expected luksFormat /dev/sda in pipeline; got %v", fx.CommandLines())
}

// TestFormatIdempotent: existing LUKS header → no-op (cryptsetup
// isLuks succeeds).
func TestFormatIdempotent(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("cryptsetup isLuks /dev/sda",
		storage.FakeResponse{Stdout: []byte("")})

	c := luks.NewCryptsetup(fx)

	err := c.Format(t.Context(), "/dev/sda", []byte("supersecret"))
	if err != nil {
		t.Fatalf("Format: %v", err)
	}

	for _, line := range fx.CommandLines() {
		if strings.Contains(line, "luksFormat") {
			t.Errorf("idempotent Format issued luksFormat: %s", line)
		}
	}
}

// TestOpenRunsLuksOpen.
func TestOpenRunsLuksOpen(t *testing.T) {
	fx := storage.NewFakeExec()

	c := luks.NewCryptsetup(fx)

	err := c.Open(t.Context(), "/dev/sda", "pvc-1-crypt", []byte("supersecret"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	for _, line := range fx.CommandLines() {
		if strings.Contains(line, "luksOpen") &&
			strings.Contains(line, "/dev/sda") &&
			strings.Contains(line, "pvc-1-crypt") {
			return
		}
	}

	t.Errorf("expected luksOpen pipeline; got %v", fx.CommandLines())
}

// TestCloseRunsLuksClose.
func TestCloseRunsLuksClose(t *testing.T) {
	fx := storage.NewFakeExec()

	c := luks.NewCryptsetup(fx)

	err := c.Close(t.Context(), "pvc-1-crypt")
	if err != nil {
		t.Fatalf("Close: %v", err)
	}

	want := "cryptsetup luksClose pvc-1-crypt"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("expected %q; got %v", want, fx.CommandLines())
	}
}

// TestDevicePath: helper returns /dev/mapper/<name>.
func TestDevicePath(t *testing.T) {
	got := luks.DevicePath("pvc-1-crypt")
	want := "/dev/mapper/pvc-1-crypt"

	if got != want {
		t.Errorf("DevicePath: got %q, want %q", got, want)
	}
}

// TestResizeRunsCryptsetupResize: when the storage layer just grew
// the underlying LV/zvol/file, the satellite calls Cryptsetup.Resize
// before drbdadm resize so the dm-crypt target picks up the new
// size. Without this step, drbdadm only sees the original
// LUKS-mapped portion and the consumer's view stays at the old size.
//
// The argv must include the dm name and the --key-file - flag
// (passphrase fed via stdin to cryptsetup, not as a cryptsetup arg)
// — runWithKey now delivers the key via cmd.Stdin (Issue 175 fix),
// and the cryptsetup argv itself MUST NOT carry the secret. A
// regression that swapped to argv would leak the passphrase into
// cryptsetup's audit logs and (worse) into procfs.
func TestResizeRunsCryptsetupResize(t *testing.T) {
	fx := storage.NewFakeExec()
	c := luks.NewCryptsetup(fx)

	err := c.Resize(t.Context(), "pvc-grow-0-luks", []byte("supersecret"))
	if err != nil {
		t.Fatalf("Resize: %v", err)
	}

	saw := func(needles ...string) bool {
		for _, line := range fx.CommandLines() {
			ok := true
			for _, n := range needles {
				if !strings.Contains(line, n) {
					ok = false
					break
				}
			}
			if ok {
				return true
			}
		}
		return false
	}

	if !saw("resize", "pvc-grow-0-luks", "--key-file", "-") {
		t.Errorf("expected `cryptsetup resize pvc-grow-0-luks --key-file -`; got %v",
			fx.CommandLines())
	}

	// Defensive: the passphrase must reach cryptsetup via stdin,
	// not as a cryptsetup arg. A regression that put the passphrase
	// directly into argv would surface here.
	for _, line := range fx.CommandLines() {
		if strings.Contains(line, "supersecret") {
			t.Errorf("passphrase leaked into cryptsetup argv: %s", line)
		}
	}
}

// TestOpenErrorWraps pins the cockroachdb error wrap on Open: a
// `cryptsetup luksOpen` failure (wrong passphrase, dm name busy)
// must surface with the "luksOpen" keyword for operator grep.
//
// Post-Issue 175 the cryptsetup invocation is direct (no `sh -c`),
// so the Expect key matches the cryptsetup argv verbatim.
func TestOpenErrorWraps(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("cryptsetup luksOpen /dev/sda pvc-1 --key-file -",
		storage.FakeResponse{Err: errLuksFailed})

	c := luks.NewCryptsetup(fx)

	err := c.Open(t.Context(), "/dev/sda", "pvc-1", []byte("k"))
	if err == nil {
		t.Fatalf("Open: got nil, want error")
	}

	if !strings.Contains(err.Error(), "luksOpen") {
		t.Errorf("wrap: %q must contain \"luksOpen\"", err.Error())
	}
}

// TestCloseErrorWraps mirrors Open for the Close path.
func TestCloseErrorWraps(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("cryptsetup luksClose pvc-1",
		storage.FakeResponse{Err: errLuksFailed})

	c := luks.NewCryptsetup(fx)

	err := c.Close(t.Context(), "pvc-1")
	if err == nil {
		t.Fatalf("Close: got nil, want error")
	}

	if !strings.Contains(err.Error(), "luksClose") {
		t.Errorf("wrap: %q must contain \"luksClose\"", err.Error())
	}
}

// TestResizeErrorWraps mirrors Open for the Resize path. Pinned
// because resize failures are silently swallowed by the satellite
// reconciler today (best-effort); a regression that dropped the
// wrap would erase the operator-visible breadcrumb in the log.
//
// Post-Issue 175 the cryptsetup invocation is direct (no `sh -c`),
// so the Expect key matches the cryptsetup argv verbatim.
func TestResizeErrorWraps(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("cryptsetup resize pvc-1 --key-file -",
		storage.FakeResponse{Err: errLuksFailed})

	c := luks.NewCryptsetup(fx)

	err := c.Resize(t.Context(), "pvc-1", []byte("k"))
	if err == nil {
		t.Fatalf("Resize: got nil, want error")
	}

	if !strings.Contains(err.Error(), "luksResize") {
		t.Errorf("wrap: %q must contain \"luksResize\"", err.Error())
	}
}

var errLuksFailed = errors.New("cryptsetup: command failed")
