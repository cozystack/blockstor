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
