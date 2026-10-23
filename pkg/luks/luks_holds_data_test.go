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

package luks_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/luks"
	"github.com/cozystack/blockstor/pkg/storage"
)

var errProbeIO = errors.New("blkid: /dev/sda: Input/output error")

const blkidCmd = "blkid -p -o export /dev/sda"

// anyBlockDevice takes every path for a block device, since the devices
// these tests format exist only in the fake Exec.
func anyBlockDevice() luks.Option {
	return luks.WithBlockDeviceCheck(func(string) (bool, error) { return true, nil })
}

// formatOutcome is what Format did to a device: its error, and whether
// it ran luksFormat.
type formatOutcome struct {
	err       error
	formatted bool
}

// formatOver runs Format on a non-LUKS /dev/sda whose blkid probe
// answers with probe.
func formatOver(t *testing.T, probe storage.FakeResponse) formatOutcome {
	t.Helper()

	fx := storage.NewFakeExec()
	fx.Expect("cryptsetup isLuks /dev/sda", storage.FakeResponse{Err: errFakeNotLuks})
	fx.Expect(blkidCmd, probe)

	out := formatOutcome{err: luks.NewCryptsetup(fx, anyBlockDevice()).Format(t.Context(), "/dev/sda", []byte("k"))}

	for _, line := range fx.CommandLines() {
		if strings.Contains(line, "luksFormat") {
			out.formatted = true
		}
	}

	return out
}

// A volume that already went to disk without LUKS carries DRBD
// internal metadata, a filesystem, or both. luksFormat over it would
// destroy the data, so Format refuses and names what it found.
func TestFormatRefusesADeviceThatHoldsData(t *testing.T) {
	t.Parallel()

	for name, probe := range map[string]struct {
		stdout string
		want   string
	}{
		"DRBD internal metadata": {stdout: "DEVNAME=/dev/sda\nTYPE=drbd\n", want: "TYPE=drbd"},
		"filesystem":             {stdout: "DEVNAME=/dev/sda\nUUID=1\nTYPE=ext4\nUSAGE=filesystem\n", want: "TYPE=ext4"},
		"partition table":        {stdout: "DEVNAME=/dev/sda\nPTTYPE=gpt\n", want: "PTTYPE=gpt"},
		"unnamed signature":      {stdout: "", want: "unnamed signature"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got := formatOver(t, storage.FakeResponse{Stdout: []byte(probe.stdout)})
			err, formatted := got.err, got.formatted
			if !errors.Is(err, luks.ErrDeviceHoldsData) {
				t.Fatalf("Format: %v, want ErrDeviceHoldsData", err)
			}

			if !strings.Contains(err.Error(), probe.want) {
				t.Errorf("error %q does not name %s", err, probe.want)
			}

			if formatted {
				t.Error("luksFormat ran over a device that holds data")
			}
		})
	}
}

// A probe that cannot say the device is empty is no licence to
// format it: several signatures at once (blkid's ambivalent exit 8) or a
// failed read both stop Format.
func TestFormatFailsClosedOnAnInconclusiveProbe(t *testing.T) {
	t.Parallel()

	for name, probeErr := range map[string]error{
		"ambivalent": storage.FakeExitError{Code: 8},
		"read error": errProbeIO,
		// blkid exits 2 for "nothing found" and for a device it could not
		// open; only its stderr tells them apart.
		"cannot open": fmt.Errorf("%w: blkid -p -o export /dev/sda: blkid: error: /dev/sda: Permission denied", storage.FakeExitError{Code: 2}),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got := formatOver(t, storage.FakeResponse{Err: probeErr})
			err, formatted := got.err, got.formatted
			if err == nil || !errors.Is(err, probeErr) {
				t.Fatalf("Format: %v, want the probe's error", err)
			}

			if formatted {
				t.Error("luksFormat ran after an inconclusive probe")
			}
		})
	}

	// blkid can exit 2 without a word for a path it could not open, so
	// "nothing found" counts only for a path that stats as a block device.
	dir := t.TempDir()
	regular := filepath.Join(dir, "regular")

	err := os.WriteFile(regular, nil, 0o600)
	if err != nil {
		t.Fatal(err)
	}

	for name, device := range map[string]string{
		"missing path": filepath.Join(dir, "missing"),
		"regular file": regular,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			fx := storage.NewFakeExec()
			fx.Expect("cryptsetup isLuks "+device, storage.FakeResponse{Err: errFakeNotLuks})
			fx.Expect("blkid -p -o export "+device, storage.FakeResponse{Err: storage.FakeExitError{Code: 2}})

			err := luks.NewCryptsetup(fx).Format(t.Context(), device, []byte("k"))
			if err == nil {
				t.Fatal("Format succeeded over a path that is not a block device")
			}

			for _, line := range fx.CommandLines() {
				if strings.Contains(line, "luksFormat") {
					t.Error("luksFormat ran over a path that is not a block device")
				}
			}
		})
	}
}

// An existing LUKS header is the idempotent path: no probe, no format.
func TestFormatOfALUKSDeviceDoesNotProbe(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	fx.Expect("cryptsetup isLuks /dev/sda", storage.FakeResponse{})

	err := luks.NewCryptsetup(fx, anyBlockDevice()).Format(t.Context(), "/dev/sda", []byte("k"))
	if err != nil {
		t.Fatalf("Format: %v", err)
	}

	for _, line := range fx.CommandLines() {
		if strings.HasPrefix(line, "blkid") || strings.Contains(line, "luksFormat") {
			t.Errorf("unexpected call on a LUKS device: %s", line)
		}
	}
}

// Only "nothing found" lets Format through.
func TestFormatFormatsAnEmptyDevice(t *testing.T) {
	t.Parallel()

	got := formatOver(t, storage.FakeResponse{Err: storage.FakeExitError{Code: 2}})
	if got.err != nil {
		t.Fatalf("Format: %v", got.err)
	}

	if !got.formatted {
		t.Error("luksFormat did not run on an empty device")
	}
}

// realBlkidStatus runs blkid as a real process that exits with the
// given status, and every other command through the fake: the exit
// status has to survive RealExec's error wrapping to be read.
type realBlkidStatus struct {
	*storage.FakeExec

	status string
}

func (r realBlkidStatus) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	return r.RunWithStdin(ctx, nil, name, args...)
}

func (r realBlkidStatus) RunWithStdin(ctx context.Context, stdin io.Reader, name string, args ...string) ([]byte, error) {
	var (
		out []byte
		err error
	)

	if name == "blkid" {
		out, err = storage.RealExec{}.Run(ctx, "sh", "-c", "exit "+r.status)
	} else {
		out, err = r.FakeExec.RunWithStdin(ctx, stdin, name, args...)
	}

	if err != nil {
		return out, fmt.Errorf("%s: %w", name, err)
	}

	return out, nil
}

// The empty-device status is read off a real process exit, through
// RealExec's wrapping, not only off the fake.
func TestFormatReadsBlkidsRealExitStatus(t *testing.T) {
	t.Parallel()

	for status, wantFormat := range map[string]bool{"2": true, "8": false} {
		t.Run("exit "+status, func(t *testing.T) {
			t.Parallel()

			fx := storage.NewFakeExec()
			fx.Expect("cryptsetup isLuks /dev/sda", storage.FakeResponse{Err: errFakeNotLuks})

			err := luks.NewCryptsetup(realBlkidStatus{FakeExec: fx, status: status}, anyBlockDevice()).Format(t.Context(), "/dev/sda", []byte("k"))
			if (err == nil) != wantFormat {
				t.Fatalf("Format after blkid exit %s: %v", status, err)
			}
		})
	}
}
