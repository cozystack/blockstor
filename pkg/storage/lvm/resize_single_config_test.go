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

package lvm_test

import (
	"context"
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/lvm"
)

// resizer is the slice of storage.Provider both LVM backends implement
// that this regression test exercises.
type resizer interface {
	ResizeVolume(ctx context.Context, vol storage.Volume) error
}

// TestResizeVolumeSingleConfig is the regression guard for the
// duplicate-`--config` resize bug: `lvextend` was invoked with TWO
// `--config` flags — one device
// filter injected by Args(...) and a second `--config
// activation{udev_sync=0 udev_rules=0}` added at the call site. The LVM
// CLI rejects a repeated `--config` ("Option --config may not be
// repeated."), so lvextend errored at command-line parse, never ran,
// and the resize hot-looped while the LV (and the DRBD device on top)
// stayed at the old size.
//
// The fix must produce EXACTLY ONE `--config` whose single value
// carries BOTH the device filter (the /dev/drbd and /dev/zd reject
// rules) AND the udev-less `activation{udev_sync=0 udev_rules=0}`
// section (Bug 269 — dropping either is a regression).
//
// This test FAILS against the pre-fix double-`--config` code and PASSES
// after it; it covers Thin AND Thick.
func TestResizeVolumeSingleConfig(t *testing.T) {
	cases := []struct {
		name     string
		provider func(ex storage.Exec) resizer
	}{
		{
			name: "thin",
			provider: func(ex storage.Exec) resizer {
				return lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "thinpool"}, ex)
			},
		},
		{
			name: "thick",
			provider: func(ex storage.Exec) resizer {
				return lvm.NewThick(lvm.ThickConfig{VolumeGroup: "vg"}, ex)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fx := storage.NewFakeExec()
			p := tc.provider(fx)

			err := p.ResizeVolume(t.Context(), storage.Volume{
				ResourceName: "pvc-1",
				VolumeNumber: 0,
				SizeKib:      2048 * 1024, // 2 GiB
			})
			if err != nil {
				t.Fatalf("ResizeVolume: %v", err)
			}

			call := findCall(t, fx, "lvextend")

			// Exactly one --config flag — a repeated --config is what the
			// LVM CLI rejected.
			cfgIdx, count := configFlag(call.Args)
			if count != 1 {
				t.Fatalf("lvextend argv must carry exactly one --config; got %d in %v",
					count, call.Args)
			}

			// The single --config value must carry BOTH sections.
			cfg := call.Args[cfgIdx+1]

			if !strings.Contains(cfg, "/dev/drbd") || !strings.Contains(cfg, "/dev/zd") {
				t.Errorf("--config value must keep the device filter (/dev/drbd, /dev/zd); got %q", cfg)
			}

			if !strings.Contains(cfg, "udev_sync=0") || !strings.Contains(cfg, "udev_rules=0") {
				t.Errorf("--config value must keep the udev-less activation settings "+
					"(udev_sync=0 udev_rules=0); got %q", cfg)
			}
		})
	}
}

// findCall returns the first recorded FakeExec call for the named
// command, failing the test if none was issued.
func findCall(t *testing.T, fx *storage.FakeExec, name string) storage.FakeCall {
	t.Helper()

	for _, c := range fx.Calls {
		if c.Name == name {
			return c
		}
	}

	t.Fatalf("expected a %q call; recorded %v", name, fx.CommandLines())

	return storage.FakeCall{}
}

// configFlag returns the index of the first `--config` token in args
// and the total number of `--config` tokens present.
func configFlag(args []string) (int, int) {
	first := -1
	count := 0

	for i, a := range args {
		if a == "--config" {
			count++
			if first == -1 {
				first = i
			}
		}
	}

	return first, count
}
