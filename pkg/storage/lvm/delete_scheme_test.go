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
	"errors"
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/lvm"
)

var (
	errLVMissing = errors.New(`exit status 5: Failed to find logical volume "vg/pvc-1_00000"`)
	errVGLocked  = errors.New("exit status 5: Volume group vg is locked by another process")
)

func lvsName(lv string) string {
	return "lvs " + strings.Join(lvm.Args("--noheadings", "-o", "lv_name", "vg/"+lv), " ")
}

func lvsSnapshotsOf(lv string) string {
	return "lvs " + strings.Join(lvm.Args("--noheadings", "-o", "lv_name", "-S", "origin="+lv, "vg"), " ")
}

func removed(fx *storage.FakeExec) []string {
	var out []string

	for _, line := range fx.CommandLines() {
		if strings.HasPrefix(line, "lvremove ") {
			out = append(out, line)
		}
	}

	return out
}

func pvc1() storage.Volume { return storage.Volume{ResourceName: "pvc-1"} }

// A failing probe must not read as "missing" on any delete path: the caller
// takes a nil return for "the storage is gone" and releases its finalizer.
func TestLVMDeleteProbeFailureIsAnError(t *testing.T) {
	t.Parallel()

	snap := storage.Snapshot{ResourceName: "pvc-1", SnapshotName: "s1"}

	cases := map[string]func(*storage.FakeExec) error{
		"thin volume": func(fx *storage.FakeExec) error {
			return lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx).DeleteVolume(t.Context(), pvc1())
		},
		"thick volume": func(fx *storage.FakeExec) error {
			return lvm.NewThick(lvm.ThickConfig{VolumeGroup: "vg"}, fx).DeleteVolume(t.Context(), pvc1())
		},
		"thin snapshot": func(fx *storage.FakeExec) error {
			return lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx).DeleteSnapshot(t.Context(), snap)
		},
		"thick snapshot": func(fx *storage.FakeExec) error {
			return lvm.NewThick(lvm.ThickConfig{VolumeGroup: "vg"}, fx).DeleteSnapshot(t.Context(), snap)
		},
	}

	for name, del := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			fx := storage.NewFakeExec()
			fx.Expect(lvsName("pvc-1_00000"), storage.FakeResponse{Err: errVGLocked})
			fx.Expect(lvsName("pvc-1_s1_00000"), storage.FakeResponse{Err: errVGLocked})

			err := del(fx)
			if err == nil {
				t.Fatalf("delete with a failing probe: got nil, want an error")
			}

			if got := removed(fx); len(got) != 0 {
				t.Errorf("lvremove ran on a failed probe: %v", got)
			}
		})
	}
}

// LVM saying the LV is not there is the one answer that means "missing".
func TestLVMDeleteMissingIsNoop(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	fx.Expect(lvsName("pvc-1_00000"), storage.FakeResponse{Err: errLVMissing})
	fx.Expect(lvsName("pvc-1_00000.bsnew"), storage.FakeResponse{Err: errLVMissing})

	err := lvm.NewThick(lvm.ThickConfig{VolumeGroup: "vg"}, fx).DeleteVolume(t.Context(), pvc1())
	if err != nil {
		t.Fatalf("DeleteVolume of a missing LV: %v", err)
	}

	if got := removed(fx); len(got) != 0 {
		t.Errorf("lvremove ran on a missing LV: %v", got)
	}
}

// A thick LV with snapshots is not removed: lvremove would take its
// copy-on-write snapshots with it.
func TestThickDeleteRefusesAnOriginWithSnapshots(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	fx.Expect(lvsName("pvc-1_00000"), storage.FakeResponse{Stdout: []byte("  pvc-1_00000\n")})
	fx.Expect(lvsSnapshotsOf("pvc-1_00000"), storage.FakeResponse{Stdout: []byte("  pvc-1_s1_00000\n")})

	err := lvm.NewThick(lvm.ThickConfig{VolumeGroup: "vg"}, fx).DeleteVolume(t.Context(), pvc1())
	if err == nil || !strings.Contains(err.Error(), "pvc-1_s1_00000") {
		t.Fatalf("DeleteVolume of an origin with snapshots: got %v, want a refusal naming the snapshot", err)
	}

	if got := removed(fx); len(got) != 0 {
		t.Errorf("lvremove ran on an origin with snapshots: %v", got)
	}
}

// Without snapshots the thick LV is removed as before.
func TestThickDeleteRemovesAnOriginWithoutSnapshots(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	fx.Expect(lvsName("pvc-1_00000"), storage.FakeResponse{Stdout: []byte("  pvc-1_00000\n")})

	err := lvm.NewThick(lvm.ThickConfig{VolumeGroup: "vg"}, fx).DeleteVolume(t.Context(), pvc1())
	if err != nil {
		t.Fatalf("DeleteVolume: %v", err)
	}

	want := "lvremove " + strings.Join(lvm.Args("--force", "vg/pvc-1_00000"), " ")
	if got := removed(fx); len(got) != 1 || got[0] != want {
		t.Errorf("lvremove calls %v, want [%s]", got, want)
	}
}
