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

// Upstream-LINSTOR naming contract.
//
// Every backend builds a volume identifier from the same template
// LINSTOR carries in its Java providers:
//
//	FORMAT_RSC_TO_*_ID = "%s%s_%05d"   //  rscName + rscNameSuffix + volNo
//
// The suffix is only non-empty for DRBD-internal "extra" LVs (meta /
// external-meta layouts). Blockstor's storage layer never emits a
// suffix — internal metadata lives in the same volume — so the
// effective form is `<rsc>_<vol5>`. Each backend wraps that ID in its
// own device-path convention:
//
//	LVM      :  /dev/<vg>/<lv-id>
//	LVM_THIN :  /dev/<vg>/<lv-id>           (same, lvcreate uses --thinpool)
//	ZFS      :  /dev/zvol/<zpool>/<lv-id>
//	ZFS_THIN :  /dev/zvol/<zpool>/<lv-id>
//	FILE     :  <dir>/<lv-id>.img → losetup → /dev/loopN
//	FILE_THIN:  <dir>/<lv-id>.img → losetup → /dev/loopN
//
// This test pins the shell-out commands the providers issue so the
// expected backing names / paths don't drift from upstream — a rename
// here means linstor-csi or any operator that parses the device path
// breaks.

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/file"
	"github.com/cozystack/blockstor/pkg/storage/lvm"
	"github.com/cozystack/blockstor/pkg/storage/zfs"
)

func TestNamingContract_LVMThick(t *testing.T) {
	t.Parallel()

	const vg = "blockstor-lvm"

	fx := storage.NewFakeExec()
	p := lvm.NewThick(lvm.ThickConfig{VolumeGroup: vg}, fx)

	err := p.CreateVolume(t.Context(), storage.Volume{
		ResourceName: "pvc-42",
		VolumeNumber: 7,
		SizeKib:      1024,
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	wantLV := "pvc-42_00007"

	if !lineContains(fx.CommandLines(), "--name "+wantLV) {
		t.Errorf("lvcreate --name drift: want %q in calls, got %v",
			"--name "+wantLV, fx.CommandLines())
	}

	if !lineContains(fx.CommandLines(), " "+vg) {
		t.Errorf("lvcreate VG target drift: want %q in calls, got %v",
			vg, fx.CommandLines())
	}
}

func TestNamingContract_LVMThin(t *testing.T) {
	t.Parallel()

	const (
		vg   = "blockstor-lvm"
		pool = "thin"
	)

	fx := storage.NewFakeExec()
	p := lvm.NewThin(lvm.ThinConfig{VolumeGroup: vg, ThinPool: pool}, fx)

	err := p.CreateVolume(t.Context(), storage.Volume{
		ResourceName: "pvc-42",
		VolumeNumber: 7,
		SizeKib:      1024,
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	wantLV := "pvc-42_00007"

	// LVM-thin uses --thin + --name <lv-id> against the thin pool.
	if !lineContains(fx.CommandLines(), "--thin") {
		t.Errorf("lvcreate --thin drift: want --thin in calls, got %v", fx.CommandLines())
	}

	if !lineContains(fx.CommandLines(), "--name "+wantLV) {
		t.Errorf("lvcreate --name drift: want %q in calls, got %v",
			"--name "+wantLV, fx.CommandLines())
	}

	if !lineContains(fx.CommandLines(), vg+"/"+pool) {
		t.Errorf("lvcreate target pool drift: want %q in calls, got %v",
			vg+"/"+pool, fx.CommandLines())
	}
}

func TestNamingContract_ZFSThick(t *testing.T) {
	t.Parallel()

	const pool = "blockstor-zfs"

	fx := storage.NewFakeExec()
	p := zfs.NewProvider(zfs.Config{Pool: pool, Thin: false}, fx)

	err := p.CreateVolume(t.Context(), storage.Volume{
		ResourceName: "pvc-42",
		VolumeNumber: 7,
		SizeKib:      1024,
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	wantDataset := pool + "/pvc-42_00007"
	if !lineContains(fx.CommandLines(), wantDataset) {
		t.Errorf("zfs create dataset drift: want %q in calls, got %v",
			wantDataset, fx.CommandLines())
	}

	// Wire `zfs list` so VolumeStatus walks the PROVISIONED branch and
	// returns the upstream-shaped /dev/zvol/<pool>/<lv-id> path.
	fx.Expect("zfs list -H -p -o name,volsize,used "+wantDataset,
		storage.FakeResponse{Stdout: []byte(wantDataset + "\t1048576\t1024\n")})

	status, err := p.VolumeStatus(t.Context(), storage.Volume{
		ResourceName: "pvc-42",
		VolumeNumber: 7,
	})
	if err != nil {
		t.Fatalf("VolumeStatus: %v", err)
	}

	wantDev := "/dev/zvol/" + wantDataset
	if status.DevicePath != wantDev {
		t.Errorf("ZFS DevicePath drift: got %q, want %q", status.DevicePath, wantDev)
	}
}

func TestNamingContract_ZFSThin(t *testing.T) {
	t.Parallel()

	const pool = "blockstor-zfs"

	fx := storage.NewFakeExec()
	p := zfs.NewProvider(zfs.Config{Pool: pool, Thin: true}, fx)

	err := p.CreateVolume(t.Context(), storage.Volume{
		ResourceName: "pvc-42",
		VolumeNumber: 7,
		SizeKib:      1024,
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	wantDataset := pool + "/pvc-42_00007"
	if !lineContains(fx.CommandLines(), wantDataset) {
		t.Errorf("zfs create -s (thin) dataset drift: want %q in calls, got %v",
			wantDataset, fx.CommandLines())
	}
}

func TestNamingContract_FILE(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	wantPath := filepath.Join(dir, "pvc-42_00007.img")

	fx := storage.NewFakeExec()
	// Wire the losetup pair so CreateVolume can complete.
	fx.Expect("losetup -j "+wantPath, storage.FakeResponse{})
	fx.Expect("losetup --find --show "+wantPath, storage.FakeResponse{Stdout: []byte("/dev/loop99\n")})

	p := file.NewProvider(file.Config{Dir: dir}, fx)

	err := p.CreateVolume(t.Context(), storage.Volume{
		ResourceName: "pvc-42",
		VolumeNumber: 7,
		SizeKib:      1024,
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	wantAlloc := "fallocate -l 1048576 " + wantPath
	if !slices.Contains(fx.CommandLines(), wantAlloc) {
		t.Errorf("file (thick) allocator drift: want %q, got %v",
			wantAlloc, fx.CommandLines())
	}

	wantAttach := "losetup --find --show " + wantPath
	if !slices.Contains(fx.CommandLines(), wantAttach) {
		t.Errorf("file (thick) losetup drift: want %q, got %v",
			wantAttach, fx.CommandLines())
	}
}

func TestNamingContract_FILEThin(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	wantPath := filepath.Join(dir, "pvc-42_00007.img")

	fx := storage.NewFakeExec()
	fx.Expect("losetup -j "+wantPath, storage.FakeResponse{})
	fx.Expect("losetup --find --show "+wantPath, storage.FakeResponse{Stdout: []byte("/dev/loop99\n")})

	p := file.NewProvider(file.Config{Dir: dir, Thin: true}, fx)

	err := p.CreateVolume(t.Context(), storage.Volume{
		ResourceName: "pvc-42",
		VolumeNumber: 7,
		SizeKib:      1024,
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	wantAlloc := "truncate -s 1048576 " + wantPath
	if !slices.Contains(fx.CommandLines(), wantAlloc) {
		t.Errorf("file (thin) allocator drift: want %q, got %v",
			wantAlloc, fx.CommandLines())
	}

	// VolumeStatus on FILE/FILE_THIN must surface /dev/loopN, not the
	// raw file path — that's what DRBD attaches against. Create the
	// backing file so stat-before-attach passes.
	f, err := os.Create(wantPath)
	if err != nil {
		t.Fatalf("create backing file: %v", err)
	}
	_ = f.Close()

	status, err := p.VolumeStatus(t.Context(), storage.Volume{
		ResourceName: "pvc-42",
		VolumeNumber: 7,
	})
	if err != nil {
		t.Fatalf("VolumeStatus: %v", err)
	}

	if !strings.HasPrefix(status.DevicePath, "/dev/loop") {
		t.Errorf("FILE_THIN DevicePath must be /dev/loopN, got %q",
			status.DevicePath)
	}
}

// lineContains reports whether any line in lines contains substr.
func lineContains(lines []string, substr string) bool {
	for _, l := range lines {
		if strings.Contains(l, substr) {
			return true
		}
	}

	return false
}
