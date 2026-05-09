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
	"slices"
	"testing"

	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/lvm"
)

// TestThickKind: round-trip the LINSTOR provider kind verbatim.
func TestThickKind(t *testing.T) {
	p := lvm.NewThick(lvm.ThickConfig{VolumeGroup: "vg"}, storage.NewFakeExec())
	if got := p.Kind(); got != "LVM" {
		t.Errorf("Kind: got %q, want LVM", got)
	}
}

// TestThickCreateVolumeIssuesLvcreate pins the create command shape.
// Diverges from Thin: no --thin / --virtualsize, uses --size + udev
// workarounds (-Wn -Zn + activation{udev_sync=0 udev_rules=0}) so the
// satellite container without a udev daemon doesn't trip on missing
// /dev symlinks.
func TestThickCreateVolumeIssuesLvcreate(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("lvs --noheadings -o lv_name vg/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("")})

	p := lvm.NewThick(lvm.ThickConfig{VolumeGroup: "vg"}, fx)

	err := p.CreateVolume(t.Context(), storage.Volume{
		ResourceName: "pvc-1",
		VolumeNumber: 0,
		SizeKib:      1024 * 1024, // 1 GiB
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	want := "lvcreate --size 1024MiB --name pvc-1_00000 --config activation{udev_sync=0 udev_rules=0} -Wn -Zn vg"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("expected %q in calls; got %v", want, fx.CommandLines())
	}
}

// TestThickCreateVolumeIdempotent: existing LV → no lvcreate.
func TestThickCreateVolumeIdempotent(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("lvs --noheadings -o lv_name vg/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("pvc-1_00000\n")})

	p := lvm.NewThick(lvm.ThickConfig{VolumeGroup: "vg"}, fx)

	err := p.CreateVolume(t.Context(), storage.Volume{
		ResourceName: "pvc-1",
		VolumeNumber: 0,
		SizeKib:      1024 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	for _, line := range fx.CommandLines() {
		if line == "lvcreate" || (len(line) > 9 && line[:9] == "lvcreate ") {
			t.Errorf("idempotent CreateVolume issued lvcreate: %s", line)
		}
	}
}

// TestThickResizeVolumeIssuesLvextend mirrors the thin variant.
func TestThickResizeVolumeIssuesLvextend(t *testing.T) {
	fx := storage.NewFakeExec()
	p := lvm.NewThick(lvm.ThickConfig{VolumeGroup: "vg"}, fx)

	err := p.ResizeVolume(t.Context(), storage.Volume{
		ResourceName: "pvc-1",
		VolumeNumber: 0,
		SizeKib:      2048 * 1024,
	})
	if err != nil {
		t.Fatalf("ResizeVolume: %v", err)
	}

	want := "lvextend --size 2048MiB vg/pvc-1_00000"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("expected %q; got %v", want, fx.CommandLines())
	}
}

// TestThickDeleteVolume: lvremove --force.
func TestThickDeleteVolume(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("lvs --noheadings -o lv_name vg/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("pvc-1_00000\n")})

	p := lvm.NewThick(lvm.ThickConfig{VolumeGroup: "vg"}, fx)

	err := p.DeleteVolume(t.Context(), storage.Volume{
		ResourceName: "pvc-1",
		VolumeNumber: 0,
	})
	if err != nil {
		t.Fatalf("DeleteVolume: %v", err)
	}

	want := "lvremove --force vg/pvc-1_00000"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("expected %q in calls; got %v", want, fx.CommandLines())
	}
}

// TestThickPoolStatusParsesVgs: PoolStatus reports VG free + total,
// snapshots disabled (LVM-classic has no copy-on-write equivalent of
// the thin pool snapshot store).
func TestThickPoolStatusParsesVgs(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("vgs --noheadings --separator | -o vg_size,vg_free --units k --nosuffix vg",
		storage.FakeResponse{Stdout: []byte("104857600.00|78643200.00\n")})

	p := lvm.NewThick(lvm.ThickConfig{VolumeGroup: "vg"}, fx)

	got, err := p.PoolStatus(t.Context())
	if err != nil {
		t.Fatalf("PoolStatus: %v", err)
	}

	if got.TotalCapacityKib != 104857600 {
		t.Errorf("TotalCapacityKib: got %d", got.TotalCapacityKib)
	}

	if got.FreeCapacityKib != 78643200 {
		t.Errorf("FreeCapacityKib: got %d", got.FreeCapacityKib)
	}

	if got.SupportsSnapshots {
		t.Errorf("SupportsSnapshots: got true, want false (LVM-classic)")
	}
}

// TestThickCreateSnapshot uses lvcreate --snapshot --extents 25%ORIGIN
// (thick-LV snapshots need an explicit allocation; 25 % is the
// hand-tuned tradeoff between waste and overflow).
func TestThickCreateSnapshot(t *testing.T) {
	fx := storage.NewFakeExec()
	p := lvm.NewThick(lvm.ThickConfig{VolumeGroup: "vg"}, fx)

	err := p.CreateSnapshot(t.Context(), storage.Snapshot{
		ResourceName: "pvc-1",
		SnapshotName: "snap-1",
	})
	if err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}

	want := "lvcreate --snapshot --extents 25%ORIGIN --name pvc-1_snap-1_00000 vg/pvc-1_00000"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("expected %q in calls; got %v", want, fx.CommandLines())
	}
}

// TestThickDeleteSnapshot mirrors TestThinDeleteSnapshot: lvremove
// --force on the snapshot LV name. Same teardown shape as thin so
// LINSTOR's snapshot-delete REST call works against either kind.
func TestThickDeleteSnapshot(t *testing.T) {
	fx := storage.NewFakeExec()
	p := lvm.NewThick(lvm.ThickConfig{VolumeGroup: "vg"}, fx)

	err := p.DeleteSnapshot(t.Context(), storage.Snapshot{
		ResourceName: "pvc-1",
		SnapshotName: "snap-1",
	})
	if err != nil {
		t.Fatalf("DeleteSnapshot: %v", err)
	}

	want := "lvremove --force vg/pvc-1_snap-1_00000"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("expected %q in calls; got %v", want, fx.CommandLines())
	}
}
