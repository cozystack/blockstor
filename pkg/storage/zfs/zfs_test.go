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

package zfs_test

import (
	"slices"
	"testing"

	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/zfs"
)

// TestKind: ZFS provider declares the LINSTOR kind.
func TestKind(t *testing.T) {
	p := zfs.NewProvider(zfs.Config{Pool: "tank"}, storage.NewFakeExec())
	if got := p.Kind(); got != "ZFS" {
		t.Errorf("Kind: got %q, want ZFS", got)
	}
}

// TestThinKind: thin variant declares ZFS_THIN.
func TestThinKind(t *testing.T) {
	p := zfs.NewProvider(zfs.Config{Pool: "tank", Thin: true}, storage.NewFakeExec())
	if got := p.Kind(); got != "ZFS_THIN" {
		t.Errorf("Kind: got %q, want ZFS_THIN", got)
	}
}

// TestCreateVolumeThick uses zfs create with -V (no -s for thick).
func TestCreateVolumeThick(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("zfs list -H -o name tank/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("")})

	p := zfs.NewProvider(zfs.Config{Pool: "tank"}, fx)

	err := p.CreateVolume(t.Context(), storage.Volume{
		ResourceName: "pvc-1",
		VolumeNumber: 0,
		SizeKib:      1024 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	want := "zfs create -V 1024M tank/pvc-1_00000"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("expected %q in calls; got %v", want, fx.CommandLines())
	}
}

// TestCreateVolumeThin adds -s for sparse (thin) volumes.
func TestCreateVolumeThin(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("zfs list -H -o name tank/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("")})

	p := zfs.NewProvider(zfs.Config{Pool: "tank", Thin: true}, fx)

	err := p.CreateVolume(t.Context(), storage.Volume{
		ResourceName: "pvc-1",
		VolumeNumber: 0,
		SizeKib:      1024 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	want := "zfs create -s -V 1024M tank/pvc-1_00000"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("expected %q in calls; got %v", want, fx.CommandLines())
	}
}

// TestCreateVolumeIdempotent: existing dataset → no-op.
func TestCreateVolumeIdempotent(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("zfs list -H -o name tank/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("tank/pvc-1_00000\n")})

	p := zfs.NewProvider(zfs.Config{Pool: "tank"}, fx)

	err := p.CreateVolume(t.Context(), storage.Volume{
		ResourceName: "pvc-1",
		VolumeNumber: 0,
		SizeKib:      1024 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	for _, line := range fx.CommandLines() {
		if len(line) >= 11 && line[:11] == "zfs create " {
			t.Errorf("idempotent CreateVolume issued zfs create: %s", line)
		}
	}
}

// TestDeleteVolume issues zfs destroy -r.
func TestDeleteVolume(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("zfs list -H -o name tank/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("tank/pvc-1_00000\n")})

	p := zfs.NewProvider(zfs.Config{Pool: "tank"}, fx)

	err := p.DeleteVolume(t.Context(), storage.Volume{
		ResourceName: "pvc-1",
		VolumeNumber: 0,
	})
	if err != nil {
		t.Fatalf("DeleteVolume: %v", err)
	}

	want := "zfs destroy -r tank/pvc-1_00000"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("expected %q in calls; got %v", want, fx.CommandLines())
	}
}

// TestVolumeStatusParsesZfsList: parses pipe-separated list.
func TestVolumeStatusParsesZfsList(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("zfs list -H -p -o name,volsize,used tank/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("tank/pvc-1_00000\t1073741824\t512\n")})

	p := zfs.NewProvider(zfs.Config{Pool: "tank"}, fx)

	got, err := p.VolumeStatus(t.Context(), storage.Volume{
		ResourceName: "pvc-1",
		VolumeNumber: 0,
	})
	if err != nil {
		t.Fatalf("VolumeStatus: %v", err)
	}

	if got.DevicePath != "/dev/zvol/tank/pvc-1_00000" {
		t.Errorf("DevicePath: got %q", got.DevicePath)
	}

	if got.UsableKib != 1048576 { // 1 GiB / 1024
		t.Errorf("UsableKib: got %d, want 1048576", got.UsableKib)
	}

	if got.State != "PROVISIONED" {
		t.Errorf("State: got %q, want PROVISIONED", got.State)
	}
}

// TestPoolStatusParsesZpoolGet: free + total via zpool list -p.
func TestPoolStatusParsesZpoolGet(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("zpool list -H -p -o size,free tank",
		storage.FakeResponse{Stdout: []byte("107374182400\t80530636800\n")})

	p := zfs.NewProvider(zfs.Config{Pool: "tank"}, fx)

	got, err := p.PoolStatus(t.Context())
	if err != nil {
		t.Fatalf("PoolStatus: %v", err)
	}

	if got.TotalCapacityKib != 104857600 {
		t.Errorf("TotalCapacityKib: got %d, want 104857600", got.TotalCapacityKib)
	}

	if got.FreeCapacityKib != 78643200 {
		t.Errorf("FreeCapacityKib: got %d, want 78643200", got.FreeCapacityKib)
	}

	if !got.SupportsSnapshots {
		t.Errorf("SupportsSnapshots: got false, want true")
	}
}

// TestCreateSnapshotIssuesZfsSnap.
func TestCreateSnapshotIssuesZfsSnap(t *testing.T) {
	fx := storage.NewFakeExec()
	p := zfs.NewProvider(zfs.Config{Pool: "tank"}, fx)

	err := p.CreateSnapshot(t.Context(), storage.Snapshot{
		ResourceName: "pvc-1",
		SnapshotName: "snap-1",
	})
	if err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}

	want := "zfs snapshot tank/pvc-1_00000@snap-1"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("expected %q in calls; got %v", want, fx.CommandLines())
	}
}

// TestDeleteSnapshotIssuesZfsDestroy.
func TestDeleteSnapshotIssuesZfsDestroy(t *testing.T) {
	fx := storage.NewFakeExec()
	p := zfs.NewProvider(zfs.Config{Pool: "tank"}, fx)

	err := p.DeleteSnapshot(t.Context(), storage.Snapshot{
		ResourceName: "pvc-1",
		SnapshotName: "snap-1",
	})
	if err != nil {
		t.Fatalf("DeleteSnapshot: %v", err)
	}

	want := "zfs destroy tank/pvc-1_00000@snap-1"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("expected %q in calls; got %v", want, fx.CommandLines())
	}
}
