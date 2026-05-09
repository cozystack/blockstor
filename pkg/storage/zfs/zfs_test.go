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
	"errors"
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

// TestZFSResizeVolumeIssuesZfsSet locks `zfs set volsize=<MiB>M`
// as the resize command. Used by the satellite reconciler when a
// VolumeDefinition update bumps the size.
func TestZFSResizeVolumeIssuesZfsSet(t *testing.T) {
	fx := storage.NewFakeExec()
	p := zfs.NewProvider(zfs.Config{Pool: "tank"}, fx)

	err := p.ResizeVolume(t.Context(), storage.Volume{
		ResourceName: "pvc-1",
		VolumeNumber: 0,
		SizeKib:      2048 * 1024,
	})
	if err != nil {
		t.Fatalf("ResizeVolume: %v", err)
	}

	want := "zfs set volsize=2048M tank/pvc-1_00000"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("expected %q in calls; got %v", want, fx.CommandLines())
	}
}

// TestVolumeStatusMissing: `zfs list` errors out → NOT_PROVISIONED
// (zfs returns non-zero when the dataset doesn't exist; we treat
// that as "not yet created", same as an empty stdout).
func TestVolumeStatusMissing(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect(
		"zfs list -H -p -o name,volsize,used tank/ghost_00000",
		storage.FakeResponse{Err: errZFSListMissing},
	)

	p := zfs.NewProvider(zfs.Config{Pool: "tank"}, fx)

	got, err := p.VolumeStatus(t.Context(), storage.Volume{
		ResourceName: "ghost",
		VolumeNumber: 0,
	})
	if err != nil {
		t.Fatalf("VolumeStatus: %v", err)
	}

	if got.State != "NOT_PROVISIONED" {
		t.Errorf("State: got %q, want NOT_PROVISIONED", got.State)
	}
}

// TestVolumeStatusEmptyOutput: `zfs list` returns empty output (no
// error, just nothing on stdout) — same NOT_PROVISIONED treatment.
// Pins the dual no-error / non-empty-but-malformed branch.
func TestVolumeStatusEmptyOutput(t *testing.T) {
	fx := storage.NewFakeExec()
	// Default is empty stdout, no error.

	p := zfs.NewProvider(zfs.Config{Pool: "tank"}, fx)

	got, err := p.VolumeStatus(t.Context(), storage.Volume{
		ResourceName: "ghost",
		VolumeNumber: 0,
	})
	if err != nil {
		t.Fatalf("VolumeStatus: %v", err)
	}

	if got.State != "NOT_PROVISIONED" {
		t.Errorf("State: got %q, want NOT_PROVISIONED", got.State)
	}
}

// TestVolumeStatusBadColumns: `zfs list` output that doesn't match
// the expected column count must surface a descriptive error rather
// than panic on slice access.
func TestVolumeStatusBadColumns(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect(
		"zfs list -H -p -o name,volsize,used tank/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("only one column\n")},
	)

	p := zfs.NewProvider(zfs.Config{Pool: "tank"}, fx)

	_, err := p.VolumeStatus(t.Context(), storage.Volume{ResourceName: "pvc-1"})
	if err == nil {
		t.Errorf("expected error on malformed zfs list output")
	}
}

// TestPoolStatusBadColumns: zpool list output with the wrong number
// of columns surfaces a parse error, not a slice panic.
func TestPoolStatusBadColumns(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect(
		"zpool list -H -p -o size,free tank",
		storage.FakeResponse{Stdout: []byte("only_one_field\n")},
	)

	p := zfs.NewProvider(zfs.Config{Pool: "tank"}, fx)

	_, err := p.PoolStatus(t.Context())
	if err == nil {
		t.Errorf("expected error on malformed zpool list output")
	}
}

// TestPoolStatusBadNumbers: well-shaped output with non-numeric
// fields → ParseInt error, no panic.
func TestPoolStatusBadNumbers(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect(
		"zpool list -H -p -o size,free tank",
		storage.FakeResponse{Stdout: []byte("nope\tnope\n")},
	)

	p := zfs.NewProvider(zfs.Config{Pool: "tank"}, fx)

	_, err := p.PoolStatus(t.Context())
	if err == nil {
		t.Errorf("expected ParseInt error on non-numeric fields")
	}
}

var errZFSListMissing = errors.New("dataset does not exist")
