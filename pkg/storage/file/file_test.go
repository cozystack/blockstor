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

package file_test

import (
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/file"
)

// TestKindThick: thick provider declares LINSTOR's `FILE` kind.
func TestKindThick(t *testing.T) {
	p := file.NewProvider(file.Config{Dir: t.TempDir()}, storage.NewFakeExec())
	if got := p.Kind(); got != "FILE" {
		t.Errorf("Kind: got %q, want FILE", got)
	}
}

// TestKindThin: thin variant declares FILE_THIN.
func TestKindThin(t *testing.T) {
	p := file.NewProvider(file.Config{Dir: t.TempDir(), Thin: true}, storage.NewFakeExec())
	if got := p.Kind(); got != "FILE_THIN" {
		t.Errorf("Kind: got %q, want FILE_THIN", got)
	}
}

// TestCreateVolumeAllocates: thick → fallocate to full size.
func TestCreateVolumeAllocates(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()

	p := file.NewProvider(file.Config{Dir: dir}, fx)

	err := p.CreateVolume(t.Context(), storage.Volume{
		ResourceName: "pvc-1",
		VolumeNumber: 0,
		SizeKib:      1024 * 1024, // 1 GiB
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	want := "fallocate -l 1073741824 " + filepath.Join(dir, "pvc-1_00000.img")
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("expected %q; got %v", want, fx.CommandLines())
	}
}

// TestCreateVolumeThinSparse: thin → truncate creates a sparse file
// (no allocation, just sets size).
func TestCreateVolumeThinSparse(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()

	p := file.NewProvider(file.Config{Dir: dir, Thin: true}, fx)

	err := p.CreateVolume(t.Context(), storage.Volume{
		ResourceName: "pvc-1",
		VolumeNumber: 0,
		SizeKib:      1024 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	want := "truncate -s 1073741824 " + filepath.Join(dir, "pvc-1_00000.img")
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("expected %q; got %v", want, fx.CommandLines())
	}
}

// TestCreateVolumeIdempotent: existing file → no-op.
func TestCreateVolumeIdempotent(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()

	// Pre-seed the volume file.
	preexisting := filepath.Join(dir, "pvc-1_00000.img")
	if err := os.WriteFile(preexisting, []byte{}, 0o600); err != nil {
		t.Fatalf("pre-seed: %v", err)
	}

	p := file.NewProvider(file.Config{Dir: dir}, fx)

	err := p.CreateVolume(t.Context(), storage.Volume{
		ResourceName: "pvc-1",
		VolumeNumber: 0,
		SizeKib:      1024 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	for _, line := range fx.CommandLines() {
		if line[:9] == "fallocate" || line[:8] == "truncate" {
			t.Errorf("idempotent CreateVolume issued allocator: %s", line)
		}
	}
}

// TestDeleteVolume removes the file. Idempotent on missing.
func TestDeleteVolume(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()

	preexisting := filepath.Join(dir, "pvc-1_00000.img")
	if err := os.WriteFile(preexisting, []byte("data"), 0o600); err != nil {
		t.Fatalf("pre-seed: %v", err)
	}

	p := file.NewProvider(file.Config{Dir: dir}, fx)

	err := p.DeleteVolume(t.Context(), storage.Volume{
		ResourceName: "pvc-1",
		VolumeNumber: 0,
	})
	if err != nil {
		t.Fatalf("DeleteVolume: %v", err)
	}

	if _, err := os.Stat(preexisting); !os.IsNotExist(err) {
		t.Errorf("expected file removed; got err=%v", err)
	}
}

// TestDeleteVolumeMissing: no error if the file isn't there.
func TestDeleteVolumeMissing(t *testing.T) {
	p := file.NewProvider(file.Config{Dir: t.TempDir()}, storage.NewFakeExec())

	err := p.DeleteVolume(t.Context(), storage.Volume{
		ResourceName: "ghost",
		VolumeNumber: 0,
	})
	if err != nil {
		t.Errorf("DeleteVolume(missing): %v", err)
	}
}

// TestVolumeStatusReportsSize: stat the file, report bytes / 1024.
func TestVolumeStatusReportsSize(t *testing.T) {
	dir := t.TempDir()

	body := make([]byte, 1024*1024) // 1 MiB
	if err := os.WriteFile(filepath.Join(dir, "pvc-1_00000.img"), body, 0o600); err != nil {
		t.Fatalf("seed: %v", err)
	}

	p := file.NewProvider(file.Config{Dir: dir}, storage.NewFakeExec())

	got, err := p.VolumeStatus(t.Context(), storage.Volume{
		ResourceName: "pvc-1",
		VolumeNumber: 0,
	})
	if err != nil {
		t.Fatalf("VolumeStatus: %v", err)
	}

	if got.State != "PROVISIONED" {
		t.Errorf("State: got %q, want PROVISIONED", got.State)
	}

	if got.UsableKib != 1024 {
		t.Errorf("UsableKib: got %d, want 1024", got.UsableKib)
	}
}

// TestVolumeStatusMissing: NOT_PROVISIONED.
func TestVolumeStatusMissing(t *testing.T) {
	p := file.NewProvider(file.Config{Dir: t.TempDir()}, storage.NewFakeExec())

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

// TestResizeVolumeGrows: bump SizeKib → truncate runs against the
// existing backing file with the new byte target. Pins the resize
// command shape CSI ControllerExpandVolume eventually drives.
func TestResizeVolumeGrows(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()

	// Pre-create a 1 GiB sparse backing file so ResizeVolume's stat
	// succeeds (truncate doesn't allocate, so the test stays cheap).
	path := filepath.Join(dir, "pvc-1_00000.img")
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create file: %v", err)
	}
	if err := f.Truncate(1024 * 1024 * 1024); err != nil {
		t.Fatalf("seed truncate: %v", err)
	}
	_ = f.Close()

	p := file.NewProvider(file.Config{Dir: dir}, fx)

	err = p.ResizeVolume(t.Context(), storage.Volume{
		ResourceName: "pvc-1",
		VolumeNumber: 0,
		SizeKib:      2 * 1024 * 1024, // 2 GiB
	})
	if err != nil {
		t.Fatalf("ResizeVolume: %v", err)
	}

	want := "truncate -s 2147483648 " + path
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("expected %q in calls; got %v", want, fx.CommandLines())
	}
}

// TestResizeVolumeMissingFile: resize against a missing path is a
// caller bug — surface error rather than silently re-creating.
func TestResizeVolumeMissingFile(t *testing.T) {
	dir := t.TempDir()

	p := file.NewProvider(file.Config{Dir: dir}, storage.NewFakeExec())

	err := p.ResizeVolume(t.Context(), storage.Volume{
		ResourceName: "pvc-1",
		VolumeNumber: 0,
		SizeKib:      1024 * 1024,
	})
	if err == nil {
		t.Errorf("expected error on missing file")
	}
}

// TestResizeVolumeShrinkNoOp: requested size <= current size is a
// silent no-op. Shrinking a backing file under DRBD would corrupt
// the replicated state, so we never emit truncate when target ≤ size.
func TestResizeVolumeShrinkNoOp(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()

	path := filepath.Join(dir, "pvc-1_00000.img")
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create file: %v", err)
	}
	if err := f.Truncate(2 * 1024 * 1024 * 1024); err != nil {
		t.Fatalf("seed truncate: %v", err)
	}
	_ = f.Close()

	p := file.NewProvider(file.Config{Dir: dir}, fx)

	err = p.ResizeVolume(t.Context(), storage.Volume{
		ResourceName: "pvc-1",
		VolumeNumber: 0,
		SizeKib:      1024 * 1024, // smaller than the 2 GiB on disk
	})
	if err != nil {
		t.Fatalf("ResizeVolume shrink: %v", err)
	}

	for _, line := range fx.CommandLines() {
		if len(line) >= 9 && line[:9] == "truncate " {
			t.Errorf("shrink must not emit truncate; got %s", line)
		}
	}
}

// TestSnapshotsUnsupported: file backend explicitly rejects snapshots
// so callers don't silently get a coherent-but-stale copy. Hard-link
// or `cp --reflink=auto` would be lossy on thick volumes.
func TestSnapshotsUnsupported(t *testing.T) {
	p := file.NewProvider(file.Config{Dir: t.TempDir()}, storage.NewFakeExec())

	err := p.CreateSnapshot(t.Context(), storage.Snapshot{ResourceName: "pvc-1", SnapshotName: "s1"})
	if err == nil {
		t.Errorf("CreateSnapshot must error")
	}

	err = p.DeleteSnapshot(t.Context(), storage.Snapshot{ResourceName: "pvc-1", SnapshotName: "s1"})
	if err == nil {
		t.Errorf("DeleteSnapshot must error")
	}
}

// TestPoolStatusReportsCapacity: PoolStatus reports the directory's
// statfs free / total in KiB. Exact figures depend on the host's
// filesystem so sanity-check the shape (positive, free <= total,
// snapshots disabled).
func TestPoolStatusReportsCapacity(t *testing.T) {
	dir := t.TempDir()
	p := file.NewProvider(file.Config{Dir: dir}, storage.NewFakeExec())

	got, err := p.PoolStatus(t.Context())
	if err != nil {
		t.Fatalf("PoolStatus: %v", err)
	}

	if got.TotalCapacityKib <= 0 {
		t.Errorf("TotalCapacityKib: got %d, want > 0", got.TotalCapacityKib)
	}

	if got.FreeCapacityKib < 0 || got.FreeCapacityKib > got.TotalCapacityKib {
		t.Errorf("FreeCapacityKib out of range: got %d, total %d",
			got.FreeCapacityKib, got.TotalCapacityKib)
	}

	if got.SupportsSnapshots {
		t.Errorf("SupportsSnapshots: file backend never supports snapshots")
	}
}
