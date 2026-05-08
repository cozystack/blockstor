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
