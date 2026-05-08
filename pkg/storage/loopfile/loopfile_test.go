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

package loopfile_test

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/loopfile"
)

// TestKindFileThin: provider declares LINSTOR's FILE_THIN kind.
func TestKindFileThin(t *testing.T) {
	p := loopfile.NewProvider(loopfile.Config{Dir: t.TempDir()}, storage.NewFakeExec())
	if got := p.Kind(); got != "FILE_THIN" {
		t.Errorf("Kind: got %q, want FILE_THIN", got)
	}
}

// TestCreateVolumeTruncatesAndAttaches: fresh volume → truncate then
// losetup --find --show.
func TestCreateVolumeTruncatesAndAttaches(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()
	fx.Expect("losetup --find --show "+filepath.Join(dir, "pvc-1_00000.img"),
		storage.FakeResponse{Stdout: []byte("/dev/loop4\n")})

	p := loopfile.NewProvider(loopfile.Config{Dir: dir}, fx)

	err := p.CreateVolume(t.Context(), storage.Volume{
		ResourceName: "pvc-1",
		VolumeNumber: 0,
		SizeKib:      1024 * 1024, // 1 GiB
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	wantTrunc := "truncate -s 1073741824 " + filepath.Join(dir, "pvc-1_00000.img")
	wantLosetup := "losetup --find --show " + filepath.Join(dir, "pvc-1_00000.img")

	cmds := fx.CommandLines()
	if !slices.Contains(cmds, wantTrunc) {
		t.Errorf("expected %q; got %v", wantTrunc, cmds)
	}

	if !slices.Contains(cmds, wantLosetup) {
		t.Errorf("expected %q; got %v", wantLosetup, cmds)
	}
}

// TestCreateVolumeIdempotent: file already exists → skip truncate,
// still re-attach (losetup is itself idempotent).
func TestCreateVolumeIdempotent(t *testing.T) {
	dir := t.TempDir()

	if err := os.WriteFile(filepath.Join(dir, "pvc-1_00000.img"), []byte{}, 0o600); err != nil {
		t.Fatalf("seed: %v", err)
	}

	fx := storage.NewFakeExec()
	fx.Expect("losetup --find --show "+filepath.Join(dir, "pvc-1_00000.img"),
		storage.FakeResponse{Stdout: []byte("/dev/loop4\n")})

	p := loopfile.NewProvider(loopfile.Config{Dir: dir}, fx)

	err := p.CreateVolume(t.Context(), storage.Volume{
		ResourceName: "pvc-1",
		VolumeNumber: 0,
		SizeKib:      1024 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	for _, line := range fx.CommandLines() {
		if strings.HasPrefix(line, "truncate") {
			t.Errorf("idempotent CreateVolume issued truncate: %s", line)
		}
	}
}

// TestVolumeStatusReturnsDevicePath: status reports the loop device.
func TestVolumeStatusReturnsDevicePath(t *testing.T) {
	dir := t.TempDir()
	imgPath := filepath.Join(dir, "pvc-1_00000.img")

	body := make([]byte, 1024*1024) // 1 MiB
	if err := os.WriteFile(imgPath, body, 0o600); err != nil {
		t.Fatalf("seed: %v", err)
	}

	fx := storage.NewFakeExec()
	fx.Expect("losetup --find --show "+imgPath,
		storage.FakeResponse{Stdout: []byte("/dev/loop7\n")})

	p := loopfile.NewProvider(loopfile.Config{Dir: dir}, fx)

	got, err := p.VolumeStatus(t.Context(), storage.Volume{
		ResourceName: "pvc-1",
		VolumeNumber: 0,
	})
	if err != nil {
		t.Fatalf("VolumeStatus: %v", err)
	}

	if got.DevicePath != "/dev/loop7" {
		t.Errorf("DevicePath: got %q, want /dev/loop7", got.DevicePath)
	}

	if got.State != "PROVISIONED" {
		t.Errorf("State: got %q, want PROVISIONED", got.State)
	}
}

// TestVolumeStatusMissing: NOT_PROVISIONED for a non-existent volume.
func TestVolumeStatusMissing(t *testing.T) {
	p := loopfile.NewProvider(loopfile.Config{Dir: t.TempDir()}, storage.NewFakeExec())

	got, err := p.VolumeStatus(t.Context(), storage.Volume{ResourceName: "ghost", VolumeNumber: 0})
	if err != nil {
		t.Fatalf("VolumeStatus: %v", err)
	}

	if got.State != "NOT_PROVISIONED" {
		t.Errorf("State: got %q, want NOT_PROVISIONED", got.State)
	}
}

// TestDeleteVolumeDetachesAndRemoves: status path → losetup -d + os.Remove.
func TestDeleteVolumeDetachesAndRemoves(t *testing.T) {
	dir := t.TempDir()
	imgPath := filepath.Join(dir, "pvc-1_00000.img")

	if err := os.WriteFile(imgPath, []byte("data"), 0o600); err != nil {
		t.Fatalf("seed: %v", err)
	}

	fx := storage.NewFakeExec()
	fx.Expect("losetup -j "+imgPath,
		storage.FakeResponse{Stdout: []byte("/dev/loop4: [2049]:1234 (" + imgPath + ")\n")})

	p := loopfile.NewProvider(loopfile.Config{Dir: dir}, fx)

	err := p.DeleteVolume(t.Context(), storage.Volume{ResourceName: "pvc-1", VolumeNumber: 0})
	if err != nil {
		t.Fatalf("DeleteVolume: %v", err)
	}

	want := "losetup -d /dev/loop4"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("expected %q; got %v", want, fx.CommandLines())
	}

	if _, err := os.Stat(imgPath); !os.IsNotExist(err) {
		t.Errorf("file not removed: err=%v", err)
	}
}

// TestCreateVolumeReusesExistingLoop: when the backing file is
// already attached (losetup -j returns a device), CreateVolume
// must reuse it and NOT call `losetup --find --show` again. Without
// this guard reconcile-heavy paths leak hundreds of loop nodes
// pointing at the same backing file.
func TestCreateVolumeReusesExistingLoop(t *testing.T) {
	dir := t.TempDir()
	imgPath := filepath.Join(dir, "pvc-1_00000.img")

	// Pre-seed the backing file so CreateVolume skips truncate.
	if err := os.WriteFile(imgPath, []byte{}, 0o600); err != nil {
		t.Fatalf("seed: %v", err)
	}

	fx := storage.NewFakeExec()
	fx.Expect("losetup -j "+imgPath,
		storage.FakeResponse{Stdout: []byte("/dev/loop9: [2049]:42 (" + imgPath + ")\n")})

	p := loopfile.NewProvider(loopfile.Config{Dir: dir}, fx)

	err := p.CreateVolume(t.Context(), storage.Volume{
		ResourceName: "pvc-1",
		VolumeNumber: 0,
		SizeKib:      1024 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	for _, line := range fx.CommandLines() {
		if strings.HasPrefix(line, "losetup --find --show") {
			t.Errorf("re-attach issued: %s", line)
		}
	}
}

// TestSnapshotsUnsupported: both Create/Delete error out.
func TestSnapshotsUnsupported(t *testing.T) {
	p := loopfile.NewProvider(loopfile.Config{Dir: t.TempDir()}, storage.NewFakeExec())

	if err := p.CreateSnapshot(t.Context(), storage.Snapshot{}); err == nil {
		t.Errorf("CreateSnapshot: expected error")
	}

	if err := p.DeleteSnapshot(t.Context(), storage.Snapshot{}); err == nil {
		t.Errorf("DeleteSnapshot: expected error")
	}
}
