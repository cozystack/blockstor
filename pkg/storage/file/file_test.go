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
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/file"
)

// fakeLosetup pre-loads the FakeExec so it answers the two losetup
// invocations every CreateVolume / VolumeStatus path issues: a
// `losetup -j <path>` lookup (returns empty = no existing loop) and
// the `losetup --find --show <path>` attach (returns /dev/loop42).
// dev is the device the caller wants the fake to surface.
func fakeLosetup(fx *storage.FakeExec, path, dev string) {
	fx.Expect("losetup -j "+path, storage.FakeResponse{})
	fx.Expect("losetup --find --show "+path, storage.FakeResponse{Stdout: []byte(dev + "\n")})
}

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
	fakeLosetup(fx, filepath.Join(dir, "pvc-1_00000.img"), "/dev/loop42")

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
	fakeLosetup(fx, filepath.Join(dir, "pvc-1_00000.img"), "/dev/loop42")

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

// TestCreateVolumeIdempotent: existing file in THIN mode → no allocator
// run (sparse / overcommit is the FILE_THIN contract); the losetup attach
// still runs to ensure the loop dev exists.
//
// Note: the thick-mode idempotent-skip MUST issue `fallocate` on every
// reconcile (Bug 256) to reconcile a sparse-by-history backing file.
// That behaviour is pinned by TestThickCreateIdempotentSkipStillFallocates
// in idempotent_thick_bug_256_test.go — keeping it in a Thin-only test
// here makes the no-allocator invariant precisely scoped to the variant
// where it still holds.
func TestCreateVolumeIdempotent(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()

	// Pre-seed the volume file.
	preexisting := filepath.Join(dir, "pvc-1_00000.img")
	if err := os.WriteFile(preexisting, []byte{}, 0o600); err != nil {
		t.Fatalf("pre-seed: %v", err)
	}

	fakeLosetup(fx, preexisting, "/dev/loop42")

	p := file.NewProvider(file.Config{Dir: dir, Thin: true}, fx)

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
			t.Errorf("idempotent CreateVolume (THIN) issued allocator: %s", line)
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
	path := filepath.Join(dir, "pvc-1_00000.img")

	body := make([]byte, 1024*1024) // 1 MiB
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatalf("seed: %v", err)
	}

	fx := storage.NewFakeExec()
	fakeLosetup(fx, path, "/dev/loop42")

	p := file.NewProvider(file.Config{Dir: dir}, fx)

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

// TestSnapshotsCpReflink: file backend now copies the .img with
// `cp --reflink=auto`. On a reflink-capable FS this is O(1) + CoW;
// otherwise cp falls back to a full byte copy. The CSI snapshot-
// restore path needs this so clone / snapshot-restore-cross-node
// e2e can function on the dev stand's FILE_THIN pool.
func TestSnapshotsCpReflink(t *testing.T) {
	dir := t.TempDir()
	fake := storage.NewFakeExec()
	p := file.NewProvider(file.Config{Dir: dir}, fake)

	// Seed a source .img so the snapshot copy has something to read.
	srcPath := filepath.Join(dir, "pvc-1_00000.img")
	if err := os.WriteFile(srcPath, []byte("hello"), 0o644); err != nil {
		t.Fatalf("seed src: %v", err)
	}

	err := p.CreateSnapshot(t.Context(), storage.Snapshot{ResourceName: "pvc-1", SnapshotName: "s1"})
	if err != nil {
		t.Errorf("CreateSnapshot: %v", err)
	}

	if len(fake.Calls) == 0 ||
		fake.Calls[0].Name != "cp" ||
		!slices.Contains(fake.Calls[0].Args, "--reflink=auto") {
		t.Errorf("expected cp --reflink=auto, got %+v", fake.Calls)
	}

	// DeleteSnapshot is a plain os.Remove — exit cleanly on missing.
	// PoolName intentionally left empty here to mirror the satellite
	// reconciler's call shape (it only fills ResourceName + SnapshotName);
	// the provider MUST resolve the path through its own cfg.Dir.
	err = p.DeleteSnapshot(t.Context(),
		storage.Snapshot{ResourceName: "pvc-1", SnapshotName: "s1"})
	if err != nil {
		t.Errorf("DeleteSnapshot: %v", err)
	}
}

// errSimulatedDetachBusy is a static sentinel the disk-leak test uses
// to simulate a real-world `losetup -d` failure. Hoisted out of the
// test body so err113 doesn't object to a dynamic errors.New.
var errSimulatedDetachBusy = errors.New("simulated: losetup detach failed: device or resource busy")

// TestDeleteVolumeUnlinksEvenWhenLosetupDetachFails pins the
// disk-leak fix for FILE / FILE_THIN. The satellite reconciler's
// DeleteResource path used to bubble up `losetup -d` errors (EBUSY
// while a kernel consumer was still tearing down, or the loop having
// been auto-cleared and re-detached racily) and return early WITHOUT
// removing the `.img`. Every RD ever created leaked one
// `<rd>_00000.img` in the pool dir until an operator manually `rm`-ed
// it; the FILE_THIN pool on `stand` would silently run out of free
// capacity.
//
// The provider's contract: detach is best-effort, unlink is mandatory.
func TestDeleteVolumeUnlinksEvenWhenLosetupDetachFails(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()

	preexisting := filepath.Join(dir, "leaky_00000.img")
	if err := os.WriteFile(preexisting, []byte("data"), 0o600); err != nil {
		t.Fatalf("pre-seed: %v", err)
	}

	// Simulate the production failure mode: `losetup -j` discovers an
	// attached /dev/loop7, but the detach errors out (EBUSY-style).
	fx.Expect("losetup -j "+preexisting,
		storage.FakeResponse{Stdout: []byte("/dev/loop7: [64772]:1 (" + preexisting + ")\n")})
	fx.Expect("losetup -d /dev/loop7",
		storage.FakeResponse{Err: errSimulatedDetachBusy})

	p := file.NewProvider(file.Config{Dir: dir, Thin: true}, fx)

	err := p.DeleteVolume(t.Context(), storage.Volume{
		ResourceName: "leaky",
		VolumeNumber: 0,
	})
	if err != nil {
		t.Fatalf("DeleteVolume MUST swallow detach failures and continue: got err=%v", err)
	}

	if _, err := os.Stat(preexisting); !os.IsNotExist(err) {
		t.Errorf("disk-leak regression: backing .img survived DeleteVolume "+
			"(stat err=%v); the pool would leak storage on every `linstor rd delete`", err)
	}
}

// TestDeleteSnapshotResolvesPathThroughProviderDir pins the second
// half of the disk-leak fix: the snapshot reconciler calls
// DeleteSnapshot with PoolName="" (only ResourceName + SnapshotName
// are populated), so the provider MUST resolve the path through its
// own cfg.Dir. The pre-fix code joined snap.PoolName + filename which
// produced a relative path; os.Remove silently no-op'd against the
// satellite's cwd and the snapshot .img leaked indefinitely.
func TestDeleteSnapshotResolvesPathThroughProviderDir(t *testing.T) {
	dir := t.TempDir()
	p := file.NewProvider(file.Config{Dir: dir, Thin: true}, storage.NewFakeExec())

	snapPath := filepath.Join(dir, "rd1_snap1_00000.img")
	if err := os.WriteFile(snapPath, []byte("snap"), 0o600); err != nil {
		t.Fatalf("pre-seed: %v", err)
	}

	// Note: PoolName intentionally left empty to mirror the
	// reconciler.DeleteSnapshot call shape on the satellite.
	err := p.DeleteSnapshot(t.Context(), storage.Snapshot{
		ResourceName: "rd1",
		SnapshotName: "snap1",
	})
	if err != nil {
		t.Fatalf("DeleteSnapshot: %v", err)
	}

	if _, err := os.Stat(snapPath); !os.IsNotExist(err) {
		t.Errorf("disk-leak regression: snapshot .img survived DeleteSnapshot "+
			"(stat err=%v); every `linstor s delete` leaks one snap file", err)
	}
}

// TestPoolStatusReportsCapacity: PoolStatus reports the directory's
// statfs free / total in KiB for the thick variant. Exact figures
// depend on the host's filesystem so sanity-check the shape (positive,
// free <= total). Thick (fallocate-backed) FILE pools keep
// SupportsSnapshots=false because fallocate'd files can't reflink —
// `cp --reflink=auto` would silently fall back to a full byte copy
// which is too expensive to advertise as a snapshot.
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
		t.Errorf("SupportsSnapshots: thick FILE pool should not support snapshots")
	}
}

// TestFileThinReportsSupportsSnapshotsTrue pins Bug 59 / scenario 6.11
// + the CLI parity audit row #3: FILE_THIN must advertise
// CanSnapshots=True so `linstor s c <rd> <snap>` is accepted for thin
// file pools, matching upstream LINSTOR's FILE_THIN provider. The
// underlying CreateSnapshot uses `cp --reflink=auto` which on a
// reflink-capable filesystem (XFS, btrfs, ZFS-backed) is O(1) CoW.
func TestFileThinReportsSupportsSnapshotsTrue(t *testing.T) {
	dir := t.TempDir()
	p := file.NewProvider(file.Config{Dir: dir, Thin: true}, storage.NewFakeExec())

	got, err := p.PoolStatus(t.Context())
	if err != nil {
		t.Fatalf("PoolStatus: %v", err)
	}

	if !got.SupportsSnapshots {
		t.Errorf("FILE_THIN SupportsSnapshots: got false, want true (parity with upstream LINSTOR)")
	}
}

// TestPoolStatusErrorWrapsOnMissingDir pins the missing-dir error
// shape: when the configured Dir doesn't exist (operator typo,
// mount race, missing privilege, or `rm -rf` of the backing
// directory), PoolStatus MUST surface an error whose message
// contains "not found".
//
// Issue 74: the satellite's writeCapacity loop reads "not found"
// from any backend's PoolStatus error as the "pool absent" signal
// that flips Status.PoolMissing=true so `linstor sp l` lands
// state=Faulty rather than silently staying state=Ok with zeroed
// capacity.
func TestPoolStatusErrorWrapsOnMissingDir(t *testing.T) {
	t.Parallel()

	bogus := filepath.Join(t.TempDir(), "does-not-exist")
	p := file.NewProvider(file.Config{Dir: bogus}, storage.NewFakeExec())

	_, err := p.PoolStatus(t.Context())
	if err == nil {
		t.Fatalf("PoolStatus on missing dir: got nil, want error")
	}

	if msg := err.Error(); !strings.Contains(msg, "not found") {
		t.Errorf("error message should mention %q to mark the pool absent; got %q",
			"not found", msg)
	}
}
