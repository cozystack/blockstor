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

package drbd_test

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
)

// TestEnsureDeviceNode_WipesRegularFile pins the dominant remediation
// case: a previous test iteration on a no-udev system (Talos) wrote
// into `/dev/drbd<N>` via `> /dev/drbd<N>` and the kernel created a
// regular file at that path. EnsureDeviceNode must wipe that regular
// file and replace it with the real block device.
//
// Requires CAP_MKNOD (root on Linux). Skipped on rootless / non-Linux
// runners — see canMknod.
func TestEnsureDeviceNode_WipesRegularFile(t *testing.T) {
	skipUnlessRoot(t)

	minor := pickMinor(t)
	path := drbd.DRBDDevicePath(minor)

	t.Cleanup(func() { _ = os.Remove(path) })

	// Seed a regular file at the would-be device path — the exact
	// state Talos leaves us in after a previous `dd if=... of=<path>`
	// or `bash > <path>` write.
	err := os.WriteFile(path, []byte("stale tmpfs"), 0o644)
	if err != nil {
		t.Fatalf("seed regular file: %v", err)
	}

	if err := drbd.EnsureDeviceNode(minor); err != nil {
		t.Fatalf("EnsureDeviceNode(%d): %v", minor, err)
	}

	assertBlockDevice(t, path, minor)
}

// TestEnsureDeviceNode_CreatesMissingPath pins the steady-state
// post-`drbdadm up` case on Talos: udev never ran, the path doesn't
// exist, EnsureDeviceNode mknods it from scratch.
func TestEnsureDeviceNode_CreatesMissingPath(t *testing.T) {
	skipUnlessRoot(t)

	minor := pickMinor(t)
	path := drbd.DRBDDevicePath(minor)

	t.Cleanup(func() { _ = os.Remove(path) })

	// Path absent — don't seed anything.

	if err := drbd.EnsureDeviceNode(minor); err != nil {
		t.Fatalf("EnsureDeviceNode(%d): %v", minor, err)
	}

	assertBlockDevice(t, path, minor)
}

// TestEnsureDeviceNode_IdempotentOnCorrectDevice pins the no-op case:
// on a node that DOES have udev (or where a previous EnsureDeviceNode
// call has already run), calling EnsureDeviceNode a second time must
// leave the existing block device untouched. Verified via inode
// comparison: a remove+mknod would allocate a fresh inode.
func TestEnsureDeviceNode_IdempotentOnCorrectDevice(t *testing.T) {
	skipUnlessRoot(t)

	minor := pickMinor(t)
	path := drbd.DRBDDevicePath(minor)

	t.Cleanup(func() { _ = os.Remove(path) })

	// First call: create the device.
	if err := drbd.EnsureDeviceNode(minor); err != nil {
		t.Fatalf("EnsureDeviceNode first call: %v", err)
	}

	before, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat before: %v", err)
	}

	beforeSys, ok := before.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatalf("Sys() is not *syscall.Stat_t")
	}

	// Second call: must be a no-op.
	if err := drbd.EnsureDeviceNode(minor); err != nil {
		t.Fatalf("EnsureDeviceNode second call: %v", err)
	}

	after, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat after: %v", err)
	}

	afterSys, ok := after.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatalf("Sys() is not *syscall.Stat_t")
	}

	if beforeSys.Ino != afterSys.Ino {
		t.Errorf("EnsureDeviceNode replaced the inode on a correct block device: before=%d after=%d",
			beforeSys.Ino, afterSys.Ino)
	}

	assertBlockDevice(t, path, minor)
}

// assertBlockDevice fails the test if path is not a block special with
// major=147 and the expected minor.
func assertBlockDevice(t *testing.T, path string, minor int) {
	t.Helper()

	info, err := os.Lstat(path)
	if err != nil {
		t.Fatalf("assertBlockDevice: lstat %s: %v", path, err)
	}

	mode := info.Mode()
	if mode&os.ModeDevice == 0 || mode&os.ModeCharDevice != 0 {
		t.Errorf("assertBlockDevice: mode=%s, want block device", mode)
	}

	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatal("assertBlockDevice: Sys() is not *syscall.Stat_t")
	}

	gotMajor := int(stat.Rdev>>8) & 0xfff
	gotMinor := int(stat.Rdev)&0xff | (int(stat.Rdev>>12) & 0xffffff00)

	if gotMajor != drbd.DRBDMajor || gotMinor != minor {
		t.Errorf("assertBlockDevice: rdev=%d:%d, want %d:%d",
			gotMajor, gotMinor, drbd.DRBDMajor, minor)
	}
}

// skipUnlessRoot is the test-suite gate: EnsureDeviceNode requires
// CAP_MKNOD which is root-only on Linux. Skips on macOS, rootless
// containers, and any other env where mknod returns EPERM.
func skipUnlessRoot(t *testing.T) {
	t.Helper()

	probe := filepath.Join(t.TempDir(), "probe")

	err := syscall.Mknod(probe, syscall.S_IFBLK|0o600, int(mkdevForTest(1, 1)))
	if err != nil {
		if errors.Is(err, syscall.EPERM) || errors.Is(err, syscall.ENOSYS) {
			t.Skipf("mknod requires CAP_MKNOD / root: %v", err)
		}

		t.Skipf("mknod probe failed: %v", err)
	}

	_ = os.Remove(probe)
}

// pickMinor returns a DRBD minor unlikely to collide with anything on
// the build host. Real allocator picks from the 1000..65535 range; we
// pick something well outside that so a developer running tests on a
// machine with live DRBD resources doesn't have their state stomped.
//
// 60000+ is well above any production DRBD allocation pattern we've
// seen and well below the 65535 minor cap. Each test gets a distinct
// minor via t.Name's hash so parallel runs don't collide either.
func pickMinor(t *testing.T) int {
	t.Helper()

	// Cheap deterministic hash over t.Name so parallel tests use
	// distinct minors. Not cryptographic — collision is acceptable
	// because the cleanup t.Cleanup unlinks after.
	h := 0
	for _, c := range t.Name() {
		h = h*31 + int(c)
	}

	if h < 0 {
		h = -h
	}

	return 60000 + (h % 5000)
}

// mkdevForTest duplicates pkg/drbd's internal mkdev so the canMknod
// probe doesn't reach across the package boundary. Kept local so the
// production API doesn't leak the helper.
func mkdevForTest(major, minor int) uint64 {
	maj := uint64(major) //nolint:gosec // small positive constants
	mn := uint64(minor)  //nolint:gosec // small positive constants

	return ((maj & 0xfff) << 8) |
		(mn & 0xff) |
		((mn & 0xffffff00) << 12) |
		((maj &^ 0xfff) << 32)
}
