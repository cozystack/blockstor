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

package drbd

import (
	"fmt"
	"os"
	"syscall"

	"github.com/cockroachdb/errors"
)

// DRBDMajor is the kernel-allocated character/block-device major number
// for DRBD's `/dev/drbd<minor>` block device nodes. Pinned at 147 by
// Documentation/admin-guide/devices.txt and unchanged for the lifetime
// of DRBD-9. Kernel udev rules `udev/65-drbd.rules` use the same value
// when they create the device nodes on systems running udev.
const DRBDMajor = 147

// DRBDDevicePath returns the canonical `/dev/drbd<minor>` path the
// kernel exports the resource's block device at. Used by EnsureDeviceNode
// and by callers that need to refer to the device path without
// duplicating the format string.
func DRBDDevicePath(minor int) string {
	return fmt.Sprintf("/dev/drbd%d", minor)
}

// EnsureDeviceNode guarantees that `/dev/drbd<minor>` exists as a block
// special device with the right major/minor pair.
//
// Why this exists (root cause): on Talos and other minimal distributions
// there is no udev daemon running, so DRBD's bundled udev rules never
// fire and `/dev/drbd<N>` is never created after `drbdadm up <res>`.
// Consumers (CSI publish, e2e tests, drbdadm primary) then open
// `/dev/drbd<N>` via plain `open(2)` / shell `> /dev/drbd<N>` and the
// kernel silently creates a regular file in tmpfs at that path. All I/O
// lands in tmpfs — never touching DRBD or the backing zvol/loop/lvm —
// and the bug surfaces later as data-loss or a test passing while
// writing to the wrong place. mknod here is the satellite-side
// replacement for the missing udev rule: idempotent, no daemon required.
//
// Semantics:
//   - path exists and is already a block device with the right
//     rdev → no-op (return nil)
//   - path exists but is NOT a block device (regular file from prior
//     `> /dev/drbd<N>` write, FIFO, symlink, …) → remove it and
//     mknod afresh. This is the dominant remediation case: a previous
//     iteration of the same test wrote into a tmpfs file at this path
//     and the next iteration must not inherit that.
//   - path exists as a block device with the WRONG rdev (a different
//     minor for some reason) → remove and recreate. Defensive: should
//     not happen in practice but cheaper than leaving a wrong-pointed
//     node around.
//   - path absent → mknod and chmod 660. Group access lets the satellite
//     pod (running as root, but for CSI publish on the same node the
//     disk group bit matters) and root open the device.
//
// Idempotent and safe to call from concurrent reconciles for the same
// minor: the worst case is one of them does the remove+mknod while
// the other waits on the same os.Stat — both observe a correct block
// device at the end. We don't take a process-wide lock because mknod
// itself is atomic at the syscall layer (kernel inode allocation is
// serialised).
func EnsureDeviceNode(minor int) error {
	path := DRBDDevicePath(minor)

	info, err := os.Stat(path)

	switch {
	case err == nil:
		// Path exists. Decide whether to keep it or replace.
		if isCorrectBlockDevice(info, minor) {
			return nil
		}

		// Wrong type (regular file, FIFO, symlink, …) or wrong rdev —
		// remove and recreate. os.Remove is safe on block devices
		// (it unlink(2)s the inode reference; the kernel-side
		// minor is unaffected and a fresh mknod re-binds the path).
		if rmErr := os.Remove(path); rmErr != nil {
			return errors.Wrapf(rmErr, "ensure %s: remove stale %s", path, describeStat(info))
		}

	case errors.Is(err, os.ErrNotExist):
		// Path absent — the dominant case on Talos (no udev). Fall
		// through to mknod.

	default:
		return errors.Wrapf(err, "ensure %s: stat", path)
	}

	// mknod /dev/drbd<minor> b 147 <minor>. The mode argument carries
	// both the file-type (S_IFBLK) and permissions in one int — Linux
	// mknod(2) reads the permission bits and the type bits from the
	// same value. The umask applies, so we always follow up with an
	// explicit chmod to get the literal 0660 we asked for regardless
	// of the inherited umask.
	mode := uint32(syscall.S_IFBLK | 0o660)
	dev := mkdev(DRBDMajor, minor)

	if err := syscall.Mknod(path, mode, int(dev)); err != nil { //nolint:gosec // dev fits in int for minor<<8 + major encoding within DRBD's 0..65535 minor range
		return errors.Wrapf(err, "ensure %s: mknod b %d %d", path, DRBDMajor, minor)
	}

	if err := os.Chmod(path, 0o660); err != nil {
		return errors.Wrapf(err, "ensure %s: chmod 0660", path)
	}

	return nil
}

// isCorrectBlockDevice reports whether the FileInfo describes a block
// device with the expected rdev (major=147, minor=<minor>).
func isCorrectBlockDevice(info os.FileInfo, minor int) bool {
	if info.Mode()&os.ModeDevice == 0 || info.Mode()&os.ModeCharDevice != 0 {
		// Not a device at all, or a character device (DRBD is block).
		return false
	}

	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		// Should never happen on Linux. Treat as wrong-pointed to be
		// safe — the recreate path is idempotent.
		return false
	}

	wantRdev := mkdev(DRBDMajor, minor)

	return uint64(stat.Rdev) == wantRdev //nolint:unconvert // Rdev type varies by arch (uint32 vs uint64)
}

// mkdev encodes a (major, minor) pair into the Linux kernel's dev_t
// layout. Mirrors the kernel's `MKDEV` macro:
//
//	major: bits 8..19 (12 bits) + bits 32..43 (extended major)
//	minor: bits 0..7 + bits 20..31
//
// For DRBD (major 147, minor 0..65535) the extended bits are unused;
// the simpler `major<<8 | minor` form is what mknod(8) emits. We use
// the full encoding so future kernels with larger minor ranges don't
// silently truncate.
func mkdev(major, minor int) uint64 {
	maj := uint64(major) //nolint:gosec // major is a small positive constant (147)
	mn := uint64(minor)  //nolint:gosec // minor is non-negative (allocator pool starts at 1000)

	return ((maj & 0xfff) << 8) |
		(mn & 0xff) |
		((mn & 0xffffff00) << 12) |
		((maj &^ 0xfff) << 32)
}

// describeStat returns a short human-readable form of the file type at
// the path, used in error messages so an operator chasing a satellite
// log immediately sees "removed stale regular file" vs "removed stale
// FIFO".
func describeStat(info os.FileInfo) string {
	if info == nil {
		return "<missing>"
	}

	mode := info.Mode()

	switch {
	case mode&os.ModeDevice != 0 && mode&os.ModeCharDevice == 0:
		return "block device"
	case mode&os.ModeDevice != 0:
		return "character device"
	case mode&os.ModeSymlink != 0:
		return "symlink"
	case mode&os.ModeNamedPipe != 0:
		return "FIFO"
	case mode.IsRegular():
		return "regular file"
	default:
		return mode.String()
	}
}
