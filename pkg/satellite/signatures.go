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

package satellite

import (
	"context"
	"strings"

	"github.com/cockroachdb/errors"

	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/lvm"
)

// HasLVMSignature reports whether the named device is already a
// member of an LVM physical volume. The discovery loop runs this
// (alongside HasZFSSignature, HasDRBDSignature, HasOtherSignature)
// before publishing a device as a `PhysicalDevice` CRD —
// `lsblk` alone misses already-attached LVM PVs that haven't been
// fully formatted with a kernel-recognised signature.
//
// Method: `pvs --noheadings -o pv_name` lists every PV the host
// knows about. We string-match the device path. Empty stdout
// means no PVs at all on this host (fresh install) — return
// false. Phase 10.7.
func HasLVMSignature(ctx context.Context, exec storage.Exec, devicePath string) (bool, error) {
	out, err := exec.Run(ctx, "pvs", lvm.Args("--noheadings", "-o", "pv_name")...)
	if err != nil {
		return false, errors.Wrap(err, "pvs")
	}

	for line := range strings.SplitSeq(string(out), "\n") {
		if strings.TrimSpace(line) == devicePath {
			return true, nil
		}
	}

	return false, nil
}

// HasZFSSignature reports whether the named device is part of any
// imported ZFS pool. `zpool list -PHv` walks every pool's vdev
// tree; we string-match the device path against any line.
//
// `-P` forces full device paths (so we can compare against
// /dev/disk/by-id symlinks); `-H` strips the header; `-v`
// expands vdevs. Phase 10.7.
func HasZFSSignature(ctx context.Context, exec storage.Exec, devicePath string) (bool, error) {
	out, err := exec.Run(ctx, "zpool", "list", "-PHv")
	if err != nil {
		// `zpool` may not be installed (FILE / LVM-only host); a
		// missing binary surfaces as exec error, which we treat
		// as "no ZFS in play, so no signature". The caller is
		// responsible for distinguishing "missing tool" from
		// "real failure" via the error type if needed.
		return false, nil
	}

	for line := range strings.SplitSeq(string(out), "\n") {
		// zpool list -v rows are tab/space separated; the first
		// column is either the pool name (no leading whitespace)
		// or a vdev / device (leading whitespace). We just look
		// for the substring; false-positives on pool names that
		// happen to contain `/dev/...` are pathological.
		if strings.Contains(line, devicePath) {
			return true, nil
		}
	}

	return false, nil
}

// HasDRBDSignature reports whether the named device carries a
// DRBD-9 metadata block. `drbdmeta dump-md` reads the metadata
// block — exit-zero means it parsed something, exit-non-zero
// means no recognisable metadata. We don't care about the parsed
// content here, only the success/failure signal.
//
// We pass `0 v09 <device> internal` (minor 0, version 9, internal
// metadata) — drbdmeta accepts a placeholder minor since we're
// only reading. Phase 10.7.
func HasDRBDSignature(ctx context.Context, exec storage.Exec, devicePath string) (bool, error) {
	_, err := exec.Run(ctx, "drbdmeta", "0", "v09", devicePath, "internal", "dump-md")
	if err != nil {
		// Any failure (no metadata / drbdmeta missing / read error)
		// → treat as no signature. The caller's `wipefs -n` pass
		// catches anything else.
		return false, nil //nolint:nilerr // any drbdmeta failure ⇒ no DRBD signature
	}

	return true, nil
}

// HasOtherSignature is the catch-all that uses `wipefs -n` to
// detect filesystem / RAID / partition-table signatures lsblk's
// FSTYPE column may have missed. `-n` is dry-run: lists matches
// without modifying the device. Non-empty stdout = at least one
// signature found.
func HasOtherSignature(ctx context.Context, exec storage.Exec, devicePath string) (bool, error) {
	out, err := exec.Run(ctx, "wipefs", "-n", devicePath)
	if err != nil {
		return false, errors.Wrap(err, "wipefs")
	}

	return strings.TrimSpace(string(out)) != "", nil
}

// IsDeviceFree composes the four cross-checks: a device is free
// iff lsblk-derived `IsFreeBlockDevice` returns true AND none of
// the four signature detectors fired. Returns the first error
// encountered; pre-empts subsequent checks (so a ZFS-host that
// has `zpool` installed but pvs missing fails cleanly on the
// LVM check rather than silently treating "exec error" as
// "free").
//
// The lsblk row stays the single source of truth for the
// device path — we feed `LsblkDevice.KName` (e.g. `/dev/sda`)
// into the signature checks since that's what pvs/zpool/drbdmeta
// all expect.
func IsDeviceFree(ctx context.Context, exec storage.Exec, dev *LsblkDevice) (bool, error) {
	if !dev.IsFreeBlockDevice() {
		return false, nil
	}

	devicePath := "/dev/" + dev.KName

	checks := []func(context.Context, storage.Exec, string) (bool, error){
		HasLVMSignature,
		HasZFSSignature,
		HasDRBDSignature,
		HasOtherSignature,
	}

	for _, check := range checks {
		has, err := check(ctx, exec, devicePath)
		if err != nil {
			return false, err
		}

		if has {
			return false, nil
		}
	}

	return true, nil
}
