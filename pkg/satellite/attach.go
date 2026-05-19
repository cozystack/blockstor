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
	"log/slog"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/lvm"
)

// AttachResult is the output of `Attach`: the resulting pool name
// + provider-kind-specific props the caller can hand to
// `NewProviderFromKind` + `Reconciler.RegisterProvider`. Phase 10.7.
type AttachResult struct {
	PoolName     string
	ProviderKind string
	Props        map[string]string
}

// Attach materialises a `PhysicalDevice.Spec.AttachTo` request:
// optionally wipes the device, runs the kind-specific
// pool-create command(s), and returns the resulting
// `AttachResult` ready to register with the satellite's
// `Reconciler`. Phase 10.7.
//
// Caller protocol:
//  1. Reconciler picks up the CRD via watch event.
//  2. Sets Status.Phase=Attaching via SSA.
//  3. Calls `Attach(ctx, exec, dev)`.
//  4. On success: registers the new provider, ensures the
//     `StoragePool` CRD exists, deletes the PhysicalDevice CRD
//     (delete-as-completion).
//  5. On failure: sets Status.Phase=Failed + a Condition
//     describing the cause; leaves the CRD present for operator
//     triage.
//
// The wipe step is gated by `Spec.AttachTo.Wipe` — without
// explicit operator consent, a device with on-disk signatures
// returns an error, surfacing on Status as a `WipeRequired`
// condition.
func Attach(ctx context.Context, exec storage.Exec, dev *apiv1.PhysicalDevice) (AttachResult, error) {
	if dev == nil || dev.AttachTo == nil {
		return AttachResult{}, errors.New("Attach: nil device or AttachTo")
	}

	// FILE / FILE_THIN attach only needs a directory — the host's
	// already-mounted filesystem; no block device path required.
	if dev.AttachTo.ProviderKind == ProviderKindFile || dev.AttachTo.ProviderKind == ProviderKindFileThin {
		return attachFile(dev)
	}

	devicePath := attachDevicePath(dev)
	if devicePath == "" {
		return AttachResult{}, errors.New("Attach: device has no DevicePath/CurrentDevPath")
	}

	if dev.AttachTo.Wipe {
		err := wipeDevice(ctx, exec, devicePath)
		if err != nil {
			return AttachResult{}, errors.Wrap(err, "wipefs")
		}
	}

	return attachOrExtend(ctx, exec, dev, devicePath)
}

// attachOrExtend dispatches the per-kind branch with the Bug 337
// flat-reconcile probe: if the underlying VG/zpool already exists
// on the host, extend it; otherwise create it. Split out of
// `Attach` to keep that function under the gocyclo budget.
//
// Bug 337: PhysicalDevice attach is flat — each device →
// independent reconcile. Pool create on the first observed
// device, `zpool add` / `vgextend` on subsequent. No "is this
// the first?" state tracking; the branch is purely a probe of
// host state. This keeps the satellite stateless and makes
// `linstor ps cdp` idempotent + online-expansion-friendly:
// re-running ps cdp a week later with a new device just
// extends the existing pool.
//
// See memory:feedback_ps_cdp_incremental for the design
// rationale.
func attachOrExtend(ctx context.Context, exec storage.Exec, dev *apiv1.PhysicalDevice, devicePath string) (AttachResult, error) {
	switch dev.AttachTo.ProviderKind {
	case ProviderKindLVM:
		if vgExists(ctx, exec, dev.AttachTo.VGName) {
			return extendLVMThick(ctx, exec, dev, devicePath)
		}

		return attachLVMThick(ctx, exec, dev, devicePath)
	case ProviderKindLVMThin:
		if vgExists(ctx, exec, dev.AttachTo.VGName) {
			return extendLVMThin(ctx, exec, dev, devicePath)
		}

		return attachLVMThin(ctx, exec, dev, devicePath)
	case ProviderKindZFS, ProviderKindZFSThin:
		if zpoolExists(ctx, exec, dev.AttachTo.ZPoolName) {
			return extendZFS(ctx, exec, dev, devicePath)
		}

		return attachZFS(ctx, exec, dev, devicePath)
	}

	return AttachResult{}, errors.Errorf("Attach: unsupported provider kind %q", dev.AttachTo.ProviderKind)
}

// zpoolExists probes whether the named zpool is already imported
// on the host. `zpool list <pool>` exits 0 with the pool name on
// stdout if it exists, and exits 1 ("no such pool") otherwise.
// Used by the Bug 337 flat reconcile branch to decide between
// `zpool create` (first device) and `zpool add` (extend).
//
// We treat the pool as present only when the probe exits 0 AND
// stdout contains the pool name — both signals are needed because
// some test/fake exec layers default missing-command-expectation
// to success-with-empty-stdout. Real `zpool list` on a missing
// pool exits 1, so production behaviour is preserved.
//
// Empty pool name → false defensively.
func zpoolExists(ctx context.Context, exec storage.Exec, pool string) bool {
	if pool == "" {
		return false
	}

	out, err := exec.Run(ctx, "zpool", "list", "-H", "-o", "name", pool)
	if err != nil {
		return false
	}

	return strings.Contains(string(out), pool)
}

// vgExists probes whether the named LVM volume group is already
// known to the host. `vgs <vg>` exits 0 if the VG exists and 5
// ("not found") otherwise. Used by the Bug 337 flat reconcile
// branch to decide between `vgcreate` (first device) and
// `vgextend` (extend).
//
// Same "exit 0 AND stdout mentions the VG" rule as zpoolExists —
// see that helper for the rationale. Empty VG name → false
// defensively.
func vgExists(ctx context.Context, exec storage.Exec, vg string) bool {
	if vg == "" {
		return false
	}

	out, err := exec.Run(ctx, "vgs",
		lvm.Args("--noheadings", "-o", "vg_name", vg)...)
	if err != nil {
		return false
	}

	return strings.Contains(string(out), vg)
}

// attachDevicePath picks the most stable device path the
// satellite can operate on. Prefers the by-id symlink (stable
// across reboots / re-cabling) and falls back to the volatile
// `/dev/sdN` only as a last resort.
func attachDevicePath(dev *apiv1.PhysicalDevice) string {
	if dev.DevicePath != "" {
		return dev.DevicePath
	}

	return dev.CurrentDevPath
}

// wipeDevice guarantees the device is `zpool create` / `pvcreate`
// -able when the operator opts in via `AttachTo.Wipe=true`.
//
// Bug 336 v1 ran `wipefs --all --force` + `blockdev --rereadpt`.
// That was insufficient — ZFS writes BOTH a primary label at LBA 0
// and a secondary label near end-of-device. `wipefs` recognises
// the front signatures most of the time but can miss the
// secondary copy, and the kernel-cached child partition nodes
// (sda1 zfs_member + sda9 zfs_reserved) survive across the wipe.
// The follow-up `zpool create -f data /dev/sda` then fails:
//
//	cannot label 'sda': failed to detect device partitions
//	on '/dev/sda1': 19
//
// User contract (P0): `linstor ps l` shows device → `linstor ps
// cdp` MUST work. No probabilistic "wipefs should handle most
// cases". Bug 336 v2 guarantees the device is create-able by:
//
//  1. wipefs --all --force — drop recognised signatures.
//  2. dd zero first 32 MiB — kill GPT primary header, ZFS primary
//     label, LVM PV header, mdraid superblock, anything at start.
//  3. dd zero last 32 MiB — kill GPT secondary header, ZFS
//     secondary label, anything mirrored at end of device.
//  4. blockdev --rereadpt — kernel drops stale partition device
//     nodes (sda1/sda9).
//  5. partprobe — belt-and-braces re-read (some kernels need
//     this in addition to BLKRRPART).
//
// This isn't beyond-upstream recovery: it automates exactly what
// `zpool labelclear` + manual `wipefs` would do, so `ps cdp`
// honours its contract.
//
// Best-effort tail: wipefs / blockdev / partprobe failures are
// logged and the chain continues — the dd zero-out is the
// load-bearing step and its failure aborts. A tiny device
// (< 64 MiB) skips the end-zero step to avoid a negative seek.
func wipeDevice(ctx context.Context, exec storage.Exec, devicePath string) error {
	// 1) wipefs known signatures. Log + continue on failure: the dd
	// zero-out below is what actually guarantees the wipe.
	if _, err := exec.Run(ctx, "wipefs", "--all", "--force", devicePath); err != nil {
		slog.Default().Info("wipefs failed; continuing with dd zero-out",
			"dev", devicePath, "err", err.Error())
	}

	// 2) zero first 32 MiB.
	if _, err := exec.Run(ctx, "dd",
		"if=/dev/zero", "of="+devicePath, "bs=1M", "count=32",
		"conv=fsync,notrunc", "status=none"); err != nil {
		return errors.Wrapf(err, "zero start of %s", devicePath)
	}

	// 3) zero last 32 MiB — query size, seek, write.
	sizeMiB, ok := readDeviceSizeMiB(ctx, exec, devicePath)
	if ok && sizeMiB > 64 { // safety: don't seek negative on tiny devices
		seekMiB := sizeMiB - 32
		if _, err := exec.Run(ctx, "dd",
			"if=/dev/zero", "of="+devicePath, "bs=1M",
			"seek="+strconv.FormatInt(seekMiB, 10),
			"count=32", "conv=fsync,notrunc", "status=none"); err != nil {
			return errors.Wrapf(err, "zero end of %s", devicePath)
		}
	}

	// 4) drop stale partition device nodes. Non-fatal: partprobe
	// below is the belt-and-braces.
	if _, err := exec.Run(ctx, "blockdev", "--rereadpt", devicePath); err != nil {
		slog.Default().Info("blockdev --rereadpt failed; relying on partprobe",
			"dev", devicePath, "err", err.Error())
	}

	// 5) partprobe — belt-and-braces (some kernels need this in
	// addition to BLKRRPART).
	if _, err := exec.Run(ctx, "partprobe", devicePath); err != nil {
		slog.Default().Info("partprobe failed; wipe completed via prior steps",
			"dev", devicePath, "err", err.Error())
	}

	return nil
}

// readDeviceSizeMiB queries `blockdev --getsize64 <dev>` and
// returns the device size in MiB. Returns (0, false) when the
// probe fails or the output cannot be parsed — callers must
// treat this as "skip end-of-device zero" rather than abort,
// since the front-zero step has already neutralised the most
// common signatures.
func readDeviceSizeMiB(ctx context.Context, exec storage.Exec, devicePath string) (int64, bool) {
	out, err := exec.Run(ctx, "blockdev", "--getsize64", devicePath)
	if err != nil {
		slog.Default().Info("blockdev --getsize64 failed; skipping end-of-device zero",
			"dev", devicePath, "err", err.Error())

		return 0, false
	}

	sz, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
	if err != nil {
		slog.Default().Info("blockdev --getsize64 returned unparseable size; skipping end-of-device zero",
			"dev", devicePath, "out", string(out))

		return 0, false
	}

	return sz / (1024 * 1024), true
}

// attachLVMThick: pvcreate + vgcreate. Returns the
// `LVM` provider kind config the satellite then registers via
// `RegisterProvider` to make the pool available for
// `ApplyResources`.
func attachLVMThick(ctx context.Context, exec storage.Exec, dev *apiv1.PhysicalDevice, devicePath string) (AttachResult, error) {
	vg := dev.AttachTo.VGName
	if vg == "" {
		return AttachResult{}, errors.New("LVM attach requires VGName")
	}

	_, err := exec.Run(ctx, "pvcreate", lvm.Args("--force", "--yes", devicePath)...)
	if err != nil {
		return AttachResult{}, errors.Wrap(err, "pvcreate")
	}

	_, err = exec.Run(ctx, "vgcreate", lvm.Args("--force", "--yes", vg, devicePath)...)
	if err != nil {
		return AttachResult{}, errors.Wrap(err, "vgcreate")
	}

	return AttachResult{
		PoolName:     dev.AttachTo.StoragePoolName,
		ProviderKind: ProviderKindLVM,
		Props: map[string]string{
			propLvmVG: vg,
		},
	}, nil
}

// attachLVMThin: pvcreate + vgcreate + lvcreate --thinpool.
// The thin-pool LV consumes the entire VG (extents=100%FREE)
// since this is the dedicated pool for replicas — leaving free
// extents would only confuse capacity accounting.
func attachLVMThin(ctx context.Context, exec storage.Exec, dev *apiv1.PhysicalDevice, devicePath string) (AttachResult, error) {
	vg := dev.AttachTo.VGName
	thin := dev.AttachTo.ThinPoolName

	if vg == "" || thin == "" {
		return AttachResult{}, errors.New("LVM_THIN attach requires both VGName and ThinPoolName")
	}

	_, err := exec.Run(ctx, "pvcreate", lvm.Args("--force", "--yes", devicePath)...)
	if err != nil {
		return AttachResult{}, errors.Wrap(err, "pvcreate")
	}

	_, err = exec.Run(ctx, "vgcreate", lvm.Args("--force", "--yes", vg, devicePath)...)
	if err != nil {
		return AttachResult{}, errors.Wrap(err, "vgcreate")
	}

	_, err = exec.Run(ctx, "lvcreate", lvm.Args(
		"--type", "thin-pool",
		"--extents", "100%FREE",
		"--name", thin,
		vg,
	)...)
	if err != nil {
		return AttachResult{}, errors.Wrap(err, "lvcreate --thinpool")
	}

	return AttachResult{
		PoolName:     dev.AttachTo.StoragePoolName,
		ProviderKind: ProviderKindLVMThin,
		Props: map[string]string{
			propLvmVG:    vg,
			propThinPool: thin,
		},
	}, nil
}

// attachZFS: zpool create. The pool name on disk matches the
// LINSTOR pool name to keep cross-host import predictable; the
// PhysicalDevice's StableID-derived path is the single vdev.
func attachZFS(ctx context.Context, exec storage.Exec, dev *apiv1.PhysicalDevice, devicePath string) (AttachResult, error) {
	pool := dev.AttachTo.ZPoolName
	if pool == "" {
		return AttachResult{}, errors.New("ZFS attach requires ZPoolName")
	}

	_, err := exec.Run(ctx, "zpool", "create", "-f",
		"-O", "compression=off",
		"-O", "atime=off",
		pool, devicePath)
	if err != nil {
		return AttachResult{}, errors.Wrap(err, "zpool create")
	}

	return AttachResult{
		PoolName:     dev.AttachTo.StoragePoolName,
		ProviderKind: dev.AttachTo.ProviderKind,
		Props: map[string]string{
			propZPool: pool,
		},
	}, nil
}

// extendVG runs `pvcreate` + `vgextend` to fold `devicePath` into
// an existing VG. Shared by extendLVMThick and extendLVMThin —
// the thick path's caller is done after this, the thin path's
// caller skips the thin-pool LV extend (see extendLVMThin
// comment). Bug 337.
//
// Idempotent: `pvcreate --force --yes` on an existing PV emits
// a warning but exits 0; `vgextend` on an already-member PV
// no-ops with "Physical volume already belongs to this VG".
func extendVG(ctx context.Context, exec storage.Exec, vg, devicePath string) error {
	_, err := exec.Run(ctx, "pvcreate", lvm.Args("--force", "--yes", devicePath)...)
	if err != nil {
		return errors.Wrap(err, "pvcreate")
	}

	_, err = exec.Run(ctx, "vgextend", lvm.Args("--force", "--yes", vg, devicePath)...)
	if err != nil {
		return errors.Wrap(err, "vgextend")
	}

	return nil
}

// extendLVMThick extends an existing VG by adding `devicePath` as
// a new PV. Mirrors the create branch's `pvcreate` + `vgextend`
// invariants: both run with the upstream-LINSTOR filter so the
// scan rejection stays applied. Bug 337 incremental-reconcile.
func extendLVMThick(ctx context.Context, exec storage.Exec, dev *apiv1.PhysicalDevice, devicePath string) (AttachResult, error) {
	vg := dev.AttachTo.VGName
	if vg == "" {
		return AttachResult{}, errors.New("LVM extend requires VGName")
	}

	err := extendVG(ctx, exec, vg, devicePath)
	if err != nil {
		return AttachResult{}, err
	}

	return AttachResult{
		PoolName:     dev.AttachTo.StoragePoolName,
		ProviderKind: ProviderKindLVM,
		Props: map[string]string{
			propLvmVG: vg,
		},
	}, nil
}

// extendLVMThin extends an existing thin-pool's backing VG by
// adding `devicePath` as a new PV. The thin-pool LV itself is
// NOT re-extended here — `lvextend` against a thin-pool would
// also need a metadata-pool size negotiation that the upstream
// `linstor ps cdp` path doesn't reach. The thin pool grows
// implicitly via `lvextend --extents 100%FREE` when the satellite's
// background autogrow kicks in or the operator runs `lvextend`
// manually. Bug 337.
func extendLVMThin(ctx context.Context, exec storage.Exec, dev *apiv1.PhysicalDevice, devicePath string) (AttachResult, error) {
	vg := dev.AttachTo.VGName
	thin := dev.AttachTo.ThinPoolName

	if vg == "" || thin == "" {
		return AttachResult{}, errors.New("LVM_THIN extend requires both VGName and ThinPoolName")
	}

	err := extendVG(ctx, exec, vg, devicePath)
	if err != nil {
		return AttachResult{}, err
	}

	return AttachResult{
		PoolName:     dev.AttachTo.StoragePoolName,
		ProviderKind: ProviderKindLVMThin,
		Props: map[string]string{
			propLvmVG:    vg,
			propThinPool: thin,
		},
	}, nil
}

// extendZFS extends an existing zpool by adding `devicePath` as
// a new top-level vdev. `zpool add -f <pool> <device>` is
// idempotent on a device already part of the pool: ZFS returns
// `/dev/sdX is part of active pool '<pool>'` and exits non-zero
// — caller probes vdev membership before calling on a known-
// member device, OR the next reconcile observes the already-
// extended pool and the create branch never re-runs anyway.
// Bug 337.
//
// `-f` is required for the same reason `zpool create -f` carries
// it: a device with stale ZFS labels from a prior attempt would
// otherwise refuse to be added.
func extendZFS(ctx context.Context, exec storage.Exec, dev *apiv1.PhysicalDevice, devicePath string) (AttachResult, error) {
	pool := dev.AttachTo.ZPoolName
	if pool == "" {
		return AttachResult{}, errors.New("ZFS extend requires ZPoolName")
	}

	_, err := exec.Run(ctx, "zpool", "add", "-f", pool, devicePath)
	if err != nil {
		return AttachResult{}, errors.Wrap(err, "zpool add")
	}

	return AttachResult{
		PoolName:     dev.AttachTo.StoragePoolName,
		ProviderKind: dev.AttachTo.ProviderKind,
		Props: map[string]string{
			propZPool: pool,
		},
	}, nil
}

// attachFile: directory-backed pool — no on-disk format runs
// satellite-side. The directory is expected to already be
// mounted by the host (Talos extension / kubelet). Returns the
// kind-specific Provider config without touching the disk.
func attachFile(dev *apiv1.PhysicalDevice) (AttachResult, error) {
	dir := dev.AttachTo.Directory
	if dir == "" {
		return AttachResult{}, errors.New("FILE attach requires Directory")
	}

	return AttachResult{
		PoolName:     dev.AttachTo.StoragePoolName,
		ProviderKind: dev.AttachTo.ProviderKind,
		Props: map[string]string{
			propFileDir: dir,
		},
	}, nil
}
