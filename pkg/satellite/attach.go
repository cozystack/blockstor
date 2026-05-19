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

// wipeDevice runs `wipefs --all --force <device>` to clear
// every detected on-disk signature, then `blockdev --rereadpt
// <device>` so the kernel drops any stale partition device
// nodes left over from a previous pool create. Operators must
// opt in via `AttachTo.Wipe=true` — without it, a device
// carrying any signature would otherwise fail the kind-specific
// create command (`vgcreate` refuses on existing PV signature,
// etc).
//
// Bug 336: wipefs alone clears the GPT/MBR signature on the
// parent disk but does NOT force the kernel to re-read its
// partition table. On a device with stale ZFS-style partitions
// (sda1 zfs_member + sda9 zfs_reserved from an aborted prior
// attempt), the partition device nodes /dev/sda1 + /dev/sda9
// persist in the kernel's BLKPG list even after wipefs returns,
// and the follow-up `zpool create -f data /dev/sda` then fails:
//
//	cannot label 'sda': failed to detect device partitions
//	on '/dev/sda1': 19
//
// `blockdev --rereadpt` issues the BLKRRPART ioctl so the
// kernel drops the now-empty partition table and removes the
// stale child nodes before the kind-specific create runs.
// Best-effort — a non-zero exit on a device the kernel can't
// reread (busy, in use by a peer) is logged via the returned
// error so the caller can decide whether to bail; for a freshly-
// wiped CDP target the call is expected to succeed.
func wipeDevice(ctx context.Context, exec storage.Exec, devicePath string) error {
	_, err := exec.Run(ctx, "wipefs", "--all", "--force", devicePath)
	if err != nil {
		return errors.Wrap(err, "wipefs")
	}

	// Bug 336: force the kernel to re-read the (now-empty)
	// partition table so stale /dev/sdaN nodes from prior
	// attempts disappear before the kind-specific create runs.
	_, err = exec.Run(ctx, "blockdev", "--rereadpt", devicePath)
	if err != nil {
		return errors.Wrap(err, "blockdev --rereadpt")
	}

	return nil
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
