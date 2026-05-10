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

	switch dev.AttachTo.ProviderKind {
	case ProviderKindLVM:
		return attachLVMThick(ctx, exec, dev, devicePath)
	case ProviderKindLVMThin:
		return attachLVMThin(ctx, exec, dev, devicePath)
	case ProviderKindZFS, ProviderKindZFSThin:
		return attachZFS(ctx, exec, dev, devicePath)
	}

	return AttachResult{}, errors.Errorf("Attach: unsupported provider kind %q", dev.AttachTo.ProviderKind)
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
// every detected on-disk signature. Operators must opt in via
// `AttachTo.Wipe=true` — without it, a device carrying any
// signature would otherwise fail the kind-specific create
// command (`vgcreate` refuses on existing PV signature, etc).
func wipeDevice(ctx context.Context, exec storage.Exec, devicePath string) error {
	_, err := exec.Run(ctx, "wipefs", "--all", "--force", devicePath)
	if err != nil {
		return errors.Wrap(err, "wipefs")
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
