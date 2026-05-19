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

package satellite_test

import (
	"slices"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/satellite"
	"github.com/cozystack/blockstor/pkg/storage"
)

// TestAttachLVMThickIssuesPvAndVgCreate pins the LVM (thick)
// attach sequence: pvcreate then vgcreate, both via `lvm.Args`
// so the upstream-LINSTOR `--config 'devices { filter=...}'`
// guards stay applied. Output PoolName + ProviderKind +
// StorDriver/LvmVg are the contract `Reconciler.RegisterProvider`
// + the StoragePool CRD reconciler consume.
func TestAttachLVMThickIssuesPvAndVgCreate(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	dev := &apiv1.PhysicalDevice{
		Name:       "n1.wwn-x",
		NodeName:   "n1",
		DevicePath: "/dev/disk/by-id/wwn-X",
		Phase:      "Available",
		AttachTo: &apiv1.PhysicalDeviceAttachTo{
			StoragePoolName: "thick1",
			ProviderKind:    "LVM",
			VGName:          "vg-thick",
		},
	}

	res, err := satellite.Attach(t.Context(), fx, dev)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}

	if res.PoolName != "thick1" || res.ProviderKind != "LVM" || res.Props["StorDriver/LvmVg"] != "vg-thick" {
		t.Errorf("result: got %+v, want thick1/LVM with VG vg-thick", res)
	}

	calls := fx.CommandLines()

	// Must contain pvcreate followed by vgcreate, both with the
	// LVM filter (lvm.Args) prepended.
	wantPv := "pvcreate --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --force --yes /dev/disk/by-id/wwn-X"
	wantVg := "vgcreate --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --force --yes vg-thick /dev/disk/by-id/wwn-X"

	if !slices.Contains(calls, wantPv) {
		t.Errorf("missing pvcreate in calls: %v", calls)
	}

	if !slices.Contains(calls, wantVg) {
		t.Errorf("missing vgcreate in calls: %v", calls)
	}
}

// TestAttachLVMThinIssuesThinpoolLvcreate pins the thin pool
// addendum: after pvcreate + vgcreate, an `lvcreate --type
// thin-pool --extents 100%FREE` runs to materialise the thin
// pool LV. Without it, ApplyResources would fail when the
// satellite's lvm.Thin provider tries `lvcreate --thin`.
func TestAttachLVMThinIssuesThinpoolLvcreate(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	dev := &apiv1.PhysicalDevice{
		DevicePath: "/dev/sda",
		AttachTo: &apiv1.PhysicalDeviceAttachTo{
			StoragePoolName: "thin1",
			ProviderKind:    "LVM_THIN",
			VGName:          "vg-thin",
			ThinPoolName:    "thinpool0",
		},
	}

	res, err := satellite.Attach(t.Context(), fx, dev)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}

	if res.Props["StorDriver/ThinPool"] != "thinpool0" {
		t.Errorf("ThinPool prop: got %+v", res.Props)
	}

	want := "lvcreate --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --type thin-pool --extents 100%FREE --name thinpool0 vg-thin"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("missing thinpool lvcreate; calls=%v", fx.CommandLines())
	}
}

// TestAttachZFSIssuesZpoolCreate pins the ZFS branch — the
// command line stays simple (no `lvm.Args` wrapping; ZFS doesn't
// need the LVM filter), but compression=off + atime=off are
// hardcoded so the pool's accounting matches LINSTOR's
// per-replica usage reporting.
func TestAttachZFSIssuesZpoolCreate(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	dev := &apiv1.PhysicalDevice{
		DevicePath: "/dev/disk/by-id/nvme-X",
		AttachTo: &apiv1.PhysicalDeviceAttachTo{
			StoragePoolName: "zfs1",
			ProviderKind:    "ZFS",
			ZPoolName:       "rpool",
		},
	}

	res, err := satellite.Attach(t.Context(), fx, dev)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}

	if res.Props["StorDriver/ZPool"] != "rpool" {
		t.Errorf("ZPool prop: got %+v", res.Props)
	}

	// Match a substring rather than the exact string to keep the
	// test robust against future flag tweaks (the load-bearing
	// part is "zpool create -f rpool /dev/...").
	found := false

	for _, line := range fx.CommandLines() {
		if strings.HasPrefix(line, "zpool create -f") && strings.Contains(line, "rpool") {
			found = true

			break
		}
	}

	if !found {
		t.Errorf("missing zpool create; calls=%v", fx.CommandLines())
	}
}

// TestAttachFileSkipsShellOut pins the FILE-backend branch:
// directory pools have no on-disk format step (the host is
// expected to mount the path), so Attach just emits the
// AttachResult without touching exec.
func TestAttachFileSkipsShellOut(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	dev := &apiv1.PhysicalDevice{
		AttachTo: &apiv1.PhysicalDeviceAttachTo{
			StoragePoolName: "file1",
			ProviderKind:    "FILE",
			Directory:       "/var/lib/blockstor/pools/file1",
		},
	}

	res, err := satellite.Attach(t.Context(), fx, dev)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}

	if res.Props["StorDriver/FileDir"] != "/var/lib/blockstor/pools/file1" {
		t.Errorf("FileDir prop: got %+v", res.Props)
	}

	if len(fx.CommandLines()) != 0 {
		t.Errorf("expected no exec calls for FILE attach; got %v", fx.CommandLines())
	}
}

// TestAttachWipeRunsWipefsFirst pins the consent-gated wipe
// path: `Wipe=true` triggers `wipefs --all --force` BEFORE
// the kind-specific create. Order matters — running wipefs
// after vgcreate would corrupt the freshly-created pool.
func TestAttachWipeRunsWipefsFirst(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	dev := &apiv1.PhysicalDevice{
		DevicePath: "/dev/sdc",
		AttachTo: &apiv1.PhysicalDeviceAttachTo{
			StoragePoolName: "thick1",
			ProviderKind:    "LVM",
			VGName:          "vg",
			Wipe:            true,
		},
	}

	_, err := satellite.Attach(t.Context(), fx, dev)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}

	calls := fx.CommandLines()

	wipeIdx := -1

	pvIdx := -1

	for i, line := range calls {
		if strings.HasPrefix(line, "wipefs --all --force") {
			wipeIdx = i
		}

		if strings.HasPrefix(line, "pvcreate ") {
			pvIdx = i
		}
	}

	if wipeIdx < 0 {
		t.Fatalf("wipefs not in calls: %v", calls)
	}

	if pvIdx < 0 {
		t.Fatalf("pvcreate not in calls: %v", calls)
	}

	if wipeIdx >= pvIdx {
		t.Errorf("ordering: wipefs@%d MUST run before pvcreate@%d", wipeIdx, pvIdx)
	}
}

// TestAttachRejectsNilDevice + TestAttachRejectsMissingFields
// pin the precondition checks. Phase=Failed branches in the
// future reconciler use these errors for Status conditions.
func TestAttachRejectsNilDevice(t *testing.T) {
	t.Parallel()

	_, err := satellite.Attach(t.Context(), storage.NewFakeExec(), nil)
	if err == nil {
		t.Errorf("Attach(nil): want error, got nil")
	}
}

func TestAttachRejectsMissingDevicePath(t *testing.T) {
	t.Parallel()

	dev := &apiv1.PhysicalDevice{
		AttachTo: &apiv1.PhysicalDeviceAttachTo{
			StoragePoolName: "thick1",
			ProviderKind:    "LVM",
			VGName:          "vg",
		},
	}

	_, err := satellite.Attach(t.Context(), storage.NewFakeExec(), dev)
	if err == nil {
		t.Errorf("Attach without DevicePath: want error, got nil")
	}
}

func TestAttachRejectsMissingVG(t *testing.T) {
	t.Parallel()

	dev := &apiv1.PhysicalDevice{
		DevicePath: "/dev/sda",
		AttachTo: &apiv1.PhysicalDeviceAttachTo{
			ProviderKind: "LVM",
			// VGName missing
		},
	}

	_, err := satellite.Attach(t.Context(), storage.NewFakeExec(), dev)
	if err == nil {
		t.Errorf("Attach without VGName for LVM: want error, got nil")
	}
}

// TestAttachWipeClearsPartitionTable pins Bug 336: a `Wipe=true`
// attach against a device that carries a stale partition table
// (left over from a previous failed pool create) MUST reread the
// partition table after wipefs so the kernel drops the stale
// partition devices BEFORE the kind-specific create command runs.
//
// Reproduction from the e2e2 stand: /dev/sda had ZFS-style stale
// partitions (sda1 zfs_member + sda9 zfs_reserved) from a previous
// `zpool create` attempt. wipefs cleared the GPT signature but the
// partition device nodes /dev/sda1 + /dev/sda9 persisted in the
// kernel's partition list. The follow-up `zpool create -f data
// /dev/sda` then failed with:
//
//	cannot label 'sda': failed to detect device partitions on
//	'/dev/sda1': 19
//
// The fix runs `blockdev --rereadpt <device>` after wipefs so the
// kernel re-reads the now-empty partition table and drops the
// stale child partition nodes before zpool / pvcreate tries to
// inspect them. Order matters — running rereadpt before wipefs
// or after the kind-specific create would still trip the failure.
func TestAttachWipeClearsPartitionTable(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	dev := &apiv1.PhysicalDevice{
		DevicePath: "/dev/sda",
		AttachTo: &apiv1.PhysicalDeviceAttachTo{
			StoragePoolName: "data",
			ProviderKind:    "ZFS",
			ZPoolName:       "data",
			Wipe:            true,
		},
	}

	_, err := satellite.Attach(t.Context(), fx, dev)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}

	calls := fx.CommandLines()

	wipeIdx := -1
	rereadIdx := -1
	createIdx := -1

	for i, line := range calls {
		switch {
		case strings.HasPrefix(line, "wipefs --all --force"):
			wipeIdx = i
		case strings.HasPrefix(line, "blockdev --rereadpt"):
			rereadIdx = i
		case strings.HasPrefix(line, "zpool create"):
			createIdx = i
		}
	}

	if wipeIdx < 0 {
		t.Fatalf("Bug 336: wipefs missing from attach commands: %v", calls)
	}

	if rereadIdx < 0 {
		t.Fatalf("Bug 336: `blockdev --rereadpt` missing — stale partition table will defeat zpool create: %v", calls)
	}

	if createIdx < 0 {
		t.Fatalf("Bug 336: zpool create missing: %v", calls)
	}

	if !(wipeIdx < rereadIdx && rereadIdx < createIdx) {
		t.Errorf("Bug 336: ordering must be wipefs@%d < rereadpt@%d < zpool create@%d; got calls=%v",
			wipeIdx, rereadIdx, createIdx, calls)
	}
}
