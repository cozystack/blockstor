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

	"github.com/cockroachdb/errors"

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

// TestAttachExtendsExistingZpool pins Bug 337's ZFS branch: when
// `zpool list <pool>` exits 0 (pool already exists on the host),
// the satellite-side attach issues `zpool add -f <pool> <device>`
// to fold the new device into the existing pool instead of
// `zpool create`. This is what makes `linstor ps cdp ... zfs
// <node> /dev/sda /dev/sdb /dev/sdc` end up as a single multi-vdev
// zpool rather than failing on the second device with
// "pool already exists".
func TestAttachExtendsExistingZpool(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	// Probe says pool exists.
	fx.Expect("zpool list -H -o name data", storage.FakeResponse{Stdout: []byte("data\n")})

	dev := &apiv1.PhysicalDevice{
		DevicePath: "/dev/sdb",
		AttachTo: &apiv1.PhysicalDeviceAttachTo{
			StoragePoolName: "data",
			ProviderKind:    "ZFS",
			ZPoolName:       "data",
		},
	}

	_, err := satellite.Attach(t.Context(), fx, dev)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}

	calls := fx.CommandLines()

	for _, line := range calls {
		if strings.HasPrefix(line, "zpool create") {
			t.Fatalf("Bug 337: zpool create MUST NOT run when pool already exists; calls=%v", calls)
		}
	}

	if !slices.Contains(calls, "zpool add -f data /dev/sdb") {
		t.Errorf("Bug 337: expected `zpool add -f data /dev/sdb`; calls=%v", calls)
	}
}

// TestAttachExtendsExistingVG pins Bug 337's LVM branch: when
// `vgs <vg>` exits 0 (VG exists), the satellite emits
// `pvcreate` + `vgextend` instead of `pvcreate` + `vgcreate`.
// The thin-pool variant is covered in TestAttachExtendsExistingVGThin.
func TestAttachExtendsExistingVG(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	// vgs probe → pool exists.
	probe := "vgs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o vg_name vg"
	fx.Expect(probe, storage.FakeResponse{Stdout: []byte("  vg\n")})

	dev := &apiv1.PhysicalDevice{
		DevicePath: "/dev/sdb",
		AttachTo: &apiv1.PhysicalDeviceAttachTo{
			StoragePoolName: "thick",
			ProviderKind:    "LVM",
			VGName:          "vg",
		},
	}

	_, err := satellite.Attach(t.Context(), fx, dev)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}

	calls := fx.CommandLines()

	for _, line := range calls {
		if strings.HasPrefix(line, "vgcreate ") {
			t.Fatalf("Bug 337: vgcreate MUST NOT run when VG already exists; calls=%v", calls)
		}
	}

	wantPv := "pvcreate --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --force --yes /dev/sdb"
	wantExtend := "vgextend --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --force --yes vg /dev/sdb"

	if !slices.Contains(calls, wantPv) {
		t.Errorf("Bug 337: missing pvcreate in calls: %v", calls)
	}

	if !slices.Contains(calls, wantExtend) {
		t.Errorf("Bug 337: missing vgextend in calls: %v", calls)
	}
}

// TestAttachExtendsExistingVGThin pins Bug 337's LVM_THIN
// branch: the thin-pool LV itself is NOT re-created (no
// duplicate lvcreate --type thin-pool); only the backing VG
// is extended with pvcreate + vgextend.
func TestAttachExtendsExistingVGThin(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	probe := "vgs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o vg_name vg"
	fx.Expect(probe, storage.FakeResponse{Stdout: []byte("  vg\n")})

	dev := &apiv1.PhysicalDevice{
		DevicePath: "/dev/sdb",
		AttachTo: &apiv1.PhysicalDeviceAttachTo{
			StoragePoolName: "thin",
			ProviderKind:    "LVM_THIN",
			VGName:          "vg",
			ThinPoolName:    "tp",
		},
	}

	_, err := satellite.Attach(t.Context(), fx, dev)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}

	calls := fx.CommandLines()

	for _, line := range calls {
		if strings.HasPrefix(line, "vgcreate ") {
			t.Fatalf("Bug 337: vgcreate MUST NOT run when VG already exists; calls=%v", calls)
		}

		if strings.Contains(line, "--type thin-pool") {
			t.Fatalf("Bug 337: lvcreate --type thin-pool MUST NOT re-run on extend; calls=%v", calls)
		}
	}

	wantExtend := "vgextend --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --force --yes vg /dev/sdb"
	if !slices.Contains(calls, wantExtend) {
		t.Errorf("Bug 337: missing vgextend in calls: %v", calls)
	}
}

// TestWipeDeviceZeroesBothEnds pins the Bug 336 v2 contract: the
// guaranteed-clean wipe MUST zero the first AND last 32 MiB of
// the device, AND re-read the partition table via both
// `blockdev --rereadpt` and `partprobe`. Without the
// end-of-device zero, ZFS secondary labels survive and
// `zpool create` then fails on stale partition entries
// (sda1/sda9). Without partprobe, some kernels keep the stale
// nodes despite BLKRRPART.
//
// Expected dd seek for a 16 GiB device: size 17179869184 B =
// 16384 MiB; seek = 16384 - 32 = 16352.
func TestWipeDeviceZeroesBothEnds(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	fx.Expect("blockdev --getsize64 /dev/sda", storage.FakeResponse{
		Stdout: []byte("17179869184\n"),
	})

	err := satellite.WipeDeviceForTest(t.Context(), fx, "/dev/sda")
	if err != nil {
		t.Fatalf("wipe: %v", err)
	}

	lines := strings.Join(fx.CommandLines(), "\n")

	requireContains := []string{
		"wipefs --all --force /dev/sda",
		"dd if=/dev/zero of=/dev/sda bs=1M count=32 conv=fsync,notrunc status=none",
		"dd if=/dev/zero of=/dev/sda bs=1M seek=16352 count=32 conv=fsync,notrunc status=none",
		"blockdev --rereadpt /dev/sda",
		"partprobe /dev/sda",
	}
	for _, want := range requireContains {
		if !strings.Contains(lines, want) {
			t.Errorf("Bug 336 v2: wipeDevice missing %q in commands:\n%s", want, lines)
		}
	}
}

// TestWipeDeviceHandlesTinyDevice pins the safety guard: when
// the device is <= 64 MiB, the end-of-device zero MUST be
// skipped — a seek of (size-32) MiB on a 32 MiB device would
// land at byte 0, mid-superblock, or wrap negative on some
// dd implementations.
func TestWipeDeviceHandlesTinyDevice(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	fx.Expect("blockdev --getsize64 /dev/loop0", storage.FakeResponse{
		Stdout: []byte("33554432\n"), // 32 MiB
	})

	err := satellite.WipeDeviceForTest(t.Context(), fx, "/dev/loop0")
	if err != nil {
		t.Fatalf("wipe: %v", err)
	}

	for _, cmd := range fx.CommandLines() {
		if strings.Contains(cmd, "seek=") {
			t.Errorf("Bug 336 v2: tiny-device wipe must not seek beyond size: %s", cmd)
		}
	}
}

// TestWipeDeviceContinuesOnWipefsFailure pins the best-effort
// chain semantics: a wipefs failure MUST NOT abort the wipe.
// The dd zero-out is the load-bearing step — if wipefs trips
// on an exotic signature the kernel doesn't recognise, the
// subsequent dd must still run to give us a clean device.
func TestWipeDeviceContinuesOnWipefsFailure(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	fx.Expect("wipefs --all --force /dev/sda", storage.FakeResponse{
		Err: errors.New("wipefs failed"),
	})
	fx.Expect("blockdev --getsize64 /dev/sda", storage.FakeResponse{
		Stdout: []byte("17179869184\n"),
	})

	err := satellite.WipeDeviceForTest(t.Context(), fx, "/dev/sda")
	if err != nil {
		t.Fatalf("Bug 336 v2: wipe must not fail when only wipefs errored: %v", err)
	}

	// Assert dd still ran on both ends.
	lines := strings.Join(fx.CommandLines(), "\n")
	if !strings.Contains(lines, "dd if=/dev/zero of=/dev/sda bs=1M count=32") {
		t.Errorf("Bug 336 v2: dd front-zero must run even when wipefs failed:\n%s", lines)
	}

	if !strings.Contains(lines, "dd if=/dev/zero of=/dev/sda bs=1M seek=16352") {
		t.Errorf("Bug 336 v2: dd end-zero must run even when wipefs failed:\n%s", lines)
	}
}

// TestAttachCreatesWhenPoolAbsent pins the negative of the
// extend branch: when the probe (`zpool list <pool>`) exits
// non-zero (pool absent), the satellite falls through to
// `zpool create` — preserving Bug 336's wipefs+rereadpt
// behaviour on the first device.
func TestAttachCreatesWhenPoolAbsent(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	// Probe says pool does NOT exist.
	fx.Expect("zpool list -H -o name fresh",
		storage.FakeResponse{Err: errors.New("no such pool")})

	dev := &apiv1.PhysicalDevice{
		DevicePath: "/dev/sda",
		AttachTo: &apiv1.PhysicalDeviceAttachTo{
			StoragePoolName: "fresh",
			ProviderKind:    "ZFS",
			ZPoolName:       "fresh",
		},
	}

	_, err := satellite.Attach(t.Context(), fx, dev)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}

	calls := fx.CommandLines()

	for _, line := range calls {
		if strings.HasPrefix(line, "zpool add") {
			t.Fatalf("Bug 337: zpool add MUST NOT run when pool is absent; calls=%v", calls)
		}
	}

	found := false

	for _, line := range calls {
		if strings.HasPrefix(line, "zpool create -f") && strings.Contains(line, "fresh") {
			found = true

			break
		}
	}

	if !found {
		t.Errorf("Bug 337: missing zpool create on absent pool; calls=%v", calls)
	}
}
