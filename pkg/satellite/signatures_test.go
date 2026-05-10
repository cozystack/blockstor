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
	"errors"
	"testing"

	"github.com/cozystack/blockstor/pkg/satellite"
	"github.com/cozystack/blockstor/pkg/storage"
)

// errZpoolNotFound stands in for the exec.Command-not-found error
// FakeExec surfaces when the host doesn't have ZFS installed. The
// LVM-only-host path is the one we most care about pinning.
var errZpoolNotFound = errors.New("zpool: command not found")

// TestHasLVMSignaturePvsMatch pins the LVM signature detection: a
// device path appearing in `pvs --noheadings -o pv_name` output
// surfaces as has=true. Each line is trimmed before comparison so
// pvs's typical leading-spaces formatting doesn't break the match.
func TestHasLVMSignaturePvsMatch(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	fx.Expect("pvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o pv_name",
		storage.FakeResponse{Stdout: []byte("  /dev/sda\n  /dev/nvme0n1\n")})

	has, err := satellite.HasLVMSignature(t.Context(), fx, "/dev/sda")
	if err != nil {
		t.Fatalf("HasLVMSignature: %v", err)
	}

	if !has {
		t.Errorf("got has=false, want true (sda is in pvs output)")
	}
}

// TestHasLVMSignatureNoMatch pins the empty-output / missing-device
// case: `pvs` reports no PVs (empty stdout) → has=false.
func TestHasLVMSignatureNoMatch(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	fx.Expect("pvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o pv_name",
		storage.FakeResponse{Stdout: []byte("")})

	has, err := satellite.HasLVMSignature(t.Context(), fx, "/dev/sda")
	if err != nil {
		t.Fatalf("HasLVMSignature: %v", err)
	}

	if has {
		t.Errorf("got has=true on empty pvs output, want false")
	}
}

// TestHasZFSSignatureMissingTool pins the LVM-only-host path:
// `zpool` not installed surfaces as exec error from FakeExec. The
// detector swallows it (has=false) — the host can't have ZFS
// signatures if there's no ZFS at all.
func TestHasZFSSignatureMissingTool(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	fx.Expect("zpool list -PHv",
		storage.FakeResponse{Err: errZpoolNotFound})

	has, err := satellite.HasZFSSignature(t.Context(), fx, "/dev/sda")
	if err != nil {
		t.Errorf("HasZFSSignature: missing tool must NOT propagate error, got %v", err)
	}

	if has {
		t.Errorf("got has=true on missing zpool, want false")
	}
}

// TestHasOtherSignatureWipefsNonEmpty: wipefs -n with non-empty
// output (any signature found) → has=true. This is the catch-all
// for filesystem / RAID / partition-table signatures lsblk's FSTYPE
// column may have missed.
func TestHasOtherSignatureWipefsNonEmpty(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	fx.Expect("wipefs -n /dev/sda",
		storage.FakeResponse{Stdout: []byte("/dev/sda: 8 bytes were erased at offset 0x00000218 (md_raid_member): fc 4e 2b a9\n")})

	has, err := satellite.HasOtherSignature(t.Context(), fx, "/dev/sda")
	if err != nil {
		t.Fatalf("HasOtherSignature: %v", err)
	}

	if !has {
		t.Errorf("got has=false on non-empty wipefs output, want true")
	}
}

// TestIsDeviceFreeAllChecksPassButLVM pins the short-circuit
// behaviour of IsDeviceFree: as soon as one of the four signature
// detectors fires, the rest are skipped. This is load-bearing —
// running drbdmeta on every device on a busy host is expensive.
func TestIsDeviceFreeAllChecksPassButLVM(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	fx.Expect("pvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o pv_name",
		storage.FakeResponse{Stdout: []byte("/dev/sda\n")})

	dev := &satellite.LsblkDevice{
		Name:  "sda",
		KName: "sda",
		Type:  "disk",
		// FSType, Mountpoint empty → passes IsFreeBlockDevice
	}

	free, err := satellite.IsDeviceFree(t.Context(), fx, dev)
	if err != nil {
		t.Fatalf("IsDeviceFree: %v", err)
	}

	if free {
		t.Errorf("got free=true, want false (sda has LVM signature)")
	}

	// Pin the short-circuit: zpool / drbdmeta / wipefs MUST NOT
	// have been called once LVM said "no".
	for _, line := range fx.CommandLines() {
		if line != "pvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o pv_name" {
			t.Errorf("unexpected exec call after LVM positive: %q", line)
		}
	}
}

// TestIsDeviceFreeShortCircuitsOnLsblkRejection: when the lsblk
// row already disqualifies the device (TYPE=part), no signature
// detectors run at all — saves time on busy hosts with thousands
// of partitions.
func TestIsDeviceFreeShortCircuitsOnLsblkRejection(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	dev := &satellite.LsblkDevice{Name: "sda1", KName: "sda1", Type: "part"}

	free, err := satellite.IsDeviceFree(t.Context(), fx, dev)
	if err != nil {
		t.Fatalf("IsDeviceFree: %v", err)
	}

	if free {
		t.Errorf("got free=true on TYPE=part, want false")
	}

	if len(fx.CommandLines()) != 0 {
		t.Errorf("expected no exec calls on lsblk-rejected device; got %v", fx.CommandLines())
	}
}
