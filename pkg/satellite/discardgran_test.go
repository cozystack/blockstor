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
	"testing"

	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/file"
	"github.com/cozystack/blockstor/pkg/storage/lvm"
	"github.com/cozystack/blockstor/pkg/storage/zfs"
)

// TestDiscardZeroesIfAligned pins the provider-kind gate for the
// `discard-zeroes-if-aligned` disk option to upstream LINSTOR's
// CtrlRscCrtApiHelper switch: only the kinds whose backing device
// deterministically discards-to-zero on an aligned range qualify.
// FILE_THIN MUST be excluded even though IsThinOrZFS includes it.
func TestDiscardZeroesIfAligned(t *testing.T) {
	cases := map[string]bool{
		ProviderKindLVMThin:  true,
		ProviderKindZFS:      true,
		ProviderKindZFSThin:  true,
		ProviderKindFileThin: false, // loop-backed sparse file: NOT zero-discard safe
		ProviderKindLVM:      false, // thick: stale bytes
		ProviderKindFile:     false,
		ProviderKindDiskless: false,
		"SOMETHING_NEW":      false, // unknown forward-compat kind: safe default
	}

	for kind, want := range cases {
		if got := discardZeroesIfAligned(kind); got != want {
			t.Errorf("discardZeroesIfAligned(%q) = %v, want %v", kind, got, want)
		}
	}
}

// TestClampDiscardGranularity pins the [4 KiB, 1 MiB] clamp mirroring
// upstream's chooseDrbdDiscGran.
func TestClampDiscardGranularity(t *testing.T) {
	cases := []struct {
		in   int64
		want int64
	}{
		{in: 512, want: 4096},        // below min → 4 KiB
		{in: 4096, want: 4096},       // at min
		{in: 16384, want: 16384},     // typical zvol volblocksize, in range
		{in: 65536, want: 65536},     // typical dm-thin chunk, in range
		{in: 1048576, want: 1048576}, // at max
		{in: 4194304, want: 1048576}, // above max → 1 MiB
	}

	for _, c := range cases {
		if got := clampDiscardGranularity(c.in); got != c.want {
			t.Errorf("clampDiscardGranularity(%d) = %d, want %d", c.in, got, c.want)
		}
	}
}

// TestParseDiscGran covers the lsblk DISC-GRAN parser: a valid value,
// a zero (no-discard-support) value, an empty field, and a missing
// column.
func TestParseDiscGran(t *testing.T) {
	cases := []struct {
		name    string
		raw     string
		wantVal int64
		wantOK  bool
	}{
		{name: "valid", raw: `DISC-GRAN="16384"`, wantVal: 16384, wantOK: true},
		{name: "zero", raw: `DISC-GRAN="0"`, wantVal: 0, wantOK: true},
		{name: "empty", raw: `DISC-GRAN=""`, wantVal: 0, wantOK: false},
		{name: "missing", raw: `NAME="zd0"`, wantVal: 0, wantOK: false},
		{name: "blank", raw: "\n  \n", wantVal: 0, wantOK: false},
		{name: "extra-columns", raw: `NAME="zd0" DISC-GRAN="65536" TYPE="disk"`, wantVal: 65536, wantOK: true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			gotVal, gotOK := parseDiscGran(c.raw)
			if gotVal != c.wantVal || gotOK != c.wantOK {
				t.Errorf("parseDiscGran(%q) = (%d, %v), want (%d, %v)",
					c.raw, gotVal, gotOK, c.wantVal, c.wantOK)
			}
		})
	}
}

// discGranResponse registers a canned lsblk DISC-GRAN reply for the
// given device path on a FakeExec.
func discGranResponse(fx *storage.FakeExec, device string, granBytes string) {
	fx.Expect("lsblk -Pbndo DISC-GRAN "+device,
		storage.FakeResponse{Stdout: []byte(`DISC-GRAN="` + granBytes + `"`)})
}

// TestAutoDiskOptions_ThinWithDiscard: a discard-capable thin provider
// with a non-zero DISC-GRAN gets BOTH options, the granularity clamped
// into range.
func TestAutoDiskOptions_ThinWithDiscard(t *testing.T) {
	fx := storage.NewFakeExec()
	discGranResponse(fx, "/dev/zvol/data/pvc-1_00000", "16384")

	opts := autoDiskOptions(context.Background(), fx, ProviderKindZFSThin, "/dev/zvol/data/pvc-1_00000")

	if opts[diskOptDiscardZeroesAligned] != "yes" {
		t.Errorf("discard-zeroes-if-aligned = %q, want yes", opts[diskOptDiscardZeroesAligned])
	}

	if opts[diskOptRsDiscardGranularity] != "16384" {
		t.Errorf("rs-discard-granularity = %q, want 16384", opts[diskOptRsDiscardGranularity])
	}
}

// TestAutoDiskOptions_LvmThinClampsUp: a thin LVM device reporting a
// sub-4-KiB DISC-GRAN is clamped UP to DRBD's 4 KiB minimum.
func TestAutoDiskOptions_LvmThinClampsUp(t *testing.T) {
	fx := storage.NewFakeExec()
	discGranResponse(fx, "/dev/vg/pvc-1_00000", "512")

	opts := autoDiskOptions(context.Background(), fx, ProviderKindLVMThin, "/dev/vg/pvc-1_00000")

	if opts[diskOptRsDiscardGranularity] != "4096" {
		t.Errorf("rs-discard-granularity = %q, want 4096 (clamped up)", opts[diskOptRsDiscardGranularity])
	}
}

// TestAutoDiskOptions_ZeroGranOmitsGranularity: a thin provider whose
// backing device reports DISC-GRAN=0 (no discard support) gets
// discard-zeroes-if-aligned but NOT rs-discard-granularity — DRBD then
// does a full copy rather than issue unsupported UNMAPs.
func TestAutoDiskOptions_ZeroGranOmitsGranularity(t *testing.T) {
	fx := storage.NewFakeExec()
	discGranResponse(fx, "/dev/vg/pvc-1_00000", "0")

	opts := autoDiskOptions(context.Background(), fx, ProviderKindLVMThin, "/dev/vg/pvc-1_00000")

	if opts[diskOptDiscardZeroesAligned] != "yes" {
		t.Errorf("discard-zeroes-if-aligned = %q, want yes", opts[diskOptDiscardZeroesAligned])
	}

	if _, present := opts[diskOptRsDiscardGranularity]; present {
		t.Errorf("rs-discard-granularity present (%q) for DISC-GRAN=0; want omitted", opts[diskOptRsDiscardGranularity])
	}
}

// TestAutoDiskOptions_ProbeFailureOmitsGranularity: when lsblk fails
// (device gone / not yet present) the granularity is omitted but the
// safe discard-zeroes flag stays.
func TestAutoDiskOptions_ProbeFailureOmitsGranularity(t *testing.T) {
	fx := storage.NewFakeExec()
	// No Expect registered for this device → FakeExec returns nil,nil;
	// but parseDiscGran on empty output returns ok=false, so the
	// granularity is omitted. (A real lsblk failure returns an error,
	// also handled.)
	opts := autoDiskOptions(context.Background(), fx, ProviderKindZFS, "/dev/zvol/data/missing")

	if opts[diskOptDiscardZeroesAligned] != "yes" {
		t.Errorf("discard-zeroes-if-aligned = %q, want yes", opts[diskOptDiscardZeroesAligned])
	}

	if _, present := opts[diskOptRsDiscardGranularity]; present {
		t.Errorf("rs-discard-granularity present on probe failure; want omitted")
	}
}

// TestAutoDiskOptions_ThickOmitsEverything: thick LVM is NOT
// discard-zero safe — NO disk options at all (full safe resync). The
// device is never even probed.
func TestAutoDiskOptions_ThickOmitsEverything(t *testing.T) {
	fx := storage.NewFakeExec()

	opts := autoDiskOptions(context.Background(), fx, ProviderKindLVM, "/dev/vg/pvc-1_00000")

	if len(opts) != 0 {
		t.Errorf("thick LVM produced disk options %v; want none", opts)
	}

	for _, line := range fx.CommandLines() {
		if strings.HasPrefix(line, "lsblk") {
			t.Errorf("thick LVM probed the device (%q); want no probe", line)
		}
	}
}

// TestAutoDiskOptions_FileThinOmitsEverything: FILE_THIN is excluded
// from discard-zeroes-if-aligned (loop-backed sparse file) — no disk
// options, matching upstream's `no` for FILE_THIN.
func TestAutoDiskOptions_FileThinOmitsEverything(t *testing.T) {
	fx := storage.NewFakeExec()

	opts := autoDiskOptions(context.Background(), fx, ProviderKindFileThin, "/var/lib/blockstor/pvc-1.img")

	if len(opts) != 0 {
		t.Errorf("FILE_THIN produced disk options %v; want none", opts)
	}
}

// TestAutoDiskOptions_DisklessLocalGetsFlagOnly: a thin provider kind
// with no backing device path (diskless local replica) gets the
// discard-zeroes flag but no granularity, and is not probed.
func TestAutoDiskOptions_DisklessLocalGetsFlagOnly(t *testing.T) {
	fx := storage.NewFakeExec()

	opts := autoDiskOptions(context.Background(), fx, ProviderKindZFSThin, "")

	if opts[diskOptDiscardZeroesAligned] != "yes" {
		t.Errorf("discard-zeroes-if-aligned = %q, want yes", opts[diskOptDiscardZeroesAligned])
	}

	if _, present := opts[diskOptRsDiscardGranularity]; present {
		t.Errorf("rs-discard-granularity present with empty device path; want omitted")
	}

	if len(fx.CommandLines()) != 0 {
		t.Errorf("empty device path probed lsblk %v; want no probe", fx.CommandLines())
	}
}

// drFor builds a minimal local-diskful DesiredResource on a named pool.
func drFor(name, pool string, flags ...string) *intent.DesiredResource {
	return &intent.DesiredResource{
		Name:     name,
		NodeName: "n1",
		Flags:    flags,
		Volumes: []*intent.DesiredVolume{
			{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: pool},
		},
	}
}

// TestAutoDiskOptionsForResource_ZfsThin: end-to-end through the
// Reconciler — a registered ZFS_THIN pool with a discard-capable
// backing device yields both options in the resource-scope map.
func TestAutoDiskOptionsForResource_ZfsThin(t *testing.T) {
	fx := storage.NewFakeExec()
	device := "/dev/zvol/data/pvc-1_00000"
	discGranResponse(fx, device, "65536")

	prov := zfs.NewProvider(zfs.Config{Pool: "data", Thin: true}, fx)
	rec := NewReconciler(ReconcilerConfig{
		Providers: map[string]storage.Provider{"zfs1": prov},
		Exec:      fx,
	})

	opts := rec.autoDiskOptionsForResource(context.Background(), drFor("pvc-1", "zfs1"),
		map[int32]string{0: device})

	if opts[diskOptDiscardZeroesAligned] != "yes" || opts[diskOptRsDiscardGranularity] != "65536" {
		t.Fatalf("autoDiskOptionsForResource = %v; want yes + 65536", opts)
	}
}

// TestAutoDiskOptionsForResource_ThickNil: a thick LVM pool yields no
// auto disk options.
func TestAutoDiskOptionsForResource_ThickNil(t *testing.T) {
	fx := storage.NewFakeExec()
	prov := lvm.NewThick(lvm.ThickConfig{VolumeGroup: "vg"}, fx)
	rec := NewReconciler(ReconcilerConfig{
		Providers: map[string]storage.Provider{"thick1": prov},
		Exec:      fx,
	})

	opts := rec.autoDiskOptionsForResource(context.Background(), drFor("pvc-1", "thick1"),
		map[int32]string{0: "/dev/vg/pvc-1_00000"})

	if len(opts) != 0 {
		t.Fatalf("thick LVM resource produced %v; want none", opts)
	}
}

// TestAutoDiskOptionsForResource_FileThinNil: FILE_THIN yields no auto
// disk options (excluded from discard-zeroes-if-aligned).
func TestAutoDiskOptionsForResource_FileThinNil(t *testing.T) {
	fx := storage.NewFakeExec()
	prov := file.NewProvider(file.Config{Dir: t.TempDir(), Thin: true}, fx)
	rec := NewReconciler(ReconcilerConfig{
		Providers: map[string]storage.Provider{"file1": prov},
		Exec:      fx,
	})

	opts := rec.autoDiskOptionsForResource(context.Background(), drFor("pvc-1", "file1"),
		map[int32]string{0: "/var/lib/blockstor/pvc-1.img"})

	if len(opts) != 0 {
		t.Fatalf("FILE_THIN resource produced %v; want none", opts)
	}
}

// TestAutoDiskOptionsForResource_DisklessNil: a DISKLESS local replica
// never gets auto disk options — there is no backing device.
func TestAutoDiskOptionsForResource_DisklessNil(t *testing.T) {
	fx := storage.NewFakeExec()
	prov := zfs.NewProvider(zfs.Config{Pool: "data", Thin: true}, fx)
	rec := NewReconciler(ReconcilerConfig{
		Providers: map[string]storage.Provider{"zfs1": prov},
		Exec:      fx,
	})

	opts := rec.autoDiskOptionsForResource(context.Background(),
		drFor("pvc-1", "zfs1", "DISKLESS"), map[int32]string{})

	if len(opts) != 0 {
		t.Fatalf("diskless resource produced %v; want none", opts)
	}
}

// TestAutoDiskOptionsForResource_NoExecNil: with no Exec wired the
// method degrades to nil (safe full resync) rather than panicking.
func TestAutoDiskOptionsForResource_NoExecNil(t *testing.T) {
	prov := zfs.NewProvider(zfs.Config{Pool: "data", Thin: true}, storage.NewFakeExec())
	rec := NewReconciler(ReconcilerConfig{
		Providers: map[string]storage.Provider{"zfs1": prov},
		// Exec deliberately unset.
	})

	opts := rec.autoDiskOptionsForResource(context.Background(), drFor("pvc-1", "zfs1"),
		map[int32]string{0: "/dev/zvol/data/pvc-1_00000"})

	if opts != nil {
		t.Fatalf("nil-Exec produced %v; want nil", opts)
	}
}

// TestMergeAutoDiskOptions_OperatorWins: an operator-set
// rs-discard-granularity is never overwritten by the auto value; auto
// only fills unset keys.
func TestMergeAutoDiskOptions_OperatorWins(t *testing.T) {
	dst := map[string]string{
		diskOptRsDiscardGranularity: "262144", // operator pinned
	}
	auto := map[string]string{
		diskOptRsDiscardGranularity: "16384", // auto-derived
		diskOptDiscardZeroesAligned: "yes",
	}

	mergeAutoDiskOptions(dst, auto)

	if dst[diskOptRsDiscardGranularity] != "262144" {
		t.Errorf("operator rs-discard-granularity overwritten: %q", dst[diskOptRsDiscardGranularity])
	}

	if dst[diskOptDiscardZeroesAligned] != "yes" {
		t.Errorf("auto discard-zeroes-if-aligned not filled: %q", dst[diskOptDiscardZeroesAligned])
	}
}

// TestBuildResFile_RendersDiscardDiskBlock: the auto disk options land
// in the rendered resource-scope `disk { }` block.
func TestBuildResFile_RendersDiscardDiskBlock(t *testing.T) {
	dr := &intent.DesiredResource{
		Name:     "pvc-disc",
		NodeName: "n1",
		Volumes: []*intent.DesiredVolume{
			{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "zfs1"},
		},
		Peers: []intent.DesiredPeer{{Name: "n2"}},
		DrbdOptions: map[string]string{
			"port":            "7000",
			"node-id":         "0",
			"address":         "10.0.0.1",
			"minor":           "1000",
			"peer.n2.address": "10.0.0.2",
			"peer.n2.node-id": "1",
			"peer.n2.port":    "7000",
		},
	}
	devices := map[int32]string{0: "/dev/zvol/data/pvc-disc_00000"}
	autoDisk := map[string]string{
		diskOptDiscardZeroesAligned: "yes",
		diskOptRsDiscardGranularity: "16384",
	}

	body, err := buildResFile(dr, "n1", "10.0.0.1", devices, autoDisk)
	if err != nil {
		t.Fatalf("buildResFile: %v", err)
	}

	if !strings.Contains(body, "disk {") {
		t.Fatalf("no disk block in:\n%s", body)
	}

	if !strings.Contains(body, "discard-zeroes-if-aligned yes;") {
		t.Errorf("missing discard-zeroes-if-aligned in:\n%s", body)
	}

	if !strings.Contains(body, "rs-discard-granularity 16384;") {
		t.Errorf("missing rs-discard-granularity in:\n%s", body)
	}
}

// TestBuildResFile_NoAutoDiskNoBlock: with no auto disk options and no
// operator disk props, no `disk { }` block is rendered (thick/file
// safe path).
func TestBuildResFile_NoAutoDiskNoBlock(t *testing.T) {
	dr := &intent.DesiredResource{
		Name:     "pvc-nodisc",
		NodeName: "n1",
		Volumes: []*intent.DesiredVolume{
			{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thick1"},
		},
		Peers: []intent.DesiredPeer{{Name: "n2"}},
		DrbdOptions: map[string]string{
			"port": "7000", "node-id": "0", "address": "10.0.0.1", "minor": "1000",
			"peer.n2.address": "10.0.0.2", "peer.n2.node-id": "1", "peer.n2.port": "7000",
		},
	}
	devices := map[int32]string{0: "/dev/vg/pvc-nodisc_00000"}

	body, err := buildResFile(dr, "n1", "10.0.0.1", devices, nil)
	if err != nil {
		t.Fatalf("buildResFile: %v", err)
	}

	if strings.Contains(body, "disk {") {
		t.Errorf("unexpected disk block in thick render:\n%s", body)
	}
}
