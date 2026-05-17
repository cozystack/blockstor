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

package zfs_test

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/zfs"
)

// TestKind: ZFS provider declares the LINSTOR kind.
func TestKind(t *testing.T) {
	p := zfs.NewProvider(zfs.Config{Pool: "tank"}, storage.NewFakeExec())
	if got := p.Kind(); got != "ZFS" {
		t.Errorf("Kind: got %q, want ZFS", got)
	}
}

// TestThinKind: thin variant declares ZFS_THIN.
func TestThinKind(t *testing.T) {
	p := zfs.NewProvider(zfs.Config{Pool: "tank", Thin: true}, storage.NewFakeExec())
	if got := p.Kind(); got != "ZFS_THIN" {
		t.Errorf("Kind: got %q, want ZFS_THIN", got)
	}
}

// TestCreateVolumeThick uses zfs create with -V (no -s for thick).
func TestCreateVolumeThick(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("zfs list -H -o name tank/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("")})
	// Post-create ensureRefreservation pass (Bug 255 retrofit): volsize
	// observable as the just-created 1 GiB, refreservation already
	// matches (thick `zfs create -V` reserves up front).
	fx.Expect("zfs get -Hp -o value volsize tank/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("1073741824\n")})
	fx.Expect("zfs get -Hp -o value refreservation tank/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("1073741824\n")})

	p := zfs.NewProvider(zfs.Config{Pool: "tank"}, fx)

	err := p.CreateVolume(t.Context(), storage.Volume{
		ResourceName: "pvc-1",
		VolumeNumber: 0,
		SizeKib:      1024 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	want := "zfs create -V 1024M tank/pvc-1_00000"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("expected %q in calls; got %v", want, fx.CommandLines())
	}
}

// TestCreateVolumeThin adds -s for sparse (thin) volumes.
func TestCreateVolumeThin(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("zfs list -H -o name tank/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("")})

	p := zfs.NewProvider(zfs.Config{Pool: "tank", Thin: true}, fx)

	err := p.CreateVolume(t.Context(), storage.Volume{
		ResourceName: "pvc-1",
		VolumeNumber: 0,
		SizeKib:      1024 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	want := "zfs create -s -V 1024M tank/pvc-1_00000"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("expected %q in calls; got %v", want, fx.CommandLines())
	}
}

// TestCreateVolumeIdempotent: existing dataset → no `zfs create`, but
// ensureRefreservation still runs (Bug 255). The volsize lookup +
// refreservation lookup are wired so the helper observes a thick-correct
// steady state (refreservation already == volsize) and emits no `zfs
// set`.
func TestCreateVolumeIdempotent(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("zfs list -H -o name tank/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("tank/pvc-1_00000\n")})
	fx.Expect("zfs get -Hp -o value volsize tank/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("1073741824\n")})
	fx.Expect("zfs get -Hp -o value refreservation tank/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("1073741824\n")})

	p := zfs.NewProvider(zfs.Config{Pool: "tank"}, fx)

	err := p.CreateVolume(t.Context(), storage.Volume{
		ResourceName: "pvc-1",
		VolumeNumber: 0,
		SizeKib:      1024 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	for _, line := range fx.CommandLines() {
		if len(line) >= 11 && line[:11] == "zfs create " {
			t.Errorf("idempotent CreateVolume issued zfs create: %s", line)
		}
	}
}

// TestDeleteVolume issues zfs destroy -r.
func TestDeleteVolume(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("zfs list -H -o name tank/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("tank/pvc-1_00000\n")})

	p := zfs.NewProvider(zfs.Config{Pool: "tank"}, fx)

	err := p.DeleteVolume(t.Context(), storage.Volume{
		ResourceName: "pvc-1",
		VolumeNumber: 0,
	})
	if err != nil {
		t.Fatalf("DeleteVolume: %v", err)
	}

	want := "zfs destroy -r tank/pvc-1_00000"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("expected %q in calls; got %v", want, fx.CommandLines())
	}
}

// TestVolumeStatusParsesZfsList: parses pipe-separated list.
func TestVolumeStatusParsesZfsList(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("zfs list -H -p -o name,volsize,used tank/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("tank/pvc-1_00000\t1073741824\t512\n")})

	p := zfs.NewProvider(zfs.Config{Pool: "tank"}, fx)

	got, err := p.VolumeStatus(t.Context(), storage.Volume{
		ResourceName: "pvc-1",
		VolumeNumber: 0,
	})
	if err != nil {
		t.Fatalf("VolumeStatus: %v", err)
	}

	if got.DevicePath != "/dev/zvol/tank/pvc-1_00000" {
		t.Errorf("DevicePath: got %q", got.DevicePath)
	}

	if got.UsableKib != 1048576 { // 1 GiB / 1024
		t.Errorf("UsableKib: got %d, want 1048576", got.UsableKib)
	}

	if got.State != "PROVISIONED" {
		t.Errorf("State: got %q, want PROVISIONED", got.State)
	}
}

// TestPoolStatusParsesZpoolGet: free + total via zpool list -p.
func TestPoolStatusParsesZpoolGet(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("zpool list -H -p -o size,free tank",
		storage.FakeResponse{Stdout: []byte("107374182400\t80530636800\n")})

	p := zfs.NewProvider(zfs.Config{Pool: "tank"}, fx)

	got, err := p.PoolStatus(t.Context())
	if err != nil {
		t.Fatalf("PoolStatus: %v", err)
	}

	if got.TotalCapacityKib != 104857600 {
		t.Errorf("TotalCapacityKib: got %d, want 104857600", got.TotalCapacityKib)
	}

	if got.FreeCapacityKib != 78643200 {
		t.Errorf("FreeCapacityKib: got %d, want 78643200", got.FreeCapacityKib)
	}

	if !got.SupportsSnapshots {
		t.Errorf("SupportsSnapshots: got false, want true")
	}
}

// TestCreateSnapshotIssuesZfsSnap.
func TestCreateSnapshotIssuesZfsSnap(t *testing.T) {
	fx := storage.NewFakeExec()
	p := zfs.NewProvider(zfs.Config{Pool: "tank"}, fx)

	err := p.CreateSnapshot(t.Context(), storage.Snapshot{
		ResourceName: "pvc-1",
		SnapshotName: "snap-1",
	})
	if err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}

	want := "zfs snapshot tank/pvc-1_00000@snap-1"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("expected %q in calls; got %v", want, fx.CommandLines())
	}
}

// TestDeleteSnapshotIssuesZfsDestroy.
func TestDeleteSnapshotIssuesZfsDestroy(t *testing.T) {
	fx := storage.NewFakeExec()
	// Bug 212 added a `datasetExists` pre-check — stub it so the
	// snapshot is reported as present, then assert destroy fires.
	fx.Expect("zfs list -H -o name tank/pvc-1_00000@snap-1",
		storage.FakeResponse{Stdout: []byte("tank/pvc-1_00000@snap-1\n")})

	p := zfs.NewProvider(zfs.Config{Pool: "tank"}, fx)

	err := p.DeleteSnapshot(t.Context(), storage.Snapshot{
		ResourceName: "pvc-1",
		SnapshotName: "snap-1",
	})
	if err != nil {
		t.Fatalf("DeleteSnapshot: %v", err)
	}

	want := "zfs destroy tank/pvc-1_00000@snap-1"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("expected %q in calls; got %v", want, fx.CommandLines())
	}
}

// TestZFSResizeVolumeIssuesZfsSet locks `zfs set volsize=<MiB>M`
// as the resize command. Used by the satellite reconciler when a
// VolumeDefinition update bumps the size.
//
// Post-Bug-252 the thick path also runs `ensureRefreservation` after
// the volsize set, which probes volsize + refreservation. Seed both so
// the helper sees refreservation already matches the new volsize and
// stays a no-op — keeps this test focused on the resize command shape
// (the dedicated Bug 252 test in refreservation_policy_bug_252_254_test.go
// pins the re-set behaviour).
func TestZFSResizeVolumeIssuesZfsSet(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("zfs get -Hp -o value volsize tank/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("2147483648\n")})
	fx.Expect("zfs get -Hp -o value refreservation tank/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("2147483648\n")})

	p := zfs.NewProvider(zfs.Config{Pool: "tank"}, fx)

	err := p.ResizeVolume(t.Context(), storage.Volume{
		ResourceName: "pvc-1",
		VolumeNumber: 0,
		SizeKib:      2048 * 1024,
	})
	if err != nil {
		t.Fatalf("ResizeVolume: %v", err)
	}

	want := "zfs set volsize=2048M tank/pvc-1_00000"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("expected %q in calls; got %v", want, fx.CommandLines())
	}
}

// TestVolumeStatusMissing: `zfs list` errors out → NOT_PROVISIONED
// (zfs returns non-zero when the dataset doesn't exist; we treat
// that as "not yet created", same as an empty stdout).
func TestVolumeStatusMissing(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect(
		"zfs list -H -p -o name,volsize,used tank/ghost_00000",
		storage.FakeResponse{Err: errZFSListMissing},
	)

	p := zfs.NewProvider(zfs.Config{Pool: "tank"}, fx)

	got, err := p.VolumeStatus(t.Context(), storage.Volume{
		ResourceName: "ghost",
		VolumeNumber: 0,
	})
	if err != nil {
		t.Fatalf("VolumeStatus: %v", err)
	}

	if got.State != "NOT_PROVISIONED" {
		t.Errorf("State: got %q, want NOT_PROVISIONED", got.State)
	}
}

// TestVolumeStatusEmptyOutput: `zfs list` returns empty output (no
// error, just nothing on stdout) — same NOT_PROVISIONED treatment.
// Pins the dual no-error / non-empty-but-malformed branch.
func TestVolumeStatusEmptyOutput(t *testing.T) {
	fx := storage.NewFakeExec()
	// Default is empty stdout, no error.

	p := zfs.NewProvider(zfs.Config{Pool: "tank"}, fx)

	got, err := p.VolumeStatus(t.Context(), storage.Volume{
		ResourceName: "ghost",
		VolumeNumber: 0,
	})
	if err != nil {
		t.Fatalf("VolumeStatus: %v", err)
	}

	if got.State != "NOT_PROVISIONED" {
		t.Errorf("State: got %q, want NOT_PROVISIONED", got.State)
	}
}

// TestVolumeStatusBadColumns: `zfs list` output that doesn't match
// the expected column count must surface a descriptive error rather
// than panic on slice access.
func TestVolumeStatusBadColumns(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect(
		"zfs list -H -p -o name,volsize,used tank/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("only one column\n")},
	)

	p := zfs.NewProvider(zfs.Config{Pool: "tank"}, fx)

	_, err := p.VolumeStatus(t.Context(), storage.Volume{ResourceName: "pvc-1"})
	if err == nil {
		t.Errorf("expected error on malformed zfs list output")
	}
}

// TestPoolStatusBadColumns: zpool list output with the wrong number
// of columns surfaces a parse error, not a slice panic.
func TestPoolStatusBadColumns(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect(
		"zpool list -H -p -o size,free tank",
		storage.FakeResponse{Stdout: []byte("only_one_field\n")},
	)

	p := zfs.NewProvider(zfs.Config{Pool: "tank"}, fx)

	_, err := p.PoolStatus(t.Context())
	if err == nil {
		t.Errorf("expected error on malformed zpool list output")
	}
}

// TestPoolStatusMissingZPoolEmptyOutput pins Issue 74: when the
// operator destroys the ZFS pool out-of-band (`zpool destroy`),
// `zpool list -H -p -o size,free <pool>` returns empty stdout in
// some configurations (or exit 0 + empty stdout in tooling chains).
// `PoolStatus` MUST surface that as an error whose message mentions
// "not found" so the satellite's writeCapacity loop flips
// `Status.PoolMissing=true` and the wire view in `linstor sp l`
// lands state=Faulty rather than silently keeping state=Ok with
// zeroed capacity.
func TestPoolStatusMissingZPoolEmptyOutput(t *testing.T) {
	fx := storage.NewFakeExec()
	// No Expect — FakeExec returns empty stdout + nil error.

	p := zfs.NewProvider(zfs.Config{Pool: "blockstor-zfs"}, fx)

	_, err := p.PoolStatus(t.Context())
	if err == nil {
		t.Fatalf("expected error on empty zpool list output, got nil")
	}

	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("error message should mention %q to mark the pool absent; got %q",
			"not found", err.Error())
	}
}

// TestPoolStatusBadNumbers: well-shaped output with non-numeric
// fields → ParseInt error, no panic.
func TestPoolStatusBadNumbers(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect(
		"zpool list -H -p -o size,free tank",
		storage.FakeResponse{Stdout: []byte("nope\tnope\n")},
	)

	p := zfs.NewProvider(zfs.Config{Pool: "tank"}, fx)

	_, err := p.PoolStatus(t.Context())
	if err == nil {
		t.Errorf("expected ParseInt error on non-numeric fields")
	}
}

var errZFSListMissing = errors.New("dataset does not exist")

// TestDeleteVolumeMissingIsNoop pins the idempotent-delete invariant:
// if `zfs list` says the dataset is gone, DeleteVolume returns nil
// without issuing a destroy. linstor-csi calls Delete on the volume
// teardown path (sometimes after a controller restart that lost the
// in-flight state); a regression that surfaced "dataset doesn't
// exist" as an error would re-fire forever.
func TestDeleteVolumeMissingIsNoop(t *testing.T) {
	fx := storage.NewFakeExec()
	// First probe → "dataset doesn't exist" (zfs list returns non-zero).
	fx.Expect("zfs list -H -o name tank/pvc-1_00000",
		storage.FakeResponse{Err: errZFSListMissing})

	p := zfs.NewProvider(zfs.Config{Pool: "tank"}, fx)

	err := p.DeleteVolume(t.Context(), storage.Volume{
		ResourceName: "pvc-1",
		VolumeNumber: 0,
	})
	if err != nil {
		t.Fatalf("DeleteVolume on missing dataset: got %v, want nil", err)
	}

	// destroy must NOT have run.
	for _, cmd := range fx.CommandLines() {
		if cmd == "zfs destroy -r tank/pvc-1_00000" {
			t.Errorf("destroy ran despite missing dataset: %v", fx.CommandLines())
		}
	}
}

// TestCreateSnapshotErrorWraps pins the cockroachdb error wrap on
// the CreateSnapshot path: a `zfs snapshot` failure (e.g. dataset
// missing, insufficient permissions) must surface with the
// "zfs snapshot" prefix so operators can grep the wrap keyword
// in logs.
func TestCreateSnapshotErrorWraps(t *testing.T) {
	fx := storage.NewFakeExec()
	// Pre-check (Bug 216): dataset reports missing so CreateSnapshot
	// proceeds past the idempotency short-circuit and actually fires
	// `zfs snapshot`. The fail-then-wrap contract applies only to
	// non-not-found errors.
	fx.Expect("zfs list -H -o name tank/pvc-1_00000@snap-1",
		storage.FakeResponse{Err: errZFSListMissing})
	fx.Expect("zfs snapshot tank/pvc-1_00000@snap-1",
		storage.FakeResponse{Err: errZFSListMissing})

	p := zfs.NewProvider(zfs.Config{Pool: "tank"}, fx)

	err := p.CreateSnapshot(t.Context(), storage.Snapshot{
		ResourceName: "pvc-1",
		SnapshotName: "snap-1",
	})
	if err == nil {
		t.Fatalf("CreateSnapshot: got nil, want error")
	}

	if !slices.Contains(fx.CommandLines(), "zfs snapshot tank/pvc-1_00000@snap-1") {
		t.Errorf("expected snapshot cmd in calls; got %v", fx.CommandLines())
	}

	if msg := err.Error(); !strings.Contains(msg, "zfs snapshot") {
		t.Errorf("wrap: %q must contain \"zfs snapshot\" for operator grep", msg)
	}
}

// TestZFSThickCreateOmitsSparseFlag pins the THICK-only behavior:
// `Thin: false` must NOT pass `-s` to `zfs create`, because the
// thick mode reserves the full volsize up front via ZFS
// `refreservation`. A regression that leaked `-s` into the thick
// path would silently degrade capacity guarantees — operators
// who picked the `ZFS` provider kind for hard reservations would
// suddenly oversubscribe the pool.
func TestZFSThickCreateOmitsSparseFlag(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("zfs list -H -o name tank/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("")})
	// Post-create ensureRefreservation pass (Bug 255 retrofit):
	// observable thick steady state — no `zfs set` issued.
	fx.Expect("zfs get -Hp -o value volsize tank/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("1073741824\n")})
	fx.Expect("zfs get -Hp -o value refreservation tank/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("1073741824\n")})

	p := zfs.NewProvider(zfs.Config{Pool: "tank", Thin: false}, fx)

	err := p.CreateVolume(t.Context(), storage.Volume{
		ResourceName: "pvc-1",
		VolumeNumber: 0,
		SizeKib:      1024 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	var createCmd string

	for _, line := range fx.CommandLines() {
		if strings.HasPrefix(line, "zfs create ") {
			createCmd = line

			break
		}
	}

	if createCmd == "" {
		t.Fatalf("no zfs create call recorded; got %v", fx.CommandLines())
	}

	// The line must contain `-V <size>M <dataset>` and must NOT
	// contain ` -s ` (sparse) — the THICK invariant.
	if !strings.Contains(createCmd, " -V 1024M tank/pvc-1_00000") {
		t.Errorf("create cmd missing `-V 1024M tank/pvc-1_00000`: %q", createCmd)
	}

	if strings.Contains(createCmd, " -s ") || strings.HasSuffix(createCmd, " -s") {
		t.Errorf("THICK CreateVolume must NOT include `-s` (sparse) flag; got %q", createCmd)
	}
}

// TestZFSThinCreateIncludesSparseFlag is the inverse-pair of
// TestZFSThickCreateOmitsSparseFlag — it pins that ZFS_THIN
// always passes `-s` so the zvol is sparse and contributes nothing
// to pool reservation accounting. Without this an accidental flip
// of the `thin` bool would still parse but break ZFS_THIN's
// oversubscription semantics.
func TestZFSThinCreateIncludesSparseFlag(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("zfs list -H -o name tank/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("")})

	p := zfs.NewProvider(zfs.Config{Pool: "tank", Thin: true}, fx)

	err := p.CreateVolume(t.Context(), storage.Volume{
		ResourceName: "pvc-1",
		VolumeNumber: 0,
		SizeKib:      1024 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	var createCmd string

	for _, line := range fx.CommandLines() {
		if strings.HasPrefix(line, "zfs create ") {
			createCmd = line

			break
		}
	}

	if createCmd == "" {
		t.Fatalf("no zfs create call recorded; got %v", fx.CommandLines())
	}

	// Both `-s` and `-V <size>M <dataset>` must be present.
	if !strings.Contains(createCmd, " -s ") {
		t.Errorf("THIN CreateVolume must include `-s` (sparse) flag; got %q", createCmd)
	}

	if !strings.Contains(createCmd, " -V 1024M tank/pvc-1_00000") {
		t.Errorf("create cmd missing `-V 1024M tank/pvc-1_00000`: %q", createCmd)
	}
}

// TestZFSThickPoolStatusMatchesZpoolFree pins the THICK
// PoolStatus arithmetic: even though thick reservations affect
// capacity SEMANTICS (no oversubscription), the math itself is
// identical to thin — `zpool list -H -p -o size,free` reports
// `free` already net of reservations from sibling thick zvols, so
// we surface `free/1024` as FreeCapacityKib and `size/1024` as
// TotalCapacityKib unchanged. The test exists to lock the
// invariant in case someone "fixes" the thick path to subtract
// allocated bytes manually (which would double-count).
func TestZFSThickPoolStatusMatchesZpoolFree(t *testing.T) {
	const (
		sizeBytes = int64(107374182400) // 100 GiB
		freeBytes = int64(80530636800)  // 75 GiB
		wantTotal = sizeBytes / 1024
		wantFree  = freeBytes / 1024
	)

	fx := storage.NewFakeExec()
	fx.Expect("zpool list -H -p -o size,free tank",
		storage.FakeResponse{Stdout: []byte("107374182400\t80530636800\n")})

	p := zfs.NewProvider(zfs.Config{Pool: "tank", Thin: false}, fx)

	got, err := p.PoolStatus(t.Context())
	if err != nil {
		t.Fatalf("PoolStatus: %v", err)
	}

	if got.TotalCapacityKib != wantTotal {
		t.Errorf("TotalCapacityKib (thick): got %d, want %d", got.TotalCapacityKib, wantTotal)
	}

	if got.FreeCapacityKib != wantFree {
		t.Errorf("FreeCapacityKib (thick): got %d, want %d", got.FreeCapacityKib, wantFree)
	}

	if !got.SupportsSnapshots {
		t.Errorf("SupportsSnapshots: got false, want true (ZFS always supports snapshots)")
	}
}

// TestDeleteSnapshotErrorWraps mirrors CreateSnapshot: a `zfs destroy
// <snap>` failure must surface with the "zfs destroy" wrap keyword.
func TestDeleteSnapshotErrorWraps(t *testing.T) {
	fx := storage.NewFakeExec()
	// Bug 212: pre-check must report the snapshot as present so we
	// reach the real destroy invocation. The fail-then-wrap contract
	// applies only to non-not-found errors.
	fx.Expect("zfs list -H -o name tank/pvc-1_00000@snap-1",
		storage.FakeResponse{Stdout: []byte("tank/pvc-1_00000@snap-1\n")})
	fx.Expect("zfs destroy tank/pvc-1_00000@snap-1",
		storage.FakeResponse{Err: errZFSListMissing})

	p := zfs.NewProvider(zfs.Config{Pool: "tank"}, fx)

	err := p.DeleteSnapshot(t.Context(), storage.Snapshot{
		ResourceName: "pvc-1",
		SnapshotName: "snap-1",
	})
	if err == nil {
		t.Fatalf("DeleteSnapshot: got nil, want error")
	}

	if msg := err.Error(); !strings.Contains(msg, "zfs destroy") {
		t.Errorf("wrap: %q must contain \"zfs destroy\" for operator grep", msg)
	}
}
