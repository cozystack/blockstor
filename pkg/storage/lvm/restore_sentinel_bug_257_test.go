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

package lvm_test

import (
	"slices"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"

	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/lvm"
)

// errFakeDDFailure simulates a mid-stream dd failure for the
// crash-recovery scenario (TestThickRestoreDDFailureLeavesSentinel).
// Declared as a static sentinel so the err113 linter is happy and
// future tests can reuse it.
var errFakeDDFailure = errors.New("dd: write error: I/O error")

// Bug 257 (P1, data integrity): LVM Thick.RestoreVolumeFromSnapshot and
// Thin.RecvSnapshot are two-step operations:
//
//  1. `lvcreate` the target LV
//  2. `dd` data from source onto the target LV
//
// If the satellite crashes BETWEEN those steps the LV exists but its
// content is garbage. The idempotent-skip on the next reconcile says
// "LV exists, done" → garbage is trusted as the restored volume. This
// is a silent data-integrity loss — the volume reports PROVISIONED, the
// PV header is whatever dd was mid-writing, and the upstream CSI/PV
// chain happily mounts it.
//
// Fix: completion sentinel via LV tag. Before step 1, add the tag
// `@blockstor-restore-incomplete` to the target LV (or, on resumed
// reconcile, detect it on a pre-existing LV). After step 2 succeeds,
// remove the tag. The idempotent-skip path inspects the tag and:
//
//   - tag absent → restore previously completed, skip.
//   - tag present → previous run crashed mid-dd, re-run the whole
//     sequence (the lvcreate is conditional on lvExists; the dd will
//     overwrite the partial bytes).
//
// We use an LV tag rather than a `__INCOMPLETE` suffix rename because
// LVM tags are first-class metadata, lvcreate accepts `--addtag` inline,
// and the lookup is a stock `lvs -o lv_tags` query the rest of the
// codebase already understands.

// ---------------------------------------------------------------------------
// Bug 257 — Thick.RestoreVolumeFromSnapshot: tag is added before dd and
// cleared after dd, and a crash leaves the tag so the next reconcile
// re-runs the whole sequence.
// ---------------------------------------------------------------------------

// TestThickRestoreSetsSentinelTagBeforeDDAndClearsAfter pins the
// happy-path: lvcreate carries `--addtag @blockstor-restore-incomplete`,
// dd runs, then `lvchange --deltag @blockstor-restore-incomplete` clears
// the sentinel.
func TestThickRestoreSetsSentinelTagBeforeDDAndClearsAfter(t *testing.T) {
	fx := storage.NewFakeExec()

	// Target LV must not exist (proceed past idempotency short-circuit).
	fx.Expect("lvs --config "+lvm.ConfigFilter+" --noheadings -o lv_name vg/pvc-2_00000",
		storage.FakeResponse{Stdout: []byte("")})
	// Source snapshot LV exists.
	fx.Expect("lvs --config "+lvm.ConfigFilter+" --noheadings -o lv_name vg/pvc-1_snap-1_00000",
		storage.FakeResponse{Stdout: []byte("pvc-1_snap-1_00000\n")})
	// VolumeStatus probe on the snapshot LV — reports a 1 GiB origin size.
	fx.Expect("lvs --config "+lvm.ConfigFilter+" --noheadings --separator | -o lv_path,lv_size --units k --nosuffix vg/pvc-1_snap-1_00000",
		storage.FakeResponse{Stdout: []byte("/dev/vg/pvc-1_snap-1_00000|1048576.00\n")})

	p := lvm.NewThick(lvm.ThickConfig{VolumeGroup: "vg"}, fx)

	err := p.RestoreVolumeFromSnapshot(t.Context(),
		storage.Volume{
			ResourceName: "pvc-2",
			VolumeNumber: 0,
			SizeKib:      1024 * 1024,
		},
		storage.Snapshot{
			ResourceName: "pvc-1",
			SnapshotName: "snap-1",
		},
	)
	if err != nil {
		t.Fatalf("RestoreVolumeFromSnapshot: %v", err)
	}

	lines := fx.CommandLines()

	lvcreateIdx, ddIdx, deltagIdx := -1, -1, -1

	for i, line := range lines {
		switch {
		case strings.HasPrefix(line, "lvcreate ") && strings.Contains(line, "--name pvc-2_00000"):
			lvcreateIdx = i
		case strings.HasPrefix(line, "dd "):
			ddIdx = i
		case strings.HasPrefix(line, "lvchange ") && strings.Contains(line, "--deltag") &&
			strings.Contains(line, lvm.RestoreIncompleteTag):
			deltagIdx = i
		}
	}

	if lvcreateIdx < 0 {
		t.Fatalf("Bug 257 fix: expected lvcreate for target LV; got %v", lines)
	}

	if ddIdx < 0 {
		t.Fatalf("Bug 257 fix: expected dd to copy snapshot bytes; got %v", lines)
	}

	if deltagIdx < 0 {
		t.Fatalf("Bug 257 fix: expected `lvchange --deltag %s` after dd to clear "+
			"the completion sentinel; got %v", lvm.RestoreIncompleteTag, lines)
	}

	// The lvcreate MUST carry the tag inline (--addtag), so a crash before
	// dd cannot leave an un-tagged LV that would be mis-trusted on the
	// next reconcile.
	if !strings.Contains(lines[lvcreateIdx], "--addtag "+lvm.RestoreIncompleteTag) {
		t.Errorf("Bug 257 fix: lvcreate must carry `--addtag %s` inline so the "+
			"sentinel is present BEFORE the dd step (otherwise a crash between "+
			"lvcreate and a separate addtag leaves the LV trusted as complete); "+
			"got %q", lvm.RestoreIncompleteTag, lines[lvcreateIdx])
	}

	// Ordering: dd between lvcreate and deltag.
	if lvcreateIdx >= ddIdx || ddIdx >= deltagIdx {
		t.Errorf("Bug 257 fix: ordering must be lvcreate < dd < deltag; "+
			"got lvcreate=%d, dd=%d, deltag=%d", lvcreateIdx, ddIdx, deltagIdx)
	}
}

// TestThickRestoreIdempotentSkipWithTagRerunsDD: when the target LV
// pre-exists AND carries the sentinel tag (previous reconcile crashed
// mid-dd), the provider MUST re-run the dd step rather than trust the
// garbage contents. The lvcreate is skipped (LV exists), but the dd
// runs and the tag is cleared.
func TestThickRestoreIdempotentSkipWithTagRerunsDD(t *testing.T) {
	fx := storage.NewFakeExec()

	// Target LV pre-exists.
	fx.Expect("lvs --config "+lvm.ConfigFilter+" --noheadings -o lv_name vg/pvc-2_00000",
		storage.FakeResponse{Stdout: []byte("pvc-2_00000\n")})
	// Sentinel tag IS present on the target → previous crash mid-dd.
	fx.Expect("lvs --config "+lvm.ConfigFilter+" --noheadings -o lv_tags vg/pvc-2_00000",
		storage.FakeResponse{Stdout: []byte(lvm.RestoreIncompleteTag + "\n")})
	// Source snapshot LV exists.
	fx.Expect("lvs --config "+lvm.ConfigFilter+" --noheadings -o lv_name vg/pvc-1_snap-1_00000",
		storage.FakeResponse{Stdout: []byte("pvc-1_snap-1_00000\n")})

	p := lvm.NewThick(lvm.ThickConfig{VolumeGroup: "vg"}, fx)

	err := p.RestoreVolumeFromSnapshot(t.Context(),
		storage.Volume{
			ResourceName: "pvc-2",
			VolumeNumber: 0,
			SizeKib:      1024 * 1024,
		},
		storage.Snapshot{
			ResourceName: "pvc-1",
			SnapshotName: "snap-1",
		},
	)
	if err != nil {
		t.Fatalf("RestoreVolumeFromSnapshot: %v", err)
	}

	lines := fx.CommandLines()

	// lvcreate MUST NOT be issued — the LV already exists.
	for _, line := range lines {
		if strings.HasPrefix(line, "lvcreate ") {
			t.Errorf("Bug 257 fix: tag-present idempotent re-run must NOT re-issue "+
				"lvcreate (LV already exists); got %q", line)
		}
	}

	// dd MUST be issued so the garbage from the crashed run is overwritten.
	sawDD := false

	for _, line := range lines {
		if strings.HasPrefix(line, "dd ") {
			sawDD = true

			break
		}
	}

	if !sawDD {
		t.Errorf("Bug 257 fix: tag-present idempotent re-run MUST re-run the dd "+
			"step (otherwise garbage from the crashed run is trusted as restored "+
			"content); got %v", lines)
	}

	// deltag MUST be issued after the dd succeeds.
	wantDeltag := func() bool {
		for _, line := range lines {
			if strings.HasPrefix(line, "lvchange ") && strings.Contains(line, "--deltag") &&
				strings.Contains(line, lvm.RestoreIncompleteTag) {
				return true
			}
		}

		return false
	}()

	if !wantDeltag {
		t.Errorf("Bug 257 fix: tag-present idempotent re-run MUST clear the "+
			"sentinel after dd completes; got %v", lines)
	}
}

// TestThickRestoreIdempotentSkipWithoutTagIsNoop: when the target LV
// pre-exists and the sentinel tag is ABSENT (previous run completed
// cleanly), the provider MUST short-circuit — no dd, no deltag.
func TestThickRestoreIdempotentSkipWithoutTagIsNoop(t *testing.T) {
	fx := storage.NewFakeExec()

	// Target LV pre-exists.
	fx.Expect("lvs --config "+lvm.ConfigFilter+" --noheadings -o lv_name vg/pvc-2_00000",
		storage.FakeResponse{Stdout: []byte("pvc-2_00000\n")})
	// Sentinel tag ABSENT → previous run completed cleanly.
	fx.Expect("lvs --config "+lvm.ConfigFilter+" --noheadings -o lv_tags vg/pvc-2_00000",
		storage.FakeResponse{Stdout: []byte("")})

	p := lvm.NewThick(lvm.ThickConfig{VolumeGroup: "vg"}, fx)

	err := p.RestoreVolumeFromSnapshot(t.Context(),
		storage.Volume{
			ResourceName: "pvc-2",
			VolumeNumber: 0,
			SizeKib:      1024 * 1024,
		},
		storage.Snapshot{
			ResourceName: "pvc-1",
			SnapshotName: "snap-1",
		},
	)
	if err != nil {
		t.Fatalf("RestoreVolumeFromSnapshot: %v", err)
	}

	for _, line := range fx.CommandLines() {
		if strings.HasPrefix(line, "dd ") {
			t.Errorf("clean-state idempotent skip must NOT re-issue dd; got %q", line)
		}

		if strings.HasPrefix(line, "lvchange ") && strings.Contains(line, "--deltag") {
			t.Errorf("clean-state idempotent skip must NOT issue deltag "+
				"(sentinel was never set); got %q", line)
		}
	}
}

// TestThickRestoreDDFailureLeavesSentinel: when dd fails mid-stream the
// sentinel tag MUST remain on the LV (deltag NOT issued) so the next
// reconcile detects the incomplete state and re-runs both steps.
func TestThickRestoreDDFailureLeavesSentinel(t *testing.T) {
	fx := storage.NewFakeExec()

	// Target LV must not exist (proceed past idempotency short-circuit).
	fx.Expect("lvs --config "+lvm.ConfigFilter+" --noheadings -o lv_name vg/pvc-2_00000",
		storage.FakeResponse{Stdout: []byte("")})
	// Source snapshot LV exists.
	fx.Expect("lvs --config "+lvm.ConfigFilter+" --noheadings -o lv_name vg/pvc-1_snap-1_00000",
		storage.FakeResponse{Stdout: []byte("pvc-1_snap-1_00000\n")})
	// VolumeStatus probe on the snapshot LV.
	fx.Expect("lvs --config "+lvm.ConfigFilter+" --noheadings --separator | -o lv_path,lv_size --units k --nosuffix vg/pvc-1_snap-1_00000",
		storage.FakeResponse{Stdout: []byte("/dev/vg/pvc-1_snap-1_00000|1048576.00\n")})
	// dd FAILS — simulates the crash-mid-copy scenario.
	fx.Expect("dd if=/dev/vg/pvc-1_snap-1_00000 of=/dev/vg/pvc-2_00000 bs=1M conv=fsync",
		storage.FakeResponse{Err: errFakeDDFailure})

	p := lvm.NewThick(lvm.ThickConfig{VolumeGroup: "vg"}, fx)

	err := p.RestoreVolumeFromSnapshot(t.Context(),
		storage.Volume{
			ResourceName: "pvc-2",
			VolumeNumber: 0,
			SizeKib:      1024 * 1024,
		},
		storage.Snapshot{
			ResourceName: "pvc-1",
			SnapshotName: "snap-1",
		},
	)
	if err == nil {
		t.Fatalf("RestoreVolumeFromSnapshot: expected dd failure to propagate")
	}

	// deltag MUST NOT be issued — the sentinel must survive so the next
	// reconcile re-runs the whole sequence.
	for _, line := range fx.CommandLines() {
		if strings.HasPrefix(line, "lvchange ") && strings.Contains(line, "--deltag") {
			t.Errorf("Bug 257 fix: dd failure must leave the sentinel in place "+
				"(deltag MUST NOT run) so the next reconcile detects the incomplete "+
				"state and re-runs the whole sequence; got %q", line)
		}
	}
}

// ---------------------------------------------------------------------------
// Bug 257 — Thin.RecvSnapshot: same completion-sentinel pattern.
// ---------------------------------------------------------------------------

// TestThinRecvSetsSentinelTagBeforeDDAndClearsAfter pins the happy
// path for the thin recv: pre-create LV carries `--addtag`, dd runs,
// deltag clears the sentinel.
func TestThinRecvSetsSentinelTagBeforeDDAndClearsAfter(t *testing.T) {
	fx := storage.NewFakeExec()

	// Target LV must not exist on the entry idempotency check.
	fx.Expect("lvs --config "+lvm.ConfigFilter+" --noheadings -o lv_name vg/pvc-2_00000",
		storage.FakeResponse{Stdout: []byte("")})

	p := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "pool"}, fx)

	err := p.RecvSnapshot(t.Context(),
		storage.Volume{
			ResourceName: "pvc-2",
			VolumeNumber: 0,
			SizeKib:      1024 * 1024,
		},
		strings.NewReader("fake stream"),
	)
	if err != nil {
		t.Fatalf("RecvSnapshot: %v", err)
	}

	lines := fx.CommandLines()

	// Find the load-bearing steps.
	var lvcreateLine string

	deltagIdx := -1

	for i, line := range lines {
		if strings.HasPrefix(line, "lvcreate ") && strings.Contains(line, "--name pvc-2_00000") {
			lvcreateLine = line
		}

		if strings.HasPrefix(line, "lvchange ") && strings.Contains(line, "--deltag") &&
			strings.Contains(line, lvm.RestoreIncompleteTag) {
			deltagIdx = i
		}
	}

	if lvcreateLine == "" {
		t.Fatalf("Bug 257 fix: expected lvcreate for target LV; got %v", lines)
	}

	if !strings.Contains(lvcreateLine, "--addtag "+lvm.RestoreIncompleteTag) {
		t.Errorf("Bug 257 fix: Thin.RecvSnapshot lvcreate must carry "+
			"`--addtag %s` inline so the sentinel is present BEFORE the dd "+
			"step; got %q", lvm.RestoreIncompleteTag, lvcreateLine)
	}

	if deltagIdx < 0 {
		t.Errorf("Bug 257 fix: Thin.RecvSnapshot must issue `lvchange --deltag "+
			"%s` after the dd completes; got %v", lvm.RestoreIncompleteTag, lines)
	}
}

// TestThinRecvIdempotentSkipWithTagRerunsRecv: when the target LV
// pre-exists AND carries the sentinel tag, the recv MUST be re-run.
func TestThinRecvIdempotentSkipWithTagRerunsRecv(t *testing.T) {
	fx := storage.NewFakeExec()

	// Target LV pre-exists.
	fx.Expect("lvs --config "+lvm.ConfigFilter+" --noheadings -o lv_name vg/pvc-2_00000",
		storage.FakeResponse{Stdout: []byte("pvc-2_00000\n")})
	// Sentinel tag IS present → previous crash mid-recv.
	fx.Expect("lvs --config "+lvm.ConfigFilter+" --noheadings -o lv_tags vg/pvc-2_00000",
		storage.FakeResponse{Stdout: []byte(lvm.RestoreIncompleteTag + "\n")})

	p := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "pool"}, fx)

	err := p.RecvSnapshot(t.Context(),
		storage.Volume{
			ResourceName: "pvc-2",
			VolumeNumber: 0,
			SizeKib:      1024 * 1024,
		},
		strings.NewReader("fake stream"),
	)
	if err != nil {
		t.Fatalf("RecvSnapshot: %v", err)
	}

	lines := fx.CommandLines()

	// lvcreate MUST NOT be issued — the LV already exists.
	for _, line := range lines {
		if strings.HasPrefix(line, "lvcreate ") {
			t.Errorf("Bug 257 fix: tag-present idempotent re-run must NOT re-issue "+
				"lvcreate (LV already exists); got %q", line)
		}
	}

	// deltag MUST be issued after the rerun completes.
	sawDeltag := slices.ContainsFunc(lines, func(line string) bool {
		return strings.HasPrefix(line, "lvchange ") && strings.Contains(line, "--deltag") &&
			strings.Contains(line, lvm.RestoreIncompleteTag)
	})

	if !sawDeltag {
		t.Errorf("Bug 257 fix: tag-present idempotent re-run MUST clear the "+
			"sentinel after the rerun completes; got %v", lines)
	}
}

// TestThinRecvIdempotentSkipWithoutTagIsNoop: tag absent → short-circuit.
func TestThinRecvIdempotentSkipWithoutTagIsNoop(t *testing.T) {
	fx := storage.NewFakeExec()

	// Target LV pre-exists.
	fx.Expect("lvs --config "+lvm.ConfigFilter+" --noheadings -o lv_name vg/pvc-2_00000",
		storage.FakeResponse{Stdout: []byte("pvc-2_00000\n")})
	// Sentinel tag ABSENT.
	fx.Expect("lvs --config "+lvm.ConfigFilter+" --noheadings -o lv_tags vg/pvc-2_00000",
		storage.FakeResponse{Stdout: []byte("")})

	p := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "pool"}, fx)

	err := p.RecvSnapshot(t.Context(),
		storage.Volume{
			ResourceName: "pvc-2",
			VolumeNumber: 0,
			SizeKib:      1024 * 1024,
		},
		strings.NewReader("fake stream"),
	)
	if err != nil {
		t.Fatalf("RecvSnapshot: %v", err)
	}

	for _, line := range fx.CommandLines() {
		if strings.HasPrefix(line, "lvchange ") && strings.Contains(line, "--deltag") {
			t.Errorf("clean-state idempotent skip must NOT issue deltag "+
				"(sentinel was never set); got %q", line)
		}
	}
}
