// SPDX-License-Identifier: Apache-2.0

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
	"fmt"
	"slices"
	"strings"
	"testing"

	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
)

func readBack(device string) string {
	return "dd if=" + device + " of=/dev/null bs=1M count=1 iflag=direct status=none"
}

func flushBufs(device string) string {
	return "blockdev --flushbufs " + device
}

// filesystemOnlyProbe is the backing probe this PR first shipped. It
// skips every non-filesystem signature, so a regression back to it
// reads LUKS or an LVM PV under the DRBD superblock as "nothing".
func filesystemOnlyProbe(device string) string {
	return "blkid -p -u filesystem -o export " + device
}

// TestApplyLatchFreeRetryRefusesNothingFoundItCannotReadBack: blkid
// exits 2 and prints nothing both for an empty device and for one
// whose reads fail (util-linux 2.41.5 on a dm "error" target), and
// the cache flush before the probe can fail too. Neither may read as
// "no filesystem".
func TestApplyLatchFreeRetryRefusesNothingFoundItCannotReadBack(t *testing.T) {
	eio := storage.FakeResponse{Err: errors.New("dd: error reading: Input/output error")}

	cases := map[string]func(device string) string{
		"read-back fails": readBack,
		"flush fails":     flushBufs,
	}

	for name, failing := range cases {
		t.Run(name, func(t *testing.T) {
			rd := "pvc-eio"
			dir := t.TempDir()
			fx := storage.NewFakeExec()
			device := expectThinBacking(fx, rd)
			fx.Expect("drbdsetup status "+rd+" --json",
				storage.FakeResponse{Stdout: []byte(statusBothUpToDateSecondary(rd))})
			expectDrbdMetaSignature(fx, device)
			fx.Expect(failing(device), eio)

			rec := newThinReconciler(fx, dir)

			_, err := rec.Apply(t.Context(), bug028FalseLatchDR(rd, "6800"))
			if err != nil {
				t.Fatalf("Apply: %v", err)
			}

			assertNoPromoteNoMkfs(t, fx.CommandLines())
			assertNoMkfsMarker(t, dir, rd)
		})
	}
}

// TestApplyLatchFreeRetrySeesNonFilesystemSignatures: LUKS, an LVM PV
// or swap a user put on a born-empty volume sits under the DRBD
// superblock just like ext4 does. The backing probe leaves out only the
// DRBD prober, so it sees them and does not promote.
func TestApplyLatchFreeRetrySeesNonFilesystemSignatures(t *testing.T) {
	cases := map[string]string{
		"luks":   "VERSION=2\nTYPE=crypto_LUKS\nUSAGE=crypto\n",
		"lvm pv": "VERSION=LVM2 001\nTYPE=LVM2_member\nUSAGE=raid\n",
		"swap":   "VERSION=1\nTYPE=swap\nUSAGE=other\n",
	}

	for name, export := range cases {
		t.Run(name, func(t *testing.T) {
			rd := "pvc-sig"
			dir := t.TempDir()
			fx := storage.NewFakeExec()
			device := expectThinBacking(fx, rd)
			fx.Expect("drbdsetup status "+rd+" --json",
				storage.FakeResponse{Stdout: []byte(statusBothUpToDateSecondary(rd))})
			fx.Expect(filesystemOnlyProbe(device), storage.FakeResponse{Err: storage.FakeExitError{Code: 2}})
			fx.Expect(backingProbe(device), storage.FakeResponse{Stdout: []byte("DEVNAME=" + device + "\n" + export)})

			rec := newThinReconciler(fx, dir)

			_, err := rec.Apply(t.Context(), bug028FalseLatchDR(rd, "6810"))
			if err != nil {
				t.Fatalf("Apply: %v", err)
			}

			assertNoPromoteNoMkfs(t, fx.CommandLines())
		})
	}
}

// TestApplyLatchFreeRetryFlushesBackingCacheFirst: DRBD holds the
// backing device open, so pages libblkid read on an earlier pass stay
// cached while a peer's replicated writes land below them. The flush
// has to come before the probe, or the probe can read a stale "empty".
func TestApplyLatchFreeRetryFlushesBackingCacheFirst(t *testing.T) {
	rd := "pvc-flush"
	dir := t.TempDir()
	fx := storage.NewFakeExec()
	device := expectThinBacking(fx, rd)
	fx.Expect("drbdsetup status "+rd+" --json",
		storage.FakeResponse{Stdout: []byte(statusBothUpToDateSecondary(rd))})
	expectDrbdMetaSignature(fx, device)

	rec := newThinReconciler(fx, dir)

	_, err := rec.Apply(t.Context(), bug028FalseLatchDR(rd, "6820"))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	cmds := fx.CommandLines()
	flush := slices.Index(cmds, flushBufs(device))
	probe := slices.Index(cmds, backingProbe(device))

	if flush < 0 || probe < 0 || flush > probe {
		t.Errorf("want %q before %q; got flush=%d probe=%d in %v", flushBufs(device), backingProbe(device), flush, probe, cmds)
	}

	if !slices.Contains(cmds, readBack(device)) {
		t.Errorf("a nothing-found answer must be read back before it is believed: %v", cmds)
	}
}

func mkfsTargetProbe(device string) string {
	return "blkid -p -o export " + device
}

// expectBlankMkfsTarget cans what `blkid -p` answers for a device that
// was never formatted: exit 2 and nothing on stderr. A test that leaves
// the probe at the FakeExec default (exit 0, no output) gets a probe
// that named nothing, which refuses mkfs.
func expectBlankMkfsTarget(fx *storage.FakeExec, devices ...string) {
	for _, device := range devices {
		fx.Expect(mkfsTargetProbe(device), storage.FakeResponse{Err: storage.FakeExitError{Code: 2}})
	}
}

// unknownMkfsTargetAnswers are the answers of the last probe before
// mkfs that do not prove the device empty. LINSTOR's hasFileSystem, and
// this probe before the fix, read every one of them as "no filesystem"
// and formatted.
func unknownMkfsTargetAnswers(device string) map[string][]storage.FakeResponse {
	nothing := storage.FakeResponse{Err: storage.FakeExitError{Code: 2}}

	return map[string][]storage.FakeResponse{
		"partition table only":  {{Stdout: []byte("DEVNAME=" + device + "\nPTUUID=4b4b8971\nPTTYPE=dos\n")}},
		"exit 0, nothing named": {{}},
		"ambivalent":            {{Err: storage.FakeExitError{Code: 8}}},
		"cannot open": {{Err: fmt.Errorf("%w: blkid -p -o export %s: blkid: error: %s: Permission denied",
			storage.FakeExitError{Code: 2}, device, device)}},
		"nothing found, read-back fails": {nothing, {Err: errors.New("dd: error reading: Input/output error")}},
	}
}

func expectMkfsTargetAnswer(fx *storage.FakeExec, device string, answer []storage.FakeResponse) {
	fx.Expect(mkfsTargetProbe(device), answer[0])

	if len(answer) > 1 {
		fx.Expect(readBack(device), answer[1])
	}
}

func assertNoMkfs(t *testing.T, cmds []string) {
	t.Helper()

	for _, line := range cmds {
		if strings.HasPrefix(line, "mkfs.") {
			t.Errorf("must NOT mkfs: %s", line)
		}
	}
}

// TestApplyPromotedMkfsRefusesUnknownTarget: on a born-empty replica
// that was promoted for its filesystem, a post-promote probe that
// cannot prove /dev/drbdN empty formats nothing and writes no marker.
func TestApplyPromotedMkfsRefusesUnknownTarget(t *testing.T) {
	for name, answer := range unknownMkfsTargetAnswers("/dev/drbd6830") {
		t.Run(name, func(t *testing.T) {
			rd := "pvc-tgt"
			dir := t.TempDir()
			fx := storage.NewFakeExec()
			device := expectThinBacking(fx, rd)
			fx.Expect("drbdsetup status "+rd+" --json",
				storage.FakeResponse{Stdout: []byte(statusBothUpToDateSecondary(rd))})
			expectDrbdMetaSignature(fx, device)
			expectMkfsTargetAnswer(fx, "/dev/drbd6830", answer)

			rec := newThinReconciler(fx, dir)

			_, err := rec.Apply(t.Context(), bug028FalseLatchDR(rd, "6830"))
			if err != nil {
				t.Fatalf("Apply: %v", err)
			}

			assertNoMkfs(t, fx.CommandLines())
			assertNoMkfsMarker(t, dir, rd)
		})
	}
}

// TestApplyStorageOnlyMkfsRefusesUnknownTarget: the storage-only stack
// formats the raw device directly, with no promote in between, so this
// probe is the only thing between mkfs and the data.
func TestApplyStorageOnlyMkfsRefusesUnknownTarget(t *testing.T) {
	const device = "/dev/vg/pvc-loc_00000"

	for name, answer := range unknownMkfsTargetAnswers(device) {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			fx := storage.NewFakeExec()
			expectThinBacking(fx, "pvc-loc")
			expectMkfsTargetAnswer(fx, device, answer)

			rec := newThinReconciler(fx, dir)

			results, err := rec.Apply(t.Context(), storageOnlyFilesystemDR("pvc-loc", skipInitTrue()))
			if err != nil {
				t.Fatalf("Apply: %v", err)
			}

			assertNoMkfs(t, fx.CommandLines())
			assertNoMkfsMarker(t, dir, "pvc-loc")

			if len(results) != 1 || results[0].GetOk() {
				t.Errorf("a refused mkfs must fail the apply so it is retried and seen: %v", results)
			}
		})
	}
}

// storageOnlyFilesystemDR is a linstor-csi "local" volume: STORAGE
// only, with FileSystem/Type set.
func storageOnlyFilesystemDR(rd string, skip *bool) []*intent.DesiredResource {
	return []*intent.DesiredResource{
		{
			Name:     rd,
			NodeName: "n1",
			Volumes: []*intent.DesiredVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
			},
			Props:           map[string]string{"FileSystem/Type": "ext4"},
			LayerStack:      []string{"STORAGE"},
			DrbdOptions:     map[string]string{},
			SkipInitialSync: skip,
		},
	}
}

// TestApplyStorageOnlyMkfsFormatsOnlyABornEmptyReplica: a storage-only
// Filesystem volume reaches mkfs on every reconcile until the marker
// exists, with no promote and no DRBD in between. An adopted one (the
// linstor-csi "local" class) has no marker, so it is formatted only if
// blockstor created it empty, never on what the probe happens to read.
func TestApplyStorageOnlyMkfsFormatsOnlyABornEmptyReplica(t *testing.T) {
	const device = "/dev/vg/pvc-loc_00000"

	cases := map[string]struct {
		skip *bool
		mkfs bool
	}{
		"born empty":      {skip: skipInitTrue(), mkfs: true},
		"adopted":         {skip: skipInitFalse()},
		"not yet stamped": {skip: nil},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			fx := storage.NewFakeExec()
			expectThinBacking(fx, "pvc-loc")
			expectBlankMkfsTarget(fx, device)

			rec := newThinReconciler(fx, dir)

			results, err := rec.Apply(t.Context(), storageOnlyFilesystemDR("pvc-loc", tc.skip))
			if err != nil {
				t.Fatalf("Apply: %v", err)
			}

			if len(results) != 1 || !results[0].GetOk() {
				t.Fatalf("Apply must succeed: %v", results)
			}

			if got := slices.Contains(fx.CommandLines(), "mkfs.ext4 "+device); got != tc.mkfs {
				t.Errorf("mkfs.ext4 ran = %v, want %v: %v", got, tc.mkfs, fx.CommandLines())
			}

			if !tc.mkfs {
				assertNoMkfsMarker(t, dir, "pvc-loc")
			}
		})
	}
}

// TestApplyPromotedMkfsDemotesWhenRefused: a mkfs the probe refused (or
// one that failed) is followed by the demote all the same. Left
// Primary, the replica would hold the volume against every other node.
func TestApplyPromotedMkfsDemotesWhenRefused(t *testing.T) {
	rd := "pvc-dem"
	dir := t.TempDir()
	fx := storage.NewFakeExec()
	device := expectThinBacking(fx, rd)
	fx.Expect("drbdsetup status "+rd+" --json",
		storage.FakeResponse{Stdout: []byte(statusBothUpToDateSecondary(rd))})
	expectDrbdMetaSignature(fx, device)
	fx.Expect(mkfsTargetProbe("/dev/drbd6840"), storage.FakeResponse{Err: storage.FakeExitError{Code: 8}})

	rec := newThinReconciler(fx, dir)

	results, err := rec.Apply(t.Context(), bug028FalseLatchDR(rd, "6840"))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	cmds := fx.CommandLines()
	promote := slices.Index(cmds, "drbdadm primary --force "+rd)
	demote := slices.Index(cmds, "drbdadm secondary "+rd)

	if promote < 0 || demote < promote {
		t.Errorf("want primary --force then secondary; got promote=%d demote=%d in %v", promote, demote, cmds)
	}

	if len(results) != 1 || results[0].GetOk() || !strings.Contains(results[0].GetMessage(), "refusing mkfs") {
		t.Errorf("the refused mkfs must still fail the apply: %v", results)
	}
}
