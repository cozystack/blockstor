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
	"slices"
	"testing"

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
