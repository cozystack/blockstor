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
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/cozystack/blockstor/pkg/satellite"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
)

// Issue #265 regression pins: after a LINSTOR adoption every unused
// Filesystem-mode volume was force-promoted for a latch-free mkfs
// retry. The adopted volumes have no `.mkfs.done` marker (LINSTOR
// never wrote one) and their backing device answered a plain blkid
// with TYPE=drbd only, which the retry read as "no filesystem". Only
// the post-promote probe on /dev/drbdN kept mkfs off the data.

// plainBlkid is the probe the retry used before the fix. It is canned
// with the answer the adopted volumes gave so a regression back to it
// reads "no user filesystem" and the tests below catch the promote.
func plainBlkid(device string) string {
	return "blkid -o export " + device
}

func expectAdoptedExt4Backing(fx *storage.FakeExec, device string) {
	fx.Expect(plainBlkid(device),
		storage.FakeResponse{Stdout: []byte("DEVNAME=" + device + "\nTYPE=drbd\n")})
	fx.Expect(backingProbe(device),
		storage.FakeResponse{Stdout: []byte("DEVNAME=" + device + "\nTYPE=ext4\nUSAGE=filesystem\n")})
}

func assertNoMkfsMarker(t *testing.T, dir, rd string) {
	t.Helper()

	_, err := os.Stat(filepath.Join(dir, rd+".mkfs.done"))
	if err == nil {
		t.Errorf(".mkfs.done must NOT be written for %s", rd)
	}
}

// TestApplyAdoptedVolumeNeverPromotedForMkfs: a replica that was not
// born empty in blockstor (linstor-migrate stamps SkipInitialSync=false
// on every adopted replica; nil is a replica not yet stamped) is never
// promoted for mkfs, even when every replica is Secondary and lock-step
// UpToDate, no marker exists, and the backing probe finds no
// filesystem it can name. The volume holds data blockstor never wrote.
func TestApplyAdoptedVolumeNeverPromotedForMkfs(t *testing.T) {
	cases := map[string]struct {
		skip   *bool
		ext4   bool
		suffix string
	}{
		"adopted, ext4 below the DRBD superblock": {skip: skipInitFalse(), ext4: true, suffix: "a"},
		"adopted, backing probe finds nothing":    {skip: skipInitFalse(), suffix: "b"},
		"not yet stamped":                         {skip: nil, suffix: "c"},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			rd := "pvc-adopt" + tc.suffix
			dir := t.TempDir()
			fx := storage.NewFakeExec()
			device := expectThinBacking(fx, rd)
			fx.Expect("drbdsetup status "+rd+" --json",
				storage.FakeResponse{Stdout: []byte(statusBothUpToDateSecondary(rd))})

			if tc.ext4 {
				expectAdoptedExt4Backing(fx, device)
			} else {
				expectDrbdMetaSignature(fx, device)
			}

			rec := newThinReconciler(fx, dir)

			dr := bug028FalseLatchDR(rd, "6700")
			dr[0].SkipInitialSync = tc.skip

			_, err := rec.Apply(t.Context(), dr)
			if err != nil {
				t.Fatalf("Apply: %v", err)
			}

			assertNoPromoteNoMkfs(t, fx.CommandLines())
			assertNoMkfsMarker(t, dir, rd)
		})
	}
}

// TestApplyLatchFreeRetryReadsFilesystemBelowDrbdMetadata is the
// probe half of #265 on its own: even on a replica born empty, a
// backing device whose plain blkid answer is TYPE=drbd but which
// carries ext4 from offset 0 is not a mkfs candidate.
func TestApplyLatchFreeRetryReadsFilesystemBelowDrbdMetadata(t *testing.T) {
	rd := "pvc-adoptp"
	dir := t.TempDir()
	fx := storage.NewFakeExec()
	device := expectThinBacking(fx, rd)
	fx.Expect("drbdsetup status "+rd+" --json",
		storage.FakeResponse{Stdout: []byte(statusBothUpToDateSecondary(rd))})
	expectAdoptedExt4Backing(fx, device)

	rec := newThinReconciler(fx, dir)

	_, err := rec.Apply(t.Context(), bug028FalseLatchDR(rd, "6710"))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	assertNoPromoteNoMkfs(t, fx.CommandLines())
	assertNoMkfsMarker(t, dir, rd)
}

// unknownBackingAnswers are the backing-probe results that do not prove
// a volume empty. Each must keep both the latch-free retry and the day0
// bypass from promoting.
func unknownBackingAnswers(device string) map[string]storage.FakeResponse {
	return map[string]storage.FakeResponse{
		"drbd signature only": {Stdout: []byte("DEVNAME=" + device + "\nVERSION=v09\nTYPE=drbd\nUSAGE=raid\n")},
		"partition table":     {Stdout: []byte("DEVNAME=" + device + "\nPTUUID=4b4b8971\nPTTYPE=dos\n")},
		"empty success":       {},
		"ambivalent":          {Err: storage.FakeExitError{Code: 8}},
		"usage error":         {Err: storage.FakeExitError{Code: 4}},
		"cannot open": {Err: fmt.Errorf("%w: blkid -p -u filesystem -o export %s: blkid: error: %s: Permission denied",
			storage.FakeExitError{Code: 2}, device, device)},
	}
}

// TestApplyLatchFreeRetryRefusesUnknownBackingProbe: an ambiguous or
// DRBD-only probe result means "unknown", never "missing". No promote,
// no mkfs.
func TestApplyLatchFreeRetryRefusesUnknownBackingProbe(t *testing.T) {
	for name, resp := range unknownBackingAnswers("/dev/vg/pvc-unk_00000") {
		t.Run(name, func(t *testing.T) {
			rd := "pvc-unk"
			dir := t.TempDir()
			fx := storage.NewFakeExec()
			device := expectThinBacking(fx, rd)
			fx.Expect("drbdsetup status "+rd+" --json",
				storage.FakeResponse{Stdout: []byte(statusBothUpToDateSecondary(rd))})
			fx.Expect(backingProbe(device), resp)

			rec := newThinReconciler(fx, dir)

			_, err := rec.Apply(t.Context(), bug028FalseLatchDR(rd, "6720"))
			if err != nil {
				t.Fatalf("Apply: %v", err)
			}

			assertNoPromoteNoMkfs(t, fx.CommandLines())
			assertNoMkfsMarker(t, dir, rd)
		})
	}
}

// TestApplyBug028BypassRefusesUnknownBackingProbe: the day0 bypass of
// the force-promote veto takes the same rule.
func TestApplyBug028BypassRefusesUnknownBackingProbe(t *testing.T) {
	for name, resp := range unknownBackingAnswers("/dev/vg/pvc-unkd_00000") {
		t.Run(name, func(t *testing.T) {
			rd := "pvc-unkd"
			dir := t.TempDir()
			fx := storage.NewFakeExec()
			device := expectThinBacking(fx, rd)
			fx.Expect("drbdsetup status "+rd+" --json",
				storage.FakeResponse{Stdout: []byte(statusBothUpToDateSecondary(rd))})
			expectGetGI(fx, rd, device, satellite.Day0GiForTest(rd, 0))
			fx.Expect(backingProbe(device), resp)

			rec := newThinReconciler(fx, dir)

			_, err := rec.Apply(t.Context(), bug028WinnerDR(rd, "6730"))
			if err != nil {
				t.Fatalf("Apply: %v", err)
			}

			assertNoPromoteNoMkfs(t, fx.CommandLines())
		})
	}
}

// expectThinBackingVolume1 cans the lvs probes for volume 1 of rd.
func expectThinBackingVolume1(fx *storage.FakeExec, rd string) string {
	lv := rd + "_00001"
	device := "/dev/vg/" + lv

	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/"+lv,
		storage.FakeResponse{Stdout: []byte(lv + "\n")})
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings --separator | -o lv_path,lv_size --units k --nosuffix vg/"+lv,
		storage.FakeResponse{Stdout: []byte(device + "|1048576\n")})

	return device
}

func statusTwoVolumesUpToDateSecondary(rd string) string {
	return `[{
	  "name":"` + rd + `","node-id":0,"role":"Secondary",
	  "devices":[{"volume":0,"disk-state":"UpToDate"},{"volume":1,"disk-state":"UpToDate"}],
	  "connections":[{
	    "peer-node-id":1,"name":"n2","connection-state":"Connected",
	    "peer-role":"Secondary",
	    "peer_devices":[{"volume":0,"peer-disk-state":"UpToDate"},{"volume":1,"peer-disk-state":"UpToDate"}]
	  }]
	}]`
}

func twoVolumeFalseLatchDR(rd, minor string) []*intent.DesiredResource {
	dr := bug028FalseLatchDR(rd, minor)
	dr[0].Volumes = append(dr[0].Volumes,
		&intent.DesiredVolume{VolumeNumber: 1, SizeKib: 1024 * 1024, StoragePool: "thin1"})

	return dr
}

// TestApplyLatchFreeRetryMultiVolume: the promote covers every volume
// of the resource, so one volume the probe cannot read refuses the
// retry even when another is provably empty; with that volume readable
// as formatted, the empty one still gets its filesystem.
func TestApplyLatchFreeRetryMultiVolume(t *testing.T) {
	cases := map[string]struct {
		vol1    storage.FakeResponse
		promote bool
	}{
		"other volume unknown":   {vol1: storage.FakeResponse{Stdout: []byte("TYPE=drbd\n")}},
		"other volume formatted": {vol1: storage.FakeResponse{Stdout: []byte("TYPE=ext4\n")}, promote: true},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			rd := "pvc-mv"
			dir := t.TempDir()
			fx := storage.NewFakeExec()
			dev0 := expectThinBacking(fx, rd)
			dev1 := expectThinBackingVolume1(fx, rd)
			fx.Expect("drbdsetup status "+rd+" --json",
				storage.FakeResponse{Stdout: []byte(statusTwoVolumesUpToDateSecondary(rd))})
			expectDrbdMetaSignature(fx, dev0)
			fx.Expect(backingProbe(dev1), tc.vol1)
			// Once promoted, volume 1's replicated device shows what its
			// backing probe saw; volume 0's stays at the FakeExec default.
			fx.Expect("blkid -o export /dev/drbd6741", tc.vol1)

			rec := newThinReconciler(fx, dir)

			_, err := rec.Apply(t.Context(), twoVolumeFalseLatchDR(rd, "6740"))
			if err != nil {
				t.Fatalf("Apply: %v", err)
			}

			if tc.promote {
				cmds := fx.CommandLines()
				assertPromoteMkfsDemoteOrder(t, cmds, rd, "/dev/drbd6740")

				if slices.Contains(cmds, "mkfs.ext4 /dev/drbd6741") {
					t.Errorf("the formatted volume must be adopted, not re-formatted: %v", cmds)
				}

				return
			}

			assertNoPromoteNoMkfs(t, fx.CommandLines())
			assertNoMkfsMarker(t, dir, rd)
		})
	}
}
