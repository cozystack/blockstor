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
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/satellite"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/lvm"
)

// BUG-028 regression pins — the day0 mkfs race and its false-latch
// terminal state.
//
// Failure chain on a fresh RWX RD (FileSystem/Type=ext4):
//
//  1. Day0 skip-initial-sync brings BOTH diskful replicas
//     Connected+UpToDate at the shared deterministic day0 GI in ~2s.
//  2. The mkfs-election winner reaches finishDRBDApply AFTER that;
//     the Bug 342 force-promote kernel veto (AnyConnectedPeerHasData)
//     sees peer-disk:UpToDate, cannot tell the EMPTY day0 sibling from
//     real data, and silently skips the one-and-only first-activation
//     mkfs. firstActivation latches false.
//  3. An external promoter (drbd-reactor RWX path) promote/demotes the
//     empty volume in 20s cycles; each promote bumps the current-UUID
//     past day0 WITHOUT writing data.
//  4. The controller reads "UpToDate diskful, CurrentGI != day0" as
//     proven data → RD.Spec.Initialized latches true (FALSELY).
//  5. The dispatcher gates the auto-primary election on !rdInitialized
//     → no replica carries auto-primary → the Bug-311 mkfs retry
//     (gated on autoPrimaryReplica) is permanently dead. Terminal:
//     promote → fsck "Bad magic number" → demote, forever.
//
// The fix is two-sided and these tests pin both sides plus the
// data-safety counter-cases:
//
//   - day0EmptyMkfsBypass: the veto may be bypassed ONLY when every
//     signal proves the whole connected set is day0-empty siblings
//     (Spec.SkipInitialSync=true, PeerHasData=false, local metadata
//     current-UUID == day0, no fs signature) → mkfs happens (step 2
//     fixed, steps 3-5 never start).
//   - latchFreeMkfsRetryAllowed: the Bug-311 retry no longer depends
//     solely on the dispatcher's auto-primary election (killed by the
//     false latch); the deterministic lowest-diskful-node-id winner
//     re-enters promote→mkfs→demote when the kernel set is provably
//     promote-safe and the filesystem is provably absent.

// statusBothUpToDateSecondary is the kernel view of the BUG-028 race /
// terminal state: local Secondary UpToDate, one diskful peer Secondary
// UpToDate, one diskless tiebreaker.
func statusBothUpToDateSecondary(rd string) string {
	return `[{
	  "name":"` + rd + `","node-id":0,"role":"Secondary",
	  "devices":[{"volume":0,"disk-state":"UpToDate"}],
	  "connections":[{
	    "peer-node-id":1,"name":"n2","connection-state":"Connected",
	    "peer-role":"Secondary",
	    "peer_devices":[{"volume":0,"peer-disk-state":"UpToDate"}]
	  }]
	}]`
}

// expectThinBacking cans the lvs probes so applyStorage resolves the
// (already-carved) thin LV and populates the devices map with its
// backing path — which the BUG-028 probes (drbdmeta get-gi current-UUID
// read, blkid fs-signature probe) target.
func expectThinBacking(fx *storage.FakeExec, rd string) string {
	lv := rd + "_00000"
	device := "/dev/vg/" + lv

	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/"+lv,
		storage.FakeResponse{Stdout: []byte(lv + "\n")})
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings --separator | -o lv_path,lv_size --units k --nosuffix vg/"+lv,
		storage.FakeResponse{Stdout: []byte(device + "|1048576\n")})

	return device
}

func expectGetGI(fx *storage.FakeExec, rd, device, currentGI string) {
	fx.Expect("drbdmeta --force "+rd+"/0 v09 "+device+" internal get-gi --node-id 0",
		storage.FakeResponse{Stdout: []byte(currentGI + ":0000000000000000:0:0:1:1:0:0:0:0\n")})
}

// expectDrbdMetaSignature cans the blkid answer real hardware gives
// for the BACKING device of an internal-metadata DRBD volume: libblkid
// recognises the DRBD meta superblock at the device tail and reports
// TYPE=drbd even on a never-formatted volume (stand forensics,
// bug028-fix-validation-20260612-054452/iter2). The BUG-028 probes
// MUST read this as "no user filesystem" — the naive any-TYPE= probe
// wedged both the bypass and the latch-free retry forever.
func expectDrbdMetaSignature(fx *storage.FakeExec, device string) {
	fx.Expect("blkid -o export "+device,
		storage.FakeResponse{Stdout: []byte("DEVNAME=" + device + "\nUUID=6715da3a6dd3182a\nTYPE=drbd\n")})
}

func newThinReconciler(fx *storage.FakeExec, dir string) *satellite.Reconciler {
	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)

	return satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		Exec:      fx,
		StateDir:  dir,
		NodeName:  "n1",
	})
}

// bug028WinnerDR is the elected mkfs winner's wire payload at first
// activation: auto-primary stamped, SkipInitialSync=true, RD-level
// FileSystem/Type, one diskful peer.
func bug028WinnerDR(rd, minor string) []*intent.DesiredResource {
	return []*intent.DesiredResource{
		{
			Name:     rd,
			NodeName: "n1",
			Volumes: []*intent.DesiredVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
			},
			Props: map[string]string{
				"FileSystem/Type": "ext4",
			},
			Peers:           []intent.DesiredPeer{{Name: "n2"}},
			SkipInitialSync: skipInitTrue(),
			DrbdOptions: map[string]string{
				"port": "7000", "node-id": "0", "address": "10.0.0.1", "minor": minor,
				"peer.n2.port": "7000", "peer.n2.node-id": "1", "peer.n2.address": "10.0.0.2",
				"auto-primary": "true",
			},
		},
	}
}

func assertPromoteMkfsDemoteOrder(t *testing.T, cmds []string, rd, drbdDev string) {
	t.Helper()

	posPrim, posMkfs, posSec := -1, -1, -1

	for i, line := range cmds {
		switch {
		case posPrim < 0 && strings.Contains(line, "drbdadm primary --force "+rd):
			posPrim = i
		case posMkfs < 0 && strings.Contains(line, "mkfs.ext4 "+drbdDev):
			posMkfs = i
		case posSec < 0 && strings.Contains(line, "drbdadm secondary "+rd):
			posSec = i
		}
	}

	if posPrim < 0 || posMkfs <= posPrim || posSec <= posMkfs {
		t.Errorf("want primary --force < mkfs < secondary; got prim=%d mkfs=%d sec=%d in %v",
			posPrim, posMkfs, posSec, cmds)
	}
}

func assertNoPromoteNoMkfs(t *testing.T, cmds []string) {
	t.Helper()

	for _, line := range cmds {
		if strings.Contains(line, "primary --force") {
			t.Errorf("must NOT force-promote: %s", line)
		}

		if strings.HasPrefix(line, "mkfs.") || strings.Contains(line, " mkfs.") {
			t.Errorf("must NOT mkfs: %s", line)
		}
	}
}

// TestApplyBug028Day0RaceVetoBypassedMkfsRuns pins the race fix: the
// day0 siblings connect Connected+UpToDate BEFORE the winner's
// first-activation pass, the Bug 342 kernel veto fires — and the
// day0-empty bypass (Spec.SkipInitialSync=true, PeerHasData=false,
// local current-UUID == day0, no fs signature) lets the promote+mkfs
// proceed anyway. Pre-fix the mkfs was silently skipped here, forever.
func TestApplyBug028Day0RaceVetoBypassedMkfsRuns(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()
	device := expectThinBacking(fx, "pvc-b028")
	// Kernel truth at the winner's finishDRBDApply: the day0 sibling is
	// already Connected+UpToDate → AnyConnectedPeerHasData vetoes.
	fx.Expect("drbdsetup status pvc-b028 --json",
		storage.FakeResponse{Stdout: []byte(statusBothUpToDateSecondary("pvc-b028"))})
	// Local metadata still sits at the deterministic day0 current-UUID —
	// the exact proof that every Connected+UpToDate peer is a
	// never-written day0 sibling of the same generation.
	expectGetGI(fx, "pvc-b028", device, satellite.Day0GiForTest("pvc-b028", 0))
	// Backing-device blkid answers TYPE=drbd (the real-hardware shape:
	// libblkid sees the internal DRBD metadata superblock) — the bypass
	// must read that as "no user filesystem". The post-promote probe on
	// /dev/drbd6500 returns the FakeExec default (no signature).
	expectDrbdMetaSignature(fx, device)

	rec := newThinReconciler(fx, dir)

	_, err := rec.Apply(t.Context(), bug028WinnerDR("pvc-b028", "6500"))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	cmds := fx.CommandLines()
	assertPromoteMkfsDemoteOrder(t, cmds, "pvc-b028", "/dev/drbd6500")

	if _, statErr := os.Stat(filepath.Join(dir, "pvc-b028.mkfs.done")); statErr != nil {
		t.Errorf(".mkfs.done marker must be written after the bypassed-veto mkfs; got stat err %v", statErr)
	}
}

// TestApplyBug028VetoHoldsOnNonDay0PeerGI is the data-safety
// counter-case: the kernel veto fires and the local current-UUID is
// NOT day0 (a real data generation — relocate survivor / post-write
// state). The bypass must refuse: no promote, no mkfs; the replica
// stays on the full-resync path. NEVER mkfs over real data.
func TestApplyBug028VetoHoldsOnNonDay0PeerGI(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()
	device := expectThinBacking(fx, "pvc-b028d")
	fx.Expect("drbdsetup status pvc-b028d --json",
		storage.FakeResponse{Stdout: []byte(statusBothUpToDateSecondary("pvc-b028d"))})
	// A runtime current-UUID (cannot equal the deterministic day0).
	expectGetGI(fx, "pvc-b028d", device, "2BCB1C8F00B058AE")

	rec := newThinReconciler(fx, dir)

	_, err := rec.Apply(t.Context(), bug028WinnerDR("pvc-b028d", "6510"))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	assertNoPromoteNoMkfs(t, fx.CommandLines())

	if _, statErr := os.Stat(filepath.Join(dir, "pvc-b028d.mkfs.done")); statErr == nil {
		t.Error(".mkfs.done must NOT be written when the veto holds")
	}
}

// TestApplyBug028VetoHoldsOnFsSignature: belt-and-braces counter-case —
// even at day0 GI, a filesystem signature on the backing device refuses
// the bypass (there are bytes a mkfs would destroy).
func TestApplyBug028VetoHoldsOnFsSignature(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()
	device := expectThinBacking(fx, "pvc-b028f")
	fx.Expect("drbdsetup status pvc-b028f --json",
		storage.FakeResponse{Stdout: []byte(statusBothUpToDateSecondary("pvc-b028f"))})
	expectGetGI(fx, "pvc-b028f", device, satellite.Day0GiForTest("pvc-b028f", 0))
	fx.Expect("blkid -o export "+device,
		storage.FakeResponse{Stdout: []byte("DEVNAME=" + device + "\nTYPE=ext4\nUSAGE=filesystem\n")})

	rec := newThinReconciler(fx, dir)

	_, err := rec.Apply(t.Context(), bug028WinnerDR("pvc-b028f", "6520"))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	assertNoPromoteNoMkfs(t, fx.CommandLines())
}

// TestApplyBug028VetoHoldsOnUnconnectedDiskfulPeer: data-safety
// counter-case for the kernel-coverage gate — a configured DISKFUL
// peer whose connection has not handshaken (peer-disk DUnknown) could
// be an offline data holder, so the bypass must refuse even when the
// local volume is day0-empty. (The OTHER connected peer being UpToDate
// is what fired the veto.)
func TestApplyBug028VetoHoldsOnUnconnectedDiskfulPeer(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()
	device := expectThinBacking(fx, "pvc-b028u")
	fx.Expect("drbdsetup status pvc-b028u --json",
		storage.FakeResponse{Stdout: []byte(`[{
		  "name":"pvc-b028u","node-id":0,"role":"Secondary",
		  "devices":[{"volume":0,"disk-state":"UpToDate"}],
		  "connections":[{
		    "peer-node-id":1,"name":"n2","connection-state":"Connected",
		    "peer-role":"Secondary",
		    "peer_devices":[{"volume":0,"peer-disk-state":"UpToDate"}]
		  },{
		    "peer-node-id":2,"name":"n3","connection-state":"Connecting",
		    "peer-role":"Unknown",
		    "peer_devices":[{"volume":0,"peer-disk-state":"DUnknown"}]
		  }]
		}]`)})
	expectGetGI(fx, "pvc-b028u", device, satellite.Day0GiForTest("pvc-b028u", 0))
	expectDrbdMetaSignature(fx, device)

	rec := newThinReconciler(fx, dir)

	dr := bug028WinnerDR("pvc-b028u", "6530")
	dr[0].Peers = []intent.DesiredPeer{{Name: "n2"}, {Name: "n3"}}
	dr[0].DrbdOptions["peer.n3.port"] = "7000"
	dr[0].DrbdOptions["peer.n3.node-id"] = "2"
	dr[0].DrbdOptions["peer.n3.address"] = "10.0.0.3"
	// n3 is DISKFUL (no peer.n3.diskless) — its DUnknown must refuse.

	_, err := rec.Apply(t.Context(), dr)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	assertNoPromoteNoMkfs(t, fx.CommandLines())
}

// TestApplyBug028BypassFiresDespitePeerHasDataLag pins the CRD-lag
// acceptance: the dispatcher conservatively reports PeerHasData=true
// for an UpToDate day0 sibling whose CurrentGI backfill has not been
// observed yet. That signal is correct for the re-computed seed gates
// but must NOT cost the one-shot first-activation mkfs when kernel
// truth (full coverage + local day0 GI + no user fs) proves the whole
// connected set is day0-empty.
func TestApplyBug028BypassFiresDespitePeerHasDataLag(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()
	device := expectThinBacking(fx, "pvc-b028g")
	fx.Expect("drbdsetup status pvc-b028g --json",
		storage.FakeResponse{Stdout: []byte(statusBothUpToDateSecondary("pvc-b028g"))})
	expectGetGI(fx, "pvc-b028g", device, satellite.Day0GiForTest("pvc-b028g", 0))
	expectDrbdMetaSignature(fx, device)

	rec := newThinReconciler(fx, dir)

	dr := bug028WinnerDR("pvc-b028g", "6540")
	dr[0].PeerHasData = true // CRD lag: day0 sibling, CurrentGI unobserved

	_, err := rec.Apply(t.Context(), dr)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	assertPromoteMkfsDemoteOrder(t, fx.CommandLines(), "pvc-b028g", "/dev/drbd6540")
}

// bug028FalseLatchDR is the wire payload of the BUG-028 TERMINAL state:
// the false RD.Spec.Initialized latch fired, so the dispatcher no
// longer stamps `auto-primary`; metadata exists (MetadataCreated=true →
// firstActivation=false); the `.mkfs.done` marker never landed; the RD
// still asks for ext4.
func bug028FalseLatchDR(rd, minor string) []*intent.DesiredResource {
	return []*intent.DesiredResource{
		{
			Name:     rd,
			NodeName: "n1",
			Volumes: []*intent.DesiredVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
			},
			Props: map[string]string{
				"FileSystem/Type": "ext4",
			},
			Peers:           []intent.DesiredPeer{{Name: "n2"}},
			SkipInitialSync: skipInitTrue(),
			MetadataCreated: true,
			DrbdOptions: map[string]string{
				"port": "7000", "node-id": "0", "address": "10.0.0.1", "minor": minor,
				"peer.n2.port": "7000", "peer.n2.node-id": "1", "peer.n2.address": "10.0.0.2",
				// NO auto-primary: the false Initialized latch killed the
				// dispatcher's election.
			},
		},
	}
}

// TestApplyBug028FalseLatchRetryFiresWithoutAutoPrimary pins the
// latch-independence fix: even with NO auto-primary election, the
// deterministic lowest-diskful-node-id winner re-enters
// promote→mkfs→demote when the kernel set is all-Secondary lock-step
// UpToDate and no volume carries a filesystem. Pre-fix this state was
// terminal (retry gated solely on autoPrimaryReplica).
func TestApplyBug028FalseLatchRetryFiresWithoutAutoPrimary(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()
	device := expectThinBacking(fx, "pvc-b028r")
	fx.Expect("drbdsetup status pvc-b028r --json",
		storage.FakeResponse{Stdout: []byte(statusBothUpToDateSecondary("pvc-b028r"))})
	// Backing blkid answers TYPE=drbd (the real-hardware shape — see
	// expectDrbdMetaSignature): the retry must read it as "no user
	// filesystem". /dev/drbd6600's post-promote probe stays at the
	// FakeExec default (no signature) → mkfs runs.
	expectDrbdMetaSignature(fx, device)

	rec := newThinReconciler(fx, dir)

	_, err := rec.Apply(t.Context(), bug028FalseLatchDR("pvc-b028r", "6600"))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	cmds := fx.CommandLines()
	assertPromoteMkfsDemoteOrder(t, cmds, "pvc-b028r", "/dev/drbd6600")

	if _, statErr := os.Stat(filepath.Join(dir, "pvc-b028r.mkfs.done")); statErr != nil {
		t.Errorf(".mkfs.done marker must be written after the latch-free retry; got stat err %v", statErr)
	}
}

// TestApplyBug028FalseLatchRetryDefersWhileForeignPrimary pins the
// external-promoter coexistence contract: while drbd-reactor holds the
// device Primary on a peer, the retry must NOT fight it — and must fire
// on a later pass once every replica is Secondary again.
func TestApplyBug028FalseLatchRetryDefersWhileForeignPrimary(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()
	expectThinBacking(fx, "pvc-b028w")
	fx.Expect("drbdsetup status pvc-b028w --json",
		storage.FakeResponse{Stdout: []byte(`[{
		  "name":"pvc-b028w","node-id":0,"role":"Secondary",
		  "devices":[{"volume":0,"disk-state":"UpToDate"}],
		  "connections":[{
		    "peer-node-id":1,"name":"n2","connection-state":"Connected",
		    "peer-role":"Primary",
		    "peer_devices":[{"volume":0,"peer-disk-state":"UpToDate"}]
		  }]
		}]`)})

	rec := newThinReconciler(fx, dir)

	dr := bug028FalseLatchDR("pvc-b028w", "6610")

	_, err := rec.Apply(t.Context(), dr)
	if err != nil {
		t.Fatalf("Apply (foreign Primary): %v", err)
	}

	assertNoPromoteNoMkfs(t, fx.CommandLines())

	// The reactor demoted (mount failed again) → all Secondary → the
	// next reconcile pass picks the retry up.
	fx.Reset()
	expectThinBacking(fx, "pvc-b028w")
	fx.Expect("drbdsetup status pvc-b028w --json",
		storage.FakeResponse{Stdout: []byte(statusBothUpToDateSecondary("pvc-b028w"))})

	_, err = rec.Apply(t.Context(), dr)
	if err != nil {
		t.Fatalf("Apply (all Secondary): %v", err)
	}

	assertPromoteMkfsDemoteOrder(t, fx.CommandLines(), "pvc-b028w", "/dev/drbd6610")
}

// TestApplyBug028FalseLatchRetryOnlyOnElectionWinner: the latch-free
// retry replicates the dispatcher's lowest-diskful-node-id election so
// AT MOST ONE node re-enters the promote dance. A node whose diskful
// peer holds a lower id must stay quiet.
func TestApplyBug028FalseLatchRetryOnlyOnElectionWinner(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()
	expectThinBacking(fx, "pvc-b028l")
	fx.Expect("drbdsetup status pvc-b028l --json",
		storage.FakeResponse{Stdout: []byte(statusBothUpToDateSecondary("pvc-b028l"))})

	rec := newThinReconciler(fx, dir)

	dr := bug028FalseLatchDR("pvc-b028l", "6620")
	dr[0].DrbdOptions["node-id"] = "1"
	dr[0].DrbdOptions["peer.n2.node-id"] = "0"

	_, err := rec.Apply(t.Context(), dr)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	assertNoPromoteNoMkfs(t, fx.CommandLines())
}

// TestApplyBug028FalseLatchRetryRefusedWhenFsPresent: data-safety
// counter-case for the retry side — a filesystem signature on the
// backing device means there is nothing to retry (and bytes a promote
// dance could disturb). No promote, no mkfs.
func TestApplyBug028FalseLatchRetryRefusedWhenFsPresent(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()
	device := expectThinBacking(fx, "pvc-b028s")
	fx.Expect("drbdsetup status pvc-b028s --json",
		storage.FakeResponse{Stdout: []byte(statusBothUpToDateSecondary("pvc-b028s"))})
	fx.Expect("blkid -o export "+device,
		storage.FakeResponse{Stdout: []byte("DEVNAME=" + device + "\nTYPE=ext4\nUSAGE=filesystem\n")})

	rec := newThinReconciler(fx, dir)

	_, err := rec.Apply(t.Context(), bug028FalseLatchDR("pvc-b028s", "6630"))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	assertNoPromoteNoMkfs(t, fx.CommandLines())
}
