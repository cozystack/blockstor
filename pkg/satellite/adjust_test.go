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

package satellite

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
)

// TestAdjustResourceCoercesSkipDiskOnKernelDiskless pins the Phase
// 11.2.c Stage 3b invariant on the extracted helper directly: when
// the kernel reports a Diskless local volume but the operator's
// `DrbdOptions/SkipDisk=True` prop has not propagated into the
// DesiredResource view yet (Bug 280 race window), adjustResource
// MUST coerce the dispatch onto `drbdadm adjust --skip-disk` rather
// than bare adjust — bare adjust would re-attach the disk the
// operator just detached, and the operator's poll would never see
// Diskless.
//
// Targets the helper directly (rather than going through Apply) so
// a regression in the helper's internal gate surfaces here rather
// than only via the end-to-end TestReconcilerCoercesAdjustToSkipDisk
// OnKernelDiskless wrapper test in reconciler_drbd_test.go.
func TestAdjustResourceCoercesSkipDiskOnKernelDiskless(t *testing.T) {
	fx := storage.NewFakeExec()

	// Kernel reports the local volume as Diskless — the post-detach
	// shape that opens the Bug 280 race window. The helper's
	// HasDisklessVolume probe shells out to this.
	fx.Expect("drbdsetup status --verbose pvc-adjust-bug280", storage.FakeResponse{
		Stdout: []byte(`pvc-adjust-bug280 node-id:0 role:Primary
  volume:0 minor:1000 disk:Diskless backing_dev:none quorum:yes
      worker-2 node-id:1 connection:Connected role:Secondary
    volume:0 replication:Established peer-disk:UpToDate
`),
	})

	rec := NewReconciler(ReconcilerConfig{
		Adm:      drbd.NewAdm(fx),
		NodeName: "n1",
	})

	dr := &intent.DesiredResource{
		Name:     "pvc-adjust-bug280",
		NodeName: "n1",
		// Deliberately NO SkipDisk in Props or DrbdOptions — this is
		// the in-flight reconcile's stale cache view that the kernel
		// probe must compensate for.
		Props: map[string]string{},
		Volumes: []*intent.DesiredVolume{
			{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
		},
		DrbdOptions: map[string]string{
			"port":    "7000",
			"node-id": "0",
			"address": "10.0.0.1",
			"minor":   "1000",
		},
	}

	// diskfulFlip=false: this is NOT the Bug 319 diskless→diskful
	// transition, so the kernel-Diskless probe is allowed to fire
	// and coerce skip-disk.
	if err := rec.adjustResource(context.Background(), dr, false); err != nil {
		t.Fatalf("adjustResource: %v", err)
	}

	cmds := fx.CommandLines()

	skipDiskCmd := "drbdadm adjust --skip-disk pvc-adjust-bug280"
	bareCmd := "drbdadm adjust pvc-adjust-bug280"

	if !slices.Contains(cmds, skipDiskCmd) {
		t.Errorf("kernel-Diskless without prop: expected %q in commands; got %v", skipDiskCmd, cmds)
	}

	if slices.Contains(cmds, bareCmd) {
		t.Errorf("kernel-Diskless without prop: bare %q must not run (would re-attach the operator-detached disk); got %v",
			bareCmd, cmds)
	}
}

// TestAdjustResourceBareWithoutSkipDisk pins the steady-state case
// on the extracted helper: no SkipDisk signal anywhere (no prop,
// kernel reports UpToDate), adjustResource MUST dispatch the bare
// `drbdadm adjust <name>` — the canonical "make kernel state match
// .res" call. A regression that always coerces skip-disk would
// suppress legitimate attach work and strand replicas in Diskless.
func TestAdjustResourceBareWithoutSkipDisk(t *testing.T) {
	fx := storage.NewFakeExec()

	// Kernel reports the local volume as UpToDate — the healthy
	// steady-state shape. HasDisklessVolume returns false, so the
	// helper falls through to bare adjust.
	fx.Expect("drbdsetup status --verbose pvc-adjust-bare", storage.FakeResponse{
		Stdout: []byte(`pvc-adjust-bare node-id:0 role:Secondary
  volume:0 minor:1000 disk:UpToDate backing_dev:/dev/vg/pvc-adjust-bare_00000 quorum:yes
      worker-2 node-id:1 connection:Connected role:Secondary
    volume:0 replication:Established peer-disk:UpToDate
`),
	})

	rec := NewReconciler(ReconcilerConfig{
		Adm:      drbd.NewAdm(fx),
		NodeName: "n1",
	})

	dr := &intent.DesiredResource{
		Name:     "pvc-adjust-bare",
		NodeName: "n1",
		Props:    map[string]string{},
		Volumes: []*intent.DesiredVolume{
			{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
		},
		DrbdOptions: map[string]string{
			"port":    "7000",
			"node-id": "0",
			"address": "10.0.0.1",
			"minor":   "1000",
		},
	}

	if err := rec.adjustResource(context.Background(), dr, false); err != nil {
		t.Fatalf("adjustResource: %v", err)
	}

	cmds := fx.CommandLines()

	bareCmd := "drbdadm adjust pvc-adjust-bare"
	skipDiskCmd := "drbdadm adjust --skip-disk pvc-adjust-bare"

	if !slices.Contains(cmds, bareCmd) {
		t.Errorf("no SkipDisk signal: expected %q in commands; got %v", bareCmd, cmds)
	}

	if slices.Contains(cmds, skipDiskCmd) {
		t.Errorf("no SkipDisk signal: %q must not run (would suppress legitimate attach); got %v",
			skipDiskCmd, cmds)
	}
}

// TestApplyDRBDAdjustsViaFsmDispatchOnly pins Phase 11.2.c Stage 4
// step 3: when applyDRBD runs against a loaded kernel slot, drbdadm
// adjust fires exactly once — through FSM dispatch, not through a
// legacy call inside runBringUpOrAdjust. The legacy path was removed
// in Stage 4 step 3 (this commit).
//
// Observation shape: SpecHasResource=true, ResFileExists=true,
// MetadataExists=true, KernelLoaded=true — Phase==Running. FSM picks
// ActionAdjust. dispatchFsmAction runs renderResFile preamble (Stage
// 4 step 1) + adjustResource. The legacy runApplyDRBDVerb's
// !firstActivation arm is now a documented no-op (the firstActivation
// arm still routes through adjustResource for Bug 319 — step 4 will
// retire that), so the only `drbdadm adjust <name>` shell-out comes
// from the FSM dispatch on a steady-state pass.
//
// A regression that re-added the legacy adjustResource call inside
// runApplyDRBDVerb's !firstActivation branch would surface as TWO
// `drbdadm adjust` calls on the same Apply pass.
func TestApplyDRBDAdjustsViaFsmDispatchOnly(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()

	rec := NewReconciler(ReconcilerConfig{
		Adm:          drbd.NewAdm(fx),
		StateDir:     dir,
		NodeName:     "n1",
		LocalAddress: "10.0.0.1",
	})

	dr := &intent.DesiredResource{
		Name:     "pvc-stage4-step3-adjust",
		NodeName: "n1",
		Volumes: []*intent.DesiredVolume{
			{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
		},
		DrbdOptions: map[string]string{
			"port":    "7000",
			"node-id": "0",
			"address": "10.0.0.1",
			"minor":   "1000",
		},
	}
	devices := map[int32]string{0: "/dev/vg/pvc-stage4-step3-adjust_00000"}

	// Seed a .res file so the FSM preamble's stat+compare path is
	// covered (content-idempotent overwrite is a no-op when bodies
	// match — Bug 315).
	if err := rec.renderResFile(context.Background(), dr, devices); err != nil {
		t.Fatalf("seed renderResFile: %v", err)
	}

	// Sanity check: the seeded .res is on disk before dispatch.
	resPath := filepath.Join(dir, "pvc-stage4-step3-adjust.res")
	if _, err := os.Stat(resPath); err != nil {
		t.Fatalf("seeded .res missing: %v", err)
	}

	// Kernel reports the local volume as UpToDate — healthy
	// steady-state. HasDisklessVolume returns false, so adjustResource
	// falls through to bare `drbdadm adjust`.
	fx2 := storage.NewFakeExec()
	fx2.Expect("drbdsetup status --verbose pvc-stage4-step3-adjust", storage.FakeResponse{
		Stdout: []byte(`pvc-stage4-step3-adjust node-id:0 role:Secondary
  volume:0 minor:1000 disk:UpToDate backing_dev:/dev/vg/pvc-stage4-step3-adjust_00000 quorum:yes
      worker-2 node-id:1 connection:Connected role:Secondary
    volume:0 replication:Established peer-disk:UpToDate
`),
	})
	rec.cfg.Adm = drbd.NewAdm(fx2)

	// Phase==Running observation shape: spec present, .res on disk,
	// metadata stamped, kernel slot loaded. NextTransition MUST return
	// ActionAdjust for this shape (no SkipDisk prop, KernelLoaded);
	// assert that here so a future FSM-table drift surfaces in this
	// test rather than only downstream in the dispatch counter.
	obs := Observation{
		SpecHasResource: true,
		ResFileExists:   true,
		MetadataExists:  true,
		KernelLoaded:    true,
	}

	phase := ObservePhase(obs)
	if phase != PhaseRunning {
		t.Fatalf("ObservePhase: got %q, want %q", phase, PhaseRunning)
	}

	next := NextTransition(phase, obs)
	if next == nil || next.Action != ActionAdjust {
		got := "nil"
		if next != nil {
			got = next.Action
		}

		t.Fatalf("NextTransition: got action %q, want %q", got, ActionAdjust)
	}

	if err := rec.dispatchFsmAction(context.Background(), dr, devices, ActionAdjust, obs); err != nil {
		t.Fatalf("dispatchFsmAction(ActionAdjust): %v", err)
	}

	// Exactly ONE `drbdadm adjust <name>` MUST land on the FakeExec.
	// More than one would mean the legacy adjustResource call inside
	// runApplyDRBDVerb's !firstActivation arm was re-introduced (or a
	// regression caused the dispatch to double-fire).
	wantAdjust := "drbdadm adjust pvc-stage4-step3-adjust"
	adjustCount := 0

	for _, line := range fx2.CommandLines() {
		if line == wantAdjust {
			adjustCount++
		}
	}

	if adjustCount != 1 {
		t.Errorf("got %d %q calls, want exactly 1; calls=%v",
			adjustCount, wantAdjust, fx2.CommandLines())
	}
}

// TestAdjustResourceUsesSkipNetOnStandAlonePeer pins the W12 +
// network-partition guard: when `drbdsetup status -j` reports any peer
// in `StandAlone` (operator-initiated disconnect — the W12 victim
// recipe pre-amble + the iptables-partition wedge case), runAdjust
// MUST dispatch `drbdadm adjust --skip-net` rather than plain
// `drbdadm adjust`. Plain adjust would re-issue `drbdsetup connect`
// and undo the operator's disconnect within ~1 s, breaking the
// split-brain recovery recipe and any test that needs StandAlone to
// survive a reconcile.
func TestAdjustResourceUsesSkipNetOnStandAlonePeer(t *testing.T) {
	fx := storage.NewFakeExec()

	// Kernel reports local UpToDate (so SkipDisk does not fire) AND
	// the peer connection slot in StandAlone (operator just ran
	// `drbdadm disconnect` / `drbdsetup disconnect --force=yes`).
	fx.Expect("drbdsetup status --verbose pvc-w12-victim", storage.FakeResponse{
		Stdout: []byte(`pvc-w12-victim node-id:0 role:Secondary
  volume:0 minor:1000 disk:UpToDate backing_dev:/dev/vg/pvc-w12-victim_00000 quorum:yes
      worker-2 node-id:1 connection:StandAlone role:Unknown
    volume:0 replication:Off peer-disk:DUnknown
`),
	})

	fx.Expect("drbdsetup status -j pvc-w12-victim", storage.FakeResponse{
		Stdout: []byte(`[
  {
    "name": "pvc-w12-victim",
    "connections": [
      {
        "peer-node-id": 1,
        "name": "worker-2",
        "connection-state": "StandAlone",
        "peer_devices": [
          {"volume": 0}
        ]
      }
    ]
  }
]
`),
	})

	rec := NewReconciler(ReconcilerConfig{
		Adm:      drbd.NewAdm(fx),
		NodeName: "n1",
	})

	dr := &intent.DesiredResource{
		Name:     "pvc-w12-victim",
		NodeName: "n1",
		Props:    map[string]string{},
		Volumes: []*intent.DesiredVolume{
			{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
		},
		DrbdOptions: map[string]string{
			"port":    "7000",
			"node-id": "0",
			"address": "10.0.0.1",
			"minor":   "1000",
		},
	}

	if err := rec.adjustResource(context.Background(), dr, false); err != nil {
		t.Fatalf("adjustResource: %v", err)
	}

	cmds := fx.CommandLines()

	skipNetCmd := "drbdadm adjust --skip-net pvc-w12-victim"
	bareCmd := "drbdadm adjust pvc-w12-victim"

	if !slices.Contains(cmds, skipNetCmd) {
		t.Errorf("StandAlone peer: expected %q in commands; got %v", skipNetCmd, cmds)
	}

	if slices.Contains(cmds, bareCmd) {
		t.Errorf("StandAlone peer: bare %q must not run (would re-connect operator-disconnected peer); got %v",
			bareCmd, cmds)
	}
}

// TestAdjustResourceSkipsBothNetAndDiskOnDoubleSignal pins the
// composite case: kernel reports Diskless (Bug 280 SkipDisk coercion
// path fires) AND a peer in StandAlone (W12 SkipNet guard fires).
// runAdjust MUST dispatch `drbdadm adjust --skip-net --skip-disk`,
// preserving both invariants — the disk stays detached AND the
// peer connection stays StandAlone.
func TestAdjustResourceSkipsBothNetAndDiskOnDoubleSignal(t *testing.T) {
	fx := storage.NewFakeExec()

	fx.Expect("drbdsetup status --verbose pvc-double-skip", storage.FakeResponse{
		Stdout: []byte(`pvc-double-skip node-id:0 role:Secondary
  volume:0 minor:1000 disk:Diskless backing_dev:none quorum:yes
      worker-2 node-id:1 connection:StandAlone role:Unknown
    volume:0 replication:Off peer-disk:DUnknown
`),
	})

	fx.Expect("drbdsetup status -j pvc-double-skip", storage.FakeResponse{
		Stdout: []byte(`[
  {
    "name": "pvc-double-skip",
    "connections": [
      {
        "peer-node-id": 1,
        "name": "worker-2",
        "connection-state": "StandAlone",
        "peer_devices": [
          {"volume": 0}
        ]
      }
    ]
  }
]
`),
	})

	rec := NewReconciler(ReconcilerConfig{
		Adm:      drbd.NewAdm(fx),
		NodeName: "n1",
	})

	dr := &intent.DesiredResource{
		Name:     "pvc-double-skip",
		NodeName: "n1",
		Props:    map[string]string{},
		Volumes: []*intent.DesiredVolume{
			{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
		},
		DrbdOptions: map[string]string{
			"port":    "7000",
			"node-id": "0",
			"address": "10.0.0.1",
			"minor":   "1000",
		},
	}

	if err := rec.adjustResource(context.Background(), dr, false); err != nil {
		t.Fatalf("adjustResource: %v", err)
	}

	cmds := fx.CommandLines()

	doubleSkip := "drbdadm adjust --skip-net --skip-disk pvc-double-skip"
	skipDiskOnly := "drbdadm adjust --skip-disk pvc-double-skip"
	skipNetOnly := "drbdadm adjust --skip-net pvc-double-skip"
	bare := "drbdadm adjust pvc-double-skip"

	if !slices.Contains(cmds, doubleSkip) {
		t.Errorf("Diskless + StandAlone peer: expected %q in commands; got %v",
			doubleSkip, cmds)
	}

	for _, forbidden := range []string{skipDiskOnly, skipNetOnly, bare} {
		if slices.Contains(cmds, forbidden) {
			t.Errorf("Diskless + StandAlone peer: %q must not run (drops one of two guards); got %v",
				forbidden, cmds)
		}
	}
}

// TestAdjustResourceBareWhenAllPeersConnected pins the steady-state
// regression guard: when `drbdsetup status -j` reports all peer slots
// in `Connected` (or any non-StandAlone state), runAdjust MUST fall
// back to plain `drbdadm adjust`. A regression that always coerced
// `--skip-net` on the presence of a Show response would strand
// new-peer-add work (the freshly-rendered .res declares a peer that
// the kernel doesn't have yet, and only `drbdadm adjust` issues the
// `drbdsetup new-peer` + `connect` to materialise it).
func TestAdjustResourceBareWhenAllPeersConnected(t *testing.T) {
	fx := storage.NewFakeExec()

	fx.Expect("drbdsetup status --verbose pvc-all-connected", storage.FakeResponse{
		Stdout: []byte(`pvc-all-connected node-id:0 role:Secondary
  volume:0 minor:1000 disk:UpToDate backing_dev:/dev/vg/pvc-all-connected_00000 quorum:yes
      worker-2 node-id:1 connection:Connected role:Secondary
    volume:0 replication:Established peer-disk:UpToDate
`),
	})

	fx.Expect("drbdsetup status -j pvc-all-connected", storage.FakeResponse{
		Stdout: []byte(`[
  {
    "name": "pvc-all-connected",
    "connections": [
      {
        "peer-node-id": 1,
        "name": "worker-2",
        "connection-state": "Connected",
        "peer_devices": [
          {"volume": 0}
        ]
      }
    ]
  }
]
`),
	})

	rec := NewReconciler(ReconcilerConfig{
		Adm:      drbd.NewAdm(fx),
		NodeName: "n1",
	})

	dr := &intent.DesiredResource{
		Name:     "pvc-all-connected",
		NodeName: "n1",
		Props:    map[string]string{},
		Volumes: []*intent.DesiredVolume{
			{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
		},
		DrbdOptions: map[string]string{
			"port":    "7000",
			"node-id": "0",
			"address": "10.0.0.1",
			"minor":   "1000",
		},
	}

	if err := rec.adjustResource(context.Background(), dr, false); err != nil {
		t.Fatalf("adjustResource: %v", err)
	}

	cmds := fx.CommandLines()

	bareCmd := "drbdadm adjust pvc-all-connected"
	skipNetCmd := "drbdadm adjust --skip-net pvc-all-connected"

	if !slices.Contains(cmds, bareCmd) {
		t.Errorf("all peers Connected: expected bare %q in commands; got %v", bareCmd, cmds)
	}

	if slices.Contains(cmds, skipNetCmd) {
		t.Errorf("all peers Connected: %q must not run (would strand new-peer-add work); got %v",
			skipNetCmd, cmds)
	}
}

// TestAdjustResourceBareOnStandAloneWithoutPeerDevices pins scenario
// 5.32 / `tests/e2e/recovery-down-reverses.sh`: when the operator
// ran `drbdadm down <rd>` and the reconciler revived the kernel
// slot via the Bug-287 `drbdadm up` fallback, the freshly-allocated
// connection slots can stick in `StandAlone` WITHOUT peer-device
// entries (the kernel created the slot but the connect handshake
// never registered the per-volume peer-device table). The next
// `drbdadm adjust` MUST be a bare adjust — not `--skip-net` — so
// `drbdsetup connect` is re-issued and the slot exits StandAlone.
//
// Smoking-gun evidence captured on PR #46 lane 1 breakpoint:
//
//	ci-lane1-worker-2 view:
//	down-reverses node-id:1 role:Secondary suspended:quorum force-io-failures:no
//	  volume:0 minor:20000 disk:Inconsistent backing_dev:/dev/loop5 quorum:no
//	  ci-lane1-worker-1 node-id:0 connection:StandAlone role:Unknown
//	  ci-lane1-worker-3 node-id:2 connection:StandAlone role:Unknown
//
// Both peers StandAlone, no peer-device entries — the recovery-
// revive signature. Pre-fix the reconciler dispatched `--skip-net`
// here (StandAlone alone was enough to flip the gate) and the
// scenario aborted after 60s with both peers still StandAlone.
// The fix narrows the gate to StandAlone AND peer-devices-present
// (the operator-disconnect signal), so this case falls through to
// bare adjust.
func TestAdjustResourceBareOnStandAloneWithoutPeerDevices(t *testing.T) {
	fx := storage.NewFakeExec()

	// Local disk reads UpToDate (so the SkipDisk coercion path stays
	// off — we're isolating the SkipNet gate). Peer slot exists in
	// the kernel but its `peer_devices` array is empty: the post-
	// `drbdadm up` recovery signature.
	fx.Expect("drbdsetup status --verbose pvc-down-revive", storage.FakeResponse{
		Stdout: []byte(`pvc-down-revive node-id:0 role:Secondary
  volume:0 minor:1000 disk:UpToDate backing_dev:/dev/vg/pvc-down-revive_00000 quorum:yes
      worker-2 node-id:1 connection:StandAlone role:Unknown
    volume:0 replication:Off peer-disk:DUnknown
`),
	})

	fx.Expect("drbdsetup status -j pvc-down-revive", storage.FakeResponse{
		Stdout: []byte(`[
  {
    "name": "pvc-down-revive",
    "connections": [
      {
        "peer-node-id": 1,
        "name": "worker-2",
        "connection-state": "StandAlone",
        "peer_devices": []
      }
    ]
  }
]
`),
	})

	rec := NewReconciler(ReconcilerConfig{
		Adm:      drbd.NewAdm(fx),
		NodeName: "n1",
	})

	dr := &intent.DesiredResource{
		Name:     "pvc-down-revive",
		NodeName: "n1",
		Props:    map[string]string{},
		Volumes: []*intent.DesiredVolume{
			{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
		},
		DrbdOptions: map[string]string{
			"port":    "7000",
			"node-id": "0",
			"address": "10.0.0.1",
			"minor":   "1000",
		},
	}

	if err := rec.adjustResource(context.Background(), dr, false); err != nil {
		t.Fatalf("adjustResource: %v", err)
	}

	cmds := fx.CommandLines()

	bareCmd := "drbdadm adjust pvc-down-revive"
	skipNetCmd := "drbdadm adjust --skip-net pvc-down-revive"

	if !slices.Contains(cmds, bareCmd) {
		t.Errorf("StandAlone without peer-devices: expected bare %q in commands; "+
			"got %v (scenario 5.32 wedge — reconciler stuck both peers StandAlone)", bareCmd, cmds)
	}

	if slices.Contains(cmds, skipNetCmd) {
		t.Errorf("StandAlone without peer-devices: %q must not run "+
			"(would strand fresh-revive slots in StandAlone forever); got %v",
			skipNetCmd, cmds)
	}
}

// TestAdjustResourceBareOnMultiPeerStandAloneAllWithoutPeerDevices
// covers the exact multi-peer shape captured in the PR #46 smoking
// gun: two peers (UpToDate sibling + Diskless TieBreaker), both
// stuck in `StandAlone` without peer-device entries after the
// operator's `drbdadm down`+ Bug-287 `up` revive. Same invariant
// as the single-peer test, but pinned for the multi-peer case so a
// future regression that special-cased "exactly one peer" would
// surface here.
func TestAdjustResourceBareOnMultiPeerStandAloneAllWithoutPeerDevices(t *testing.T) {
	fx := storage.NewFakeExec()

	fx.Expect("drbdsetup status --verbose down-reverses", storage.FakeResponse{
		Stdout: []byte(`down-reverses node-id:1 role:Secondary suspended:quorum force-io-failures:no
  volume:0 minor:20000 disk:Inconsistent backing_dev:/dev/loop5 quorum:no
      ci-lane1-worker-1 node-id:0 connection:StandAlone role:Unknown
    volume:0 replication:Off peer-disk:DUnknown
      ci-lane1-worker-3 node-id:2 connection:StandAlone role:Unknown
    volume:0 replication:Off peer-disk:DUnknown
`),
	})

	fx.Expect("drbdsetup status -j down-reverses", storage.FakeResponse{
		Stdout: []byte(`[
  {
    "name": "down-reverses",
    "connections": [
      {
        "peer-node-id": 0,
        "name": "ci-lane1-worker-1",
        "connection-state": "StandAlone",
        "peer_devices": []
      },
      {
        "peer-node-id": 2,
        "name": "ci-lane1-worker-3",
        "connection-state": "StandAlone",
        "peer_devices": []
      }
    ]
  }
]
`),
	})

	rec := NewReconciler(ReconcilerConfig{
		Adm:      drbd.NewAdm(fx),
		NodeName: "ci-lane1-worker-2",
	})

	dr := &intent.DesiredResource{
		Name:     "down-reverses",
		NodeName: "ci-lane1-worker-2",
		Props:    map[string]string{},
		Volumes: []*intent.DesiredVolume{
			{VolumeNumber: 0, SizeKib: 65536, StoragePool: "thin1"},
		},
		DrbdOptions: map[string]string{
			"port":    "7000",
			"node-id": "1",
			"address": "10.0.0.2",
			"minor":   "20000",
		},
	}

	if err := rec.adjustResource(context.Background(), dr, false); err != nil {
		t.Fatalf("adjustResource: %v", err)
	}

	cmds := fx.CommandLines()

	bareCmd := "drbdadm adjust down-reverses"
	skipNetCmd := "drbdadm adjust --skip-net down-reverses"

	if !slices.Contains(cmds, bareCmd) {
		t.Errorf("multi-peer StandAlone without peer-devices: expected bare %q; got %v",
			bareCmd, cmds)
	}

	if slices.Contains(cmds, skipNetCmd) {
		t.Errorf("multi-peer StandAlone without peer-devices: %q must not run; got %v",
			skipNetCmd, cmds)
	}
}

// TestAdjustResourceSkipNetOnMixedStandAlonePeerDevices pins the
// disambiguator's tie-break direction in the multi-peer mixed case:
// when ONE peer is StandAlone with peer-device entries (operator-
// disconnect signal) and another is StandAlone WITHOUT peer-device
// entries (fresh revive), the operator-disconnect signal wins —
// `--skip-net` MUST fire so the operator's manual disconnect on
// the first peer survives the reconcile. The fresh-revive peer
// can wait for the next observer-trigger cycle to be picked up:
// preserving operator intent on the first peer is the stricter
// invariant of the two (an unwanted reconnect breaks W12 / split-
// brain recovery; a delayed reconnect is recoverable).
func TestAdjustResourceSkipNetOnMixedStandAlonePeerDevices(t *testing.T) {
	fx := storage.NewFakeExec()

	fx.Expect("drbdsetup status --verbose pvc-mixed-standalone", storage.FakeResponse{
		Stdout: []byte(`pvc-mixed-standalone node-id:0 role:Secondary
  volume:0 minor:1000 disk:UpToDate backing_dev:/dev/vg/pvc-mixed-standalone_00000 quorum:yes
      worker-2 node-id:1 connection:StandAlone role:Unknown
    volume:0 replication:Off peer-disk:DUnknown
      worker-3 node-id:2 connection:StandAlone role:Unknown
    volume:0 replication:Off peer-disk:DUnknown
`),
	})

	fx.Expect("drbdsetup status -j pvc-mixed-standalone", storage.FakeResponse{
		Stdout: []byte(`[
  {
    "name": "pvc-mixed-standalone",
    "connections": [
      {
        "peer-node-id": 1,
        "name": "worker-2",
        "connection-state": "StandAlone",
        "peer_devices": [
          {"volume": 0}
        ]
      },
      {
        "peer-node-id": 2,
        "name": "worker-3",
        "connection-state": "StandAlone",
        "peer_devices": []
      }
    ]
  }
]
`),
	})

	rec := NewReconciler(ReconcilerConfig{
		Adm:      drbd.NewAdm(fx),
		NodeName: "n1",
	})

	dr := &intent.DesiredResource{
		Name:     "pvc-mixed-standalone",
		NodeName: "n1",
		Props:    map[string]string{},
		Volumes: []*intent.DesiredVolume{
			{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
		},
		DrbdOptions: map[string]string{
			"port":    "7000",
			"node-id": "0",
			"address": "10.0.0.1",
			"minor":   "1000",
		},
	}

	if err := rec.adjustResource(context.Background(), dr, false); err != nil {
		t.Fatalf("adjustResource: %v", err)
	}

	cmds := fx.CommandLines()

	skipNetCmd := "drbdadm adjust --skip-net pvc-mixed-standalone"
	bareCmd := "drbdadm adjust pvc-mixed-standalone"

	if !slices.Contains(cmds, skipNetCmd) {
		t.Errorf("mixed StandAlone (one with peer-devices): expected %q "+
			"(operator-disconnect signal wins); got %v", skipNetCmd, cmds)
	}

	if slices.Contains(cmds, bareCmd) {
		t.Errorf("mixed StandAlone (one with peer-devices): %q must not run "+
			"(would re-connect operator-disconnected peer); got %v", bareCmd, cmds)
	}
}
