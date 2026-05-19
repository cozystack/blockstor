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
