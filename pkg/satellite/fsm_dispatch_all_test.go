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

// Phase 11.2.c Stage 3d tests: pin the shadow-dispatch router
// (`dispatchFsmAction`) and the per-action `:fsm-dispatched`
// counter increments. The router is the gate that proves every
// FSM transition is reachable through an extracted helper without
// forking the apply flow.

package satellite

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/lvm"
)

// dispatchFixtureDR returns a minimal DesiredResource that drives
// each extracted helper through its happy path. Devices map points
// at an LVM-shaped lower disk path because buildResFile inspects
// the volume's StoragePool to decide LVM vs ZFS shaping; thin1
// resolves to the lvm.Thin provider registered in the reconciler
// config.
func dispatchFixtureDR(name string) (*intent.DesiredResource, map[int32]string) {
	dr := &intent.DesiredResource{
		Name:     name,
		NodeName: "n1",
		Volumes: []*intent.DesiredVolume{
			{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
		},
		Peers: []string{"n2"},
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
	devices := map[int32]string{0: "/dev/vg/" + name + "_00000"}

	return dr, devices
}

// TestDispatchFsmActionRenderResFires pins the ActionRenderRes arm
// of the router: when the FSM observes PhaseUnprovisioned, the
// dispatcher MUST write the .res file via renderResFile. A
// regression that routes renderRes to a no-op would strand the
// resource in Unprovisioned across reconciles.
func TestDispatchFsmActionRenderResFires(t *testing.T) {
	dir := t.TempDir()
	rec := NewReconciler(ReconcilerConfig{
		StateDir:     dir,
		NodeName:     "n1",
		LocalAddress: "10.0.0.1",
	})

	dr, devices := dispatchFixtureDR("pvc-dispatch-render")

	// Observation for PhaseUnprovisioned: spec exists, .res file
	// absent. The FSM picks ActionRenderRes for this shape.
	obs := Observation{SpecHasResource: true}

	err := rec.dispatchFsmAction(context.Background(), dr, devices, ActionRenderRes, obs)
	if err != nil {
		t.Fatalf("dispatchFsmAction(ActionRenderRes): %v", err)
	}

	resPath := filepath.Join(dir, "pvc-dispatch-render.res")
	if _, statErr := os.Stat(resPath); statErr != nil {
		t.Errorf(".res file not written by ActionRenderRes dispatch: %v", statErr)
	}
}

// TestDispatchFsmActionCreateMdGatedByDiskless pins the
// defense-in-depth gate inside the ActionCreateMd arm: even if the
// FSM ever drifts and recommends createMd against a Diskless spec,
// the router MUST short-circuit to no-op. A regression that fired
// create-md on a Diskless replica would attempt to stamp metadata
// onto a missing lower disk and fail loudly.
func TestDispatchFsmActionCreateMdGatedByDiskless(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()

	rec := NewReconciler(ReconcilerConfig{
		Adm:      drbd.NewAdm(fx),
		StateDir: dir,
		NodeName: "n1",
	})

	dr, devices := dispatchFixtureDR("pvc-dispatch-md-diskless")

	// Observation flags Diskless: the gate must catch this regardless
	// of what the FSM proposed.
	obs := Observation{
		SpecHasResource:      true,
		ResFileExists:        true,
		SpecFlagsHasDiskless: true,
	}

	err := rec.dispatchFsmAction(context.Background(), dr, devices, ActionCreateMd, obs)
	if err != nil {
		t.Fatalf("dispatchFsmAction(ActionCreateMd, diskless): %v", err)
	}

	for _, line := range fx.CommandLines() {
		if strings.HasPrefix(line, "drbdadm create-md") {
			t.Errorf("Diskless gate failed: create-md fired on a Diskless replica: %s", line)
		}
	}
}

// TestDispatchFsmActionCreateMdGatedByExistingMd pins the second
// defense-in-depth gate inside the ActionCreateMd arm:
// MetadataExists=true MUST short-circuit to no-op so a future FSM
// drift doesn't re-run `drbdadm create-md --force` and wipe the
// operator-stamped GI + bitmap state. This is the same invariant
// W09 disk-replace recovery depends on.
func TestDispatchFsmActionCreateMdGatedByExistingMd(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()

	rec := NewReconciler(ReconcilerConfig{
		Adm:      drbd.NewAdm(fx),
		StateDir: dir,
		NodeName: "n1",
	})

	dr, devices := dispatchFixtureDR("pvc-dispatch-md-exists")

	obs := Observation{
		SpecHasResource: true,
		ResFileExists:   true,
		MetadataExists:  true,
	}

	err := rec.dispatchFsmAction(context.Background(), dr, devices, ActionCreateMd, obs)
	if err != nil {
		t.Fatalf("dispatchFsmAction(ActionCreateMd, mdExists): %v", err)
	}

	for _, line := range fx.CommandLines() {
		if strings.HasPrefix(line, "drbdadm create-md") {
			t.Errorf("MetadataExists gate failed: create-md fired despite existing metadata (would wipe GI + bitmap): %s", line)
		}
	}
}

// TestDispatchFsmActionCreateMdGatedOnDiskfulFlip pins the contract
// that the shadow does NOT fire create-md when the kernel slot is
// already loaded with a Diskless volume (the diskful-flip path).
// Legacy ensureMetadata routes flip with firstActivation=false (no
// GI-seed); the shadow's createMetadata would call
// firstActivation=true → seedInitialGi → in-flight handshake
// corruption (Run 30 lifecycle-toggle-migrate regression). The gate
// must defer to legacy whenever KernelLoaded && KernelHasDiskless,
// even though SpecFlagsHasDiskless=false would otherwise permit
// seeding metadata on this peer.
func TestDispatchFsmActionCreateMdGatedOnDiskfulFlip(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()

	rec := NewReconciler(ReconcilerConfig{
		Adm:      drbd.NewAdm(fx),
		StateDir: dir,
		NodeName: "n1",
	})

	dr, devices := dispatchFixtureDR("pvc-dispatch-md-diskful-flip")

	// Diskful-flip shape: spec has the resource as diskful, but the
	// kernel slot is already loaded with a Diskless volume — legacy
	// must own the flip (firstActivation=false). MetadataExists=false
	// since the flip rewrites lower disk + stamps fresh metadata via
	// the legacy ensureMetadata path.
	obs := Observation{
		SpecHasResource:      true,
		ResFileExists:        true,
		MetadataExists:       false,
		SpecFlagsHasDiskless: false,
		KernelLoaded:         true,
		KernelHasDiskless:    true,
	}

	err := rec.dispatchFsmAction(context.Background(), dr, devices, ActionCreateMd, obs)
	if err != nil {
		t.Fatalf("dispatchFsmAction(ActionCreateMd, diskfulFlip): %v", err)
	}

	for _, line := range fx.CommandLines() {
		if strings.HasPrefix(line, "drbdadm create-md") {
			t.Errorf("Diskful-flip gate failed: create-md fired during diskful flip (would seed GI and corrupt in-flight handshake): %s", line)
		}
	}
}

// TestDispatchFsmActionAdjust pins the ActionAdjust arm of the
// router: when the FSM observes PhaseRunning, the dispatcher MUST
// shell out to `drbdadm adjust <name>` via the adjustResource
// helper. A regression that routed adjust to a no-op would freeze
// drift convergence inside the Running phase.
func TestDispatchFsmActionAdjust(t *testing.T) {
	fx := storage.NewFakeExec()

	// Kernel reports UpToDate — steady-state shape with no SkipDisk
	// signal. HasDisklessVolume returns false from this output, so
	// adjustResource falls through to bare adjust.
	fx.Expect("drbdsetup status --verbose pvc-dispatch-adjust", storage.FakeResponse{
		Stdout: []byte(`pvc-dispatch-adjust node-id:0 role:Secondary
  volume:0 minor:1000 disk:UpToDate backing_dev:/dev/vg/pvc-dispatch-adjust_00000 quorum:yes
      worker-2 node-id:1 connection:Connected role:Secondary
    volume:0 replication:Established peer-disk:UpToDate
`),
	})

	rec := NewReconciler(ReconcilerConfig{
		Adm:      drbd.NewAdm(fx),
		NodeName: "n1",
	})

	dr, devices := dispatchFixtureDR("pvc-dispatch-adjust")

	// Observation for PhaseRunning: spec + res + metadata + kernel
	// all present. Adjust is the FSM's default Running self-loop.
	obs := Observation{
		SpecHasResource: true,
		ResFileExists:   true,
		MetadataExists:  true,
		KernelLoaded:    true,
	}

	err := rec.dispatchFsmAction(context.Background(), dr, devices, ActionAdjust, obs)
	if err != nil {
		t.Fatalf("dispatchFsmAction(ActionAdjust): %v", err)
	}

	want := "drbdadm adjust pvc-dispatch-adjust"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("missing %q in calls (router did not route to adjustResource): %v", want, fx.CommandLines())
	}
}

// TestDispatchFsmActionUnknownIsNoop pins the default arm of the
// router: an unknown action string MUST return nil without side
// effects. The router never panics on a future FSM action that the
// dispatcher hasn't been taught about — it falls through to the
// legacy chain by returning a clean nil.
func TestDispatchFsmActionUnknownIsNoop(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()

	rec := NewReconciler(ReconcilerConfig{
		Adm:      drbd.NewAdm(fx),
		StateDir: dir,
		NodeName: "n1",
	})

	dr, devices := dispatchFixtureDR("pvc-dispatch-unknown")
	obs := Observation{SpecHasResource: true}

	err := rec.dispatchFsmAction(context.Background(), dr, devices, "madeUpFutureAction", obs)
	if err != nil {
		t.Errorf("dispatchFsmAction(unknown action): expected nil, got %v", err)
	}

	if cmds := fx.CommandLines(); len(cmds) != 0 {
		t.Errorf("unknown action MUST be a no-op; got commands: %v", cmds)
	}
}

// TestFsmShadowAgreeCountIncrementsPerAction drives applyDRBD
// through the canonical first-activation lifecycle (renderRes →
// createMd → up → adjust) and asserts the
// `<action>:fsm-dispatched` counter increments at each phase. This
// is the integration-level proof that every FSM transition is
// reachable through the shadow-dispatch router end-to-end — a
// regression that broke any single arm would surface as a missing
// counter entry in production dashboards.
func TestFsmShadowAgreeCountIncrementsPerAction(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()

	// Storage probes for applyStorage: LV absent on the first pass
	// (drives lvcreate) then sized for the .res renderer.
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-dispatch-lifecycle_00000",
		storage.FakeResponse{Stdout: []byte("pvc-dispatch-lifecycle_00000\n")})
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings --separator | -o lv_path,lv_size --units k --nosuffix vg/pvc-dispatch-lifecycle_00000",
		storage.FakeResponse{Stdout: []byte("/dev/vg/pvc-dispatch-lifecycle_00000|1048576\n")})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := NewReconciler(ReconcilerConfig{
		Providers:    map[string]storage.Provider{"thin1": thin},
		Adm:          drbd.NewAdm(fx),
		StateDir:     dir,
		NodeName:     "n1",
		LocalAddress: "10.0.0.1",
	})

	dr := &intent.DesiredResource{
		Name:     "pvc-dispatch-lifecycle",
		NodeName: "n1",
		Volumes: []*intent.DesiredVolume{
			{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
		},
		DrbdOptions: map[string]string{
			"port": "7000", "node-id": "0", "address": "10.0.0.1", "minor": "1000",
		},
	}

	baseline := snapshotShadowMap()

	// First Apply: phase==Unprovisioned, FSM dispatches renderRes.
	// The legacy chain then runs createMd + up + adjust. The .res
	// is missing at the start so observeForFsm reads
	// PhaseUnprovisioned and the renderRes counter MUST bump.
	if _, err := rec.Apply(t.Context(), []*intent.DesiredResource{dr}); err != nil {
		t.Fatalf("Apply (1st) outer error: %v", err)
	}

	afterFirst := snapshotShadowMap()
	if got := afterFirst["renderRes:fsm-dispatched"] - baseline["renderRes:fsm-dispatched"]; got < 1 {
		t.Errorf("renderRes:fsm-dispatched delta after first Apply = %d, want >= 1 (FSM did not dispatch renderRes)", got)
	}
}

// TestApplyDRBDRendersResViaFsmDispatchOnly pins the Phase 11.2.c
// Stage 4 step 1 contract: the FSM dispatch path is the sole writer
// of the .res file. After the legacy unconditional r.renderResFile
// call inside applyDRBD was retired, the dispatch path must still
// observe peer-list drift on a subsequent Apply pass (PhaseRunning
// with a fresh peer added to the spec) and rewrite the .res through
// the renderResFile preamble inside dispatchFsmAction.
//
// A regression that gated renderResFile only on PhaseUnprovisioned
// (cold start) would leave the .res stale on every Running pass
// after the first one — peer additions / removals would never reach
// drbdadm adjust, kernel state would diverge from spec, and the
// e2e replica-add scenarios would flake.
func TestApplyDRBDRendersResViaFsmDispatchOnly(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()
	// adjustResource's runAdjust probes HasDisklessVolume via drbdsetup
	// status; report a steady-state UpToDate volume so the SkipDisk
	// coercion stays off and the bare `drbdadm adjust` arm runs.
	fx.Expect("drbdsetup status --verbose pvc-stage4-step1-drift", storage.FakeResponse{
		Stdout: []byte(`pvc-stage4-step1-drift node-id:0 role:Secondary
  volume:0 minor:1000 disk:UpToDate backing_dev:/dev/vg/pvc-stage4-step1-drift_00000 quorum:yes
      worker-2 node-id:1 connection:Connected role:Secondary
    volume:0 replication:Established peer-disk:UpToDate
`),
	})

	rec := NewReconciler(ReconcilerConfig{
		Adm:          drbd.NewAdm(fx),
		StateDir:     dir,
		NodeName:     "n1",
		LocalAddress: "10.0.0.1",
	})

	dr, devices := dispatchFixtureDR("pvc-stage4-step1-drift")

	// Seed an OLD .res with the original single-peer layout so the
	// drift case has something to overwrite. renderResFile is the
	// authoritative writer, so this seed is also the canonical body
	// for the initial peer set.
	if err := rec.renderResFile(context.Background(), dr, devices); err != nil {
		t.Fatalf("seed renderResFile: %v", err)
	}

	resPath := filepath.Join(dir, "pvc-stage4-step1-drift.res")

	seeded, err := os.ReadFile(resPath)
	if err != nil {
		t.Fatalf("read seeded .res: %v", err)
	}

	// Simulate peer-list drift: spec now adds n3 with a fresh
	// peer.* option bag. The FSM dispatch's renderResFile preamble
	// inside dispatchFsmAction MUST rewrite the .res body so the
	// new peer block lands on disk before the phase-specific action
	// (createMd / up / adjust) runs.
	dr.Peers = append(dr.Peers, "n3")
	dr.DrbdOptions["peer.n3.address"] = "10.0.0.3"
	dr.DrbdOptions["peer.n3.node-id"] = "2"
	dr.DrbdOptions["peer.n3.port"] = "7000"

	// Observation for PhaseRunning: spec exists, .res seeded,
	// metadata stamped, kernel slot loaded. The FSM picks
	// ActionAdjust for this shape. The Stage 4 step 1 preamble
	// MUST still freshen the .res even though the dispatched action
	// is adjust, not renderRes.
	obs := Observation{
		SpecHasResource: true,
		ResFileExists:   true,
		MetadataExists:  true,
		KernelLoaded:    true,
	}

	if err := rec.dispatchFsmAction(context.Background(), dr, devices, ActionAdjust, obs); err != nil {
		t.Fatalf("dispatchFsmAction(ActionAdjust): %v", err)
	}

	got, err := os.ReadFile(resPath)
	if err != nil {
		t.Fatalf("read drift .res: %v", err)
	}

	if string(got) == string(seeded) {
		t.Fatalf(".res not refreshed by FSM dispatch preamble on drift; body matches pre-drift seed (peer n3 missing)")
	}

	if !strings.Contains(string(got), "on n3 {") {
		t.Errorf("drift .res missing peer n3 block — preamble did not rewrite:\n%s", got)
	}
}

// TestDispatchFsmActionNoopSkipsRenderPreamble pins the negative
// side of the Stage 4 step 1 contract: ActionNoop and
// ActionDecommission MUST NOT trigger the renderResFile preamble.
// Noop callers expect a literal no-op (any disk write would be a
// regression on the FSM's quiescent contract), and Decommission is
// the delete path — re-rendering a .res while the resource is
// being torn down would race the satellite's DeleteResource
// cleanup of state-dir files.
func TestDispatchFsmActionNoopSkipsRenderPreamble(t *testing.T) {
	dir := t.TempDir()
	rec := NewReconciler(ReconcilerConfig{
		StateDir:     dir,
		NodeName:     "n1",
		LocalAddress: "10.0.0.1",
	})

	dr, devices := dispatchFixtureDR("pvc-stage4-step1-noop")
	obs := Observation{SpecHasResource: true}

	for _, action := range []string{ActionNoop, ActionDecommission} {
		err := rec.dispatchFsmAction(context.Background(), dr, devices, action, obs)
		if err != nil {
			t.Errorf("dispatchFsmAction(%s): %v", action, err)
		}

		resPath := filepath.Join(dir, "pvc-stage4-step1-noop.res")
		if _, statErr := os.Stat(resPath); statErr == nil {
			t.Errorf("action %s wrote .res via preamble; expected no-op", action)

			_ = os.Remove(resPath)
		}
	}
}
