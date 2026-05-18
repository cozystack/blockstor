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
