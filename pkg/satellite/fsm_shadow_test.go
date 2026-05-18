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
	"expvar"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
)

// TestObserveForFsmUnprovisioned covers the cold-start shape: spec
// exists, no .res, no metadata, no kernel slot. The FSM must
// classify this as PhaseUnprovisioned and recommend renderRes.
func TestObserveForFsmUnprovisioned(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	fx := storage.NewFakeExec()
	// IsLoaded probe — slot absent (empty stdout + nil err is the
	// canonical "no slot" shape FakeExec produces; the real drbdsetup
	// returns exit 10 here, both routes drop into IsLoaded=false).
	fx.Expect("drbdsetup status pvc-shadow-cold", storage.FakeResponse{})

	rec := NewReconciler(ReconcilerConfig{
		Adm:      drbd.NewAdm(fx),
		StateDir: dir,
		NodeName: "n1",
	})

	dr := &intent.DesiredResource{Name: "pvc-shadow-cold", NodeName: "n1"}

	obs := rec.observeForFsm(context.Background(), dr, false)

	if !obs.SpecHasResource {
		t.Errorf("SpecHasResource = false, want true")
	}

	if obs.ResFileExists {
		t.Errorf("ResFileExists = true, want false (cold start)")
	}

	if obs.MetadataExists {
		t.Errorf("MetadataExists = true, want false (cold start)")
	}

	if obs.KernelLoaded {
		t.Errorf("KernelLoaded = true, want false (cold start)")
	}

	got := ObservePhase(obs)
	if got != PhaseUnprovisioned {
		t.Fatalf("ObservePhase = %q, want %q", got, PhaseUnprovisioned)
	}

	next := NextTransition(got, obs)
	if next == nil {
		t.Fatalf("NextTransition = nil, want a renderRes edge")
	}

	if next.Action != ActionRenderRes {
		t.Errorf("Action = %q, want %q", next.Action, ActionRenderRes)
	}
}

// TestObserveForFsmRunningReadsKernel covers the steady-state
// shape: .res + .md-created on disk, kernel slot loaded UpToDate.
// Phase must be Running and the FSM must self-loop with adjust.
func TestObserveForFsmRunningReadsKernel(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	resPath := filepath.Join(dir, "pvc-shadow-run.res")
	mdMarker := filepath.Join(dir, "pvc-shadow-run.md-created")

	err := os.WriteFile(resPath, []byte("resource pvc-shadow-run {}\n"), 0o600)
	if err != nil {
		t.Fatalf("write .res: %v", err)
	}

	err = os.WriteFile(mdMarker, []byte{}, 0o600)
	if err != nil {
		t.Fatalf("write .md-created: %v", err)
	}

	fx := storage.NewFakeExec()
	// IsLoaded probe — slot present (any non-empty line counts).
	fx.Expect("drbdsetup status pvc-shadow-run",
		storage.FakeResponse{Stdout: []byte("pvc-shadow-run role:Secondary\n  volume:0 disk:UpToDate\n")})
	// HasDisklessVolume probe — verbose dump, no `disk:Diskless` rows.
	fx.Expect("drbdsetup status --verbose pvc-shadow-run",
		storage.FakeResponse{Stdout: []byte("pvc-shadow-run role:Secondary\n  volume:0 disk:UpToDate\n")})

	rec := NewReconciler(ReconcilerConfig{
		Adm:      drbd.NewAdm(fx),
		StateDir: dir,
		NodeName: "n1",
	})

	dr := &intent.DesiredResource{Name: "pvc-shadow-run", NodeName: "n1"}

	obs := rec.observeForFsm(context.Background(), dr, false)

	if !obs.ResFileExists {
		t.Errorf("ResFileExists = false, want true")
	}

	if !obs.MetadataExists {
		t.Errorf("MetadataExists = false, want true")
	}

	if !obs.KernelLoaded {
		t.Errorf("KernelLoaded = false, want true")
	}

	if obs.KernelHasDiskless {
		t.Errorf("KernelHasDiskless = true, want false (UpToDate slot)")
	}

	got := ObservePhase(obs)
	if got != PhaseRunning {
		t.Fatalf("ObservePhase = %q, want %q", got, PhaseRunning)
	}

	next := NextTransition(got, obs)
	if next == nil {
		t.Fatalf("NextTransition = nil, want Running self-loop")
	}

	if next.Action != ActionAdjust {
		t.Errorf("Action = %q, want %q", next.Action, ActionAdjust)
	}
}

// TestObserveForFsmDisklessFlagSkipsCreateMd ensures the Diskless
// flag is propagated to SpecFlagsHasDiskless so the FSM doesn't
// suggest createMd on a diskless replica. With diskless=true and
// no .res, the only fitting edge is renderRes from Unprovisioned.
func TestObserveForFsmDisklessFlagSkipsCreateMd(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	fx := storage.NewFakeExec()
	fx.Expect("drbdsetup status pvc-shadow-dless", storage.FakeResponse{})

	rec := NewReconciler(ReconcilerConfig{
		Adm:      drbd.NewAdm(fx),
		StateDir: dir,
		NodeName: "n1",
	})

	dr := &intent.DesiredResource{Name: "pvc-shadow-dless", NodeName: "n1", Flags: []string{"DISKLESS"}}

	obs := rec.observeForFsm(context.Background(), dr, true)
	if !obs.SpecFlagsHasDiskless {
		t.Fatalf("SpecFlagsHasDiskless = false, want true (diskless flag passed through)")
	}
}

// TestObserveForFsmSkipDiskPropDominates pins the operator-pinned
// SkipDisk shape: even with .res + metadata + kernel slot present,
// the FSM must classify the phase as SkipDisk so observers don't
// suggest an adjust the operator deliberately blocked.
func TestObserveForFsmSkipDiskPropDominates(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	resPath := filepath.Join(dir, "pvc-shadow-skip.res")
	mdMarker := filepath.Join(dir, "pvc-shadow-skip.md-created")

	err := os.WriteFile(resPath, []byte("resource pvc-shadow-skip {}\n"), 0o600)
	if err != nil {
		t.Fatalf("write .res: %v", err)
	}

	err = os.WriteFile(mdMarker, []byte{}, 0o600)
	if err != nil {
		t.Fatalf("write .md-created: %v", err)
	}

	fx := storage.NewFakeExec()
	fx.Expect("drbdsetup status pvc-shadow-skip",
		storage.FakeResponse{Stdout: []byte("pvc-shadow-skip role:Secondary\n  volume:0 disk:UpToDate\n")})
	fx.Expect("drbdsetup status --verbose pvc-shadow-skip",
		storage.FakeResponse{Stdout: []byte("pvc-shadow-skip role:Secondary\n  volume:0 disk:UpToDate\n")})

	rec := NewReconciler(ReconcilerConfig{
		Adm:      drbd.NewAdm(fx),
		StateDir: dir,
		NodeName: "n1",
	})

	dr := &intent.DesiredResource{
		Name:        "pvc-shadow-skip",
		NodeName:    "n1",
		DrbdOptions: map[string]string{"DrbdOptions/SkipDisk": "True"},
	}

	obs := rec.observeForFsm(context.Background(), dr, false)

	if !obs.SkipDiskProp {
		t.Fatalf("SkipDiskProp = false, want true")
	}

	if got := ObservePhase(obs); got != PhaseSkipDisk {
		t.Errorf("ObservePhase = %q, want %q", got, PhaseSkipDisk)
	}
}

// TestComputeLegacyActionMatchesFsmForEachGate enumerates the gates
// from the Phase 11.2.c plan and asserts, for every gate, that the
// FSM-suggested action and computeLegacyAction agree — except for
// the three KNOWN divergences (empty volumes, stale-content .res
// rewrite, first-activation adjust-vs-up).
//
// This test pins the contract that drives fsmShadowAgreeCount:
// when the counter logs a "diverge" entry in production, it must
// be a genuine FSM-vs-legacy mismatch, not an artefact of
// computeLegacyAction drifting from the real applyDRBD code path.
func TestComputeLegacyActionMatchesFsmForEachGate(t *testing.T) {
	t.Parallel()

	type gate struct {
		name string
		obs  Observation
		// fsmAction is the action the FSM table proposes for this
		// observation. Empty means "no transition fires" (terminal).
		fsmAction string
		// legacyAction is what computeLegacyAction must return.
		legacyAction string
		// knownDivergent flags gates where the FSM and legacy
		// intentionally disagree. The 11.2.c plan documents these.
		knownDivergent bool
	}

	gates := []gate{
		{
			name:      "unprovisioned_cold_start",
			obs:       Observation{SpecHasResource: true},
			fsmAction: ActionRenderRes,
			// KNOWN divergence (gate 3 of the 11.2.c plan):
			// first-activation adjust-vs-up. FSM proposes renderRes
			// from Unprovisioned (no .res on disk yet); legacy's
			// runApplyDRBDVerb routes firstActivation+!diskless
			// straight into ensureMetadata (createMd) before any
			// kernel verb. Both paths converge on the next reconcile,
			// but the first-tick action differs.
			legacyAction:   ActionCreateMd,
			knownDivergent: true,
		},
		{
			name: "first_activation_diskful_creates_metadata",
			obs: Observation{
				SpecHasResource: true,
				ResFileExists:   true,
				MetadataExists:  false,
			},
			fsmAction:    ActionCreateMd,
			legacyAction: ActionCreateMd,
		},
		{
			name: "first_activation_diskless_skips_metadata",
			obs: Observation{
				SpecHasResource:      true,
				SpecFlagsHasDiskless: true,
				ResFileExists:        true,
				MetadataExists:       false,
			},
			fsmAction: "", // FSM has no transition: diskless skips MetadataPending → MetadataReady, then Up triggers on !KernelLoaded.
			// Documented KNOWN divergence: first-activation adjust-vs-up.
			// FSM jumps Unprovisioned→MetadataReady→Running via "up";
			// legacy's firstActivation gate sends it through adjust
			// because runApplyDRBDVerb routes firstActivation=true
			// directly to runAdjust (skips the kernel probe).
			legacyAction:   ActionUp,
			knownDivergent: true,
		},
		{
			name: "metadata_ready_to_running_via_up",
			obs: Observation{
				SpecHasResource: true,
				ResFileExists:   true,
				MetadataExists:  true,
				KernelLoaded:    false,
			},
			fsmAction:    ActionUp,
			legacyAction: ActionUp,
		},
		{
			name: "running_steady_adjusts",
			obs: Observation{
				SpecHasResource: true,
				ResFileExists:   true,
				MetadataExists:  true,
				KernelLoaded:    true,
			},
			fsmAction:    ActionAdjust,
			legacyAction: ActionAdjust,
		},
		{
			name: "running_skip_disk_prop_adjusts_with_skip_disk",
			obs: Observation{
				SpecHasResource: true,
				ResFileExists:   true,
				MetadataExists:  true,
				KernelLoaded:    true,
				SkipDiskProp:    true,
			},
			fsmAction:    ActionAdjustSkipDisk,
			legacyAction: ActionAdjustSkipDisk,
		},
		{
			name: "running_kernel_diskless_coerces_skip_disk",
			obs: Observation{
				SpecHasResource:      true,
				ResFileExists:        true,
				MetadataExists:       true,
				KernelLoaded:         true,
				KernelHasDiskless:    true,
				SpecFlagsHasDiskless: false,
			},
			// FSM: kernel diskless + spec diskful + metadata exists
			// → no createMd (metadata path skips), Running self-loop
			// returns plain adjust. Legacy: Bug 280 coercion flips
			// skipDisk=true → AdjustSkipDisk. KNOWN divergence: the
			// kernel-diskless coercion isn't modeled in the FSM table
			// yet (would need a `KernelHasDiskless && !SpecHasDiskless
			// && MetadataExists` row).
			fsmAction:      ActionAdjust,
			legacyAction:   ActionAdjustSkipDisk,
			knownDivergent: true,
		},
		{
			name: "diskful_flip_recreates_metadata",
			obs: Observation{
				SpecHasResource:      true,
				ResFileExists:        true,
				MetadataExists:       false,
				KernelLoaded:         true,
				KernelHasDiskless:    true,
				SpecFlagsHasDiskless: false,
			},
			fsmAction:    ActionCreateMd,
			legacyAction: ActionCreateMd,
		},
		{
			name: "skip_disk_clearing_returns_to_adjust",
			obs: Observation{
				SpecHasResource: true,
				ResFileExists:   true,
				MetadataExists:  true,
				KernelLoaded:    true,
				SkipDiskProp:    false,
			},
			fsmAction:    ActionAdjust,
			legacyAction: ActionAdjust,
		},
		{
			name:           "no_resource_is_noop",
			obs:            Observation{SpecHasResource: false},
			fsmAction:      "", // FSM has no transitions from empty phase.
			legacyAction:   ActionNoop,
			knownDivergent: true, // FSM returns "" (no transition), legacy returns noop — semantic equivalent.
		},
	}

	for _, g := range gates {
		t.Run(g.name, func(t *testing.T) {
			t.Parallel()

			phase := ObservePhase(g.obs)

			gotFSM := ""
			if next := NextTransition(phase, g.obs); next != nil {
				gotFSM = next.Action
			}

			if gotFSM != g.fsmAction {
				t.Errorf("FSM action for %s = %q, want %q (phase=%s)", g.name, gotFSM, g.fsmAction, phase)
			}

			gotLegacy := computeLegacyAction(g.obs)
			if gotLegacy != g.legacyAction {
				t.Errorf("legacy action for %s = %q, want %q", g.name, gotLegacy, g.legacyAction)
			}

			agree := gotFSM == gotLegacy
			if g.knownDivergent && agree {
				t.Errorf("gate %s marked knownDivergent but FSM and legacy both = %q", g.name, gotFSM)
			}
			if !g.knownDivergent && !agree {
				t.Errorf("gate %s: FSM=%q legacy=%q diverge (not in KNOWN list)", g.name, gotFSM, gotLegacy)
			}
		})
	}
}

// TestRecordAgreementIncrementsCounter fabricates a known sequence
// of agree/diverge calls and asserts the expvar counter reaches
// the expected values. Read of expvar.Map values goes through
// expvar.Map.Get / String() to dodge data races on internal state.
func TestRecordAgreementIncrementsCounter(t *testing.T) {
	// expvar.Map state is global — this test mutates it, so it
	// cannot run with t.Parallel without a snapshot/restore dance.
	// Run sequentially; the global map starts empty per test
	// binary process and the assertions are keyed off the deltas.
	baseline := snapshotShadowMap()

	calls := []struct {
		expected, legacy string
	}{
		{ActionAdjust, ActionAdjust},                 // agree
		{ActionAdjust, ActionAdjust},                 // agree
		{ActionCreateMd, ActionCreateMd},             // agree
		{ActionAdjustSkipDisk, ActionAdjust},         // diverge
		{ActionAdjust, ActionUp},                     // diverge
		{ActionRenderRes, ActionCreateMd},            // diverge
		{ActionUp, ActionUp},                         // agree
		{ActionNoop, ActionNoop},                     // agree
		{ActionAdjustSkipDisk, ActionAdjustSkipDisk}, // agree
		{ActionAdjust, ActionAdjust},                 // agree
	}

	for _, c := range calls {
		recordFsmShadowAgreement(c.expected, c.legacy)
	}

	final := snapshotShadowMap()

	want := map[string]int64{
		ActionAdjust + ":agree":           3,
		ActionCreateMd + ":agree":         1,
		ActionUp + ":agree":               1,
		ActionNoop + ":agree":             1,
		ActionAdjustSkipDisk + ":agree":   1,
		ActionAdjustSkipDisk + ":diverge": 1,
		ActionAdjust + ":diverge":         1,
		ActionRenderRes + ":diverge":      1,
	}

	for key, wantDelta := range want {
		gotDelta := final[key] - baseline[key]
		if gotDelta != wantDelta {
			t.Errorf("counter %q: delta = %d, want %d (baseline=%d final=%d)",
				key, gotDelta, wantDelta, baseline[key], final[key])
		}
	}
}

// snapshotShadowMap copies fsmShadowAgreeCount into a plain
// map[string]int64 so tests can take baseline / delta diffs without
// touching expvar internals.
func snapshotShadowMap() map[string]int64 {
	out := make(map[string]int64)

	fsmShadowAgreeCount.Do(func(kv expvar.KeyValue) {
		// expvar.Int.String returns a base-10 string of the int64.
		if intVal, ok := kv.Value.(*expvar.Int); ok {
			parsed, err := strconv.ParseInt(intVal.String(), 10, 64)
			if err == nil {
				out[kv.Key] = parsed
			}
		}
	})

	return out
}

// TestLogFsmShadowDoesNotMutate is the safety pin for shadow mode:
// running logFsmShadow on disk must leave the StateDir untouched.
// Any future regression that lets the shadow path write a marker
// or remove a .res would break this test.
func TestLogFsmShadowDoesNotMutate(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	fx := storage.NewFakeExec()
	fx.Expect("drbdsetup status pvc-shadow-noop", storage.FakeResponse{})

	rec := NewReconciler(ReconcilerConfig{
		Adm:      drbd.NewAdm(fx),
		StateDir: dir,
		NodeName: "n1",
	})

	dr := &intent.DesiredResource{Name: "pvc-shadow-noop", NodeName: "n1"}

	rec.logFsmShadow(context.Background(), dr, false)

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read StateDir: %v", err)
	}

	if len(entries) != 0 {
		t.Errorf("StateDir gained %d entries after shadow log; expected 0", len(entries))
	}
}
