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

import "testing"

// TestObservePhase pins, for every phase, a representative
// Observation that ObservePhase classifies as that phase.
func TestObservePhase(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		obs  Observation
		want DRBDPhase
	}{
		{
			name: "no_resource_returns_empty",
			obs:  Observation{SpecHasResource: false},
			want: "",
		},
		{
			name: "deletion_ts_dominates",
			obs: Observation{
				SpecHasResource:   true,
				SpecHasDeletionTS: true,
				ResFileExists:     true,
				KernelLoaded:      true,
				MetadataExists:    true,
			},
			want: PhaseDecommissioning,
		},
		{
			name: "skip_disk_dominates_over_running",
			obs: Observation{
				SpecHasResource: true,
				ResFileExists:   true,
				MetadataExists:  true,
				KernelLoaded:    true,
				SkipDiskProp:    true,
			},
			want: PhaseSkipDisk,
		},
		{
			name: "unprovisioned_when_no_res_file",
			obs: Observation{
				SpecHasResource: true,
				ResFileExists:   false,
			},
			want: PhaseUnprovisioned,
		},
		{
			name: "metadata_pending_when_res_but_no_metadata_diskful",
			obs: Observation{
				SpecHasResource:      true,
				ResFileExists:        true,
				MetadataExists:       false,
				SpecFlagsHasDiskless: false,
			},
			want: PhaseMetadataPending,
		},
		{
			name: "metadata_ready_when_metadata_no_kernel",
			obs: Observation{
				SpecHasResource: true,
				ResFileExists:   true,
				MetadataExists:  true,
				KernelLoaded:    false,
			},
			want: PhaseMetadataReady,
		},
		{
			name: "running_when_kernel_loaded",
			obs: Observation{
				SpecHasResource: true,
				ResFileExists:   true,
				MetadataExists:  true,
				KernelLoaded:    true,
			},
			want: PhaseRunning,
		},
		{
			name: "diskless_skips_metadata_pending",
			obs: Observation{
				SpecHasResource:      true,
				SpecFlagsHasDiskless: true,
				ResFileExists:        true,
				MetadataExists:       false,
				KernelLoaded:         false,
			},
			want: PhaseMetadataReady,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := ObservePhase(tc.obs); got != tc.want {
				t.Fatalf("ObservePhase(%+v) = %q, want %q", tc.obs, got, tc.want)
			}
		})
	}
}

// TestNextTransitionInitialProvisioning walks the provisioning
// chain Unprovisioned → MetadataPending → MetadataReady → Running,
// asserting one transition fires per step.
func TestNextTransitionInitialProvisioning(t *testing.T) {
	t.Parallel()

	steps := []struct {
		name       string
		from       DRBDPhase
		obs        Observation
		wantTo     DRBDPhase
		wantAction string
	}{
		{
			name:       "unprovisioned_to_metadata_pending",
			from:       PhaseUnprovisioned,
			obs:        Observation{SpecHasResource: true, ResFileExists: false},
			wantTo:     PhaseMetadataPending,
			wantAction: ActionRenderRes,
		},
		{
			name: "metadata_pending_to_metadata_ready",
			from: PhaseMetadataPending,
			obs: Observation{
				SpecHasResource: true,
				ResFileExists:   true,
				MetadataExists:  false,
			},
			wantTo:     PhaseMetadataReady,
			wantAction: ActionCreateMd,
		},
		{
			name: "metadata_ready_to_running",
			from: PhaseMetadataReady,
			obs: Observation{
				SpecHasResource: true,
				ResFileExists:   true,
				MetadataExists:  true,
				KernelLoaded:    false,
			},
			wantTo:     PhaseRunning,
			wantAction: ActionUp,
		},
	}

	for _, tc := range steps {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tr := NextTransition(tc.from, tc.obs)
			if tr == nil {
				t.Fatalf("NextTransition(%s, %+v) = nil, want transition", tc.from, tc.obs)
			}
			if tr.To != tc.wantTo {
				t.Errorf("To = %q, want %q", tr.To, tc.wantTo)
			}
			if tr.Action != tc.wantAction {
				t.Errorf("Action = %q, want %q", tr.Action, tc.wantAction)
			}
		})
	}
}

// TestNextTransitionDisklessToDiskfulFlip is Bug 319 in FSM form:
// kernel slot is Diskless, Spec has flipped to diskful, metadata
// is absent → FSM must drop back to MetadataPending to lay down
// metadata before adjusting.
func TestNextTransitionDisklessToDiskfulFlip(t *testing.T) {
	t.Parallel()

	obs := Observation{
		SpecHasResource:      true,
		SpecFlagsHasDiskless: false, // Spec flipped to diskful
		ResFileExists:        true,
		KernelLoaded:         true,
		KernelHasDiskless:    true, // kernel still Diskless
		MetadataExists:       false,
	}

	tr := NextTransition(PhaseRunning, obs)
	if tr == nil {
		t.Fatalf("NextTransition(Running, diskless→diskful flip) = nil, want createMd")
	}
	if tr.To != PhaseMetadataPending {
		t.Errorf("To = %q, want %q", tr.To, PhaseMetadataPending)
	}
	if tr.Action != ActionCreateMd {
		t.Errorf("Action = %q, want %q", tr.Action, ActionCreateMd)
	}
}

// TestNextTransitionDecommission proves DeletionTimestamp wins
// from every non-terminal phase.
func TestNextTransitionDecommission(t *testing.T) {
	t.Parallel()

	phases := []DRBDPhase{
		PhaseUnprovisioned,
		PhaseMetadataPending,
		PhaseMetadataReady,
		PhaseRunning,
		PhaseSkipDisk,
	}

	for _, from := range phases {
		t.Run(string(from), func(t *testing.T) {
			t.Parallel()
			obs := Observation{
				SpecHasResource:   true,
				SpecHasDeletionTS: true,
				// Throw in noise that would otherwise route elsewhere.
				ResFileExists:  true,
				MetadataExists: true,
				KernelLoaded:   true,
				SkipDiskProp:   from == PhaseSkipDisk,
			}
			tr := NextTransition(from, obs)
			if tr == nil {
				t.Fatalf("NextTransition(%s, deletion) = nil, want decommission", from)
			}
			if tr.To != PhaseDecommissioning {
				t.Errorf("To = %q, want %q", tr.To, PhaseDecommissioning)
			}
			if tr.Action != ActionDecommission {
				t.Errorf("Action = %q, want %q", tr.Action, ActionDecommission)
			}
		})
	}
}

// TestNextTransitionSkipDisk covers both directions of the
// operator pin: Running→SkipDisk (noop) and SkipDisk→Running
// (adjust).
func TestNextTransitionSkipDisk(t *testing.T) {
	t.Parallel()

	t.Run("running_to_skip_disk", func(t *testing.T) {
		t.Parallel()
		obs := Observation{
			SpecHasResource: true,
			ResFileExists:   true,
			MetadataExists:  true,
			KernelLoaded:    true,
			SkipDiskProp:    true,
		}
		tr := NextTransition(PhaseRunning, obs)
		if tr == nil {
			t.Fatalf("NextTransition(Running, SkipDisk=true) = nil, want noop")
		}
		if tr.To != PhaseSkipDisk {
			t.Errorf("To = %q, want %q", tr.To, PhaseSkipDisk)
		}
		if tr.Action != ActionNoop {
			t.Errorf("Action = %q, want %q", tr.Action, ActionNoop)
		}
	})

	t.Run("skip_disk_to_running", func(t *testing.T) {
		t.Parallel()
		obs := Observation{
			SpecHasResource: true,
			ResFileExists:   true,
			MetadataExists:  true,
			KernelLoaded:    true,
			SkipDiskProp:    false,
		}
		tr := NextTransition(PhaseSkipDisk, obs)
		if tr == nil {
			t.Fatalf("NextTransition(SkipDisk, SkipDisk=false) = nil, want adjust")
		}
		if tr.To != PhaseRunning {
			t.Errorf("To = %q, want %q", tr.To, PhaseRunning)
		}
		if tr.Action != ActionAdjust {
			t.Errorf("Action = %q, want %q", tr.Action, ActionAdjust)
		}
	})
}

// TestNextTransitionRunningSteady asserts that a healthy Running
// state (kernel loaded, no flip, no SkipDisk, no deletion) still
// fires the routine adjust self-loop. The reconciler relies on
// this to converge config drift.
func TestNextTransitionRunningSteady(t *testing.T) {
	t.Parallel()

	obs := Observation{
		SpecHasResource: true,
		ResFileExists:   true,
		MetadataExists:  true,
		KernelLoaded:    true,
	}

	tr := NextTransition(PhaseRunning, obs)
	if tr == nil {
		t.Fatalf("NextTransition(Running, steady) = nil, want adjust self-loop")
	}
	if tr.From != PhaseRunning || tr.To != PhaseRunning {
		t.Errorf("From/To = %q/%q, want Running/Running", tr.From, tr.To)
	}
	if tr.Action != ActionAdjust {
		t.Errorf("Action = %q, want %q", tr.Action, ActionAdjust)
	}
}

// TestNextTransitionTerminalGood — a "no-op" world (e.g. no spec
// at all) yields no transition from any phase.
func TestNextTransitionTerminalGood(t *testing.T) {
	t.Parallel()

	// Empty observation from MetadataReady: kernel not loaded and
	// metadata not present → no Trigger in the table fires.
	obs := Observation{SpecHasResource: true, ResFileExists: true}
	if tr := NextTransition(PhaseMetadataReady, obs); tr != nil {
		t.Fatalf("NextTransition(MetadataReady, no-metadata) = %+v, want nil", tr)
	}

	// Decommissioning is terminal: no outgoing rows in the table.
	if tr := NextTransition(PhaseDecommissioning, Observation{SpecHasDeletionTS: true}); tr != nil {
		t.Fatalf("NextTransition(Decommissioning, *) = %+v, want nil (terminal)", tr)
	}
}

// TestFsmTransitionsHaveKnownActions guards the action vocabulary.
// Every Action in the table must be one of the named constants.
func TestFsmTransitionsHaveKnownActions(t *testing.T) {
	t.Parallel()

	known := map[string]struct{}{
		ActionRenderRes:    {},
		ActionCreateMd:     {},
		ActionUp:           {},
		ActionAdjust:       {},
		ActionDecommission: {},
		ActionNoop:         {},
	}

	for i, tr := range fsm {
		if _, ok := known[tr.Action]; !ok {
			t.Errorf("fsm[%d]: unknown Action %q (From=%s To=%s)", i, tr.Action, tr.From, tr.To)
		}
	}
}

// TestEveryPhaseHasAtLeastOneOutgoingTransition ensures the table
// covers every non-terminal phase. Decommissioning is terminal —
// it intentionally has no outgoing rows.
func TestEveryPhaseHasAtLeastOneOutgoingTransition(t *testing.T) {
	t.Parallel()

	want := []DRBDPhase{
		PhaseUnprovisioned,
		PhaseMetadataPending,
		PhaseMetadataReady,
		PhaseRunning,
		PhaseSkipDisk,
	}

	have := make(map[DRBDPhase]int, len(want))
	for _, tr := range fsm {
		have[tr.From]++
	}

	for _, p := range want {
		if have[p] == 0 {
			t.Errorf("phase %q has no outgoing transitions in fsm table", p)
		}
	}

	if have[PhaseDecommissioning] != 0 {
		t.Errorf("Decommissioning is terminal but has %d outgoing rows", have[PhaseDecommissioning])
	}
}
