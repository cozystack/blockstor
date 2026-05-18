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
