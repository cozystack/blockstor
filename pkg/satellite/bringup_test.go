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
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
)

// errDrbdadmUpFail is the sentinel canned-failure for the
// `drbdadm up <name>` shell-out in TestBringUpResourcePropagates
// Error. Package-level static error keeps err113 happy (no
// dynamic errors.New at the test call site).
var errDrbdadmUpFail = errors.New("drbdadm: exit status 1")

// TestBringUpResourceCallsDrbdadmUp pins the Phase 11.2.c Stage 3c
// invariant on the extracted helper directly: bringUpResource MUST
// shell out to `drbdadm up <name>` and return nil on success. The
// helper is the first-load path after createMetadata — distinct
// from runAdjust's Bug-287 `(158) Unknown resource` fallback to
// `drbdadm up` which is the recovery verb in the half-torn
// kernel-slot window and lives at its own call site.
//
// Targets the helper directly (rather than going through applyDRBD)
// so a regression in the helper's shell-out surfaces here rather
// than only via the end-to-end first-activation tests.
func TestBringUpResourceCallsDrbdadmUp(t *testing.T) {
	fx := storage.NewFakeExec()

	rec := NewReconciler(ReconcilerConfig{
		Adm:      drbd.NewAdm(fx),
		NodeName: "n1",
	})

	dr := &intent.DesiredResource{
		Name:     "pvc-bringup-ok",
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

	if err := rec.bringUpResource(context.Background(), dr); err != nil {
		t.Fatalf("bringUpResource: %v", err)
	}

	want := "drbdadm up pvc-bringup-ok"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("missing %q in calls: %v", want, fx.CommandLines())
	}
}

// TestBringUpResourcePropagatesError pins the helper's error
// contract: when `drbdadm up <name>` fails (FakeExec returns a
// canned error to mimic exit 1), bringUpResource MUST return a
// wrapped error that preserves the resource name in the message so
// the reconciler retry loop and operator logs have actionable
// context. A regression that swallowed the error would silently
// strand the resource down across reconciles.
func TestBringUpResourcePropagatesError(t *testing.T) {
	fx := storage.NewFakeExec()

	// Canned failure for the up shell-out: FakeResponse.Err drives
	// the wrapper into the error arm. Mirrors `exit 1` on the real
	// drbdadm CLI.
	fx.Expect("drbdadm up pvc-bringup-fail", storage.FakeResponse{
		Err: errDrbdadmUpFail,
	})

	rec := NewReconciler(ReconcilerConfig{
		Adm:      drbd.NewAdm(fx),
		NodeName: "n1",
	})

	dr := &intent.DesiredResource{
		Name:     "pvc-bringup-fail",
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

	err := rec.bringUpResource(context.Background(), dr)
	if err == nil {
		t.Fatalf("bringUpResource: expected wrapped error, got nil")
	}

	// Wrap MUST preserve the resource name + the verb so operator
	// logs surface "which resource failed which verb". The exact
	// format mirrors the inline call site this helper replaced.
	if !strings.Contains(err.Error(), "drbdadm up pvc-bringup-fail") {
		t.Errorf("error wrap missing %q context: %v", "drbdadm up pvc-bringup-fail", err)
	}
}

// TestApplyDRBDBringsUpViaFsmDispatchOnly pins the Stage 4 step 2
// contract: when applyDRBD runs against an unloaded kernel slot with
// fresh metadata, the FSM dispatch (not a legacy call inside
// runBringUpOrAdjust) is the sole source of drbdadm up. The legacy
// path was removed in Phase 11.2.c Stage 4 step 2.
//
// Observation shape: SpecHasResource=true, ResFileExists=true,
// MetadataExists=true, KernelLoaded=false — Phase==MetadataReady.
// FSM picks ActionUp. dispatchFsmAction runs renderResFile preamble
// (Stage 4 step 1) + bringUpResource. The legacy
// runBringUpOrAdjust's !loaded arm is now a documented no-op, so the
// only `drbdadm up <name>` shell-out comes from the FSM dispatch.
//
// A regression that re-added the legacy bringUp call inside
// runBringUpOrAdjust would surface as TWO `drbdadm up` calls on the
// same Apply pass.
func TestApplyDRBDBringsUpViaFsmDispatchOnly(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()

	rec := NewReconciler(ReconcilerConfig{
		Adm:          drbd.NewAdm(fx),
		StateDir:     dir,
		NodeName:     "n1",
		LocalAddress: "10.0.0.1",
	})

	dr := &intent.DesiredResource{
		Name:     "pvc-stage4-step2-up",
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
	devices := map[int32]string{0: "/dev/vg/pvc-stage4-step2-up_00000"}

	// Seed a .res file so the FSM preamble's stat+compare path is
	// covered (content-idempotent overwrite is a no-op when bodies
	// match — Bug 315).
	if err := rec.renderResFile(context.Background(), dr, devices); err != nil {
		t.Fatalf("seed renderResFile: %v", err)
	}

	// Sanity check: the seeded .res is on disk before dispatch.
	resPath := filepath.Join(dir, "pvc-stage4-step2-up.res")
	if _, err := os.Stat(resPath); err != nil {
		t.Fatalf("seeded .res missing: %v", err)
	}

	// Reset the FakeExec call recorder so we only count shell-outs
	// from the dispatch under test (renderResFile may have shelled
	// out to lvs while computing volume paths).
	fx = storage.NewFakeExec()
	rec.cfg.Adm = drbd.NewAdm(fx)

	// Phase==MetadataReady observation shape: spec present, .res on
	// disk, metadata stamped, kernel slot NOT loaded. NextTransition
	// MUST return ActionUp for this shape; assert that here so a
	// future FSM-table drift surfaces in this test rather than only
	// downstream in the dispatch counter.
	obs := Observation{
		SpecHasResource: true,
		ResFileExists:   true,
		MetadataExists:  true,
		KernelLoaded:    false,
	}

	phase := ObservePhase(obs)
	if phase != PhaseMetadataReady {
		t.Fatalf("ObservePhase: got %q, want %q", phase, PhaseMetadataReady)
	}

	next := NextTransition(phase, obs)
	if next == nil || next.Action != ActionUp {
		got := "nil"
		if next != nil {
			got = next.Action
		}

		t.Fatalf("NextTransition: got action %q, want %q", got, ActionUp)
	}

	if err := rec.dispatchFsmAction(context.Background(), dr, devices, ActionUp, obs); err != nil {
		t.Fatalf("dispatchFsmAction(ActionUp): %v", err)
	}

	// Exactly ONE `drbdadm up <name>` MUST land on the FakeExec.
	// More than one would mean the legacy bringUpResource call
	// inside runBringUpOrAdjust was re-introduced (or a regression
	// caused the dispatch to double-fire).
	wantUp := "drbdadm up pvc-stage4-step2-up"
	upCount := 0

	for _, line := range fx.CommandLines() {
		if line == wantUp {
			upCount++
		}
	}

	if upCount != 1 {
		t.Errorf("got %d %q calls, want exactly 1; calls=%v",
			upCount, wantUp, fx.CommandLines())
	}
}
