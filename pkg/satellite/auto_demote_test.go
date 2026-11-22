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
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/satellite"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/lvm"
)

// errHeldOpen is drbdadm refusing the demote of a device a host process
// still has open, verbatim from the alpha.15 report.
var errHeldOpen = errors.New("exit status 11: pvc-demote: State change failed: (-12) Device is held open by someone")

// errSecondaryOther is a demote failure that is not a held-open device.
var errSecondaryOther = errors.New("exit status 10: pvc-demote: State change failed: (-2) Need access to UpToDate data")

const demoteSecondary = "drbdadm secondary pvc-demote"

// demoteFixture is a single-replica RD that asks for a filesystem, so
// its first apply runs primary --force, mkfs and secondary.
func demoteFixture(t *testing.T) (*satellite.Reconciler, *storage.FakeExec, []*intent.DesiredResource, string) {
	t.Helper()

	dir := t.TempDir()
	fx := storage.NewFakeExec()
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-demote_00000",
		storage.FakeResponse{Stdout: []byte("")})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		Exec:      fx,
		StateDir:  dir,
		NodeName:  "n1",
	})
	rec.SetDemoteRetryForTest([]time.Duration{0}, 0)

	dr := []*intent.DesiredResource{{
		Name:     "pvc-demote",
		NodeName: "n1",
		Volumes: []*intent.DesiredVolume{
			{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
		},
		Props:           map[string]string{"FileSystem/Type": "ext4"},
		SkipInitialSync: skipInitTrue(),
		DrbdOptions: map[string]string{
			"port": "7000", "node-id": "0", "address": "10.0.0.1", "minor": "1000",
			"auto-primary": "true",
		},
	}}

	return rec, fx, dr, filepath.Join(dir, "pvc-demote.demote-pending")
}

// steadyApply runs a later apply of the fixture's RD and returns the
// demotes it issued.
func steadyApply(t *testing.T, rec *satellite.Reconciler, fx *storage.FakeExec, dr []*intent.DesiredResource) (bool, int) {
	t.Helper()

	fx.Reset()
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-demote_00000",
		storage.FakeResponse{Stdout: []byte("pvc-demote_00000\n")})
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings --separator | -o lv_path,lv_size --units k --nosuffix vg/pvc-demote_00000",
		storage.FakeResponse{Stdout: []byte("/dev/vg/pvc-demote_00000|1048576\n")})

	results, err := rec.Apply(t.Context(), dr)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	return results[0].GetOk(), countLines(fx.CommandLines(), demoteSecondary)
}

func countLines(lines []string, want string) int {
	n := 0

	for _, line := range lines {
		if line == want {
			n++
		}
	}

	return n
}

func demoteOwed(t *testing.T, path string) bool {
	t.Helper()

	_, err := os.Stat(path)

	return err == nil
}

// alpha.15 issue 01: the demote after mkfs found the device held open
// by a host process, and the retried apply no longer demoted, leaving
// the replica Primary with nobody using it. The demote must stay owed
// until it goes through, without the held-open device failing the
// apply.
func TestAutoSecondaryHeldOpenIsRetriedByLaterApplies(t *testing.T) {
	t.Parallel()

	rec, fx, dr, owed := demoteFixture(t)
	fx.ExpectSequence(demoteSecondary,
		storage.FakeResponse{Err: errHeldOpen},
		storage.FakeResponse{Err: errHeldOpen},
		storage.FakeResponse{Err: errHeldOpen})

	results, err := rec.Apply(t.Context(), dr)
	if err != nil {
		t.Fatalf("Apply (1st): %v", err)
	}

	if !results[0].GetOk() {
		t.Errorf("a device held open after mkfs failed the apply: %q", results[0].GetMessage())
	}

	if got := countLines(fx.CommandLines(), demoteSecondary); got != 2 {
		t.Errorf("first apply: %d demotes, want 2 (one and one retry)", got)
	}

	if !demoteOwed(t, owed) {
		t.Fatal("the demote that did not go through is not owed")
	}

	ok, demotes := steadyApply(t, rec, fx, dr)
	if !ok || demotes != 1 || !demoteOwed(t, owed) {
		t.Errorf("second apply (still held open): ok=%v demotes=%d owed=%v, want true, 1, true",
			ok, demotes, demoteOwed(t, owed))
	}

	ok, demotes = steadyApply(t, rec, fx, dr)
	if !ok || demotes != 1 || demoteOwed(t, owed) {
		t.Errorf("third apply (released): ok=%v demotes=%d owed=%v, want true, 1, false",
			ok, demotes, demoteOwed(t, owed))
	}

	ok, demotes = steadyApply(t, rec, fx, dr)
	if !ok || demotes != 0 {
		t.Errorf("after the demote went through: ok=%v demotes=%d, want true, 0", ok, demotes)
	}
}

// A device held open for a moment is waited for within the apply: the
// demote goes through on its retry and nothing is left owed.
func TestAutoSecondaryWaitsOutABriefOpener(t *testing.T) {
	t.Parallel()

	rec, fx, dr, owed := demoteFixture(t)
	fx.ExpectSequence(demoteSecondary, storage.FakeResponse{Err: errHeldOpen})

	results, err := rec.Apply(t.Context(), dr)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if !results[0].GetOk() {
		t.Errorf("apply failed: %q", results[0].GetMessage())
	}

	if got := countLines(fx.CommandLines(), demoteSecondary); got != 2 {
		t.Errorf("%d demotes, want 2", got)
	}

	if demoteOwed(t, owed) {
		t.Error("a demote that went through is still owed")
	}

	if _, demotes := steadyApply(t, rec, fx, dr); demotes != 0 {
		t.Errorf("a later apply demoted again (%d)", demotes)
	}
}

// Any other demote failure still fails the apply, and the demote stays
// owed so the next apply tries it again.
func TestAutoSecondaryOtherFailureFailsTheApply(t *testing.T) {
	t.Parallel()

	rec, fx, dr, owed := demoteFixture(t)
	fx.ExpectSequence(demoteSecondary, storage.FakeResponse{Err: errSecondaryOther})

	results, err := rec.Apply(t.Context(), dr)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if results[0].GetOk() {
		t.Error("a demote that failed for another reason left the apply Ok")
	}

	if got := countLines(fx.CommandLines(), demoteSecondary); got != 1 {
		t.Errorf("%d demotes, want 1: only a held-open device is waited for", got)
	}

	ok, demotes := steadyApply(t, rec, fx, dr)
	if !ok || demotes != 1 || demoteOwed(t, owed) {
		t.Errorf("next apply: ok=%v demotes=%d owed=%v, want true, 1, false", ok, demotes, demoteOwed(t, owed))
	}
}

// A device that stays held open, say by a consumer that mounted the
// replica meanwhile, costs one demote attempt per pacing interval, not
// one per apply.
func TestAutoSecondaryOwedDemoteIsPaced(t *testing.T) {
	t.Parallel()

	rec, fx, dr, owed := demoteFixture(t)
	rec.SetDemoteRetryForTest(nil, time.Hour)
	fx.ExpectSequence(demoteSecondary, storage.FakeResponse{Err: errHeldOpen})

	_, err := rec.Apply(t.Context(), dr)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if !demoteOwed(t, owed) {
		t.Fatal("the demote that did not go through is not owed")
	}

	if ok, demotes := steadyApply(t, rec, fx, dr); !ok || demotes != 0 {
		t.Errorf("apply within the pacing interval: ok=%v demotes=%d, want true, 0", ok, demotes)
	}
}
