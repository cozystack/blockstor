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

// skipInitGateFixtureDR returns a fresh-diskful DesiredResource with a
// resolved local node-id (so the Bug 360 node-id gate passes) and the
// caller's SkipInitialSync tri-state. The node-id being present isolates
// the skip-init-sync gate as the only thing that can defer bring-up.
func skipInitGateFixtureDR(name string, skip *bool) *intent.DesiredResource {
	return &intent.DesiredResource{
		Name:     name,
		NodeName: "n1",
		Volumes: []*intent.DesiredVolume{
			{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
		},
		Peers:           []intent.DesiredPeer{{Name: "n2"}},
		SkipInitialSync: skip,
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
}

// TestApplyRefusesSeedWhenSkipInitialSyncUnstamped is the regression pin
// for CI run 26500468866 (all 7 e2e lanes RED): the skip-hardening
// branch deadlocked fresh deployments. The satellite reconciles on its
// own Watch and could observe a Resource whose node-id/port were already
// stamped but whose Spec.SkipInitialSync was still nil — the controller
// had not yet committed the decision. resolveVolumeSeed then read nil,
// refused EVERY day0 skip (including the case-B winner UpToDate seed),
// so NO replica was seeded as the UpToDate winner and both diskful
// replicas came up Inconsistent / PausedSyncT with no sync source →
// permanent deadlock.
//
// The fix gates the fresh-diskful first-activation seed on
// Spec.SkipInitialSync being non-nil (mirroring the Bug 360 node-id
// gate). While nil, the satellite MUST NOT seed metadata: it renders no
// .res, runs no create-md/up/adjust, and surfaces a not-Ok result so
// runApply requeues until the controller stamps the decision. This
// removes the nil read from resolveVolumeSeed entirely.
func TestApplyRefusesSeedWhenSkipInitialSyncUnstamped(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()
	// Storage provisioning is allowed to run (idempotent); only the DRBD
	// burn is deferred. lvs returns empty (fresh create).
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-skipgate_00000",
		storage.FakeResponse{Stdout: []byte("")})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		StateDir:  dir,
		NodeName:  "n1",
	})

	// SkipInitialSync deliberately nil — the controller has not yet
	// stamped the decision (the async-K8s ordering window).
	results, err := rec.Apply(t.Context(), []*intent.DesiredResource{
		skipInitGateFixtureDR("pvc-skipgate", nil),
	})
	if err != nil {
		t.Fatalf("Apply returned hard error (want per-resource not-Ok): %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("Apply results: got %d, want 1", len(results))
	}

	if results[0].Ok {
		t.Errorf("result Ok=true; want false (seed must be deferred until SkipInitialSync is stamped)")
	}

	// No .res file may exist and no DRBD bring-up / seed verb may fire —
	// otherwise a fresh deploy would be brought up Inconsistent with no
	// elected winner (the deadlock).
	resPath := filepath.Join(dir, "pvc-skipgate.res")
	if _, statErr := os.Stat(resPath); statErr == nil {
		body, _ := os.ReadFile(resPath)
		t.Errorf(".res must NOT be rendered while SkipInitialSync is unstamped; got:\n%s", string(body))
	}

	for _, line := range fx.CommandLines() {
		if strings.Contains(line, "create-md") ||
			strings.Contains(line, "set-gi") ||
			strings.Contains(line, "drbdadm up") ||
			strings.Contains(line, "drbdadm adjust") ||
			strings.Contains(line, "new-resource") {
			t.Errorf("no DRBD seed/bring-up verb may fire while SkipInitialSync is nil; got %q in:\n%v",
				line, fx.CommandLines())
		}
	}
}

// TestApplyElectsWinnerOnceSkipInitialSyncStampedTrue is the converge
// half of the regression: once the controller stamps SkipInitialSync =
// true (genuinely-fresh RD), the elected winner (auto-primary, no
// peer-data) must reach Consistent+UpToDate purely from the PR #20
// set-gi winner seed — NOT a runtime `primary --force`, and NOT left
// Inconsistent. We assert the winner seed lands the UpToDate flags
// (set-gi with the Consistent+UpToDate generation) and the bring-up
// proceeds (create-md + adjust fire), proving the gate does not leave a
// stamped-true fresh deploy stuck.
func TestApplyElectsWinnerOnceSkipInitialSyncStampedTrue(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-skipgate_00000",
		storage.FakeResponse{Stdout: []byte("")})
	// VolumeStatus reports the LV path after CreateVolume so the
	// reconciler picks up the device for drbdmeta seeding.
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings --separator | -o lv_path,lv_size --units k --nosuffix vg/pvc-skipgate_00000",
		storage.FakeResponse{Stdout: []byte("/dev/vg/pvc-skipgate_00000|1048576\n")})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		StateDir:  dir,
		NodeName:  "n1",
	})

	// Winner election: auto-primary on this node, no peer holding data,
	// SkipInitialSync stamped true → case-B winner UpToDate seed. The
	// dispatcher stamps the `auto-primary` DrbdOptions key on exactly the
	// elected lowest-diskful-node-id replica (isInitialUpToDateWinner).
	dr := skipInitGateFixtureDR("pvc-skipgate", skipInitTrue())
	dr.DrbdOptions["auto-primary"] = "true"

	results, err := rec.Apply(t.Context(), []*intent.DesiredResource{dr})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if len(results) != 1 || !results[0].Ok {
		t.Fatalf("expected Ok=true once SkipInitialSync=true; got results=%+v", results)
	}

	calls := fx.CommandLines()

	// The fresh diskful first-activation must now run create-md + adjust
	// (the gate no longer defers it) ...
	if indexOfPrefix(calls, "drbdadm create-md --force --max-peers=") < 0 {
		t.Errorf("create-md must fire once SkipInitialSync is stamped; got %v", calls)
	}

	if indexOfPrefix(calls, "drbdadm adjust pvc-skipgate") < 0 {
		t.Errorf("adjust must fire once SkipInitialSync is stamped; got %v", calls)
	}

	// ... and the elected winner must be seeded UpToDate from metadata
	// (PR #20 set-gi winner seed), so the resource converges without a
	// peer to SyncTarget from. The winner seed encodes the Consistent +
	// UpToDate flags as the `:1:1` suffix of the GI string drbdmeta
	// set-gi receives (gi.go GISeed.String: <current>:<bitmap>:0:0:1:1);
	// a non-winner / left-Inconsistent shape ends `:0:0`. Assert at least
	// one set-gi for vol 0 carries the UpToDate-flag suffix — proving the
	// gate did not strand the fresh winner Inconsistent.
	var sawWinnerSeed bool

	for _, line := range calls {
		if strings.HasPrefix(line, "drbdmeta --force pvc-skipgate/0 v09 ") &&
			strings.Contains(line, "set-gi") &&
			strings.HasSuffix(strings.TrimSpace(line), ":1:1") {
			sawWinnerSeed = true

			break
		}
	}

	if !sawWinnerSeed {
		t.Errorf("expected a drbdmeta set-gi UpToDate winner seed (`:1:1` suffix) once SkipInitialSync=true; got %v", calls)
	}
}
