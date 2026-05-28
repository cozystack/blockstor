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
	"slices"
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/satellite"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/lvm"
)

// TestApplyRefusesCreateMdWhenLocalNodeIDUnresolved is the Bug 360
// prevention pin. When the dispatcher OMITS the local `node-id` from
// DrbdOptions (controller has not yet allocated Status.DRBDNodeID),
// Apply MUST refuse the first-activation: no .res render, no
// `drbdadm create-md`, no `drbdadm up/adjust`. Letting create-md run
// with a zero-defaulted id permanently burns `node-id 0` into the
// on-disk v09 metadata, after which `drbdsetup attach` fails
// `(119) ambiguous node id` forever and the replica is stuck
// Diskless. The result is marked not-Ok so the controller's runApply
// requeues until the id is allocated.
//
// Storage provisioning is allowed to run (idempotent); only the DRBD
// burn is deferred — hence we assert specifically that no .res file
// exists and no create-md/up command was issued.
func TestApplyRefusesCreateMdWhenLocalNodeIDUnresolved(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-b360_00000",
		storage.FakeResponse{Stdout: []byte("")})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		StateDir:  dir,
		NodeName:  "n1",
	})

	results, err := rec.Apply(t.Context(), []*intent.DesiredResource{
		{
			Name:     "pvc-b360",
			NodeName: "n1",
			Volumes: []*intent.DesiredVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
			},
			Peers: []intent.DesiredPeer{{Name: "n2"}},
			// node-id deliberately ABSENT — the unresolved-allocation
			// signal the dispatcher emits during the auto-place burst.
			SkipInitialSync: skipInitTrue(),
			DrbdOptions: map[string]string{
				"port":            "7000",
				"address":         "10.0.0.1",
				"minor":           "1000",
				"peer.n2.address": "10.0.0.2",
				"peer.n2.node-id": "1",
				"peer.n2.port":    "7000",
			},
		},
	})
	if err != nil {
		t.Fatalf("Apply returned hard error (want per-resource not-Ok, not a fatal): %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("Apply results: got %d, want 1", len(results))
	}

	if results[0].Ok {
		t.Errorf("result Ok=true; want false (apply must be deferred until node-id is allocated)")
	}

	// No .res file may exist — buildResFile must never have rendered
	// the ambiguous node-id 0 onto disk.
	resPath := filepath.Join(dir, "pvc-b360.res")
	if _, statErr := os.Stat(resPath); statErr == nil {
		body, _ := os.ReadFile(resPath)
		t.Errorf(".res must NOT be rendered while local node-id is unresolved; got:\n%s", string(body))
	}

	// No DRBD bring-up verb may have fired.
	for _, line := range fx.CommandLines() {
		if strings.Contains(line, "create-md") ||
			strings.Contains(line, "drbdadm up") ||
			strings.Contains(line, "drbdadm adjust") ||
			strings.Contains(line, "new-resource") {
			t.Errorf("no DRBD bring-up verb may fire with unresolved node-id; got %q in:\n%v",
				line, fx.CommandLines())
		}
	}
}

// TestApplyProceedsWhenLocalNodeIDIsZero pins the counterpart: a
// PRESENT node-id "0" (a legitimate LowestFreeNodeID allocation) must
// NOT be refused — the .res is rendered and create-md runs. The
// Bug 360 gate keys on absence, never on the value 0.
func TestApplyProceedsWhenLocalNodeIDIsZero(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-b360z_00000",
		storage.FakeResponse{Stdout: []byte("")})

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		StateDir:  dir,
		NodeName:  "n1",
	})

	results, err := rec.Apply(t.Context(), []*intent.DesiredResource{
		{
			Name:     "pvc-b360z",
			NodeName: "n1",
			Volumes: []*intent.DesiredVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
			},
			Peers:           []intent.DesiredPeer{{Name: "n2"}},
			SkipInitialSync: skipInitTrue(),
			DrbdOptions: map[string]string{
				"port":            "7000",
				"node-id":         "0",
				"address":         "10.0.0.1",
				"minor":           "1000",
				"peer.n2.address": "10.0.0.2",
				"peer.n2.node-id": "1",
				"peer.n2.port":    "7000",
			},
		},
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if len(results) != 1 || !results[0].Ok {
		t.Fatalf("legitimate node-id 0 must apply; results=%+v", results)
	}

	resPath := filepath.Join(dir, "pvc-b360z.res")
	if _, statErr := os.Stat(resPath); statErr != nil {
		t.Errorf(".res must be rendered for a legitimate node-id 0: %v", statErr)
	}

	if !slices.Contains(fx.CommandLines(), "drbdadm create-md --force pvc-b360z") &&
		!slices.ContainsFunc(fx.CommandLines(), func(s string) bool {
			return strings.Contains(s, "create-md") && strings.Contains(s, "pvc-b360z")
		}) {
		t.Errorf("create-md must fire for a legitimate node-id 0; got %v", fx.CommandLines())
	}
}
