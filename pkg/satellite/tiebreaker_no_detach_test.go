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
	"slices"
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/satellite"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
)

// A diskless replica whose kernel slot is loaded but shows no attached
// volume, as a TieBreaker's slot did after its kernel resource was
// taken down by hand, has no disk to detach. The apply detached whenever
// it found no volume reporting Diskless, so it ran `drbdadm detach
// --force` on a resource configured without a disk, drbdadm refused,
// and the apply failed with an ApplyFailed Warning on a witness that
// never had a disk. LINSTOR detaches only a volume that has one. The
// same refusal followed a disk probe that failed because the slot was
// going away, which fell through to the detach. A witness migrated
// from LINSTOR names the diskless pool on its volumes, so a pool name
// alone cannot be what tells a disk apart.
func TestDisklessReplicaWithoutAnAttachedDiskIsNotDetached(t *testing.T) {
	// drbdsetup exits 10 with this on the verbose status when the slot
	// went away between the two probes, as it does while the kernel
	// resource is being taken down.
	slotGone := storage.FakeResponse{
		Stdout: []byte("No such resource: pvc-tb\n"),
		Err:    storage.FakeExitError{Code: 10},
	}
	// A probe failure that says nothing about the slot.
	probeFails := storage.FakeResponse{Err: storage.FakeExitError{Code: 20}}

	for _, tc := range []struct {
		name    string
		pool    string
		verbose storage.FakeResponse
	}{
		{"no volume yet", "", storage.FakeResponse{Stdout: []byte("pvc-tb node-id:2 role:Secondary suspended:no\n" +
			"  n1 node-id:0 connection:Connecting role:Unknown\n" +
			"    volume:0 replication:Off peer-disk:UpToDate\n")}},
		{"diskless volume", "", storage.FakeResponse{Stdout: []byte("pvc-tb node-id:2 role:Secondary\n" +
			"  volume:0 minor:1000 disk:Diskless client:yes quorum:yes\n" +
			"  n1 node-id:0 connection:Connected role:Secondary\n" +
			"    volume:0 replication:Established peer-disk:UpToDate\n")}},
		{"slot gone before the disk probe", "", slotGone},
		{"migrated witness, slot gone before the disk probe", "DfltDisklessStorPool", slotGone},
		{"disk probe fails", "", probeFails},
		{"migrated witness, disk probe fails", "DfltDisklessStorPool", probeFails},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fx := storage.NewFakeExec()
			fx.Expect("drbdsetup status pvc-tb", storage.FakeResponse{Stdout: []byte("pvc-tb role:Secondary\n")})
			fx.Expect("drbdsetup status --verbose pvc-tb", tc.verbose)

			rec := satellite.NewReconciler(satellite.ReconcilerConfig{
				Adm:      drbd.NewAdm(fx),
				StateDir: t.TempDir(),
				NodeName: "n3",
			})

			dr := &intent.DesiredResource{
				Name:     "pvc-tb",
				NodeName: "n3",
				Flags:    []string{"DISKLESS", "TIE_BREAKER"},
				Volumes:  []*intent.DesiredVolume{{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: tc.pool}},
				DrbdOptions: map[string]string{
					"port": "7000", "node-id": "2", "address": "10.0.0.3", "minor": "1000",
				},
			}

			_, err := rec.Apply(t.Context(), []*intent.DesiredResource{dr})
			if err != nil {
				t.Fatalf("Apply: %v", err)
			}

			if i := slices.IndexFunc(fx.CommandLines(), func(cmd string) bool {
				return strings.HasPrefix(cmd, "drbdadm detach")
			}); i >= 0 {
				t.Errorf("detached a replica with no disk attached: %q; commands:\n%v", fx.CommandLines()[i], fx.CommandLines())
			}
		})
	}
}

// A replica toggled from diskful still detaches when the disk probe
// fails for a reason that says nothing about the slot: its
// configuration may still name the disk, and the backing volume is
// reclaimed right after, so skipping the detach could pull the volume
// out from under a disk the kernel still holds. When drbdsetup instead
// says the slot is gone, the kernel holds no disk to detach, and the
// volume is reclaimed without one.
func TestToggledReplicaDetachesWhenTheDiskProbeFails(t *testing.T) {
	for _, tc := range []struct {
		name       string
		verbose    storage.FakeResponse
		wantDetach bool
	}{
		{"probe fails", storage.FakeResponse{Err: storage.FakeExitError{Code: 20}}, true},
		{"slot gone", storage.FakeResponse{
			Stdout: []byte("No such resource: pvc-td\n"),
			Err:    storage.FakeExitError{Code: 10},
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fx := storage.NewFakeExec()
			fx.Expect("drbdsetup status pvc-td", storage.FakeResponse{Stdout: []byte("pvc-td role:Secondary\n")})
			fx.Expect("drbdsetup status --verbose pvc-td", tc.verbose)

			prov := &disklessCleanupFakeProvider{}
			rec := satellite.NewReconciler(satellite.ReconcilerConfig{
				Providers: map[string]storage.Provider{"thin1": prov},
				Adm:       drbd.NewAdm(fx),
				StateDir:  t.TempDir(),
				NodeName:  "n1",
			})

			dr := &intent.DesiredResource{
				Name:     "pvc-td",
				NodeName: "n1",
				Flags:    []string{"DISKLESS"},
				Volumes:  []*intent.DesiredVolume{{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"}},
				DrbdOptions: map[string]string{
					"port": "7000", "node-id": "0", "address": "10.0.0.1", "minor": "1000",
				},
			}

			results, err := rec.Apply(t.Context(), []*intent.DesiredResource{dr})
			if err != nil {
				t.Fatalf("Apply: %v", err)
			}

			if len(results) != 1 || !results[0].GetOk() {
				t.Fatalf("apply result not ok: %+v", results)
			}

			if got := slices.Contains(fx.CommandLines(), "drbdadm detach --force pvc-td"); got != tc.wantDetach {
				t.Errorf("detach issued = %v, want %v; commands:\n%v", got, tc.wantDetach, fx.CommandLines())
			}

			prov.mu.Lock()
			deletes := prov.deleteCalls
			prov.mu.Unlock()

			if deletes != 1 {
				t.Errorf("DeleteVolume called %d times, want 1: the toggled volume must still be reclaimed", deletes)
			}
		})
	}
}
