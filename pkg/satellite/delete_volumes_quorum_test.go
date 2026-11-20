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
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/satellite"
	"github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
)

// vdQuorumStatus is `drbdsetup status pvc-vd --json` on a replica that has
// only volume 0 left, in drbd-utils' layout.
func vdQuorumStatus(suspendedQuorum bool) []byte {
	susp := "false"
	if suspendedQuorum {
		susp = "true"
	}

	return []byte(`[
{
  "name": "pvc-vd",
  "node-id": 2,
  "role": "Secondary",
  "suspended": ` + susp + `,
  "suspended-user": false,
  "suspended-no-data": false,
  "suspended-fencing": false,
  "suspended-quorum": ` + susp + `,
  "force-io-failures": false,
  "write-ordering": "flush",
  "devices": [
    {
      "volume": 0,
      "minor": 20055,
      "disk-state": "UpToDate",
      "client": false,
      "open": false,
      "quorum": true
    }
  ],
  "connections": []
}
]
`)
}

// `vd d` of volume 1 of a three-replica resource with quorum majority and
// on-no-quorum suspend-io. The replicas drop the volume one after another;
// del-minor turns replication to every peer off before it unregisters the
// device, so the volume ends with no peer and DRBD suspends the resource on
// quorum. A peer's later state change clears that on the replicas that went
// first. The one that goes last sees none, and was left suspended with
// volume 0 reporting quorum:yes and both peers connected: on a Primary the
// I/O of every remaining volume hangs.
//
// The replica that removes the volume must resume that suspension itself,
// once the volume is out of DRBD, whether it removed the minor here or the
// apply's `drbdadm adjust` already had; one whose removal did not leave the
// resource suspended runs no resume-io.
func TestDeleteVolumesResumesAQuorumSuspensionTheRemovedVolumeLeft(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name       string
		adjusted   bool
		suspended  bool
		wantResume bool
	}{
		{name: "last replica to remove the volume", suspended: true, wantResume: true},
		{name: "adjust removed the volume already", adjusted: true, suspended: true, wantResume: true},
		{name: "replica the removal left running", suspended: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			verbose := "pvc-vd node-id:2 role:Secondary suspended:no\n" +
				"  volume:0 minor:20055 disk:UpToDate open:no\n"
			if !tc.adjusted {
				verbose += "  volume:1 minor:20056 disk:UpToDate open:no\n"
			}

			fx := storage.NewFakeExec()
			fx.Expect("drbdsetup status --verbose", storage.FakeResponse{Stdout: []byte(verbose)})
			fx.Expect("drbdsetup status pvc-vd --json", storage.FakeResponse{Stdout: vdQuorumStatus(tc.suspended)})

			rec := satellite.NewReconciler(satellite.ReconcilerConfig{
				Adm:      drbd.NewAdm(fx),
				StateDir: t.TempDir(),
				NodeName: "n3",
			})

			minor := int32(20056)

			err := rec.DeleteVolumes(t.Context(), &intent.DeleteVolumesRequest{
				Name:    "pvc-vd",
				Volumes: []intent.VolumeToDelete{{VolumeNumber: 1, Minor: &minor}},
			})
			if err != nil {
				t.Fatalf("DeleteVolumes: %v", err)
			}

			var got []string

			for _, line := range fx.CommandLines() {
				if line != "drbdsetup status --verbose" {
					got = append(got, line)
				}
			}

			var want []string
			if !tc.adjusted {
				want = append(want, "drbdsetup detach 20056", "drbdsetup del-minor 20056")
			}

			want = append(want, "drbdsetup status pvc-vd --json")
			if tc.wantResume {
				want = append(want, "drbdsetup resume-io 20055")
			}

			if !slices.Equal(got, want) {
				t.Errorf("commands %q, want %q", got, want)
			}
		})
	}
}
