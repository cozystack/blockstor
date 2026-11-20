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
	"slices"
	"strings"
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

// vdQuorumShow is `drbdsetup show pvc-vd --json` for a resource with quorum
// majority, the given on-no-quorum policy, and two peers with the given
// fencing policy ("" leaves it at its default, which show leaves out).
func vdQuorumShow(onNoQuorum, fencing string) []byte {
	net := ""
	if fencing != "" {
		net = `, "net": {"fencing": "` + fencing + `"}`
	}

	return []byte(`[{"resource": "pvc-vd",
  "options": {"quorum": "majority", "on-no-quorum": "` + onNoQuorum + `"},
  "_this_host": {"node-id": 2, "volumes": []},
  "connections": [{"_peer_node_id": 0` + net + `}, {"_peer_node_id": 1` + net + `}]}]`)
}

var errOptionsRefused = errors.New("state change failed")

const (
	vdToIOError   = "drbdsetup resource-options pvc-vd --on-no-quorum=io-error"
	vdToSuspendIO = "drbdsetup resource-options pvc-vd --on-no-quorum=suspend-io"
)

// `vd d` of volume 1 of a three-replica resource with quorum majority.
// del-minor turns replication to every peer off before it unregisters the
// device, so the volume ends with no peer and no quorum, and DRBD's verdict
// for the resource is computed with it and never again once it is gone.
// With on-no-quorum suspend-io the replica that removed it last was left
// suspended with volume 0 reporting quorum:yes and both peers connected;
// with io-error a Primary's requests on volume 0 would fail with EIO while
// nothing shows suspended.
//
// The satellite removes the volume itself under io-error and sets
// suspend-io back afterwards, so the resource is never suspended. A volume
// `drbdadm adjust` removed already gets its verdict recomputed by switching
// on-no-quorum to the other policy and back, which lifts no other
// suspension and so runs on a resource that suspends on no data or fences
// peers too, as the replicated StorageClasses of a real cluster do.
func TestDeleteVolumesRechecksTheQuorumTheRemovedVolumeLeft(t *testing.T) {
	t.Parallel()

	const (
		detach   = "drbdsetup detach 20056"
		delMinor = "drbdsetup del-minor 20056"
	)

	for _, tc := range []struct {
		name       string
		adjusted   bool
		suspended  bool
		onNoQuorum string
		fencing    string
		want       []string
	}{
		{
			name: "the satellite removes the volume", onNoQuorum: "suspend-io",
			want: []string{vdToIOError, detach, delMinor, vdToSuspendIO},
		},
		{
			name: "adjust removed the volume and left it suspended", adjusted: true, suspended: true, onNoQuorum: "suspend-io",
			want: []string{vdToIOError, vdToSuspendIO},
		},
		{name: "adjust removed the volume, nothing suspended", adjusted: true, onNoQuorum: "suspend-io"},
		{
			name: "io-error, the satellite removes the volume", onNoQuorum: "io-error",
			want: []string{detach, delMinor, vdToSuspendIO, vdToIOError},
		},
		{
			name: "io-error, adjust removed the volume", adjusted: true, onNoQuorum: "io-error",
			want: []string{vdToSuspendIO, vdToIOError},
		},
		{
			name: "a resource that fences peers", adjusted: true, suspended: true, onNoQuorum: "suspend-io", fencing: "resource-and-stonith",
			want: []string{vdToIOError, vdToSuspendIO},
		},
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
			fx.Expect("drbdsetup show pvc-vd --json", storage.FakeResponse{Stdout: vdQuorumShow(tc.onNoQuorum, tc.fencing)})

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
				if !strings.HasPrefix(line, "drbdsetup status ") && !strings.HasPrefix(line, "drbdsetup show ") {
					got = append(got, line)
				}
			}

			if !slices.Equal(got, tc.want) {
				t.Errorf("commands %q, want %q", got, tc.want)
			}

			if slices.Contains(fx.CommandLines(), "drbdsetup resume-io 20055") {
				t.Error("resume-io run; it lifts every other suspension too")
			}
		})
	}
}

// A recheck that fails stops the delete before the storage, so the replica
// does not confirm and the recheck is retried; confirming would leave the
// remaining volumes held for good.
func TestDeleteVolumesStopsWhenTheRecheckFails(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	fx.Expect("drbdsetup status --verbose", storage.FakeResponse{Stdout: []byte("pvc-vd node-id:2 role:Secondary suspended:quorum\n" +
		"  volume:0 minor:20055 disk:UpToDate open:no\n")})
	fx.Expect("drbdsetup status pvc-vd --json", storage.FakeResponse{Stdout: vdQuorumStatus(true)})
	fx.Expect("drbdsetup show pvc-vd --json", storage.FakeResponse{Stdout: vdQuorumShow("suspend-io", "")})
	fx.Expect(vdToIOError, storage.FakeResponse{Err: errOptionsRefused})

	rec := satellite.NewReconciler(satellite.ReconcilerConfig{Adm: drbd.NewAdm(fx), StateDir: t.TempDir(), NodeName: "n3"})

	minor := int32(20056)

	err := rec.DeleteVolumes(t.Context(), &intent.DeleteVolumesRequest{
		Name:    "pvc-vd",
		Volumes: []intent.VolumeToDelete{{VolumeNumber: 1, Minor: &minor, StoragePool: "unregistered"}},
	})
	if err == nil || !strings.Contains(err.Error(), "recheck the quorum of pvc-vd") {
		t.Fatalf("DeleteVolumes: %v, want the recheck failure", err)
	}

	if strings.Contains(err.Error(), "not registered") {
		t.Errorf("DeleteVolumes reached the storage after the recheck failed: %v", err)
	}
}

// A volume with no DRBD minor, on a definition without DRBD, has nothing
// to recheck.
func TestDeleteVolumesWithoutAMinorReadsNoQuorum(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{Adm: drbd.NewAdm(fx), StateDir: t.TempDir(), NodeName: "n3"})

	err := rec.DeleteVolumes(t.Context(), &intent.DeleteVolumesRequest{
		Name:    "pvc-vd",
		Volumes: []intent.VolumeToDelete{{VolumeNumber: 1}},
	})
	if err != nil {
		t.Fatalf("DeleteVolumes: %v", err)
	}

	if lines := fx.CommandLines(); len(lines) != 0 {
		t.Errorf("commands %q, want none", lines)
	}
}

// A minor the kernel now gives to another resource is not this volume's
// any more: it is neither removed nor wrapped in an on-no-quorum change.
func TestDeleteVolumesLeavesAMinorAnotherResourceHolds(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	fx.Expect("drbdsetup status --verbose", storage.FakeResponse{Stdout: []byte("pvc-other node-id:1 role:Secondary suspended:no\n" +
		"  volume:0 minor:20056 disk:UpToDate open:no\n")})
	fx.Expect("drbdsetup status pvc-vd --json", storage.FakeResponse{Stdout: vdQuorumStatus(false)})
	fx.Expect("drbdsetup show pvc-vd --json", storage.FakeResponse{Stdout: vdQuorumShow("suspend-io", "")})

	rec := satellite.NewReconciler(satellite.ReconcilerConfig{Adm: drbd.NewAdm(fx), StateDir: t.TempDir(), NodeName: "n3"})

	minor := int32(20056)

	err := rec.DeleteVolumes(t.Context(), &intent.DeleteVolumesRequest{
		Name:    "pvc-vd",
		Volumes: []intent.VolumeToDelete{{VolumeNumber: 1, Minor: &minor}},
	})
	if err != nil {
		t.Fatalf("DeleteVolumes: %v", err)
	}

	for _, line := range fx.CommandLines() {
		if !strings.HasPrefix(line, "drbdsetup status ") && !strings.HasPrefix(line, "drbdsetup show ") {
			t.Errorf("%q run on a minor pvc-other holds", line)
		}
	}
}
