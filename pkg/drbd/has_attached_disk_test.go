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

package drbd_test

import (
	"errors"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/storage"
)

// errStatusFailed is a drbdsetup status that exits non-zero.
var errStatusFailed = errors.New("exit status 10")

// HasAttachedDisk reads the local volumes' disk states only: a peer's
// disk state is not this node's, and a slot without a volume has no
// disk, which is what tells it from an attached one.
func TestHasAttachedDisk(t *testing.T) {
	t.Parallel()

	peer := "  n1 node-id:0 connection:Connected role:Secondary\n" +
		"    volume:0 replication:Established peer-disk:UpToDate\n"

	for _, tc := range []struct {
		name, out string
		want      bool
	}{
		{"attached", "r node-id:1 role:Secondary\n  volume:0 minor:1000 disk:UpToDate client:no\n" + peer, true},
		{"attaching", "r node-id:1 role:Secondary\n  volume:0 minor:1000 disk:Attaching client:no\n" + peer, true},
		{"one of two attached", "r node-id:1 role:Secondary\n  volume:0 minor:1000 disk:Diskless\n" +
			"  volume:1 minor:1001 disk:Inconsistent\n" + peer, true},
		{"diskless", "r node-id:1 role:Secondary\n  volume:0 minor:1000 disk:Diskless client:yes\n" + peer, false},
		{"detaching", "r node-id:1 role:Secondary\n  volume:0 minor:1000 disk:Detaching\n" + peer, false},
		{"failed", "r node-id:1 role:Secondary\n  volume:0 minor:1000 disk:Failed\n" + peer, false},
		{"no volume", "r node-id:1 role:Secondary\n" + peer, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			fx := storage.NewFakeExec()
			fx.Expect("drbdsetup status --verbose r", storage.FakeResponse{Stdout: []byte(tc.out)})

			got, err := drbd.NewAdm(fx).HasAttachedDisk(t.Context(), "r")
			if err != nil {
				t.Fatalf("HasAttachedDisk: %v", err)
			}

			if got != tc.want {
				t.Errorf("HasAttachedDisk = %v, want %v for\n%s", got, tc.want, tc.out)
			}
		})
	}

	t.Run("drbdsetup output", func(t *testing.T) {
		t.Parallel()

		for _, tc := range []struct {
			name, out string
			want      bool
		}{
			{"diskful with diskful peers", verboseDiskful, true},
			{"diskless witness", verboseWitness, false},
			{"diskful beside a diskless peer", verboseBesideWitness, true},
			{"two volumes, one detached", verboseOneDetached, true},
			{"two volumes, both diskless", verboseTwoDiskless, false},
			{"no volume yet", verboseNoVolume, false},
		} {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				fx := storage.NewFakeExec()
				fx.Expect("drbdsetup status --verbose pvc-a", storage.FakeResponse{Stdout: []byte(tc.out)})

				got, err := drbd.NewAdm(fx).HasAttachedDisk(t.Context(), "pvc-a")
				if err != nil {
					t.Fatalf("HasAttachedDisk: %v", err)
				}

				if got != tc.want {
					t.Errorf("HasAttachedDisk = %v, want %v for\n%s", got, tc.want, tc.out)
				}
			})
		}
	})

	t.Run("probe fails", func(t *testing.T) {
		t.Parallel()

		fx := storage.NewFakeExec()
		fx.Expect("drbdsetup status --verbose r", storage.FakeResponse{Err: errStatusFailed})

		if _, err := drbd.NewAdm(fx).HasAttachedDisk(t.Context(), "r"); err == nil {
			t.Error("a failed probe reported a disk state, want the error")
		}
	})

	// drbdsetup exits 10 with "No such resource" for a slot taken down
	// between the caller's load probe and this one. That is an answer,
	// not a failure: the kernel holds no disk for a resource it does
	// not hold at all.
	t.Run("slot not loaded", func(t *testing.T) {
		t.Parallel()

		fx := storage.NewFakeExec()
		fx.Expect("drbdsetup status --verbose r", storage.FakeResponse{
			Stdout: []byte("No such resource: r\n"),
			Err:    storage.FakeExitError{Code: 10},
		})

		attached, err := drbd.NewAdm(fx).HasAttachedDisk(t.Context(), "r")
		if err != nil {
			t.Fatalf("HasAttachedDisk on a slot that is not loaded: %v", err)
		}

		if attached {
			t.Error("a slot that is not loaded reported an attached disk")
		}
	})
}

// The fixtures below are `drbdsetup status --verbose <resource>` as
// drbd-utils 9.36 prints it for DRBD 9.3: verboseDiskful is a capture
// from a three-node cluster with the names replaced; the others vary
// it the way device_status and peer_device_status in drbdsetup.c print
// a diskless volume (`disk:Diskless client:yes`, `backing_dev:none`), a
// diskless peer (`peer-disk:Diskless peer-client:yes`), a second volume,
// and a slot with no volume yet.
const verboseDiskful = `pvc-a node-id:2 role:Secondary suspended:no
    force-io-failures:no
  volume:0 minor:1005 disk:UpToDate
      backing_dev:/dev/zvol/data/pvc-a_00000
      quorum:yes open:no blocked:no
  n1 node-id:1 connection:Connected role:Secondary tls:no congested:no
      ap-in-flight:0 rs-in-flight:0
    volume:0 replication:Established peer-disk:UpToDate resync-suspended:no
  n3 node-id:0 connection:Connected role:Secondary tls:no congested:no
      ap-in-flight:0 rs-in-flight:0
    volume:0 replication:Established peer-disk:UpToDate resync-suspended:no
`

const verboseWitness = `pvc-a node-id:2 role:Secondary suspended:no
    force-io-failures:no
  volume:0 minor:1005 disk:Diskless client:yes
      backing_dev:none
      quorum:yes open:no blocked:no
  n1 node-id:1 connection:Connected role:Secondary tls:no congested:no
      ap-in-flight:0 rs-in-flight:0
    volume:0 replication:Established peer-disk:UpToDate resync-suspended:no
  n3 node-id:0 connection:Connected role:Secondary tls:no congested:no
      ap-in-flight:0 rs-in-flight:0
    volume:0 replication:Established peer-disk:UpToDate resync-suspended:no
`

const verboseBesideWitness = `pvc-a node-id:2 role:Secondary suspended:no
    force-io-failures:no
  volume:0 minor:1005 disk:UpToDate
      backing_dev:/dev/zvol/data/pvc-a_00000
      quorum:yes open:no blocked:no
  n1 node-id:1 connection:Connected role:Secondary tls:no congested:no
      ap-in-flight:0 rs-in-flight:0
    volume:0 replication:Established peer-disk:Diskless peer-client:yes resync-suspended:no
  n3 node-id:0 connection:Connected role:Secondary tls:no congested:no
      ap-in-flight:0 rs-in-flight:0
    volume:0 replication:Established peer-disk:UpToDate resync-suspended:no
`

const verboseOneDetached = `pvc-a node-id:2 role:Secondary suspended:no
    force-io-failures:no
  volume:0 minor:1005 disk:Diskless client:no
      backing_dev:none
      quorum:yes open:no blocked:no
  volume:1 minor:1006 disk:UpToDate
      backing_dev:/dev/zvol/data/pvc-a_00001
      quorum:yes open:no blocked:no
  n1 node-id:1 connection:Connected role:Secondary tls:no congested:no
      ap-in-flight:0 rs-in-flight:0
    volume:0 replication:Established peer-disk:UpToDate resync-suspended:no
    volume:1 replication:Established peer-disk:UpToDate resync-suspended:no
`

const verboseTwoDiskless = `pvc-a node-id:2 role:Secondary suspended:no
    force-io-failures:no
  volume:0 minor:1005 disk:Diskless client:yes
      backing_dev:none
      quorum:yes open:no blocked:no
  volume:1 minor:1006 disk:Diskless client:yes
      backing_dev:none
      quorum:yes open:no blocked:no
  n1 node-id:1 connection:Connected role:Secondary tls:no congested:no
      ap-in-flight:0 rs-in-flight:0
    volume:0 replication:Established peer-disk:UpToDate resync-suspended:no
    volume:1 replication:Established peer-disk:UpToDate resync-suspended:no
`

const verboseNoVolume = `pvc-a node-id:2 role:Secondary suspended:no
    force-io-failures:no
  n1 node-id:1 connection:Connecting role:Unknown tls:no congested:no
      ap-in-flight:0 rs-in-flight:0
  n3 node-id:0 connection:Connecting role:Unknown tls:no congested:no
      ap-in-flight:0 rs-in-flight:0
`
