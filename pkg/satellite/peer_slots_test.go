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
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/satellite"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/lvm"
)

// applyFreshReplica applies a new diskful replica with the given peer
// slots and returns the commands it ran.
func applyFreshReplica(t *testing.T, peerSlots *int32) []string {
	t.Helper()

	fx := storage.NewFakeExec()
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-slots_00000",
		storage.FakeResponse{Stdout: []byte("")})

	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"vg": lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)},
		Adm:       drbd.NewAdm(fx),
		StateDir:  t.TempDir(),
		NodeName:  "n1",
	})

	_, err := rec.Apply(t.Context(), []*intent.DesiredResource{{
		Name:            "pvc-slots",
		NodeName:        "n1",
		Volumes:         []*intent.DesiredVolume{{VolumeNumber: 0, SizeKib: 20 * 1024 * 1024, StoragePool: "vg"}},
		Peers:           []intent.DesiredPeer{{Name: "n2"}},
		SkipInitialSync: skipInitTrue(),
		PeerSlots:       peerSlots,
		DrbdOptions: map[string]string{
			"port": "7000", "node-id": "0", "address": "10.0.0.1", "minor": "1000",
			"peer.n2.address": "10.0.0.2", "peer.n2.node-id": "1", "peer.n2.port": "7000",
		},
	}})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	return fx.CommandLines()
}

// A replica's metadata is created with its own peer slots, which LINSTOR
// copies from the definition (`create-md --max-peers` = peerSlots), and its
// lower disk is sized for them: 20 GiB of data with LINSTOR's seven slots
// is 20976040 KiB, 20485 MiB once rounded up to the provider's unit.
func TestANewReplicaIsCreatedWithItsPeerSlots(t *testing.T) {
	seven := int32(drbd.DefaultPeerSlots)
	calls := applyFreshReplica(t, &seven)

	if !slices.Contains(calls, "drbdadm create-md --force --max-peers=7 pvc-slots/0") {
		t.Errorf("want create-md with 7 peer slots; cmds:\n%s", strings.Join(calls, "\n"))
	}

	if !slices.ContainsFunc(calls, func(c string) bool {
		return strings.HasPrefix(c, "lvcreate ") && strings.Contains(c, "--virtualsize 20485MiB")
	}) {
		t.Errorf("want a 20485 MiB lower disk for 7 peer slots; cmds:\n%s", strings.Join(calls, "\n"))
	}
}

// A replica blockstor created before it recorded a count was created with
// 15 peer slots, and keeps getting them: nothing already deployed changes.
func TestAReplicaWithoutARecordedCountKeepsFifteenPeerSlots(t *testing.T) {
	calls := applyFreshReplica(t, nil)

	if !slices.Contains(calls, fmt.Sprintf("drbdadm create-md --force --max-peers=%d pvc-slots/0", drbd.LegacyPeerSlots)) {
		t.Errorf("want create-md with 15 peer slots; cmds:\n%s", strings.Join(calls, "\n"))
	}

	if !slices.ContainsFunc(calls, func(c string) bool {
		return strings.HasPrefix(c, "lvcreate ") && strings.Contains(c, "--virtualsize 20490MiB")
	}) {
		t.Errorf("want the 20490 MiB lower disk 15 peer slots get; cmds:\n%s", strings.Join(calls, "\n"))
	}
}
