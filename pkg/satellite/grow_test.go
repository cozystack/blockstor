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

package satellite

import (
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
)

// TestDrbdGrossKibLeavesTheNetSize: the bitmap covers the whole backing
// device, so the gross size must leave the net size for data once the
// metadata of the gross size itself is taken off, at every size.
func TestDrbdGrossKibLeavesTheNetSize(t *testing.T) {
	for _, net := range []int64{4, 1024, 1 << 20, 1<<20 + 4, 100 << 20, 1 << 30, 16<<30 + 12} {
		gross := drbdGrossKib(net, 15)

		if gross <= net {
			t.Errorf("net %d: gross %d leaves no room for metadata", net, gross)
		}

		if capacity := gross - drbdInternalMetaKib(gross, 15); capacity < net {
			t.Errorf("net %d: gross %d offers %d", net, gross, capacity)
		}
	}
}

// TestDrbdInternalMetaCoversLinstorDefault: the metadata estimate for
// seven peer slots must not be below what LINSTOR reserves for its
// default seven, or a grow of an adopted volume would end short of the
// definition.
func TestDrbdInternalMetaCoversLinstorDefault(t *testing.T) {
	const linstorPeers = 7

	for _, backing := range []int64{1 << 20, 10 << 20, 1 << 30} {
		bits := alignUp(ceilDiv(backing, kibPerBitmapBit), 64)
		linstor := drbdMetaSuperblockKib + drbdMetaActivityLogKib + alignUp(ceilDiv(bits/8*linstorPeers, 1024), 4)

		if got := drbdInternalMetaKib(backing, linstorPeers); got < linstor {
			t.Errorf("backing %d: estimate %d below LINSTOR's %d", backing, got, linstor)
		}
	}
}

// TestGrowBackingKibNeverSizesBelowFifteenPeerSlots: a grow sizes the
// backing of a device that already exists, whose superblock may hold
// more slots than the replica records: a satellite that predates the
// record creates 15 whatever the controller stamped. Sized for the
// record alone, a 7-slot grow on such a disk would leave the device
// short of the definition; sized for 15 it only oversizes the bitmap.
func TestGrowBackingKibNeverSizesBelowFifteenPeerSlots(t *testing.T) {
	const definition = 20 << 20

	fifteen := alignUp(drbdGrossKib(definition, drbd.LegacyPeerSlots), kibPerMib)
	vol := &intent.DesiredVolume{SizeKib: definition}

	seven := int32(7)
	if got := growBackingKib(&intent.DesiredResource{PeerSlots: &seven}, vol, true); got != fifteen {
		t.Errorf("7 recorded peer slots: got %d KiB, want %d, the size for 15", got, fifteen)
	}

	if got := growBackingKib(&intent.DesiredResource{}, vol, true); got != fifteen {
		t.Errorf("no recorded count: got %d KiB, want %d for 15 slots", got, fifteen)
	}

	thirtyOne := int32(31)

	got := growBackingKib(&intent.DesiredResource{PeerSlots: &thirtyOne}, vol, true)
	if want := alignUp(drbdGrossKib(definition, 31), kibPerMib); got != want {
		t.Errorf("31 recorded peer slots: got %d KiB, want %d", got, want)
	}
}
