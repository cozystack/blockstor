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
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
)

const (
	gib20KiB         = 20 * 1024 * 1024
	zvolBlock16KiB   = 16
	sectorsPerKiB    = 2
	linstorPeerSlots = 7
)

func roundUp(kib, unit int64) int64 {
	return (kib + unit - 1) / unit * unit
}

// TestInternalMetadataSizesMatchAdoptedLinstorVolume pins the sizes a
// 20 GiB volume LINSTOR v1.33.1 created on a ZFS pool with a 16 KiB
// volblocksize was measured with: a zvol of 21479473152 bytes (41952096
// sectors) and a DRBD device of 41943056 sectors.
func TestInternalMetadataSizesMatchAdoptedLinstorVolume(t *testing.T) {
	t.Parallel()

	geometry := drbd.DefaultGeometry().WithMaxPeers(linstorPeerSlots)

	gross := drbd.InternalMetadataGrossKiB(gib20KiB, geometry)
	if gross != 20976040 {
		t.Fatalf("gross for 20 GiB with 7 peer slots: got %d KiB, want 20976040", gross)
	}

	zvol := roundUp(gross, zvolBlock16KiB)
	if zvol*sectorsPerKiB != 41952096 || zvol*1024 != 21479473152 {
		t.Fatalf("zvol: got %d sectors (%d bytes), want 41952096 (21479473152)", zvol*sectorsPerKiB, zvol*1024)
	}

	net := drbd.InternalMetadataNetKiB(zvol, geometry)
	if net*sectorsPerKiB != 41943056 {
		t.Fatalf("DRBD device on the zvol: got %d sectors, want 41943056", net*sectorsPerKiB)
	}
}

// TestInternalMetadataNetMatchesKernelOnBareVolume pins the data area the
// kernel reported for metadata with blockstor's 15 peer slots on a lower
// disk of exactly 20 GiB: "cannot attach smaller (41923768) disk".
func TestInternalMetadataNetMatchesKernelOnBareVolume(t *testing.T) {
	t.Parallel()

	net := drbd.InternalMetadataNetKiB(gib20KiB, drbd.DefaultGeometry())
	if net*sectorsPerKiB != 41923768 {
		t.Fatalf("data area of a 20 GiB disk with 15 peer slots: got %d sectors, want 41923768",
			net*sectorsPerKiB)
	}
}

// TestInternalMetadataGrossHoldsItsNet checks that the lower disk sized
// for a data area holds it, for every peer-slot count and across sizes
// that straddle bitmap and alignment boundaries.
func TestInternalMetadataGrossHoldsItsNet(t *testing.T) {
	t.Parallel()

	sizes := []int64{4, 64, 1000, 1024, 4093, 32768, 1 << 20, 1<<20 + 4, gib20KiB, gib20KiB + 8, 1 << 30, 3<<30 + 12}

	for peers := 1; peers <= drbd.MetadataMaxPeerSlots; peers++ {
		geometry := drbd.DefaultGeometry().WithMaxPeers(peers)

		for _, net := range sizes {
			gross := drbd.InternalMetadataGrossKiB(net, geometry)

			got := drbd.InternalMetadataNetKiB(gross, geometry)
			if got < net {
				t.Fatalf("peers %d, net %d: gross %d holds only %d", peers, net, gross, got)
			}
		}
	}
}

// TestInternalMetadataActivityLog checks that a larger activity log
// grows the metadata by its size.
func TestInternalMetadataActivityLog(t *testing.T) {
	t.Parallel()

	base := drbd.DefaultGeometry()
	wide := base
	wide.ALStripes = 4

	grown := drbd.InternalMetadataGrossKiB(gib20KiB, wide) - drbd.InternalMetadataGrossKiB(gib20KiB, base)
	if grown != 3*32 {
		t.Fatalf("four 32 KiB stripes instead of one: metadata grew by %d KiB, want 96", grown)
	}
}
