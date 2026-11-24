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

import "testing"

// TestDrbdGrossKibLeavesTheNetSize: the bitmap covers the whole backing
// device, so the gross size must leave the net size for data once the
// metadata of the gross size itself is taken off, at every size.
func TestDrbdGrossKibLeavesTheNetSize(t *testing.T) {
	for _, net := range []int64{4, 1024, 1 << 20, 1<<20 + 4, 100 << 20, 1 << 30, 16<<30 + 12} {
		gross := drbdGrossKib(net)

		if gross <= net {
			t.Errorf("net %d: gross %d leaves no room for metadata", net, gross)
		}

		if capacity := gross - drbdInternalMetaKib(gross); capacity < net {
			t.Errorf("net %d: gross %d offers %d", net, gross, capacity)
		}
	}
}

// TestDrbdInternalMetaCoversLinstorDefault: the metadata estimate must
// not be below what LINSTOR reserves for its default seven peer slots,
// or a grow of an adopted volume would end short of the definition.
func TestDrbdInternalMetaCoversLinstorDefault(t *testing.T) {
	const linstorPeers = 7

	for _, backing := range []int64{1 << 20, 10 << 20, 1 << 30} {
		bits := alignUp(ceilDiv(backing, kibPerBitmapBit), 64)
		linstor := drbdMetaSuperblockKib + drbdMetaActivityLogKib + alignUp(ceilDiv(bits/8*linstorPeers, 1024), 4)

		if got := drbdInternalMetaKib(backing); got < linstor {
			t.Errorf("backing %d: estimate %d below LINSTOR's %d", backing, got, linstor)
		}
	}
}
