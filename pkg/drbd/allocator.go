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

package drbd

import "github.com/cockroachdb/errors"

// NodeIDMax is the highest DRBD 9 node id. The kernel and drbdmeta
// accept 0..31 whatever peer-slot count create-md was given (verified
// empirically on drbd-utils 9.22 against a v09 loop device created with
// `create-md 15`): the peer slots bound how many peers with a disk a
// replica keeps a bitmap for, not the node ids. LINSTOR hands out node
// ids from the same range (NodeId.NODE_ID_MAX). GI seeding stamps the
// full 0..NodeIDMax range so no slot is ever left with a stale per-peer
// bitmap UUID; see seedPerPeerGI.
const NodeIDMax = 31

// ErrNodeIDExhausted means no free id remains in 0..NodeIDMax for a new
// replica: the definition already has as many replicas as DRBD has node
// ids.
var ErrNodeIDExhausted = errors.New("DRBD node-id pool exhausted (max 32 replicas per resource definition)")

// LowestFreeNodeID returns the smallest non-negative id not present in
// taken. The result is deterministic so two reconciles that see the
// same taken set produce the same id — important when two satellites
// race to allocate; the eventual conflict resolves to the same value.
//
// Returns ErrNodeIDExhausted when taken covers 0..NodeIDMax.
func LowestFreeNodeID(taken []int32) (int32, error) {
	used := make(map[int32]bool, len(taken))
	for _, id := range taken {
		used[id] = true
	}

	for i := range int32(NodeIDMax + 1) {
		if !used[i] {
			return i, nil
		}
	}

	return 0, ErrNodeIDExhausted
}
