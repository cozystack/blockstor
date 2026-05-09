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

// MaxPeers is the upstream drbd-9 connection-mesh limit. node-id values
// that go past this won't be accepted by the kernel module; the
// allocator refuses to hand out 16+.
const MaxPeers = 16

// ErrNodeIDExhausted means no free id remains in 0..MaxPeers-1 for a
// new replica. Means the RD is at the max-peers limit and we'd need a
// dedicated tiebreaker eviction or a higher max-peers compile.
var ErrNodeIDExhausted = errors.New("DRBD node-id pool exhausted (max 16 peers per RD)")

// LowestFreeNodeID returns the smallest non-negative id not present in
// taken. The result is deterministic so two reconciles that see the
// same taken set produce the same id — important when two satellites
// race to allocate; the eventual conflict resolves to the same value.
//
// Returns ErrNodeIDExhausted when taken covers 0..MaxPeers-1.
func LowestFreeNodeID(taken []int32) (int32, error) {
	used := make(map[int32]bool, len(taken))
	for _, id := range taken {
		used[id] = true
	}

	for i := range int32(MaxPeers) {
		if !used[i] {
			return i, nil
		}
	}

	return 0, ErrNodeIDExhausted
}
