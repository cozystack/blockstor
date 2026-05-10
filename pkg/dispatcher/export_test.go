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

package dispatcher

import (
	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// PeerAddress exposes the internal peerAddress lookup so the test
// suite can pin its host-extraction + fallback contract without
// going through the full Apply path. Production wiring keeps the
// unexported name.
func PeerAddress(nodeName string, nodes []blockstoriov1alpha1.Node) string {
	return peerAddress(nodeName, nodes)
}

// DrbdAddrAny exposes the placeholder address peerAddress falls back
// to when the node is unknown or hasn't registered yet. Tests assert
// against this constant so a refactor that changed the placeholder
// string would surface immediately.
const DrbdAddrAny = drbdAddrAny

// ReadDRBDPort exposes the persisted-port lookup so the test suite
// can pin both branches (Status set → use persisted; nil → derive).
func ReadDRBDPort(target *blockstoriov1alpha1.Resource, peers []blockstoriov1alpha1.Resource) int {
	return readDRBDPort(target, peers)
}

// ReadDRBDMinor mirrors ReadDRBDPort for the local /dev/drbd<N> path.
func ReadDRBDMinor(target *blockstoriov1alpha1.Resource, peers []blockstoriov1alpha1.Resource) int {
	return readDRBDMinor(target, peers)
}

// PeerPortOf exposes the per-peer port lookup. Peer's own DRBDPort
// wins; falls back to the supplied default (target's port) when the
// peer hasn't been allocated yet.
func PeerPortOf(r *blockstoriov1alpha1.Resource, fallback int) int {
	return peerPortOf(r, fallback)
}

// LowestDiskfulID exposes the diskful-replica node-id picker. Tests
// pin the DISKLESS-skip + unallocated-skip rules so the auto-primary
// seed always lands on a real diskful replica (never on a witness or
// an as-yet-unallocated replica).
func LowestDiskfulID(target *blockstoriov1alpha1.Resource, peers []blockstoriov1alpha1.Resource) int32 {
	return lowestDiskfulID(target, peers)
}
