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
