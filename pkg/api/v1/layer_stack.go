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

package v1

import "strings"

// ResolveLayerStack picks the layer composition for an RD by walking
// the upstream LINSTOR scope hierarchy: RD → RG → default. The first
// non-empty entry wins. Same precedence rule as
// ResourceGroup.SelectFilter inheritance.
//
// Empty arrays at every level fall through to DefaultLayerStack
// (`["DRBD","STORAGE"]`).
func ResolveLayerStack(rdLayers, rgLayers []string) []string {
	if len(rdLayers) > 0 {
		return rdLayers
	}

	if len(rgLayers) > 0 {
		return rgLayers
	}

	return DefaultLayerStack()
}

// LayerInStack reports whether kind is present in stack
// (case-insensitive). Used by the dispatcher / satellite to decide
// whether to render a `.res` or just provision the storage layer.
func LayerInStack(stack []string, kind string) bool {
	for _, s := range stack {
		if strings.EqualFold(s, kind) {
			return true
		}
	}

	return false
}

// ContainsReplicationLayer reports whether stack contains a
// replication-capable layer. Today that's DRBD only — DRBD-9 is the
// sole layer that ships block-level inter-node replication and the
// quorum machinery (`quorum: majority` + TIE_BREAKER witnesses) the
// rest of the codebase relies on.
//
// Used by Bug 334 (skip TIE_BREAKER spawn when LayerStack has no
// replication layer — without DRBD there is no quorum machinery to
// arbitrate) and Bug 335 (reject auto-place=N with N>1 on a
// non-replicated LayerStack — N independent local volumes diverge
// silently on the first write).
//
// TODO(shared-lun): when shared-LUN active-active support lands
// (likely thin LVM with lvmlockd-cooperative active-on-one + deactivate-
// others), the multi-place gate in pkg/rest/autoplace.go can extend
// past this helper to permit multi-place with an explicit
// `--shared-lun` flag. Replication-via-shared-storage is a different
// kind of "replication layer"; we keep the predicate narrow to DRBD
// until the shared-LUN code path actually exists.
func ContainsReplicationLayer(stack []string) bool {
	return LayerInStack(stack, LayerKindDRBD)
}
