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
