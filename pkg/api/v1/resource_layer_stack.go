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

// ApplyLayerStack re-stamps a Resource's `LayerObject` chain and every
// volume's `LayerDataList` from the supplied RD-resolved layer stack.
//
// Background — Bug 95: the K8s ResourceCRD does not store the parent
// RD's LayerStack on each replica (that lives on the RD spec), so the
// store-level `crdToWireResource` projection falls back to
// `DefaultLayerStack()` (`[DRBD, STORAGE]`) for both surfaces. For an
// RD created with `--layer-list DRBD,LUKS,STORAGE` that means the
// wire shape silently drops LUKS — operators believe they have an
// encrypted volume while DRBD is replicating plaintext. The REST
// `/v1/view/resources` aggregator, which already fetches the parent
// RD for the effective-props walk, re-stamps both surfaces via this
// helper so the wire shape matches the RD spec.
//
// Behaviour:
//   - len(stack)==0 → no-op (the caller's existing default-stack shape
//     wins, preserving back-compat for callers that haven't yet wired
//     the RD lookup).
//   - LayerObject is rebuilt from the stack while preserving any
//     existing per-layer runtime payload (DRBD ports/connections,
//     STORAGE provider_kind + storage_volumes) so the regenerated
//     chain doesn't lose the observed state.
//   - Volume[i].LayerDataList is rewritten to one entry per stack
//     layer, matching the upstream LINSTOR wire shape the Python CLI's
//     `_walk(layer_data, type==LUKS)` predicate relies on for State /
//     `--faulty` rendering.
func ApplyLayerStack(r *Resource, stack []string) {
	if r == nil || len(stack) == 0 {
		return
	}

	r.LayerObject = rebuildLayerObject(r.LayerObject, stack)
}

// ApplyLayerStackToVolumes rewrites each volume's `layer_data_list`
// in-place from the supplied stack. Pulled out of `ApplyLayerStack`
// so callers that hold a `[]Volume` separately (e.g. the REST
// `ResourceWithVolumes` builder, which carries an annotated copy of
// Resource.Volumes) can re-stamp without going through Resource.
func ApplyLayerStackToVolumes(vols []Volume, stack []string) {
	if len(vols) == 0 || len(stack) == 0 {
		return
	}

	for i := range vols {
		vols[i].LayerDataList = volumeLayerDataFromStack(stack)
	}
}

// volumeLayerDataFromStack mirrors the apiv1-side equivalent of the
// k8s store's private helper of the same name. Exported caller is
// `ApplyLayerStackToVolumes`; kept private to avoid leaking an
// implementation detail (the one-entry-per-layer projection).
func volumeLayerDataFromStack(stack []string) []VolumeLayerData {
	out := make([]VolumeLayerData, 0, len(stack))
	for _, kind := range stack {
		out = append(out, VolumeLayerData{Type: kind})
	}

	return out
}

// rebuildLayerObject walks the supplied stack top-to-bottom and
// produces a single-branch ResourceLayer chain. Existing runtime
// payload on the prior chain (DRBD ports/connections, STORAGE
// provider_kind / storage_volumes) is preserved by matching layer
// `Type` against the new stack — a layer that newly appears in the
// stack (e.g. LUKS that wasn't there before) starts with an empty
// payload, which is the correct wire shape for an as-yet-unhydrated
// layer.
func rebuildLayerObject(existing *ResourceLayer, stack []string) *ResourceLayer {
	if len(stack) == 0 {
		return existing
	}

	priorByType := indexLayerByType(existing)

	top := &ResourceLayer{Type: stack[0]}
	if p, ok := priorByType[stack[0]]; ok {
		top.NameSuffix = p.NameSuffix
		top.Drbd = p.Drbd
		top.Storage = p.Storage
		top.Data = p.Data
	}

	cursor := top

	for _, t := range stack[1:] {
		child := ResourceLayer{Type: t}
		if p, ok := priorByType[t]; ok {
			child.NameSuffix = p.NameSuffix
			child.Drbd = p.Drbd
			child.Storage = p.Storage
			child.Data = p.Data
		}

		cursor.Children = []ResourceLayer{child}
		cursor = &cursor.Children[0]
	}

	return top
}

// indexLayerByType walks the existing layer chain and returns a
// Type → *ResourceLayer index so the rebuilder can lift the
// per-layer runtime payload onto the new chain. Returns an empty
// map for a nil chain.
func indexLayerByType(layer *ResourceLayer) map[string]*ResourceLayer {
	out := map[string]*ResourceLayer{}

	for cursor := layer; cursor != nil; {
		out[cursor.Type] = cursor

		if len(cursor.Children) == 0 {
			break
		}

		cursor = &cursor.Children[0]
	}

	return out
}
