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

package linstormigrate

import (
	"maps"
	"strings"
)

// PropsIndex groups the flat PROPS_CONTAINERS rows by owner object.
// LINSTOR keys each property row by an instance path; the shapes are:
//
//	/CTRL                                        controller
//	/NODES/<node>                                node
//	/STOR_POOLS/<node>/<pool>                    storage pool
//	/RSC_GRPS/<rg>                               resource group
//	/RSC_DFNS/<rd>                               resource definition
//	/VLM_DFNS/<rd>/<vlmNr>                       volume definition
//	/RSCS/<node>/<rd>                            resource (replica)
//	/VLMS/<node>/<rd>/<vlmNr>                    volume (replica volume)
//	/SNAP_DFNS/<rd>/<snap>                       snapshot definition
//	/SNAP_DFNS_RSC_DFN/<rd>/<snap>               RD props frozen into the snapshot
//	/SNAP_VLM_DFNS_VLM_DFN/<rd>/<snap>/<vlmNr>   VD props frozen into the snapshot
//	/SNAPS/<node>/<rd>/<snap>                    per-node snapshot
//	/SNAPS_RSC/<node>/<rd>/<snap>                resource props frozen into the snapshot
//	/SNAP_VLMS_VLM/<node>/<rd>/<snap>/<vlmNr>    volume props frozen into the snapshot
//
// All identifiers in the paths are the UPPERCASE key names
// (resource_name, not resource_dsp_name).
type PropsIndex struct {
	// Controller is the /CTRL property bag.
	Controller map[string]string

	byInstance map[string]map[string]string
}

// PropsInstanceController is the props_instance of the cluster-wide
// bag. LINSTOR resolves StorDriver/* and DrbdOptions/* through it as
// the last rung of the priority chain, so a value set here applies to
// every pool that does not override it.
const PropsInstanceController = "/CTRL"

// NewPropsIndex builds the lookup from the raw table rows.
func NewPropsIndex(rows []PropsContainerRow) *PropsIndex {
	idx := &PropsIndex{
		Controller: map[string]string{},
		byInstance: map[string]map[string]string{},
	}

	for _, row := range rows {
		if row.PropsInstance == PropsInstanceController {
			idx.Controller[row.PropKey] = row.PropValue

			continue
		}

		bag := idx.byInstance[row.PropsInstance]
		if bag == nil {
			bag = map[string]string{}
			idx.byInstance[row.PropsInstance] = bag
		}

		bag[row.PropKey] = row.PropValue
	}

	return idx
}

// Node returns the /NODES/<node> props.
func (idx *PropsIndex) Node(node string) map[string]string {
	return idx.get("NODES", node)
}

// StorPool returns the /STOR_POOLS/<node>/<pool> props.
func (idx *PropsIndex) StorPool(node, pool string) map[string]string {
	return idx.get("STOR_POOLS", node, pool)
}

// ResourceGroup returns the /RSC_GRPS/<rg> props.
func (idx *PropsIndex) ResourceGroup(rg string) map[string]string {
	return idx.get("RSC_GRPS", rg)
}

// ResourceDefinition returns the /RSC_DFNS/<rd> props.
func (idx *PropsIndex) ResourceDefinition(rd string) map[string]string {
	return idx.get("RSC_DFNS", rd)
}

// VolumeDefinition returns the /VLM_DFNS/<rd>/<vlmNr> props.
func (idx *PropsIndex) VolumeDefinition(rd, vlmNr string) map[string]string {
	return idx.get("VLM_DFNS", rd, vlmNr)
}

// Resource returns the /RSCS/<node>/<rd> props.
func (idx *PropsIndex) Resource(node, rd string) map[string]string {
	return idx.get("RSCS", node, rd)
}

// Volume returns the /VLMS/<node>/<rd>/<vlmNr> props.
func (idx *PropsIndex) Volume(node, rd, vlmNr string) map[string]string {
	return idx.get("VLMS", node, rd, vlmNr)
}

// SnapshotDefinition returns the /SNAP_DFNS/<rd>/<snap> props.
func (idx *PropsIndex) SnapshotDefinition(rd, snap string) map[string]string {
	return idx.get("SNAP_DFNS", rd, snap)
}

// SnapshotRDFrozen returns the /SNAP_DFNS_RSC_DFN/<rd>/<snap> props —
// the parent RD's property bag frozen at snapshot time.
func (idx *PropsIndex) SnapshotRDFrozen(rd, snap string) map[string]string {
	return idx.get("SNAP_DFNS_RSC_DFN", rd, snap)
}

// SnapshotVDFrozen returns the
// /SNAP_VLM_DFNS_VLM_DFN/<rd>/<snap>/<vlmNr> props — the parent VD's
// property bag frozen at snapshot time.
func (idx *PropsIndex) SnapshotVDFrozen(rd, snap, vlmNr string) map[string]string {
	return idx.get("SNAP_VLM_DFNS_VLM_DFN", rd, snap, vlmNr)
}

// get returns a copy of the property bag at the given instance path
// (nil when the object has no props). Copies so converters can mutate
// (pop consumed keys) without corrupting the index.
func (idx *PropsIndex) get(segments ...string) map[string]string {
	bag := idx.byInstance["/"+strings.Join(segments, "/")]
	if len(bag) == 0 {
		return nil
	}

	out := make(map[string]string, len(bag))
	maps.Copy(out, bag)

	return out
}
