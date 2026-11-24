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
	"slices"
	"strings"

	"github.com/cozystack/blockstor/pkg/passphrase"
)

// PropsIndex groups the flat PROPS_CONTAINERS rows by owner object.
// LINSTOR keys each property row by an instance path; the shapes are:
//
//	/CTRL                                        controller
//	/STLT                                        satellite (cluster-wide)
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

	// Satellite is the /STLT property bag: the cluster-wide props the
	// LINSTOR controller ships to every satellite. See
	// PropsInstanceSatellite.
	Satellite map[string]string

	byInstance map[string]map[string]string

	// carried records the keys of a partly carried instance (/CTRL,
	// /STLT) the converter wrote onto a blockstor object, so whatever is
	// left can be reported instead of vanishing.
	carried map[string]map[string]bool

	// withheld are the rows NewPropsIndex keeps out of the index because
	// they would carry a passphrase, by instance.
	withheld map[string][]string
}

// DroppedProps names the keys of one LINSTOR props instance that no
// blockstor object carries.
type DroppedProps struct {
	Instance string
	Keys     []string
	// Withheld marks keys left out because they would carry a passphrase
	// in plaintext, rather than for lack of a blockstor equivalent.
	Withheld bool
}

// PropsInstanceController is the props_instance of the cluster-wide
// bag. LINSTOR resolves StorDriver/* and DrbdOptions/* through it as
// the last rung of the priority chain, so a value set here applies to
// every pool that does not override it.
const PropsInstanceController = "/CTRL"

// PropsInstanceSatellite is the props_instance of the cluster-wide
// satellite bag. `linstor controller set-property` files every key it
// has no controller-side handling for here, DrbdOptions/Net/* and the
// other .res knobs among them, and piraeus-operator's
// LinstorCluster.spec.properties goes through the same call. The
// satellites render .res files from this bag, not from /CTRL, as the
// lowest rung below resource, definition, group and node.
const PropsInstanceSatellite = "/STLT"

// NewPropsIndex builds the lookup from the raw table rows. A row that would
// carry a LUKS passphrase in a property is left out: blockstor never reads
// one, and the generated YAML is applied with kubectl, past the REST guard,
// so the value would otherwise land in plaintext on a CR.
func NewPropsIndex(rows []PropsContainerRow) *PropsIndex {
	idx := &PropsIndex{
		Controller: map[string]string{},
		Satellite:  map[string]string{},
		byInstance: map[string]map[string]string{},
		carried:    map[string]map[string]bool{},
		withheld:   map[string][]string{},
	}

	for _, row := range rows {
		if passphrase.IsPlaintextPropKey(row.PropKey) {
			idx.withheld[row.PropsInstance] = append(idx.withheld[row.PropsInstance], row.PropKey)

			continue
		}

		switch row.PropsInstance {
		case PropsInstanceController:
			idx.Controller[row.PropKey] = row.PropValue

			continue
		case PropsInstanceSatellite:
			idx.Satellite[row.PropKey] = row.PropValue

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

// VolumeGroup returns the /VLM_GRPS/<rg>/<vlmNr> props.
func (idx *PropsIndex) VolumeGroup(rg, vlmNr string) map[string]string {
	return idx.get("VLM_GRPS", rg, vlmNr)
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

// Snapshot returns the /SNAPS/<node>/<rd>/<snap> props, one node's
// snapshot.
func (idx *PropsIndex) Snapshot(node, rd, snap string) map[string]string {
	return idx.get("SNAPS", node, rd, snap)
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

// wholeBagKinds are the instance kinds whose bag the converter writes
// onto the matching blockstor object as it is: node, storage pool,
// resource group, definition, volume definition, replica and snapshot
// definition. An instance of one of them that reaches no object belongs
// to an object that did not convert, which the report already names
// with its reason; its props go with it.
//
//nolint:gochecknoglobals // immutable lookup table
var wholeBagKinds = map[string]bool{
	"NODES":      true,
	"STOR_POOLS": true,
	"RSC_GRPS":   true,
	"RSC_DFNS":   true,
	"VLM_DFNS":   true,
	"RSCS":       true,
	"SNAP_DFNS":  true,
}

// Dropped lists, by instance, every key no blockstor object carries:
// instances of a kind the converter does not carry (wholeBagKinds), the
// keys of /CTRL and /STLT it left behind, and the passphrase rows
// NewPropsIndex withheld. Sorted by instance, then key, so the report is
// stable.
func (idx *PropsIndex) Dropped() []DroppedProps {
	bags := map[string]map[string]string{
		PropsInstanceController: idx.Controller,
		PropsInstanceSatellite:  idx.Satellite,
	}
	maps.Copy(bags, idx.byInstance)

	var out []DroppedProps

	for _, instance := range slices.Sorted(maps.Keys(bags)) {
		if wholeBagKinds[instanceKind(instance)] {
			continue
		}

		var keys []string

		for key := range bags[instance] {
			if !idx.carried[instance][key] {
				keys = append(keys, key)
			}
		}

		if len(keys) > 0 {
			slices.Sort(keys)
			out = append(out, DroppedProps{Instance: instance, Keys: keys})
		}
	}

	for _, instance := range slices.Sorted(maps.Keys(idx.withheld)) {
		keys := slices.Clone(idx.withheld[instance])
		slices.Sort(keys)
		out = append(out, DroppedProps{Instance: instance, Keys: slices.Compact(keys), Withheld: true})
	}

	return out
}

// carryKey records one key of an instance as carried.
func (idx *PropsIndex) carryKey(instance, key string) {
	keys := idx.carried[instance]
	if keys == nil {
		keys = map[string]bool{}
		idx.carried[instance] = keys
	}

	keys[key] = true
}

// instanceKind is the first segment of a props_instance path: NODES for
// /NODES/<node>, CTRL for /CTRL.
func instanceKind(instance string) string {
	kind, _, _ := strings.Cut(strings.TrimPrefix(instance, "/"), "/")

	return kind
}

// instancePath joins props_instance segments the way LINSTOR spells
// the path.
func instancePath(segments ...string) string {
	return "/" + strings.Join(segments, "/")
}

// get returns a copy of the property bag at the given instance path
// (nil when the object has no props). Copies so converters can mutate
// (pop consumed keys) without corrupting the index.
func (idx *PropsIndex) get(segments ...string) map[string]string {
	bag := idx.byInstance[instancePath(segments...)]
	if len(bag) == 0 {
		return nil
	}

	out := make(map[string]string, len(bag))
	maps.Copy(out, bag)

	return out
}
