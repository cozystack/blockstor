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
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	k8sstore "github.com/cozystack/blockstor/pkg/store/k8s"
)

// drbdSharedSecretProp is the RD property key blockstor's satellite
// renders into the `net {}` section as `shared-secret` (see
// pkg/rest/drbd_passphrase.go). The converter carries the per-RD DRBD
// authentication secret from LAYER_DRBD_RESOURCE_DEFINITIONS here so an
// adopted resource renders the same net config the LINSTOR cluster ran.
//
//nolint:gosec // this is a property-name constant, not the secret value itself
const drbdSharedSecretProp = "DrbdOptions/Net/shared-secret"

// nodeTypeController is the LINSTOR node type blockstor never adopts —
// blockstor runs its own control plane.
const nodeTypeController = "CONTROLLER"

// NODES.node_type integer values. Calibrated against a production dump
// whose every node reported type 2 while `linstor n l` showed them all
// as Satellite; 1/3/4 follow LINSTOR's documented
// CONTROLLER/COMBINED/AUXILIARY ordering around it.
const (
	nodeTypeIntController int32 = 1
	nodeTypeIntSatellite  int32 = 2
	nodeTypeIntCombined   int32 = 3
	nodeTypeIntAuxiliary  int32 = 4
)

// nodeTypeName maps the NODES.node_type integer to the LINSTOR node
// type string blockstor's Node CRD enum accepts ("" when unknown).
func nodeTypeName(nodeType int32) string {
	switch nodeType {
	case nodeTypeIntController:
		return nodeTypeController
	case nodeTypeIntSatellite:
		return "SATELLITE"
	case nodeTypeIntCombined:
		return "COMBINED"
	case nodeTypeIntAuxiliary:
		return "AUXILIARY"
	default:
		return ""
	}
}

// Result is the converted blockstor object set plus the migration
// report (warnings about rows the converter skipped or bits it could
// not represent).
type Result struct {
	Nodes               []crdv1alpha1.Node
	StoragePools        []crdv1alpha1.StoragePool
	ResourceGroups      []crdv1alpha1.ResourceGroup
	ResourceDefinitions []crdv1alpha1.ResourceDefinition
	Resources           []crdv1alpha1.Resource
	Snapshots           []crdv1alpha1.Snapshot
	Warnings            []string
}

// converter carries the indexes built once per Convert call.
type converter struct {
	dump  *Dump
	props *PropsIndex

	// display-name lookups: UPPERCASE key → original display case.
	nodeDsp map[string]string
	poolDsp map[string]string
	rgDsp   map[string]string
	rdDsp   map[string]string

	// DRBD layer joins (non-snapshot, base suffix "" only).
	drbdRD    map[string]LayerDrbdResourceDefinitionRow // by resource_name
	drbdMinor map[volumeKey]*int32                      // by (rd, vlmNr)
	drbdNode  map[replicaKey]*int32                     // node-id by (node, rd)
	storVol   map[volumeReplicaKey]LayerStorageVolumeRow
	luksVol   map[volumeReplicaKey]LayerLuksVolumeRow

	warnings []string
}

type volumeKey struct {
	rd    string
	vlmNr int32
}

type replicaKey struct {
	node string
	rd   string
}

type volumeReplicaKey struct {
	node  string
	rd    string
	vlmNr int32
}

// Convert translates a LINSTOR database dump into blockstor CRDs.
func Convert(dump *Dump) (*Result, error) {
	conv := &converter{
		dump:      dump,
		props:     NewPropsIndex(dump.PropsContainers),
		nodeDsp:   map[string]string{},
		poolDsp:   map[string]string{},
		rgDsp:     map[string]string{},
		rdDsp:     map[string]string{},
		drbdRD:    map[string]LayerDrbdResourceDefinitionRow{},
		drbdMinor: map[volumeKey]*int32{},
		drbdNode:  map[replicaKey]*int32{},
		storVol:   map[volumeReplicaKey]LayerStorageVolumeRow{},
		luksVol:   map[volumeReplicaKey]LayerLuksVolumeRow{},
	}

	conv.buildIndexes()

	res := &Result{
		Nodes:               conv.convertNodes(),
		StoragePools:        conv.convertStoragePools(),
		ResourceGroups:      conv.convertResourceGroups(),
		ResourceDefinitions: conv.convertResourceDefinitions(),
		Resources:           conv.convertResources(),
		Snapshots:           conv.convertSnapshots(),
	}

	res.Warnings = conv.warnings

	return res, nil
}

func (c *converter) buildIndexes() {
	c.buildNameIndexes()
	c.buildLayerIndexes()
}

func (c *converter) buildNameIndexes() {
	for i := range c.dump.Nodes {
		row := &c.dump.Nodes[i]
		c.nodeDsp[row.NodeName] = displayName(row.NodeDspName, row.NodeName)
	}

	for i := range c.dump.StorPoolDefinitions {
		row := &c.dump.StorPoolDefinitions[i]
		c.poolDsp[row.PoolName] = displayName(row.PoolDspName, row.PoolName)
	}

	for i := range c.dump.ResourceGroups {
		row := &c.dump.ResourceGroups[i]
		c.rgDsp[row.ResourceGroupName] = displayName(row.ResourceGroupDspName, row.ResourceGroupName)
	}

	for i := range c.dump.ResourceDefinitions {
		row := &c.dump.ResourceDefinitions[i]
		if row.SnapshotName == "" {
			c.rdDsp[row.ResourceName] = displayName(row.ResourceDspName, row.ResourceName)
		}
	}
}

func (c *converter) buildLayerIndexes() {
	for i := range c.dump.LayerDrbdResourceDefinitions {
		row := &c.dump.LayerDrbdResourceDefinitions[i]
		if row.SnapshotName == "" && row.ResourceNameSuffix == "" {
			c.drbdRD[row.ResourceName] = *row
		}
	}

	for i := range c.dump.LayerDrbdVolumeDefinitions {
		row := &c.dump.LayerDrbdVolumeDefinitions[i]
		if row.SnapshotName == "" && row.ResourceNameSuffix == "" && row.VlmMinorNr != nil {
			c.drbdMinor[volumeKey{rd: row.ResourceName, vlmNr: row.VlmNr}] = ptr(*row.VlmMinorNr)
		}
	}

	c.buildLayerInstanceIndexes()
}

func (c *converter) buildLayerInstanceIndexes() {
	// LAYER_RESOURCE_IDS assigns each (node, resource, layer) instance
	// the integer id the per-layer tables key on. baseLayer filters to
	// the live (non-snapshot, base-suffix) instances the converter maps.
	layerByID := map[int32]LayerResourceIDRow{}

	for i := range c.dump.LayerResourceIDs {
		row := &c.dump.LayerResourceIDs[i]
		layerByID[row.LayerResourceID] = *row
	}

	baseLayer := func(id int32) (LayerResourceIDRow, bool) {
		lri, ok := layerByID[id]
		if !ok || lri.SnapshotName != "" || lri.LayerResourceSuffix != "" {
			return LayerResourceIDRow{}, false
		}

		return lri, true
	}

	for i := range c.dump.LayerDrbdResources {
		row := &c.dump.LayerDrbdResources[i]
		if lri, ok := baseLayer(row.LayerResourceID); ok {
			c.drbdNode[replicaKey{node: lri.NodeName, rd: lri.ResourceName}] = ptr(row.NodeID)
		}
	}

	for i := range c.dump.LayerStorageVolumes {
		row := &c.dump.LayerStorageVolumes[i]
		if lri, ok := baseLayer(row.LayerResourceID); ok {
			c.storVol[volumeReplicaKey{node: lri.NodeName, rd: lri.ResourceName, vlmNr: row.VlmNr}] = *row
		}
	}

	for i := range c.dump.LayerLuksVolumes {
		row := &c.dump.LayerLuksVolumes[i]
		if lri, ok := baseLayer(row.LayerResourceID); ok {
			c.luksVol[volumeReplicaKey{node: lri.NodeName, rd: lri.ResourceName, vlmNr: row.VlmNr}] = *row
		}
	}
}

func (c *converter) convertNodes() []crdv1alpha1.Node {
	nodes := make([]crdv1alpha1.Node, 0, len(c.dump.Nodes))

	for _, row := range c.dump.Nodes {
		typeName := nodeTypeName(row.NodeType)
		if typeName == "" {
			c.warnf("node %s: unknown node_type %d — skipped", row.NodeDspName, row.NodeType)

			continue
		}

		if typeName == nodeTypeController {
			c.warnf("node %s: CONTROLLER node not migrated (blockstor runs its own control plane)", row.NodeDspName)

			continue
		}

		dsp := displayName(row.NodeDspName, row.NodeName)

		node := crdv1alpha1.Node{
			TypeMeta:   typeMeta("Node"),
			ObjectMeta: objectMeta(dsp),
			Spec: crdv1alpha1.NodeSpec{
				Type:  typeName,
				Props: c.props.Node(row.NodeName),
			},
		}

		for _, nic := range c.netInterfacesFor(row.NodeName) {
			node.Spec.NetInterfaces = append(node.Spec.NetInterfaces, crdv1alpha1.NodeNetInterface{
				Name:                    displayName(nic.NodeNetDspName, nic.NodeNetName),
				Address:                 nic.InetAddress,
				SatellitePort:           orZero(nic.StltConnPort),
				SatelliteEncryptionType: nic.StltConnEncrType,
			})
		}

		nodes = append(nodes, node)
	}

	sortByName(nodes, func(n crdv1alpha1.Node) string { return n.Name })

	return nodes
}

func (c *converter) netInterfacesFor(nodeName string) []NodeNetInterfaceRow {
	var out []NodeNetInterfaceRow

	for _, nic := range c.dump.NodeNetInterfaces {
		if nic.NodeName == nodeName {
			out = append(out, nic)
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].NodeNetName < out[j].NodeNetName })

	return out
}

func (c *converter) convertStoragePools() []crdv1alpha1.StoragePool {
	pools := make([]crdv1alpha1.StoragePool, 0, len(c.dump.NodeStorPools))

	for _, row := range c.dump.NodeStorPools {
		nodeDsp := c.displayNode(row.NodeName)
		poolDsp := displayName(c.poolDsp[row.PoolName], row.PoolName)

		pool := crdv1alpha1.StoragePool{
			TypeMeta:   typeMeta("StoragePool"),
			ObjectMeta: objectMeta(strings.ToLower(poolDsp) + "." + strings.ToLower(nodeDsp)),
			Spec: crdv1alpha1.StoragePoolSpec{
				NodeName:     nodeDsp,
				PoolName:     poolDsp,
				ProviderKind: row.DriverName,
				Props:        c.props.StorPool(row.NodeName, row.PoolName),
			},
		}

		pools = append(pools, pool)
	}

	sortByName(pools, func(p crdv1alpha1.StoragePool) string { return p.Name })

	return pools
}

func (c *converter) convertResourceGroups() []crdv1alpha1.ResourceGroup {
	groups := make([]crdv1alpha1.ResourceGroup, 0, len(c.dump.ResourceGroups))

	for i := range c.dump.ResourceGroups {
		row := &c.dump.ResourceGroups[i]
		dsp := displayName(row.ResourceGroupDspName, row.ResourceGroupName)

		typed, residual, extra := k8sstore.SplitProps(c.props.ResourceGroup(row.ResourceGroupName))

		group := crdv1alpha1.ResourceGroup{
			TypeMeta:   typeMeta("ResourceGroup"),
			ObjectMeta: objectMeta(dsp),
			Spec: crdv1alpha1.ResourceGroupSpec{
				Description: row.Description,
				Props:       residual,
				DRBDOptions: typed,
				ExtraProps:  extra,
				SelectFilter: crdv1alpha1.ResourceGroupSelectFilter{
					PlaceCount:              row.ReplicaCount,
					StoragePoolList:         c.parseList(row.PoolName, "resource group "+dsp+" pool_name"),
					StoragePoolDisklessList: c.parseList(row.PoolNameDiskless, "resource group "+dsp+" pool_name_diskless"),
					NodeNameList:            c.parseList(row.NodeNameList, "resource group "+dsp+" node_name_list"),
					ReplicasOnSame:          c.parseList(row.ReplicasOnSame, "resource group "+dsp+" replicas_on_same"),
					ReplicasOnDifferent:     c.parseList(row.ReplicasOnDifferent, "resource group "+dsp+" replicas_on_different"),
					NotPlaceWithRsc:         c.parseList(row.DoNotPlaceWithRsc, "resource group "+dsp+" do_not_place_with_rsc_list"),
					ProviderList:            c.parseList(row.AllowedProviderList, "resource group "+dsp+" allowed_provider_list"),
					LayerStack:              c.parseList(row.LayerStack, "resource group "+dsp+" layer_stack"),
				},
			},
		}

		for _, vg := range c.volumeGroupsFor(row.ResourceGroupName) {
			group.Spec.VolumeGroups = append(group.Spec.VolumeGroups, crdv1alpha1.ResourceGroupVolumeGroup{
				VolumeNumber: vg.VlmNr,
			})

			if vg.Flags != 0 {
				c.warnf("resource group %s volume group %d: unhandled flags bitmask %d dropped", dsp, vg.VlmNr, vg.Flags)
			}
		}

		groups = append(groups, group)
	}

	sortByName(groups, func(g crdv1alpha1.ResourceGroup) string { return g.Name })

	return groups
}

func (c *converter) volumeGroupsFor(rgName string) []VolumeGroupRow {
	var out []VolumeGroupRow

	for _, vg := range c.dump.VolumeGroups {
		if vg.ResourceGroupName == rgName {
			out = append(out, vg)
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].VlmNr < out[j].VlmNr })

	return out
}

func (c *converter) convertResourceDefinitions() []crdv1alpha1.ResourceDefinition {
	defs := make([]crdv1alpha1.ResourceDefinition, 0, len(c.dump.ResourceDefinitions))

	for i := range c.dump.ResourceDefinitions {
		row := &c.dump.ResourceDefinitions[i]
		if row.SnapshotName != "" {
			continue // snapshot definitions convert via convertSnapshots
		}

		dsp := displayName(row.ResourceDspName, row.ResourceName)

		if row.ResourceFlags&resourceFlagDelete != 0 {
			c.warnf("resource definition %s: marked DELETE in the source cluster — skipped", dsp)

			continue
		}

		if row.ResourceFlags != 0 {
			c.warnf("resource definition %s: unhandled flags bitmask %d dropped", dsp, row.ResourceFlags)
		}

		typed, residual, extra := k8sstore.SplitProps(c.props.ResourceDefinition(row.ResourceName))

		def := crdv1alpha1.ResourceDefinition{
			TypeMeta:   typeMeta("ResourceDefinition"),
			ObjectMeta: objectMeta(dsp),
			Spec: crdv1alpha1.ResourceDefinitionSpec{
				ResourceGroupName: c.displayRG(row.ResourceGroupName),
				Props:             residual,
				DRBDOptions:       typed,
				ExtraProps:        extra,
				LayerStack:        c.parseList(row.LayerStack, "resource definition "+dsp+" layer_stack"),
				// The volumes already hold committed data in the source
				// cluster: latch Initialized so any replica added AFTER
				// the migration must SyncTarget the real data instead of
				// skipping the initial sync against an adopted set.
				Initialized: ptr(true),
			},
		}

		if drbd, ok := c.drbdRD[row.ResourceName]; ok {
			def.Spec.DRBDPort = drbd.TCPPort

			if drbd.Secret != "" {
				if def.Spec.ExtraProps == nil {
					def.Spec.ExtraProps = map[string]string{}
				}
				// Carried so the satellite renders the same `net {
				// shared-secret }` the source cluster's kernels already
				// run — adoption then re-applies an identical config
				// instead of re-keying the mesh.
				def.Spec.ExtraProps[drbdSharedSecretProp] = drbd.Secret
			}
		}

		def.Spec.VolumeDefinitions = c.volumeDefinitionsFor(row.ResourceName, dsp)

		if c.rdHasLuks(row.ResourceName) {
			// LAYER_LUKS_VOLUMES carries the volume passphrase encrypted
			// with the LINSTOR master key; decrypting it needs the
			// operator's master passphrase and LINSTOR's KDF, which this
			// converter does not implement yet. The layer stack is
			// preserved, but the volume cannot be opened until the
			// passphrase is provisioned into blockstor by hand.
			c.warnf("resource definition %s: LUKS passphrase NOT migrated (encrypted with the LINSTOR master key) — provision spec.encryption manually before adopting", dsp)
		}

		defs = append(defs, def)
	}

	sortByName(defs, func(d crdv1alpha1.ResourceDefinition) string { return d.Name })

	return defs
}

// rdHasLuks reports whether any live replica of the RD carries a LUKS
// layer volume (an encrypted passphrase row).
func (c *converter) rdHasLuks(rdName string) bool {
	for key := range c.luksVol {
		if key.rd == rdName {
			return true
		}
	}

	return false
}

func (c *converter) volumeDefinitionsFor(rdName, rdDsp string) []crdv1alpha1.ResourceDefinitionVolume {
	var out []crdv1alpha1.ResourceDefinitionVolume

	for _, vd := range c.dump.VolumeDefinitions {
		if vd.ResourceName != rdName || vd.SnapshotName != "" {
			continue
		}

		if vd.VlmFlags&resourceFlagDelete != 0 {
			c.warnf("volume definition %s/%d: marked DELETE in the source cluster — skipped", rdDsp, vd.VlmNr)

			continue
		}

		if vd.VlmFlags != 0 {
			c.warnf("volume definition %s/%d: unhandled flags bitmask %d dropped", rdDsp, vd.VlmNr, vd.VlmFlags)
		}

		out = append(out, crdv1alpha1.ResourceDefinitionVolume{
			VolumeNumber: vd.VlmNr,
			SizeKib:      vd.VlmSize,
			Props:        c.props.VolumeDefinition(rdName, strconv.Itoa(int(vd.VlmNr))),
			DRBDMinor:    c.drbdMinor[volumeKey{rd: rdName, vlmNr: vd.VlmNr}],
		})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].VolumeNumber < out[j].VolumeNumber })

	return out
}

func (c *converter) convertResources() []crdv1alpha1.Resource {
	resources := make([]crdv1alpha1.Resource, 0, len(c.dump.Resources))

	for _, row := range c.dump.Resources {
		if row.SnapshotName != "" {
			continue // snapshot placement rows feed convertSnapshots
		}

		rdDsp := c.displayRD(row.ResourceName)
		nodeDsp := c.displayNode(row.NodeName)
		replicaName := rdDsp + "." + nodeDsp

		if row.ResourceFlags&resourceFlagDelete != 0 {
			c.warnf("resource %s: marked DELETE in the source cluster — skipped", replicaName)

			continue
		}

		storVol, hasStorVol := c.storVol[volumeReplicaKey{node: row.NodeName, rd: row.ResourceName, vlmNr: 0}]

		flags, rest := decodeResourceFlags(row.ResourceFlags)
		if rest != 0 {
			c.warnf("resource %s: unhandled flags bits %d dropped (of %d)", replicaName, rest, row.ResourceFlags)
		}

		typed, residual, extra := k8sstore.SplitProps(c.props.Resource(row.NodeName, row.ResourceName))

		resource := crdv1alpha1.Resource{
			TypeMeta:   typeMeta("Resource"),
			ObjectMeta: objectMeta(replicaName),
			Spec: crdv1alpha1.ResourceSpec{
				ResourceDefinitionName: rdDsp,
				NodeName:               nodeDsp,
				Props:                  residual,
				DRBDOptions:            typed,
				ExtraProps:             extra,
				Flags:                  flags,
				DRBDNodeID:             c.drbdNode[replicaKey{node: row.NodeName, rd: row.ResourceName}],
				// Adopted replicas carry real data (or are the witness of
				// a data-bearing set): never allow the day0 skip.
				SkipInitialSync: ptr(false),
			},
		}

		if hasStorVol {
			resource.Spec.StoragePool = displayName(c.poolDsp[storVol.StorPoolName], storVol.StorPoolName)
		} else if sp := resource.Spec.Props["StorPoolName"]; sp != "" {
			resource.Spec.StoragePool = sp
		}

		if drbd, ok := c.drbdRD[row.ResourceName]; ok {
			// LINSTOR allocates one cluster-wide TCP port per RD; every
			// replica listens on it. blockstor's per-replica allocator
			// honours a preset value verbatim.
			resource.Spec.DRBDPort = drbd.TCPPort
		}

		resources = append(resources, resource)
	}

	sortByName(resources, func(r crdv1alpha1.Resource) string { return r.Name })

	return resources
}

func (c *converter) convertSnapshots() []crdv1alpha1.Snapshot {
	var snaps []crdv1alpha1.Snapshot

	for i := range c.dump.ResourceDefinitions {
		row := &c.dump.ResourceDefinitions[i]
		if row.SnapshotName == "" {
			continue
		}

		rdDsp := c.displayRD(row.ResourceName)
		snapDsp := displayName(row.SnapshotDspName, row.SnapshotName)
		name := rdDsp + "." + snapDsp

		switch {
		case row.ResourceFlags&snapDfnFlagFailedDeployment != 0:
			c.warnf("snapshot %s: FAILED_DEPLOYMENT in the source cluster — skipped", name)

			continue
		case row.ResourceFlags&snapDfnFlagSuccessful == 0:
			c.warnf("snapshot %s: not marked SUCCESSFUL (flags %d, take in flight?) — skipped", name, row.ResourceFlags)

			continue
		}

		snap := crdv1alpha1.Snapshot{
			TypeMeta:   typeMeta("Snapshot"),
			ObjectMeta: objectMeta(name),
			Spec: crdv1alpha1.SnapshotSpec{
				ResourceDefinitionName: rdDsp,
				SnapshotName:           snapDsp,
				Props:                  c.props.SnapshotDefinition(row.ResourceName, row.SnapshotName),
				Nodes:                  c.snapshotNodesFor(row.ResourceName, row.SnapshotName),
			},
		}

		for _, vd := range c.dump.VolumeDefinitions {
			if vd.ResourceName != row.ResourceName || vd.SnapshotName != row.SnapshotName {
				continue
			}

			snap.Spec.VolumeDefinitions = append(snap.Spec.VolumeDefinitions, crdv1alpha1.SnapshotVolumeRef{
				VolumeNumber: vd.VlmNr,
				SizeKib:      vd.VlmSize,
			})
		}

		sort.Slice(snap.Spec.VolumeDefinitions, func(i, j int) bool {
			return snap.Spec.VolumeDefinitions[i].VolumeNumber < snap.Spec.VolumeDefinitions[j].VolumeNumber
		})

		snaps = append(snaps, snap)
	}

	sortByName(snaps, func(s crdv1alpha1.Snapshot) string { return s.Name })

	return snaps
}

func (c *converter) snapshotNodesFor(rdName, snapName string) []string {
	var out []string

	for _, row := range c.dump.Resources {
		if row.ResourceName == rdName && row.SnapshotName == snapName {
			out = append(out, c.displayNode(row.NodeName))
		}
	}

	sort.Strings(out)

	return out
}

// parseList decodes LINSTOR's JSON-array-in-a-string columns
// (`"[\"DRBD\",\"STORAGE\"]"`). Empty input, `[]` and `null` all yield
// nil; a malformed value is reported and yields nil.
func (c *converter) parseList(raw, context string) []string {
	if raw == "" || raw == "[]" || raw == "null" {
		return nil
	}

	var out []string

	err := json.Unmarshal([]byte(raw), &out)
	if err != nil {
		c.warnf("%s: unparsable list %q dropped: %v", context, raw, err)

		return nil
	}

	if len(out) == 0 {
		return nil
	}

	return out
}

func (c *converter) displayNode(name string) string {
	return displayName(c.nodeDsp[name], name)
}

func (c *converter) displayRD(name string) string {
	return displayName(c.rdDsp[name], name)
}

func (c *converter) displayRG(name string) string {
	if name == "" {
		return ""
	}

	return displayName(c.rgDsp[name], name)
}

func (c *converter) warnf(format string, args ...any) {
	c.warnings = append(c.warnings, fmt.Sprintf(format, args...))
}

// displayName prefers the display-case column, falling back to the
// UPPERCASE key column when the display column is empty.
func displayName(dsp, key string) string {
	if dsp != "" {
		return dsp
	}

	return key
}

func typeMeta(kind string) metav1.TypeMeta {
	return metav1.TypeMeta{
		APIVersion: crdv1alpha1.GroupVersion.String(),
		Kind:       kind,
	}
}

// objectMeta builds the CRD metadata for a LINSTOR object name:
// metadata.name via the store's normalization (lowercase, slugged when
// not RFC-1123-clean) and the original spelling preserved in the
// blockstor.io/linstor-name annotation when it differs.
func objectMeta(original string) metav1.ObjectMeta {
	meta := metav1.ObjectMeta{Name: k8sstore.Name(original)}
	k8sstore.SetOriginalName(&meta, original)

	return meta
}

func sortByName[T any](items []T, name func(T) string) {
	sort.Slice(items, func(i, j int) bool { return name(items[i]) < name(items[j]) })
}

func ptr[T any](v T) *T {
	return &v
}

func orZero(v *int32) int32 {
	if v == nil {
		return 0
	}

	return *v
}
