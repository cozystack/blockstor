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

// annotationTrue is the canonical truthy annotation value.
const annotationTrue = "true"

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
	// ControllerConfig is the cluster-wide DRBD option singleton
	// distilled from the LINSTOR /CTRL property bag; nil when the
	// source cluster carries no DrbdOptions/* controller props.
	ControllerConfig    *crdv1alpha1.ControllerConfig
	Nodes               []crdv1alpha1.Node
	StoragePools        []crdv1alpha1.StoragePool
	ResourceGroups      []crdv1alpha1.ResourceGroup
	ResourceDefinitions []crdv1alpha1.ResourceDefinition
	Resources           []crdv1alpha1.Resource
	Snapshots           []crdv1alpha1.Snapshot
	Warnings            []string
}

// Options tunes a Convert run.
type Options struct {
	// DRBDPorts maps a ResourceDefinition name (any case — matched
	// case-insensitively) to the TCP port its DRBD replication mesh
	// is CURRENTLY listening on in the running kernel. LINSTOR 1.33
	// stores port=-1 in its database for most RDs (the port is
	// assigned at runtime and not persisted), so the dump alone
	// cannot recover it. Capturing the live ports and feeding them
	// here makes blockstor render the SAME ports it adopts, so
	// `drbdadm adjust` is a no-op on the connection endpoint — no
	// reconnect blip, no transient quorum loss. When a port is
	// absent here and also absent from the dump, the RD's replicas
	// get nil DRBDPort and blockstor's allocator picks a fresh port
	// (a controlled replication blip on adoption — see the runbook).
	DRBDPorts map[string]int32
}

// converter carries the indexes built once per Convert call.
type converter struct {
	dump  *Dump
	opts  Options
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

	// referential-integrity sets, populated as each kind converts so
	// later kinds can drop rows that would dangle (a replica of a
	// skipped/absent RD, a replica on an unknown node, ...).
	convertedRD   map[string]bool // by resource_name (UPPERCASE key)
	convertedNode map[string]bool // by node_name (UPPERCASE key)
	convertedRG   map[string]bool // by resource_group_name (UPPERCASE key)

	warnings  []string
	warnedKey map[string]bool
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

// Convert translates a LINSTOR database dump into blockstor CRDs with
// default options.
func Convert(dump *Dump) (*Result, error) {
	return ConvertWithOptions(dump, Options{})
}

// ConvertWithOptions translates a LINSTOR database dump into blockstor
// CRDs, honouring opts (e.g. a live DRBD port map — see Options).
func ConvertWithOptions(dump *Dump, opts Options) (*Result, error) {
	conv := &converter{
		dump:          dump,
		opts:          opts,
		props:         NewPropsIndex(dump.PropsContainers),
		nodeDsp:       map[string]string{},
		poolDsp:       map[string]string{},
		rgDsp:         map[string]string{},
		rdDsp:         map[string]string{},
		drbdRD:        map[string]LayerDrbdResourceDefinitionRow{},
		drbdMinor:     map[volumeKey]*int32{},
		drbdNode:      map[replicaKey]*int32{},
		storVol:       map[volumeReplicaKey]LayerStorageVolumeRow{},
		luksVol:       map[volumeReplicaKey]LayerLuksVolumeRow{},
		convertedRD:   map[string]bool{},
		convertedNode: map[string]bool{},
		convertedRG:   map[string]bool{},
		warnedKey:     map[string]bool{},
	}

	conv.buildIndexes()

	res := &Result{
		ControllerConfig:    conv.convertControllerConfig(),
		Nodes:               conv.convertNodes(),
		StoragePools:        conv.convertStoragePools(),
		ResourceGroups:      conv.convertResourceGroups(),
		ResourceDefinitions: conv.convertResourceDefinitions(),
		Resources:           conv.convertResources(),
		Snapshots:           conv.convertSnapshots(),
	}

	conv.reportRemotes()

	res.Warnings = conv.warnings

	return res, nil
}

// convertControllerConfig distills the LINSTOR /CTRL property bag into
// the blockstor ControllerConfig singleton. Only `DrbdOptions/*` keys
// carry over — they set cluster-wide DRBD behaviour (auto tiebreaker,
// verify-alg allow-list, ...) exactly like `linstor controller
// set-property` would through blockstor's REST. Everything else under
// /CTRL is LINSTOR runtime plumbing (NetCom connector ports, TLS
// keystores, Cluster/LocalID, the master-passphrase crypto material)
// that must NOT leak into blockstor.
func (c *converter) convertControllerConfig() *crdv1alpha1.ControllerConfig {
	drbdProps := map[string]string{}

	for key, val := range c.props.Controller {
		if strings.HasPrefix(key, "DrbdOptions/") {
			drbdProps[key] = val
		}
	}

	if len(drbdProps) == 0 {
		return nil
	}

	typed, _, extra := k8sstore.SplitProps(drbdProps)

	return &crdv1alpha1.ControllerConfig{
		TypeMeta:   typeMeta("ControllerConfig"),
		ObjectMeta: metav1.ObjectMeta{Name: crdv1alpha1.ControllerConfigName},
		Spec: crdv1alpha1.ControllerConfigSpec{
			DRBDOptions: typed,
			ExtraProps:  extra,
		},
	}
}

// reportRemotes surfaces operator-created backup-shipping remotes,
// which have no blockstor equivalent. The self-referential
// `local-remote-generated-by-linstor` entry every LINSTOR ≥1.21
// creates automatically is noise and is skipped silently.
func (c *converter) reportRemotes() {
	for i := range c.dump.LinstorRemotes {
		remote := &c.dump.LinstorRemotes[i]
		if strings.HasSuffix(remote.Name, "-GENERATED-BY-LINSTOR") {
			continue
		}

		c.warnf("linstor remote %s (%s): backup-shipping remotes have no blockstor equivalent — not migrated",
			displayName(remote.DspName, remote.Name), remote.URL)
	}
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
		if row.SnapshotName != "" {
			continue
		}

		if row.ResourceNameSuffix != "" {
			// Suffixed layer instances carry external-metadata /
			// cache sub-devices; neither production dump exercises
			// them and blockstor has no equivalent yet. Never drop
			// one silently.
			c.warnf("resource definition %s: DRBD layer suffix %q not migrated", row.ResourceName, row.ResourceNameSuffix)

			continue
		}

		c.drbdRD[row.ResourceName] = *row
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

		// NODES.node_flags carries DELETE / EVICTED markers. The bit
		// values are not calibrated against a live cluster (no dump
		// observed a non-zero value), so decoding them would be a guess
		// — report instead, consistent with every other kind's
		// never-guess handling, so an operator sees a node the source
		// cluster had marked for removal or eviction.
		if row.NodeFlags != 0 {
			c.warnf("node %s: non-zero node_flags %d (DELETE/EVICTED markers are not decoded) — node migrated as-is, verify it should be adopted", dsp, row.NodeFlags)
		}

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

		c.convertedNode[row.NodeName] = true

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

	for i := range c.dump.NodeStorPools {
		row := &c.dump.NodeStorPools[i]
		nodeDsp := c.displayNode(row.NodeName)
		poolDsp := displayName(c.poolDsp[row.PoolName], row.PoolName)

		// A pool on a node that did not convert (e.g. a CONTROLLER-only
		// node blockstor never adopts) is dead config — drop it.
		if !c.convertedNode[row.NodeName] {
			c.warnf("storage pool %s.%s: host node was not migrated — skipped", poolDsp, nodeDsp)

			continue
		}

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

		c.convertedRG[row.ResourceGroupName] = true

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

		// Referential integrity: an RD may name a resource group that was
		// not migrated (an incomplete dump, or an RG the source cluster
		// deleted). blockstor spawns from an RG but does not require it to
		// exist for an already-materialised RD, so keep the RD (dropping
		// it would lose a real volume) but clear the dangling reference
		// and report it rather than emit an RD pointing at nothing.
		rgRef := c.displayRG(row.ResourceGroupName)
		if row.ResourceGroupName != "" && !c.convertedRG[row.ResourceGroupName] {
			c.warnf("resource definition %s: resource group %q was not migrated — reference cleared", dsp, rgRef)

			rgRef = ""
		}

		typed, residual, extra := k8sstore.SplitProps(c.props.ResourceDefinition(row.ResourceName))

		def := crdv1alpha1.ResourceDefinition{
			TypeMeta:   typeMeta("ResourceDefinition"),
			ObjectMeta: objectMeta(dsp),
			Spec: crdv1alpha1.ResourceDefinitionSpec{
				ResourceGroupName: rgRef,
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

		c.attachRDLayerFields(&def, row, dsp)

		c.convertedRD[row.ResourceName] = true

		defs = append(defs, def)
	}

	sortByName(defs, func(d crdv1alpha1.ResourceDefinition) string { return d.Name })

	return defs
}

// attachRDLayerFields fills the DRBD (port, shared-secret), volume and
// LUKS-report bits onto a converted ResourceDefinition.
func (c *converter) attachRDLayerFields(def *crdv1alpha1.ResourceDefinition, row *ResourceDefinitionRow, dsp string) {
	if drbd, ok := c.drbdRD[row.ResourceName]; ok {
		def.Spec.DRBDPort = c.resolveDRBDPort(row.ResourceName, drbd.TCPPort, dsp)

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

	for i := range c.dump.Resources {
		row := &c.dump.Resources[i]
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

		// Referential integrity: never emit a replica whose parent RD
		// or host node did not convert. Such a Resource would dangle
		// (the CRD's <rd>.<node> name references an object that is not
		// applied), so drop it loudly rather than adopt an orphan.
		if !c.convertedRD[row.ResourceName] {
			c.warnf("resource %s: parent resource definition was not migrated — replica skipped", replicaName)

			continue
		}

		if !c.convertedNode[row.NodeName] {
			c.warnf("resource %s: host node %s was not migrated — replica skipped", replicaName, nodeDsp)

			continue
		}

		// blockstor's Resource carries ONE storage pool for the whole
		// replica, while LINSTOR allows a per-volume pool. Collapsing a
		// divergent set to volume 0's pool would send blockstor looking
		// for the other volumes' backing devices in the wrong pool — a
		// fresh empty zvol next to the real data. Never guess: report
		// and skip the replica.
		if pools, divergent := c.replicaPoolDivergence(row); divergent {
			c.warnf("resource %s: volumes span multiple storage pools (%s) but blockstor holds one pool per replica — replica skipped", replicaName, strings.Join(pools, ", "))

			continue
		}

		resources = append(resources, c.buildResource(row, rdDsp, nodeDsp, replicaName))
	}

	sortByName(resources, func(r crdv1alpha1.Resource) string { return r.Name })

	return resources
}

// reportReplicaVolumeFlags surfaces non-zero VOLUMES.vlm_flags on this
// replica's per-volume rows. blockstor's Resource carries no per-volume
// flag field, so nothing is decoded; the bits (DELETE, RESIZE, and
// friends) are reported so a volume the source cluster had marked is
// never adopted silently.
func (c *converter) reportReplicaVolumeFlags(row *ResourceRow, replicaName string) {
	for i := range c.dump.Volumes {
		vol := &c.dump.Volumes[i]
		if vol.NodeName != row.NodeName || vol.ResourceName != row.ResourceName ||
			vol.SnapshotName != "" || vol.VlmFlags == 0 {
			continue
		}

		c.warnf("resource %s volume %d: non-zero vlm_flags %d (not decoded; blockstor has no per-volume flag field) — volume adopted as-is",
			replicaName, vol.VlmNr, vol.VlmFlags)
	}
}

// replicaPoolDivergence reports whether this replica's volumes are
// backed by more than one storage pool (LINSTOR allows a per-volume
// pool; blockstor's Resource holds a single one). Returns the distinct
// pool names, sorted, and whether they diverge. A replica with no
// LAYER_STORAGE_VOLUMES rows at all (diskless) never diverges.
func (c *converter) replicaPoolDivergence(row *ResourceRow) ([]string, bool) {
	seen := map[string]bool{}

	for key, storVol := range c.storVol {
		if key.node == row.NodeName && key.rd == row.ResourceName {
			seen[displayName(c.poolDsp[storVol.StorPoolName], storVol.StorPoolName)] = true
		}
	}

	if len(seen) < 2 {
		return nil, false
	}

	pools := make([]string, 0, len(seen))
	for pool := range seen {
		pools = append(pools, pool)
	}

	sort.Strings(pools)

	return pools, true
}

// buildResource assembles one replica CRD from its RESOURCES row (the
// caller has already applied the DELETE / referential-integrity
// skips).
func (c *converter) buildResource(row *ResourceRow, rdDsp, nodeDsp, replicaName string) crdv1alpha1.Resource {
	flags, rest := decodeResourceFlags(row.ResourceFlags)
	if rest != 0 {
		c.warnf("resource %s: unhandled flags bits %d dropped (of %d)", replicaName, rest, row.ResourceFlags)
	}

	c.reportReplicaVolumeFlags(row, replicaName)

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

	if storVol, ok := c.storVol[volumeReplicaKey{node: row.NodeName, rd: row.ResourceName, vlmNr: 0}]; ok {
		resource.Spec.StoragePool = displayName(c.poolDsp[storVol.StorPoolName], storVol.StorPoolName)
	} else if sp := resource.Spec.Props["StorPoolName"]; sp != "" {
		resource.Spec.StoragePool = sp
	}

	if drbd, ok := c.drbdRD[row.ResourceName]; ok {
		// LINSTOR allocates one cluster-wide TCP port per RD; every
		// replica listens on it. blockstor's per-replica allocator
		// honours a preset value verbatim. The live port map (when
		// supplied) wins over the dump's often-empty tcp_port so the
		// adopted mesh keeps its current endpoint.
		resource.Spec.DRBDPort = c.resolveDRBDPort(row.ResourceName, drbd.TCPPort, rdDsp)
	}

	return resource
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
		case !c.convertedRD[row.ResourceName]:
			c.warnf("snapshot %s: parent resource definition was not migrated — skipped", name)

			continue
		}

		nodes := c.snapshotNodesFor(row.ResourceName, row.SnapshotName, snapDsp)
		if len(nodes) == 0 {
			c.warnf("snapshot %s: no placement on a migrated node — skipped", name)

			continue
		}

		snaps = append(snaps, c.buildSnapshot(row, rdDsp, snapDsp, name, nodes))
	}

	sortByName(snaps, func(s crdv1alpha1.Snapshot) string { return s.Name })

	return snaps
}

// buildSnapshot assembles one adopted Snapshot CRD (the caller has
// applied the FAILED/SUCCESSFUL/parent/nodes skips).
func (c *converter) buildSnapshot(row *ResourceDefinitionRow, rdDsp, snapDsp, name string, nodes []string) crdv1alpha1.Snapshot {
	snap := crdv1alpha1.Snapshot{
		TypeMeta:   typeMeta("Snapshot"),
		ObjectMeta: objectMeta(name),
		Spec: crdv1alpha1.SnapshotSpec{
			ResourceDefinitionName: rdDsp,
			SnapshotName:           snapDsp,
			Props:                  c.props.SnapshotDefinition(row.ResourceName, row.SnapshotName),
			Nodes:                  nodes,
		},
	}

	// The on-disk snapshot ALREADY EXISTS on every listed node — mark the
	// CRD adopted so the controller backfills the terminal Ready state
	// instead of running the suspend→take→resume orchestration (which
	// would freeze production I/O to re-take an existing snapshot). The
	// original creation time rides along for `linstor s l` parity.
	snap.Annotations = map[string]string{crdv1alpha1.AnnotationSnapshotAdopted: annotationTrue}

	if ts := c.snapshotCreatedAtFor(row.ResourceName, row.SnapshotName); ts > 0 {
		snap.Annotations[crdv1alpha1.AnnotationSnapshotAdoptedCreatedAt] = strconv.FormatInt(ts, 10)
	}

	snap.Spec.VolumeDefinitions = c.snapshotVolumeRefsFor(row.ResourceName, row.SnapshotName)

	return snap
}

// resolveDRBDPort picks the DRBD listen port to preset on an RD /
// replica. The live port map (Options.DRBDPorts, captured from the
// running kernel) wins over the dump's tcp_port, because LINSTOR 1.33
// leaves tcp_port empty for most RDs while the mesh really is
// listening on a concrete port — matching it makes adoption's
// `drbdadm adjust` a no-op on the connection endpoint. When the port
// is available in NEITHER source the RD's replicas get nil (blockstor
// allocates a fresh port → a controlled reconnect blip on adoption),
// and the gap is reported so the operator can capture the live ports.
func (c *converter) resolveDRBDPort(rdName string, dumpPort *int32, rdDsp string) *int32 {
	if live, ok := c.opts.DRBDPorts[strings.ToLower(rdName)]; ok {
		return ptr(live)
	}

	if dumpPort != nil && *dumpPort > 0 {
		return dumpPort
	}

	c.warnOncef("drbdport:"+rdName,
		"resource definition %s: no DRBD port in the dump or live map — blockstor will allocate a fresh port; the adopted mesh will reconnect on the new port. Capture live ports (see runbook) to avoid the blip.", rdDsp)

	return nil
}

// snapshotVolumeRefsFor collects the snapshot's captured volume slots
// (number + size) from the snapshot-scoped VOLUME_DEFINITIONS rows.
func (c *converter) snapshotVolumeRefsFor(rdName, snapName string) []crdv1alpha1.SnapshotVolumeRef {
	var out []crdv1alpha1.SnapshotVolumeRef

	for i := range c.dump.VolumeDefinitions {
		vd := &c.dump.VolumeDefinitions[i]
		if vd.ResourceName != rdName || vd.SnapshotName != snapName {
			continue
		}

		out = append(out, crdv1alpha1.SnapshotVolumeRef{
			VolumeNumber: vd.VlmNr,
			SizeKib:      vd.VlmSize,
		})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].VolumeNumber < out[j].VolumeNumber })

	return out
}

// snapshotCreatedAtFor returns the newest create_timestamp (ms epoch)
// across the snapshot's per-node RESOURCES rows — the instant the take
// completed cluster-wide; 0 when the rows carry no timestamp.
func (c *converter) snapshotCreatedAtFor(rdName, snapName string) int64 {
	var newest int64

	for i := range c.dump.Resources {
		row := &c.dump.Resources[i]
		if row.ResourceName == rdName && row.SnapshotName == snapName && row.CreateTimestamp > newest {
			newest = row.CreateTimestamp
		}
	}

	return newest
}

// snapshotNodesFor lists the nodes an adopted Snapshot targets, dropping
// any placement on a node that did not convert (CONTROLLER / unknown /
// skipped). Keeping such a node would make the adopted Snapshot wait for
// terminal readiness from a Node object that never exists. snapName is
// used for the per-exclusion warning.
func (c *converter) snapshotNodesFor(rdName, snapName, snapDsp string) []string {
	var out []string

	for i := range c.dump.Resources {
		row := &c.dump.Resources[i]
		if row.ResourceName != rdName || row.SnapshotName != snapName {
			continue
		}

		if !c.convertedNode[row.NodeName] {
			c.warnf("snapshot %s.%s: placement on un-migrated node %s dropped", c.displayRD(rdName), snapDsp, c.displayNode(row.NodeName))

			continue
		}

		out = append(out, c.displayNode(row.NodeName))
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

// warnOncef emits a warning only the first time it sees dedupKey, so a
// per-RD gap reported from multiple call sites (RD + each replica)
// surfaces once instead of once per object.
func (c *converter) warnOncef(dedupKey, format string, args ...any) {
	if c.warnedKey[dedupKey] {
		return
	}

	c.warnedKey[dedupKey] = true
	c.warnf(format, args...)
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
