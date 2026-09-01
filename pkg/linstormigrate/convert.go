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
	"slices"
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
	convertedRD map[string]bool // by resource_name (UPPERCASE key)
	// remappedPools names the ZFS pools carried over as ZFS_THIN, so the
	// resource groups that could place on one can learn about the new
	// kind — and, just as importantly, so the ones that could not are
	// left alone.
	//
	// Keyed by pool NAME, though a LINSTOR pool is node-scoped and the
	// same name routinely exists on every node with different drivers.
	// That is deliberate rather than a simplification: the only consumer
	// is a resource group's placement policy, and a group's pool list
	// carries no node. A group admitted to "TANK" is admitted to TANK on
	// every node, so a per-node distinction here could not be expressed
	// downstream even if it were carried. Where the same name is thick on
	// one node and thin on another, that ambiguity is the finding, and
	// placementLists reports it rather than resolving it.
	remappedPools map[string]bool
	// declaredThinPools names the pools the source cluster already
	// declared ZFS_THIN, by name for the same reason. One node declaring
	// a name thin is enough to taint the name for a group that was kept
	// away from thin pools.
	declaredThinPools map[string]bool
	convertedNode     map[string]bool // by node_name (UPPERCASE key)
	convertedRG       map[string]bool // by resource_group_name (UPPERCASE key)

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
		dump:              dump,
		opts:              opts,
		props:             NewPropsIndex(dump.PropsContainers),
		nodeDsp:           map[string]string{},
		poolDsp:           map[string]string{},
		rgDsp:             map[string]string{},
		rdDsp:             map[string]string{},
		drbdRD:            map[string]LayerDrbdResourceDefinitionRow{},
		drbdMinor:         map[volumeKey]*int32{},
		drbdNode:          map[replicaKey]*int32{},
		storVol:           map[volumeReplicaKey]LayerStorageVolumeRow{},
		luksVol:           map[volumeReplicaKey]LayerLuksVolumeRow{},
		convertedRD:       map[string]bool{},
		convertedNode:     map[string]bool{},
		convertedRG:       map[string]bool{},
		remappedPools:     map[string]bool{},
		declaredThinPools: map[string]bool{},
		warnedKey:         map[string]bool{},
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

const (
	// providerKindZFS / providerKindZFSThin are the StoragePool
	// providerKind values for thick and thin ZFS.
	providerKindZFS     = "ZFS"
	providerKindZFSThin = "ZFS_THIN"

	// storDriverZfscreateOptions holds extra flags LINSTOR appends to
	// `zfs create`; zfsSparseFlag is the one that makes a zvol sparse.
	storDriverZfscreateOptions = "StorDriver/ZfscreateOptions"
	zfsSparseFlag              = "-s"
)

// createsSparseZvols reports whether a pool LINSTOR declares as thick
// ZFS in fact holds sparse volumes.
//
// LINSTOR allows the combination: the driver says ZFS, while
// StorDriver/ZfscreateOptions carries `-s`, so every zvol is created
// sparse. The pool is thick by declaration and thin in practice, and
// LINSTOR is content to leave it oversubscribed.
//
// blockstor cannot express that. Its thick ZFS provider reserves each
// volume's full size, and applies the rule to volumes it adopts as
// readily as to ones it creates, so migrating such a pool as ZFS
// retroactively converts every volume to thick: the pool fills up
// during adoption and whatever no longer fits cannot be adopted at all.
// On the cluster this was found on, one node went from 238G free to
// 8.63G before a 50G volume finally had nowhere to go.
//
// Mapping the pool to ZFS_THIN preserves what the volumes are, which is
// what the data plane depends on, at the cost of the declared kind
// changing. The reverse — honouring the declaration — silently changes
// the capacity model of a running cluster, so this is the safer of the
// two lies. Only ZFS is affected: LVM's thin provisioning is a separate
// driver over a thin pool, not a create-time flag.
func createsSparseZvols(driver, options string) bool {
	if driver != providerKindZFS {
		return false
	}

	// A flag, not a substring: `-o something-s` must not match, and
	// neither must a longer option that merely starts with `-s`.
	return slices.Contains(strings.Fields(options), zfsSparseFlag)
}

// zfscreateOptions resolves StorDriver/ZfscreateOptions the way LINSTOR
// resolves it, rather than reading a single bag.
//
// LINSTOR looks StorDriver/* up through a priority chain: the storage
// pool's own props, then the node's, then the controller's. The value
// it finds first is what gets appended to `zfs create`. So an operator
// who made a whole cluster sparse with
//
//	linstor controller set-property StorDriver/ZfscreateOptions -s
//
// — the natural way to do it, and the way it was done on the cluster
// this detection came from — leaves the per-pool bags empty. Reading
// only the pool bag then reports the pool as genuinely thick, migrates
// it as ZFS, and walks straight into the pool-overflow during adoption
// that createsSparseZvols exists to prevent.
func (c *converter) zfscreateOptions(node, pool string) string {
	for _, bag := range []map[string]string{
		c.props.StorPool(node, pool),
		c.props.Node(node),
		c.props.Controller,
	} {
		if value, ok := bag[storDriverZfscreateOptions]; ok {
			return value
		}
	}

	return ""
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

		props := c.props.StorPool(row.NodeName, row.PoolName)

		kind := row.DriverName
		if kind == providerKindZFSThin {
			c.declaredThinPools[row.PoolName] = true
		}

		if createsSparseZvols(kind, c.zfscreateOptions(row.NodeName, row.PoolName)) {
			kind = providerKindZFSThin
			c.remappedPools[row.PoolName] = true

			c.warnf("storage pool %s.%s: declared %s, but %s makes every zvol sparse — migrated as %s so the adopted volumes keep the provisioning they actually have",
				poolDsp, nodeDsp, providerKindZFS, storDriverZfscreateOptions, providerKindZFSThin)
		}

		pool := crdv1alpha1.StoragePool{
			TypeMeta:   typeMeta("StoragePool"),
			ObjectMeta: objectMeta(strings.ToLower(poolDsp) + "." + strings.ToLower(nodeDsp)),
			Spec: crdv1alpha1.StoragePoolSpec{
				NodeName:     nodeDsp,
				PoolName:     poolDsp,
				ProviderKind: kind,
				Props:        props,
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

		providerPins, poolPins := c.placementLists(row, dsp)

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
					StoragePoolList:         poolPins,
					StoragePoolDisklessList: c.parseList(row.PoolNameDiskless, "resource group "+dsp+" pool_name_diskless"),
					NodeNameList:            c.parseList(row.NodeNameList, "resource group "+dsp+" node_name_list"),
					ReplicasOnSame:          c.parseList(row.ReplicasOnSame, "resource group "+dsp+" replicas_on_same"),
					ReplicasOnDifferent:     c.parseList(row.ReplicasOnDifferent, "resource group "+dsp+" replicas_on_different"),
					NotPlaceWithRsc:         c.parseList(row.DoNotPlaceWithRsc, "resource group "+dsp+" do_not_place_with_rsc_list"),
					ProviderList:            providerPins,
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

// placementLists carries the resource group's provider allow-list and
// storage-pool pins over together.
//
// They are decided in one place because they answer one question: after
// this migration, can the group still place exactly where it could
// before, and nowhere else? The placer filters candidate pools on the
// provider list with an exact match, so a group pinned to ZFS stops
// seeing a pool that migrated as ZFS_THIN — and a cluster that
// deliberately ran thick-declared sparse ZFS is exactly the kind that
// pins it. Left alone, the adopted volumes keep working but nothing new
// can be placed and a lost replica cannot be healed, silently, until
// someone edits the group.
//
// ZFS_THIN is added rather than substituted: a cluster may hold both a
// remapped pool and a genuinely thick one, and replacing the entry
// would evict the second from a policy that legitimately named it.
func (c *converter) placementLists(row *ResourceGroupRow, dsp string) ([]string, []string) {
	providers := c.parseList(row.AllowedProviderList, "resource group "+dsp+" allowed_provider_list")
	pools := c.parseList(row.PoolName, "resource group "+dsp+" pool_name")

	if !slices.Contains(providers, providerKindZFS) || slices.Contains(providers, providerKindZFSThin) {
		return providers, pools
	}

	// Only a group that could actually place on a remapped pool needs
	// the widening. Applying it whenever ANY pool in the cluster
	// remapped hands ZFS_THIN to groups that never had it, which is how
	// a group that named [ZFS] precisely to keep its replicas off a thin
	// pool ends up allowed onto it.
	if !c.reaches(pools, c.remappedPools) {
		return providers, pools
	}

	widened := append(providers, providerKindZFSThin) //nolint:gocritic // a new list, not an append to the caller's

	excluded := c.declaredThinPools

	// A pin scopes the widening only when none of the pools it names was
	// already thin in the source. Where one was, the group could reach
	// that pool by name but the provider filter kept it out, and widening
	// the kind removes the only thing that did — the two filters are
	// independent at the placer, so the named pool becomes placeable.
	//
	// Where the pins can be scoped, they are. Dropping a pool the group
	// could not place on anyway takes no reachability away: the provider
	// filter already excluded it, and the pin list that remains describes
	// exactly what the group could reach before. Leaving the allow-list
	// alone instead is not the conservative choice it looks like — every
	// named pool may have remapped, and then the group's own [ZFS] filter
	// matches none of them and it has nowhere left to place at all.
	if len(pools) > 0 {
		if pinnedNames(pools, excluded) {
			scoped := pinsExcept(pools, excluded)

			// Scoping answers the shape where the excluded pool has
			// its own name. It cannot answer one name that is thick
			// on one node and thin on another: dropping it removes
			// the remapped pool along with the thin one, which is
			// why this falls through rather than returning an empty
			// pin list — an empty list reads as "no restriction" at
			// the placer and would widen the group to the cluster.
			if len(scoped) > 0 && c.reaches(scoped, c.remappedPools) {
				c.warnPoolRemapScoped(dsp, pools, scoped)

				return widened, scoped
			}

			c.warnPoolRemapUnresolvable(dsp)

			return providers, pools
		}

		c.warnPoolRemapWidening(dsp)

		return widened, pools
	}

	// Unpinned, so the widening also admits every pool the source
	// cluster declared thin. With none of those, the kind is enough.
	if len(excluded) == 0 {
		c.warnPoolRemapWidening(dsp)

		return widened, pools
	}

	scoped := c.poolsExcept(excluded)

	// An empty pin list is not a narrow one: the placer reads it as "no
	// restriction". So when nothing survives the exclusion — the pool
	// name that remapped on one node is the same name the source
	// cluster declared thin on another, which is what happens whenever
	// pools are named uniformly across nodes — the allow-list simply
	// cannot express the operator's intent any more.
	//
	// Refuse to widen rather than pick a wrong side quietly. The group
	// keeps its original list, so a new placement fails visibly instead
	// of landing on the pool the operator excluded; the warning says
	// what to fix by hand.
	if len(scoped) == 0 {
		c.warnf("resource group %s: allow-list left as-is — a pool it can place on migrated to %s, but the "+
			"same pool name is declared thin elsewhere in the cluster, so widening the list would admit a pool "+
			"this group could not use before. Pin the group to the pools it should use, by hand",
			dsp, providerKindZFSThin)

		return providers, pools
	}

	c.warnf("resource group %s: pinned to %d pool(s) — it named none, and widening its allow-list to %s "+
		"to keep the remapped pool reachable would otherwise also admit the %d pool name(s) the source cluster "+
		"declared thin, which it could not place on before",
		dsp, len(scoped), providerKindZFSThin, len(excluded))

	return widened, scoped
}

// pinnedNames reports whether a group's pool pins name any pool the source
// cluster declared thin. Compared case-insensitively on the upper-cased key
// the dump uses, since a pin is written by an operator and the dump's keys
// are canonical.
func pinnedNames(pinned []string, excluded map[string]bool) bool {
	for _, pool := range pinned {
		if excluded[strings.ToUpper(pool)] {
			return true
		}
	}

	return false
}

// pinsExcept lists the pins that are not in the excluded set, keeping the
// operator's own casing and order. Compared on the upper-cased key for the
// same reason pinnedNames is: the dump's pool names are canonical while its
// resource-group pins carry whatever case the operator typed.
func pinsExcept(pinned []string, excluded map[string]bool) []string {
	kept := make([]string, 0, len(pinned))

	for _, pool := range pinned {
		if !excluded[strings.ToUpper(pool)] {
			kept = append(kept, pool)
		}
	}

	return kept
}

// warnPoolRemapScoped reports the case where the widening was scoped by
// dropping pins the group could not place on before.
func (c *converter) warnPoolRemapScoped(dsp string, before, after []string) {
	c.warnf("resource group %s: allows %s and a pool it can place on migrated as %s — %s added to the "+
		"allow-list and the pins narrowed from %v to %v, dropping pools this group's provider filter "+
		"already kept it off, so it places exactly where it did before",
		dsp, providerKindZFS, providerKindZFSThin, providerKindZFSThin, before, after)
}

// warnPoolRemapUnresolvable reports the case where the group's own pins
// already reach a pool the source declared thin, so widening the kind cannot
// be scoped and the allow-list is left as it was.
func (c *converter) warnPoolRemapUnresolvable(dsp string) {
	c.warnf("resource group %s: allow-list left as-is — a pool it can place on migrated to %s, but the "+
		"group is pinned to a pool the source cluster declared thin, so widening the list would admit a "+
		"pool this group could not use before. Resolve the pins by hand",
		dsp, providerKindZFSThin)
}

// warnPoolRemapWidening reports the plain widening case, where nothing
// beyond the remapped pool becomes reachable.
func (c *converter) warnPoolRemapWidening(dsp string) {
	c.warnf("resource group %s: allows %s and a pool it can place on migrated as %s — %s added to the allow-list, "+
		"or the placer would stop finding any eligible pool",
		dsp, providerKindZFS, providerKindZFSThin, providerKindZFSThin)
}

// reaches reports whether a group pinned to the named pools (or to none
// at all, meaning any) can place on one of the pools in the set.
func (c *converter) reaches(pinned []string, set map[string]bool) bool {
	if len(pinned) == 0 {
		return len(set) > 0
	}

	for _, pool := range pinned {
		if set[strings.ToUpper(pool)] {
			return true
		}
	}

	return false
}

// poolsExcept lists every converted pool that is not in the excluded
// set, sorted so the output is stable.
func (c *converter) poolsExcept(excluded map[string]bool) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(c.dump.NodeStorPools))

	for i := range c.dump.NodeStorPools {
		row := &c.dump.NodeStorPools[i]
		if excluded[row.PoolName] || seen[row.PoolName] || !c.convertedNode[row.NodeName] {
			continue
		}

		seen[row.PoolName] = true

		out = append(out, displayName(c.poolDsp[row.PoolName], row.PoolName))
	}

	sort.Strings(out)

	return out
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

		if !c.rdConvertible(row, dsp) {
			continue
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

		latch := c.rdInitializedLatch(row.ResourceName)

		// A definition none of whose replicas came across is only
		// "fresh" when they were all being deleted anyway. If any was
		// held back because its node did not migrate, the data is
		// still out there and the latch stays on.
		adopted := latch.adopted || latch.heldByAbsentNode

		switch {
		case latch.onlyDivergent:
			c.warnf("resource definition %s: every replica spans multiple storage pools and is skipped, so the definition arrives with no replicas at all — Initialized stays latched because the data exists, but resolve the divergence or replicas placed later sit Inconsistent", dsp)
		case latch.adopted:
		case latch.heldByAbsentNode:
			c.warnf("resource definition %s: every replica lives on a node that was not migrated — Initialized kept latched, because that data still exists; adopt those nodes or the definition stays without replicas", dsp)
		default:
			c.warnf("resource definition %s: no replica is being migrated — Initialized left unlatched so blockstor can seed a first sync", dsp)
		}

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
				//
				// Only where a replica is actually being adopted. The
				// latch also suppresses the auto-primary election that
				// seeds a first sync, so latching it on a definition
				// whose replicas were all skipped strands the volume:
				// nothing holds the data, and nothing is allowed to
				// become the source.
				Initialized: ptr(adopted),
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
// rdConvertible reports whether this resource definition should convert
// at all, and says why whenever it should not. Both rejections are
// terminal for the definition and for every replica of it, since
// convertResources drops replicas whose parent did not convert.
func (c *converter) rdConvertible(row *ResourceDefinitionRow, dsp string) bool {
	if row.ResourceFlags&resourceFlagDelete != 0 {
		c.warnf("resource definition %s: marked DELETE in the source cluster — skipped", dsp)

		return false
	}

	if row.ResourceFlags != 0 {
		c.warnf("resource definition %s: unhandled flags bitmask %d dropped", dsp, row.ResourceFlags)
	}

	if reason := c.volumelessReason(row.ResourceName); reason != "" {
		c.warnf("resource definition %s: %s — skipped; there is no volume to adopt, and migrating it leaves the controller placing replicas the satellite can never bring up", dsp, reason)

		return false
	}

	return true
}

// rdHasVolumeDefinitions reports whether this resource definition owns a
// volume that will convert.
//
// A definition with no volumes cannot become a usable volume: there is
// no size, and no DRBD minor to allocate. Migrating one still gives the
// controller something to place replicas for, so it allocates a port and
// three Resources, and the satellite then spins forever on "waiting for
// controller-side DRBD-ID allocation" — no .res file, no backing device,
// and a hot reconcile loop. The CLI shows the replicas as `Unknown`
// because there is no kernel state to report.
//
// LINSTOR leaves such definitions behind: a production dump carried one
// with zero volumes whose only replica was already flagged DELETE. Skip
// it and say so. An operator who genuinely wanted an empty definition
// can recreate it in one command; a wedged reconcile loop is the worse
// outcome. Deliberately checked silently: volumeDefinitionsFor reports
// each DELETE'd volume itself, and calling it here would double up.
func (c *converter) volumelessReason(rdName string) string {
	sawDeleted := false

	for i := range c.dump.VolumeDefinitions {
		vd := &c.dump.VolumeDefinitions[i]

		if vd.ResourceName != rdName || vd.SnapshotName != "" {
			continue
		}

		if vd.VlmFlags&resourceFlagDelete != 0 {
			sawDeleted = true

			continue
		}

		return ""
	}

	// Diagnostic, but it is the operator's only trail: a definition
	// rejected here never reaches volumeDefinitionsFor, so the
	// per-volume "marked DELETE" lines never appear for exactly these.
	if sawDeleted {
		return "every volume definition is marked DELETE in the source cluster"
	}

	return "no volume definitions"
}

// rdLatchState is what rdInitializedLatch found out about a resource
// definition's replicas.
type rdLatchState struct {
	// adopted is true when at least one replica survives conversion.
	adopted bool
	// heldByAbsentNode is true when a replica was skipped only because
	// its host node did not migrate. The data is still on that node's
	// disk, so the definition is not fresh even though nothing was
	// adopted from it.
	heldByAbsentNode bool
	// onlyDivergent is true when every replica that kept the latch on
	// spans multiple storage pools. convertResources drops each of
	// those, so the definition lands latched and with no replicas at
	// all — the shape that sits Inconsistent forever if the controller
	// later places fresh ones.
	onlyDivergent bool
}

// rdInitializedLatch decides whether Initialized may be dropped.
//
// Only one reason to skip a replica means the data is genuinely gone:
// the source cluster had already flagged it DELETE. A replica skipped
// because its host node is missing from the dump — an incomplete or
// staged dump, an unknown node_type, a CONTROLLER node — still exists
// on disk. Unlatching there is the direction this must never take: the
// controller would place fresh replicas, elect an auto-primary and seed
// a blank first sync, and when the operator later brings that node in,
// its real replica becomes SyncTarget of the blank set and is
// overwritten.
//
// The per-replica pool-divergence skip is deliberately not counted:
// treating a doubtful replica as adopted keeps the latch ON, the safe
// direction.
func (c *converter) rdInitializedLatch(rdName string) rdLatchState {
	var state rdLatchState

	for i := range c.dump.Resources {
		row := &c.dump.Resources[i]

		if row.ResourceName != rdName || row.SnapshotName != "" {
			continue
		}

		if row.ResourceFlags&resourceFlagDelete != 0 {
			continue
		}

		if !c.convertedNode[row.NodeName] {
			state.heldByAbsentNode = true

			continue
		}

		// A replica with no storage cannot be the evidence that data
		// was adopted — a diskless client or a quorum witness carries
		// no copy of anything. The latch means "this definition already
		// holds data, do not re-initialise it", so letting a witness
		// set it would assert that about a definition whose every
		// data-bearing replica is gone.
		//
		// Keyed on the diskless bit ALONE, matching decodeResourceFlags:
		// TIE_BREAKER is only meaningful together with diskless, and
		// production dumps carry the bit stuck on replicas that are
		// diskful, left behind by an auto-diskful toggle. Testing it on
		// its own would drop such a replica from the evidence set while
		// convertResources adopts it as diskful — and a definition whose
		// only adopted replica was dropped this way un-latches, which
		// re-enables auto-primary election and lets an empty first sync
		// overwrite the data that was just adopted. The skip has to be
		// narrower than the adoption, never wider.
		if row.ResourceFlags&resourceFlagDiskless != 0 {
			continue
		}

		// A pool-divergent replica still counts: treating a doubtful
		// one as adopted keeps the latch ON, the safe direction. But
		// convertResources will drop it, so remember that this is all
		// that held the latch.
		if _, divergent := c.replicaPoolDivergence(row); divergent {
			state.adopted = true
			state.onlyDivergent = true

			continue
		}

		state.adopted = true
		state.onlyDivergent = false

		return state
	}

	return state
}

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

	// blockstor's ZFS provider addresses a snapshot as
	// `<pool>/<resource>_00000@<snap>` — volume 0 only (multi-volume
	// snapshot support is not implemented). A snapshot that captured
	// more than one volume therefore adopts with only its first volume
	// reachable for restore/delete; report it rather than let the extra
	// volume slots imply coverage that does not exist.
	if len(snap.Spec.VolumeDefinitions) > 1 {
		c.warnf("snapshot %s: captured %d volumes but blockstor addresses only volume 0 (<pool>/<rd>_00000@<snap>) — restore/delete of the other volumes will not find a dataset",
			name, len(snap.Spec.VolumeDefinitions))
	}

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
