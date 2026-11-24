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
	"maps"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/naming"
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

// drbdCramHMACAlgProp is the RD property that turns DRBD peer
// authentication on. blockstor renders it only when it is set, so the
// converter writes it, next to the secret, on exactly the definitions
// LINSTOR authenticated.
const drbdCramHMACAlgProp = "DrbdOptions/Net/cram-hmac-alg"

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
	// is CURRENTLY listening on in the running kernel. LINSTOR keeps
	// each replica's port in its database (tcp_port_list), and the
	// converter takes it from there; the live map is the cross-check.
	// Where the two disagree the kernel's port is kept, so blockstor
	// renders the port it adopts and `drbdadm adjust` is a no-op on
	// the connection endpoint, and the disagreement is reported. When
	// a replica has a port in neither, it gets nil DRBDPort and
	// blockstor's allocator picks a fresh port (a controlled
	// replication blip on adoption — see the runbook).
	DRBDPorts map[string]int32
	// ReplicaDRBDPorts maps a ResourceDefinition name and a node name
	// (both lowercased) to the port that one replica listens on.
	// LINSTOR 1.33 can give each node its own port for one
	// definition; a definition here cross-checks each of its replicas
	// against the port of its own node, and takes precedence over
	// DRBDPorts.
	ReplicaDRBDPorts map[string]map[string]int32

	// LUKS carries the LINSTOR master passphrase and the blockstor
	// master key, for the import of LUKS volume passwords. A dump with
	// LUKS volumes is refused without it: the volumes would never open.
	LUKS *LUKSImport
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
	// dumpPorts is each live DRBD replica's tcp_port_list, by (node,
	// rd); a replica whose row has no such column is absent, and an
	// empty or unreadable list holds no port.
	dumpPorts map[replicaKey][]int32
	// drbdReplicaRows counts the live DRBD replica rows, and
	// portlessDRBDRows the ones with no port in either place LINSTOR
	// keeps it, for checkStorageVersion.
	drbdReplicaRows  int
	portlessDRBDRows int
	drbdSlots        map[replicaKey]*int32 // peer slots by (node, rd)
	storVol          map[volumeReplicaKey]LayerStorageVolumeRow
	luksVol          map[volumeReplicaKey]LayerLuksVolumeRow
	// luksKeys are the imported volume passwords, wrapped under the
	// blockstor master key, by (node, rd, snapshot, vlmNr); luksCount
	// counts them per RD for the report.
	luksKeys  map[luksKeyAt]crdv1alpha1.LUKSKey
	luksCount map[string]int
	// diskStacks is the layer stack each live replica of an RD was
	// actually built with, read off its LAYER_RESOURCE_IDS chain: by
	// resource_name, then by the joined stack, to the stack itself.
	diskStacks map[string]map[string][]string

	// chained is every live replica indexDiskStacks found a layer chain
	// for. A diskful replica without one was built under a stack the
	// dump does not show.
	chained map[replicaKey]bool

	// metaPoolDropped names the definitions whose StorPoolNameDrbdMeta is
	// left off the converted definition and its replicas: their replicas
	// keep their metadata on the data device, and the dispatcher would
	// otherwise read the property and look for a metadata volume.
	metaPoolDropped map[string]bool

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
	// declaredZFSPools names the pools the source declared plain ZFS —
	// the ones a group filtering on [ZFS] could actually place on before
	// the migration. A name in BOTH this and declaredThinPools is thin on
	// one node and thick on another, which is the ambiguity a pool list
	// carrying no node cannot express, so it is reported rather than
	// resolved.
	declaredZFSPools map[string]bool
	// zfsAfterMigration names the pools that still come out ZFS. Used to
	// tell an operator whether a group left unwidened can place anywhere
	// at all, which is the fact the refusal otherwise leaves out.
	zfsAfterMigration map[string]bool
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

// luksKeyAt addresses one volume password: a live replica's when snap is
// empty, a snapshot's otherwise.
type luksKeyAt struct {
	node, rd, snap string
	vlmNr          int32
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
		dumpPorts:         map[replicaKey][]int32{},
		drbdSlots:         map[replicaKey]*int32{},
		storVol:           map[volumeReplicaKey]LayerStorageVolumeRow{},
		luksVol:           map[volumeReplicaKey]LayerLuksVolumeRow{},
		luksKeys:          map[luksKeyAt]crdv1alpha1.LUKSKey{},
		luksCount:         map[string]int{},
		diskStacks:        map[string]map[string][]string{},
		chained:           map[replicaKey]bool{},
		metaPoolDropped:   map[string]bool{},
		convertedRD:       map[string]bool{},
		convertedNode:     map[string]bool{},
		convertedRG:       map[string]bool{},
		remappedPools:     map[string]bool{},
		declaredThinPools: map[string]bool{},
		declaredZFSPools:  map[string]bool{},
		zfsAfterMigration: map[string]bool{},
		warnedKey:         map[string]bool{},
	}

	conv.buildIndexes()

	err := conv.preflight()
	if err != nil {
		return nil, err
	}

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
	conv.reportUnmatchedPortNodes()
	conv.reportDroppedProps()

	err = checkPortCollisions(res.Resources)
	if err != nil {
		return nil, err
	}

	res.Warnings = conv.warnings

	return res, nil
}

// preflight refuses a dump before anything is converted. A dump the API
// server pruned goes first: what it lost cannot be told from the rows
// that are left. Then a wrong passphrase or a password that does not
// decrypt stops the run with no output.
func (c *converter) preflight() error {
	err := c.checkStorageVersion()
	if err != nil {
		return err
	}

	return c.importLUKSKeys()
}

// convertControllerConfig distills the LINSTOR /CTRL and /STLT property
// bags into the blockstor ControllerConfig singleton. Only `DrbdOptions/*`
// keys carry over; everything else in either bag is LINSTOR runtime
// plumbing (NetCom connector ports, TLS keystores, Cluster/LocalID, the
// master-passphrase crypto material) and is listed in the report by
// reportDroppedProps rather than written anywhere.
//
// Both bags are what an operator sets with `linstor controller
// set-property`: LINSTOR keeps the keys its controller acts on (the auto
// tiebreaker, the verify-alg allow-list, ...) in /CTRL and files every
// other key, the `.res` knobs among them, under /STLT, which is the bag
// the satellites render from. It sits below resource, definition, group
// and node in their lookup, which is the rung ControllerConfig holds in
// blockstor's (effectiveprops.Resolve), so the values keep their
// precedence. Node is the one rung blockstor does not resolve, see
// warnUnrenderedDRBDOptions.
//
// A key present in both bags with different values keeps the one LINSTOR
// itself acted on: the /STLT value for a key with a section (it is what
// the .res carried), the /CTRL value for a section-less controller key.
// The other one is reported.
func (c *converter) convertControllerConfig() *crdv1alpha1.ControllerConfig {
	drbdProps := map[string]string{}

	for key, val := range c.props.Controller {
		if strings.HasPrefix(key, drbd.PropPrefix) {
			drbdProps[key] = val
			c.props.carryKey(PropsInstanceController, key)
		}
	}

	for _, key := range slices.Sorted(maps.Keys(c.props.Satellite)) {
		if !strings.HasPrefix(key, drbd.PropPrefix) {
			continue
		}

		val := c.props.Satellite[key]
		c.props.carryKey(PropsInstanceSatellite, key)

		ctrlVal, both := drbdProps[key]

		switch {
		case !both:
			drbdProps[key] = val
		case ctrlVal == val:
		case drbd.SectionLess(key):
			c.warnf("controller property %s: %s and %s disagree (%q vs %q) — kept the %s value, which LINSTOR's controller acts on",
				key, PropsInstanceController, PropsInstanceSatellite, ctrlVal, val, PropsInstanceController)
		default:
			drbdProps[key] = val
			c.warnf("controller property %s: %s and %s disagree (%q vs %q) — kept the %s value, which LINSTOR's satellites rendered",
				key, PropsInstanceController, PropsInstanceSatellite, ctrlVal, val, PropsInstanceSatellite)
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

// reportDroppedProps names every LINSTOR property no blockstor object
// carries, one line per props instance, keys only: some of these hold
// key material, and the key is enough to look the value up in LINSTOR.
func (c *converter) reportDroppedProps() {
	for _, dropped := range c.props.Dropped() {
		if dropped.Withheld {
			c.warnf("LINSTOR props %s: not migrated, would carry a passphrase in plaintext: %s",
				dropped.Instance, strings.Join(dropped.Keys, ", "))

			continue
		}

		c.warnf("LINSTOR props %s: not migrated, no blockstor object carries them: %s",
			dropped.Instance, strings.Join(dropped.Keys, ", "))
	}
}

// warnUnrenderedDRBDOptions reports the DrbdOptions/* keys of a bag that
// is carried onto a blockstor object at a scope blockstor does not
// resolve DRBD options from, so they reach the object but never a .res.
func (c *converter) warnUnrenderedDRBDOptions(what string, props map[string]string) {
	var keys []string

	for key := range props {
		if strings.HasPrefix(key, drbd.PropPrefix) {
			keys = append(keys, key)
		}
	}

	if len(keys) == 0 {
		return
	}

	slices.Sort(keys)

	c.warnf("%s: %s carried onto the object, but blockstor does not resolve DRBD options at this level — the adopted resources render without them; set them on the resource group, definition or controller instead",
		what, strings.Join(keys, ", "))
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

	c.indexDiskStacks()

	for i := range c.dump.LayerDrbdResources {
		row := &c.dump.LayerDrbdResources[i]
		if lri, ok := baseLayer(row.LayerResourceID); ok {
			key := replicaKey{node: lri.NodeName, rd: lri.ResourceName}
			c.drbdNode[key] = ptr(row.NodeID)
			c.indexDumpPort(&lri, row)

			// The count this replica's metadata was created with: kept,
			// so a grow sizes its lower disk for the metadata it has.
			if drbd.ValidatePeerSlots(int64(row.PeerSlots)) == nil {
				c.drbdSlots[key] = ptr(row.PeerSlots)
			}
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

// indexDiskStacks reads the layer stack every live replica was built
// with off LAYER_RESOURCE_IDS: the replica's root layer (no parent),
// then each base-suffix child down to STORAGE. The RD row's own
// layer_stack is only what was asked for at creation, and is often
// empty, LINSTOR having resolved the stack per resource; this chain is
// what is on disk. A diskless replica is left out: LINSTOR builds it as
// DRBD over STORAGE whatever the definition's stack, so it says nothing
// about how the data was written.
func (c *converter) indexDiskStacks() {
	rows := c.dump.LayerResourceIDs
	children := map[int32]LayerResourceIDRow{}

	diskless := map[replicaKey]bool{}

	for i := range c.dump.Resources {
		res := &c.dump.Resources[i]
		if res.SnapshotName == "" && res.ResourceFlags&resourceFlagDiskless != 0 {
			diskless[replicaKey{node: res.NodeName, rd: res.ResourceName}] = true
		}
	}

	for i := range rows {
		if rows[i].LayerResourceParentID != nil && rows[i].LayerResourceSuffix == "" {
			children[*rows[i].LayerResourceParentID] = rows[i]
		}
	}

	for i := range rows {
		row := rows[i]
		if row.LayerResourceParentID != nil || row.SnapshotName != "" || row.LayerResourceSuffix != "" {
			continue
		}

		if diskless[replicaKey{node: row.NodeName, rd: row.ResourceName}] {
			continue
		}

		var stack []string

		// Bounded by the row count, so a corrupt chain that loops ends.
		for cur, ok := row, true; ok && len(stack) <= len(rows); cur, ok = children[cur.LayerResourceID] {
			stack = append(stack, cur.LayerResourceKind)
		}

		c.chained[replicaKey{node: row.NodeName, rd: row.ResourceName}] = true

		if c.diskStacks[row.ResourceName] == nil {
			c.diskStacks[row.ResourceName] = map[string][]string{}
		}

		c.diskStacks[row.ResourceName][strings.Join(stack, ",")] = stack
	}
}

// stackDisagreement says why the RD's replicas cannot be adopted under one
// stack, or "" when they can: replicas built under different stacks when
// the RD row names none, or a replica built under another stack than the
// one the row names. A row whose stack cannot be read counts as naming
// none, and the refusal names the value; rdLayerStack reports it when the
// definition converts, so it is reported once either way.
func (c *converter) stackDisagreement(row *ResourceDefinitionRow) string {
	stack, readable := rowLayerStack(row)

	// An unreadable column is taken as no stack, the replicas settling it,
	// and a refusal says which it was.
	ownStack := "no layer stack of its own"
	if !readable {
		ownStack = "an unreadable layer_stack " + strconv.Quote(row.LayerStack)
	}

	// With no stack of its own, the RD takes the one its replicas were
	// built with, so every diskful replica has to show it; one the dump
	// carries no chain for leaves that stack unknown. A stack the row names
	// is what LINSTOR built the replicas under, so a missing chain there
	// says nothing new.
	if len(stack) == 0 {
		if node := c.unchainedReplica(row.ResourceName); node != "" {
			return ownStack + ", and the replica on " + c.displayNode(node) +
				" has no layer chain in the dump, so the stack it was built with is unknown"
		}
	}

	keys := make([]string, 0, len(c.diskStacks[row.ResourceName]))
	for key := range c.diskStacks[row.ResourceName] {
		keys = append(keys, key)
	}

	slices.Sort(keys)

	if len(stack) == 0 {
		if len(keys) > 1 {
			return ownStack + ", and its replicas were built as " + strings.Join(keys, " and ")
		}

		return ""
	}

	for _, key := range keys {
		if key != strings.Join(stack, ",") {
			return "layer stack " + strings.Join(stack, ",") + ", but a replica was built as " + key
		}
	}

	return ""
}

// unchainedReplica names the node of a live diskful replica of rdName that
// the dump carries no layer chain for, or "" when every one has its chain.
// Such a replica was built under a stack nobody can read, and taking it as
// agreeing with the others is how a LUKS volume ends up adopted as plain.
// The filter is indexDiskStacks's: no snapshot, not diskless.
func (c *converter) unchainedReplica(rdName string) string {
	for i := range c.dump.Resources {
		res := &c.dump.Resources[i]
		if res.ResourceName != rdName || res.SnapshotName != "" || res.ResourceFlags&resourceFlagDiskless != 0 {
			continue
		}

		if !c.chained[replicaKey{node: res.NodeName, rd: res.ResourceName}] {
			return res.NodeName
		}
	}

	return ""
}

// rdLayerStack is the layer stack a converted RD carries. An RD row
// that names one keeps it; an empty one takes the stack its replicas
// were built with, so the satellite adopts each volume as it is rather
// than as the default, and the group's stack, which blockstor would
// otherwise pin, is never laid over data built without it. An RD whose
// replicas disagree, or whose stack its replicas were not built with,
// never gets here: rdConvertible skips it (stackDisagreement).
func (c *converter) rdLayerStack(row *ResourceDefinitionRow, dsp string) []string {
	stack, readable := rowLayerStack(row)
	onDisk := c.diskStacks[row.ResourceName]

	keys := make([]string, 0, len(onDisk))
	for key := range onDisk {
		keys = append(keys, key)
	}

	slices.Sort(keys)

	if len(stack) > 0 {
		return stack
	}

	if len(keys) == 1 {
		if !readable {
			c.warnf("resource definition %s: unreadable layer_stack %q; took the stack its replicas were built with, %s",
				dsp, row.LayerStack, keys[0])
		}

		return slices.Clone(onDisk[keys[0]])
	}

	if !readable {
		c.warnf("resource definition %s: unreadable layer_stack %q, and no diskful replica to read one from; left unset",
			dsp, row.LayerStack)
	}

	return nil
}

// rowLayerStack reads the RD row's layer_stack column. readable is false
// for a value that is not a JSON list; the stack is then nil, as for an
// empty column, and the caller says so.
func rowLayerStack(row *ResourceDefinitionRow) ([]string, bool) {
	if emptyList(row.LayerStack) {
		return nil, true
	}

	var stack []string

	err := json.Unmarshal([]byte(row.LayerStack), &stack)
	if err != nil {
		return nil, false
	}

	return stack, true
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

		props := c.props.Node(row.NodeName)
		c.warnUnrenderedDRBDOptions("node "+dsp, props)

		node := crdv1alpha1.Node{
			TypeMeta:   typeMeta("Node"),
			ObjectMeta: objectMeta(dsp),
			Spec: crdv1alpha1.NodeSpec{
				Type:  typeName,
				Props: props,
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
//
// A value taken from /STLT or /CTRL is recorded as carried: it decides
// the pool's provider kind, so the report must not list it among the
// props nothing reads.
func (c *converter) zfscreateOptions(node, pool string) string {
	for _, scope := range []struct {
		bag      map[string]string
		instance string
	}{
		{c.props.StorPool(node, pool), ""},
		{c.props.Node(node), ""},
		{c.props.Satellite, PropsInstanceSatellite},
		{c.props.Controller, PropsInstanceController},
	} {
		if value, ok := scope.bag[storDriverZfscreateOptions]; ok {
			if scope.instance != "" {
				c.props.carryKey(scope.instance, storDriverZfscreateOptions)
			}

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
		c.warnUnrenderedDRBDOptions("storage pool "+poolDsp+"."+nodeDsp, props)

		kind := row.DriverName
		if kind == providerKindZFSThin {
			c.declaredThinPools[row.PoolName] = true
		}

		if kind == providerKindZFS {
			c.declaredZFSPools[row.PoolName] = true
		}

		if createsSparseZvols(kind, c.zfscreateOptions(row.NodeName, row.PoolName)) {
			kind = providerKindZFSThin
			c.remappedPools[row.PoolName] = true

			c.warnf("storage pool %s.%s: declared %s, but %s makes every zvol sparse — migrated as %s so the adopted volumes keep the provisioning they actually have",
				poolDsp, nodeDsp, providerKindZFS, storDriverZfscreateOptions, providerKindZFSThin)
		}

		if kind == providerKindZFS {
			c.zfsAfterMigration[row.PoolName] = true
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
				PeerSlots:   c.rgPeerSlots(row, dsp),
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

		c.reportXReplicasOnDifferent(row, dsp)

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

// reportXReplicasOnDifferent names a resource group whose
// x_replicas_on_different the migration drops. The column caps how many
// replicas may share one value of a node property; blockstor's
// ResourceGroup has no field for it, so the replicas the group places
// after the migration are not held to the cap.
func (c *converter) reportXReplicasOnDifferent(row *ResourceGroupRow, dsp string) {
	raw := strings.TrimSpace(row.XReplicasOnDifferent)
	if raw == "{}" || emptyList(raw) {
		return
	}

	var caps map[string]int32

	err := json.Unmarshal([]byte(raw), &caps)
	if err == nil && len(caps) == 0 {
		return
	}

	c.warnf("resource group %s: x_replicas_on_different %s dropped: blockstor has no equivalent, so replicas it places from this group are not held to that limit",
		dsp, raw)
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
		scoped, ambiguous := c.scopePins(pools, excluded)

		switch {
		case ambiguous:
			// One name, thick on one node and thin on another. The
			// pool list carries no node, so neither keeping nor
			// dropping the name expresses what the group had: keeping
			// it admits the thin instance, dropping it loses the
			// thick one. That is the finding, not something to
			// resolve here.
			c.warnPoolRemapUnresolvable(dsp, c.canStillPlace(pools))

			return providers, pools
		case len(scoped) < len(pools):
			c.warnPoolRemapScoped(dsp, pools, scoped)

			return widened, scoped
		default:
			c.warnPoolRemapWidening(dsp)

			return widened, pools
		}
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

// scopePins narrows a group's pool pins to the ones that still describe where
// it could place, and reports the pins it cannot decide by name at all.
//
// A pin every one of whose instances the source declared thin is dropped: the
// provider filter already kept the group off it, so removing the name takes no
// reachability away and stops the widened kind from handing it over. A pin the
// source declared thin on one node and plain ZFS on another cannot be decided
// — the thick instance was reachable and the thin one was not, and one name
// cannot say both — so it is reported instead.
//
// Compared on the upper-cased key: the dump's pool names are canonical while
// its resource-group pins carry whatever case the operator typed.
func (c *converter) scopePins(pinned []string, excluded map[string]bool) ([]string, bool) {
	kept := make([]string, 0, len(pinned))

	for _, pool := range pinned {
		key := strings.ToUpper(pool)

		if !excluded[key] {
			kept = append(kept, pool)

			continue
		}

		if c.declaredZFSPools[key] {
			return nil, true
		}
	}

	return kept, false
}

// canStillPlace reports whether a group left on its original provider list has
// any eligible pool once the migration is done.
func (c *converter) canStillPlace(pinned []string) bool {
	for _, pool := range pinned {
		if c.zfsAfterMigration[strings.ToUpper(pool)] {
			return true
		}
	}

	return false
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
// cannot be decided by name, so the allow-list is left as it was.
//
// stillPlaceable says whether the group has anywhere to put a replica
// meanwhile. When it does not, that is the fact the operator needs first: the
// group is not merely losing new placements on the remapped pool, it cannot
// place at all until someone edits it.
func (c *converter) warnPoolRemapUnresolvable(dsp string, stillPlaceable bool) {
	consequence := "the group keeps placing on its remaining pools, but not on the remapped one"
	if !stillPlaceable {
		consequence = "the group has NO eligible pool at all until this is resolved — every pool it names " +
			"comes out " + providerKindZFSThin + " while its allow-list still says " + providerKindZFS
	}

	c.warnf("resource group %s: allow-list left as-is — it is pinned to a pool name the source cluster "+
		"declared %s on one node and %s on another, so neither keeping nor dropping the name says what "+
		"the group had. Resolve the pins by hand; %s",
		dsp, providerKindZFSThin, providerKindZFS, consequence)
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

		typed, residual, extra := k8sstore.SplitProps(c.withoutDroppedMetaPool(row.ResourceName, c.props.ResourceDefinition(row.ResourceName)))

		latch := c.rdInitializedLatch(row.ResourceName)

		// A definition none of whose replicas came across is only
		// "fresh" when they were all being deleted anyway. If any was
		// held back because its node did not migrate, the data is
		// still out there and the latch stays on.
		adopted := latch.adopted || latch.heldByAbsentNode

		c.reportLatch(dsp, latch, adopted)

		def := crdv1alpha1.ResourceDefinition{
			TypeMeta:   typeMeta("ResourceDefinition"),
			ObjectMeta: objectMeta(dsp),
			Spec: crdv1alpha1.ResourceDefinitionSpec{
				ResourceGroupName: rgRef,
				Props:             residual,
				DRBDOptions:       typed,
				ExtraProps:        extra,
				LayerStack:        c.rdLayerStack(row, dsp),
				PeerSlots:         c.rdPeerSlots(row, dsp),
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
	if row.ResourceFlags&rscDfnFlagDelete != 0 {
		c.warnf("resource definition %s: marked DELETE in the source cluster — skipped", dsp)

		return false
	}

	if row.ResourceFlags != 0 {
		c.warnf("resource definition %s: unhandled flags bitmask %d dropped", dsp, row.ResourceFlags)
	}

	if reason := c.stackDisagreement(row); reason != "" {
		c.warnf("resource definition %s: %s — skipped with its replicas; adopted as it is, a replica would be "+
			"brought up under a stack it was not built with, which for a LUKS volume writes DRBD metadata "+
			"over its ciphertext; settle the stack in LINSTOR and convert again", dsp, reason)

		return false
	}

	reason, remedy, internal := c.externalMetadataReason(row)
	if reason != "" {
		c.warnf("resource definition %s: %s — skipped with its replicas; %s", dsp, reason, remedy)

		return false
	}

	if reason := c.volumelessReason(row.ResourceName); reason != "" {
		c.warnf("resource definition %s: %s — skipped; there is no volume to adopt, and migrating it leaves the controller placing replicas the satellite can never bring up", dsp, reason)

		return false
	}

	if internal != "" {
		c.metaPoolDropped[row.ResourceName] = true
		c.warnf("resource definition %s: %s", dsp, internal)
	}

	return true
}

// externalMetadataReason answers why the replicas of row cannot be adopted
// for where LINSTOR keeps their DRBD metadata, and what the operator does
// about it, both "" when nothing stands in the way. The third answer is a
// note for a definition that converts although a meta pool is set for it.
//
// LINSTOR builds a replica with external metadata with a ".meta" STORAGE
// child under its DRBD layer and records the meta pool on each DRBD volume.
// blockstor cannot adopt that layout: the satellite looks for the metadata
// under its own volume name and does not provision one, so a replica
// converted without it comes up with internal metadata and create-md lands
// on the tail of the data.
//
// StorPoolNameDrbdMeta alone is not that evidence: LINSTOR applies it to
// replicas created after it is set, and decides from the ".meta" child when
// it loads its database. A replica the dump has a chain for and no ".meta"
// keeps its metadata on the data device and converts; the property is left
// off the converted definition and replicas, the two scopes the dispatcher
// reads it from, and the other scopes convert onto objects nothing reads it
// from. A replica the dump has no chain for is decided by the property
// alone, read through every scope LINSTOR takes it from, per volume.
func (c *converter) externalMetadataReason(row *ResourceDefinitionRow) (string, string, string) {
	rdName := row.ResourceName

	if node := c.externalMetadataReplica(rdName); node != "" {
		return "the replica on " + c.displayNode(node) + " keeps its DRBD metadata on a separate volume",
			"blockstor keeps DRBD metadata on the data device, so adopted as it is the satellite would write " +
				"new metadata over the end of the volume's data; copy its data to a volume with internal metadata " +
				"in LINSTOR and convert that one", ""
	}

	if !c.runsDRBD(row) {
		return "", "", ""
	}

	var internal string

	for i := range c.dump.Resources {
		res := &c.dump.Resources[i]
		if res.ResourceName != rdName || res.SnapshotName != "" ||
			res.ResourceFlags&(resourceFlagDiskless|resourceFlagDelete) != 0 {
			continue
		}

		pools := c.drbdMetaPools(row, res.NodeName)
		if len(pools) == 0 {
			continue
		}

		if !c.chained[replicaKey{node: res.NodeName, rd: rdName}] {
			for _, mp := range pools {
				if isExternalMetaPool(mp.pool) {
					return "the dump has no layer chain for the replica on " + c.displayNode(res.NodeName) + ", and " +
							storPoolNameDrbdMeta + " " + mp.pool + " on " + mp.scope +
							" would put its DRBD metadata on a separate volume",
						"check in LINSTOR where that replica keeps its metadata: if on a separate volume, copy its data " +
							"to a volume with internal metadata and convert that one; if on the data device, remove " +
							"the property and convert again", ""
				}
			}
		}

		if internal == "" {
			internal = "its replicas keep their DRBD metadata on the data device, though " + storPoolNameDrbdMeta +
				" " + pools[0].pool + " is set on " + pools[0].scope + "; the property is not carried over to the " +
				"definition or its replicas, so blockstor does not look for a separate metadata volume"
		}
	}

	return "", "", internal
}

// runsDRBD says whether the replicas of row come up with a DRBD layer, and
// so whether a meta pool property means anything for them. A stack the row
// names decides; an empty one is the stack the replicas were built with,
// which stackDisagreement has already found to be one; with neither, the
// controller pins DRBD over STORAGE.
func (c *converter) runsDRBD(row *ResourceDefinitionRow) bool {
	// An unreadable column counts as no stack, as everywhere else in the
	// converter: a partial decode of it says nothing about DRBD.
	stack, _ := rowLayerStack(row)

	if len(stack) == 0 {
		for _, onDisk := range c.diskStacks[row.ResourceName] {
			stack = onDisk
		}
	}

	return len(stack) == 0 || slices.ContainsFunc(stack, func(kind string) bool { return strings.EqualFold(kind, "DRBD") })
}

// externalMetadataReplica names the node of a replica of rdName that keeps
// its DRBD metadata on a separate volume, by its ".meta" layer child or by
// the meta pool on one of its DRBD volumes, or "" when none does. Only what
// converts counts: a replica marked DELETE, and a volume marked DELETE, do
// not. A ".meta" child the dump has no storage rows for still counts, since
// nothing says which volumes it holds.
func (c *converter) externalMetadataReplica(rdName string) string {
	deleted, live := c.deletedReplicaNodes(rdName), c.liveVolumes(rdName)

	byID := map[int32]*LayerResourceIDRow{}
	metaChildren := map[int32]*LayerResourceIDRow{}

	for i := range c.dump.LayerResourceIDs {
		lri := &c.dump.LayerResourceIDs[i]
		if lri.ResourceName != rdName || lri.SnapshotName != "" || deleted[lri.NodeName] {
			continue
		}

		if lri.LayerResourceSuffix == drbdMetaSuffix {
			metaChildren[lri.LayerResourceID] = lri

			continue
		}

		byID[lri.LayerResourceID] = lri
	}

	if node := c.liveMetaChild(metaChildren, live); node != "" {
		return node
	}

	for i := range c.dump.LayerDrbdVolumes {
		vol := &c.dump.LayerDrbdVolumes[i]
		if lri, ok := byID[vol.LayerResourceID]; ok && live[vol.VlmNr] && isExternalMetaPool(vol.PoolName) {
			return lri.NodeName
		}
	}

	return ""
}

// deletedReplicaNodes is the nodes whose replica of rdName is marked DELETE.
func (c *converter) deletedReplicaNodes(rdName string) map[string]bool {
	deleted := map[string]bool{}

	for i := range c.dump.Resources {
		res := &c.dump.Resources[i]
		if res.ResourceName == rdName && res.SnapshotName == "" && res.ResourceFlags&resourceFlagDelete != 0 {
			deleted[res.NodeName] = true
		}
	}

	return deleted
}

// liveVolumes is the volume numbers of rdName that are not marked DELETE.
func (c *converter) liveVolumes(rdName string) map[int32]bool {
	live := map[int32]bool{}

	for i := range c.dump.VolumeDefinitions {
		vd := &c.dump.VolumeDefinitions[i]
		if vd.ResourceName == rdName && vd.SnapshotName == "" && vd.VlmFlags&resourceFlagDelete == 0 {
			live[vd.VlmNr] = true
		}
	}

	return live
}

// liveMetaChild names the node of a ".meta" child that holds a volume in
// live, or that the dump shows no volume for.
func (c *converter) liveMetaChild(children map[int32]*LayerResourceIDRow, live map[int32]bool) string {
	if len(children) == 0 {
		return ""
	}

	vols := map[int32][]int32{}

	for i := range c.dump.LayerStorageVolumes {
		row := &c.dump.LayerStorageVolumes[i]
		if _, ok := children[row.LayerResourceID]; ok {
			vols[row.LayerResourceID] = append(vols[row.LayerResourceID], row.VlmNr)
		}
	}

	ids := make([]int32, 0, len(children))
	for id := range children {
		ids = append(ids, id)
	}

	slices.Sort(ids)

	for _, id := range ids {
		if len(vols[id]) == 0 || slices.ContainsFunc(vols[id], func(nr int32) bool { return live[nr] }) {
			return children[id].NodeName
		}
	}

	return ""
}

// metaPool is the StorPoolNameDrbdMeta one volume resolves to, and the
// scope it came from.
type metaPool struct {
	pool  string
	scope string
}

// drbdMetaPools is the StorPoolNameDrbdMeta LINSTOR resolves for each volume
// of the replica of row on node, for the volumes that resolve one. Each
// volume is resolved on its own, most specific scope first: volume
// definition, volume group, resource, resource definition, resource group,
// node (getExtMetaDataStorPoolName in LINSTOR's RscDrbdLayerHelper).
func (c *converter) drbdMetaPools(row *ResourceDefinitionRow, node string) []metaPool {
	rd, rg := row.ResourceName, row.ResourceGroupName

	shared := func() (metaPool, bool) {
		for _, scope := range []struct {
			props map[string]string
			name  string
		}{
			{c.props.Resource(node, rd), "the replica on " + c.displayNode(node)},
			{c.props.ResourceDefinition(rd), "the definition"},
			{c.resourceGroupProps(rg), "resource group " + rg},
			{c.props.Node(node), "node " + c.displayNode(node)},
		} {
			if v := scope.props[storPoolNameDrbdMeta]; v != "" {
				return metaPool{pool: v, scope: scope.name}, true
			}
		}

		return metaPool{}, false
	}

	var (
		out  []metaPool
		vols int
	)

	for i := range c.dump.VolumeDefinitions {
		vd := &c.dump.VolumeDefinitions[i]
		// A volume marked DELETE is not converted, so its metadata
		// setting does not decide anything for the live ones.
		if vd.ResourceName != rd || vd.SnapshotName != "" || vd.VlmFlags&resourceFlagDelete != 0 {
			continue
		}

		vols++

		vlm := strconv.Itoa(int(vd.VlmNr))
		if v := c.props.VolumeDefinition(rd, vlm)[storPoolNameDrbdMeta]; v != "" {
			out = append(out, metaPool{pool: v, scope: "volume definition " + c.displayRD(rd) + "/" + vlm})

			continue
		}

		if v := c.resourceGroupVolumeProps(rg, vlm)[storPoolNameDrbdMeta]; v != "" {
			out = append(out, metaPool{pool: v, scope: "volume group " + rg + "/" + vlm})

			continue
		}

		if mp, ok := shared(); ok {
			out = append(out, mp)
		}
	}

	if vols == 0 {
		if mp, ok := shared(); ok {
			out = append(out, mp)
		}
	}

	return out
}

func (c *converter) resourceGroupProps(rg string) map[string]string {
	if rg == "" {
		return nil
	}

	return c.props.ResourceGroup(rg)
}

func (c *converter) resourceGroupVolumeProps(rg, vlm string) map[string]string {
	if rg == "" {
		return nil
	}

	return c.props.VolumeGroup(rg, vlm)
}

// withoutDroppedMetaPool takes StorPoolNameDrbdMeta out of the props of a
// definition named in metaPoolDropped, or of one of its replicas. It runs
// before SplitProps, which routes a non-DRBD key to the residual props and to
// the overflow map both: the store merges the overflow back into the props on
// every write, so a copy left there returns with the next property update.
func (c *converter) withoutDroppedMetaPool(rdName string, props map[string]string) map[string]string {
	if !c.metaPoolDropped[rdName] {
		return props
	}

	if _, ok := props[storPoolNameDrbdMeta]; !ok {
		return props
	}

	out := maps.Clone(props)
	delete(out, storPoolNameDrbdMeta)

	if len(out) == 0 {
		return nil
	}

	return out
}

// isExternalMetaPool says whether a meta pool value names a separate pool,
// read as LINSTOR v1.33.0 reads it: blank is unset, and ".internal" is matched
// in any case but untrimmed, so a value with spaces around it is a pool name.
func isExternalMetaPool(pool string) bool {
	return strings.TrimSpace(pool) != "" && !strings.EqualFold(pool, drbdMetaInternal)
}

// The layer suffix and the property LINSTOR marks external DRBD metadata
// with.
const (
	drbdMetaSuffix       = ".meta"
	storPoolNameDrbdMeta = "StorPoolNameDrbdMeta"
	// drbdMetaInternal is the StorPoolNameDrbdMeta value that asks for
	// internal metadata.
	drbdMetaInternal = ".internal"
)

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

// reportLatch names every reason the definition ended up in the latch state
// it did.
//
// Reported independently rather than as a switch: a definition can be held by
// an absent node AND pool-divergent at once, and a switch names only whichever
// branch comes first while the document promises every state is named with the
// reason that applies.
func (c *converter) reportLatch(dsp string, latch rdLatchState, adopted bool) {
	if latch.onlyDivergent {
		c.warnf("resource definition %s: every replica spans multiple storage pools and is skipped, so the definition arrives with no replicas at all — Initialized stays latched because the data exists, but resolve the divergence or replicas placed later sit Inconsistent", dsp)
	}

	if latch.heldByAbsentNode {
		c.warnf("resource definition %s: a replica lives on a node that was not migrated — Initialized kept latched, because that data still exists; adopt those nodes or the definition stays short of replicas", dsp)
	}

	if adopted {
		return
	}

	if latch.onlyDiskless {
		c.warnf("resource definition %s: replicas are being migrated but every one is diskless — a client or a quorum witness carries no copy, so Initialized is left unlatched and blockstor seeds a first sync when a diskful replica is added", dsp)

		return
	}

	c.warnf("resource definition %s: no replica is being migrated — Initialized left unlatched so blockstor can seed a first sync", dsp)
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
	// onlyDiskless is true when replicas ARE being migrated but every one
	// of them is diskless — a client or a quorum witness, carrying no copy
	// of anything. The definition arrives unlatched with its replicas
	// emitted, which is neither of the two shapes above and reads nothing
	// like "no replica is being migrated".
	onlyDiskless bool
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
			state.onlyDiskless = true

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
		state.onlyDiskless = false

		return state
	}

	return state
}

// rdPeerSlots is the peer-slot count a converted definition's new
// replicas get: the count on its DRBD layer, which LINSTOR resolved when
// it created that layer and copied to each replica. A definition with no
// DRBD layer yet, one no replica was ever placed for, gets what LINSTOR
// would resolve when it creates one: PeerSlotsNewResource on the
// definition, its group and the controller, else 7
// (RscDrbdLayerHelper.getPeerSlotsForNewResource). Without a count it
// would be read as the 15 of a definition blockstor created before it
// recorded one.
func (c *converter) rdPeerSlots(row *ResourceDefinitionRow, dsp string) *int32 {
	if layer, ok := c.drbdRD[row.ResourceName]; ok {
		if drbd.ValidatePeerSlots(int64(layer.PeerSlots)) == nil {
			return ptr(layer.PeerSlots)
		}

		c.warnf("resource definition %s: DRBD layer peer_slots %d out of range; resolved as for a new definition",
			dsp, layer.PeerSlots)
	}

	for _, props := range []map[string]string{
		c.props.ResourceDefinition(row.ResourceName),
		c.resourceGroupProps(row.ResourceGroupName),
		c.props.Controller,
	} {
		value, ok := props[drbd.PeerSlotsNewResourceProp]
		if !ok {
			continue
		}

		slots, err := drbd.ParsePeerSlotsProp(value)
		if err == nil {
			return ptr(slots)
		}

		c.warnf("resource definition %s: %v; the next scope decides", dsp, err)
	}

	return ptr(int32(drbd.DefaultPeerSlots))
}

// rgPeerSlots carries a group's `--peer-slots`, which definitions spawned
// from it get.
func (c *converter) rgPeerSlots(row *ResourceGroupRow, dsp string) int32 {
	if row.PeerSlots == nil {
		return 0
	}

	if err := drbd.ValidatePeerSlots(int64(*row.PeerSlots)); err != nil {
		c.warnf("resource group %s: %v; not migrated", dsp, err)

		return 0
	}

	return *row.PeerSlots
}

func (c *converter) attachRDLayerFields(def *crdv1alpha1.ResourceDefinition, row *ResourceDefinitionRow, dsp string) {
	if layer, ok := c.drbdRD[row.ResourceName]; ok {
		def.Spec.DRBDPort = c.resolveDefinitionDRBDPort(row.ResourceName, layer.TCPPort)

		if layer.Secret != "" && c.carrySharedSecret(row, dsp) {
			c.carryAuthentication(def, row.ResourceName, layer.Secret)
		}
	}

	def.Spec.VolumeDefinitions = c.volumeDefinitionsFor(row.ResourceName, dsp)

	if c.rdHasLuks(row.ResourceName) {
		c.warnf("resource definition %s: %d LUKS volume passwords migrated, wrapped under the blockstor master key",
			dsp, c.luksCount[row.ResourceName])
	}
}

// carryAuthentication writes LINSTOR's authentication pair onto the
// converted definition, so the satellite renders the same `net {
// cram-hmac-alg; shared-secret }` the source cluster's kernels already
// run and adoption re-applies an identical config instead of re-keying
// the mesh. The digest is LINSTOR's hard-coded one unless the definition
// names its own, which is then already among the carried props.
func (c *converter) carryAuthentication(def *crdv1alpha1.ResourceDefinition, rdName, secret string) {
	if def.Spec.ExtraProps == nil {
		def.Spec.ExtraProps = map[string]string{}
	}

	def.Spec.ExtraProps[drbdSharedSecretProp] = secret

	if !hasKeyFold(c.props.ResourceDefinition(rdName), drbdCramHMACAlgProp) {
		def.Spec.ExtraProps[drbdCramHMACAlgProp] = drbd.DefaultCramHMACAlg
	}
}

// carrySharedSecret says whether the definition's DRBD secret goes onto
// the converted definition, which is whether LINSTOR put it in the .res.
//
// LINSTOR v1.33 writes `cram-hmac-alg sha1` and `shared-secret` into the
// net section only for a replica whose DrbdOptions/auto-verify-alg
// resolves, through the replica, definition, group, node and satellite
// bags, to a value other than `windrbd`. Where it does not, the kernels
// run with no peer authentication. Carrying the pair there would switch
// authentication on during adoption, one node at a time, and a
// connection re-established between a node already adjusted and one not
// yet would fail the handshake. The secret is left off and the report
// says how to turn authentication on afterwards.
//
// A definition with no live replica has nothing to keep in step with, and
// keeps its secret; one whose replicas disagree keeps it too, since some
// of its kernels authenticate already, and is reported: its other
// replicas start authenticating during adoption, which is that same
// window for them.
func (c *converter) carrySharedSecret(row *ResourceDefinitionRow, dsp string) bool {
	replicas := c.liveReplicas(row.ResourceName)

	authenticated := 0

	for _, replica := range replicas {
		if c.linstorRendersAuth(row, replica.NodeName) {
			authenticated++
		}
	}

	switch {
	case len(replicas) == 0, authenticated == len(replicas):
		return true
	case authenticated == 0:
		c.warnf("resource definition %s: LINSTOR rendered no peer authentication for it (no %s resolves), so the DRBD shared secret is not carried and adoption leaves the connections as they are — to authenticate peers after the cutover set %s and %s together; each satellite applies them on its own, and a connection that re-establishes before both of its peers have does not come up until they have",
			dsp, autoVerifyAlgProp, drbdSharedSecretProp, drbdCramHMACAlgProp)

		return false
	default:
		c.warnf("resource definition %s: LINSTOR authenticated peers on %d of %d replicas (%s resolves on some only) — the shared secret is carried, so every replica authenticates after adoption; until every satellite of the definition has adjusted, a connection that re-establishes between a replica that switched and one that did not does not come up",
			dsp, authenticated, len(replicas), autoVerifyAlgProp)

		return true
	}
}

// linstorRendersAuth resolves DrbdOptions/auto-verify-alg for one replica
// the way LINSTOR's satellite does and reports whether it then writes the
// authentication pair. See carrySharedSecret.
func (c *converter) linstorRendersAuth(row *ResourceDefinitionRow, node string) bool {
	for _, bag := range []map[string]string{
		c.props.Resource(node, row.ResourceName),
		c.props.ResourceDefinition(row.ResourceName),
		c.resourceGroupProps(row.ResourceGroupName),
		c.props.Node(node),
		c.props.Satellite,
	} {
		if value, ok := bag[autoVerifyAlgProp]; ok {
			return !strings.EqualFold(value, autoVerifyAlgWinDRBD)
		}
	}

	return false
}

// hasKeyFold reports whether props has key in any letter case.
func hasKeyFold(props map[string]string, key string) bool {
	for name := range props {
		if strings.EqualFold(name, key) {
			return true
		}
	}

	return false
}

const (
	// autoVerifyAlgProp is the verify algorithm LINSTOR's controller
	// picks for a definition from what every diskful node supports.
	autoVerifyAlgProp = drbd.PropPrefix + drbd.AutoVerifyAlgKey
	// autoVerifyAlgWinDRBD is the value LINSTOR reads as "a Windows peer
	// is involved", which also turns its peer authentication off.
	autoVerifyAlgWinDRBD = "windrbd"
)

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
			Props:        c.volumeDefinitionProps(rdName, rdDsp, vd.VlmNr),
			DRBDMinor:    c.drbdMinor[volumeKey{rd: rdName, vlmNr: vd.VlmNr}],
		})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].VolumeNumber < out[j].VolumeNumber })

	return out
}

// volumeDefinitionProps is the carried bag of one volume definition.
// LINSTOR renders a volume's own DrbdOptions/Disk/* into its volume
// block; blockstor does not, which the report says.
func (c *converter) volumeDefinitionProps(rdName, rdDsp string, vlmNr int32) map[string]string {
	props := c.props.VolumeDefinition(rdName, strconv.Itoa(int(vlmNr)))
	c.warnUnrenderedDRBDOptions(fmt.Sprintf("volume definition %s/%d", rdDsp, vlmNr), props)

	return props
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

	typed, residual, extra := k8sstore.SplitProps(c.withoutDroppedMetaPool(row.ResourceName, c.props.Resource(row.NodeName, row.ResourceName)))

	resource := crdv1alpha1.Resource{
		TypeMeta:   typeMeta("Resource"),
		ObjectMeta: objectMeta(replicaName),
		Spec: crdv1alpha1.ResourceSpec{
			ResourceDefinitionName: k8sstore.Name(rdDsp),
			NodeName:               k8sstore.Name(nodeDsp),
			Props:                  residual,
			DRBDOptions:            typed,
			ExtraProps:             extra,
			Flags:                  flags,
			DRBDNodeID:             c.drbdNode[replicaKey{node: row.NodeName, rd: row.ResourceName}],
			PeerSlots:              c.drbdSlots[replicaKey{node: row.NodeName, rd: row.ResourceName}],
			// Adopted replicas carry real data (or are the witness of
			// a data-bearing set): never allow the day0 skip. The
			// satellite reads false as "not created empty here" too and
			// never promotes such a replica to give it a filesystem.
			SkipInitialSync: ptr(false),
		},
	}

	annotateParent(&resource.ObjectMeta, k8sstore.AnnotationLinstorRDName, rdDsp)
	annotateParent(&resource.ObjectMeta, k8sstore.AnnotationLinstorNodeName, nodeDsp)

	resource.Spec.Volumes = c.importedReplicaKeys(row.NodeName, row.ResourceName)

	if storVol, ok := c.storVol[volumeReplicaKey{node: row.NodeName, rd: row.ResourceName, vlmNr: 0}]; ok {
		resource.Spec.StoragePool = displayName(c.poolDsp[storVol.StorPoolName], storVol.StorPoolName)
	} else if sp := resource.Spec.Props["StorPoolName"]; sp != "" {
		resource.Spec.StoragePool = sp
	}

	if layer, ok := c.drbdRD[row.ResourceName]; ok {
		// blockstor's per-replica allocator honours a preset value
		// verbatim, so each replica gets the port it listens on now.
		resource.Spec.DRBDPort = c.resolveReplicaDRBDPort(row, layer.TCPPort, rdDsp, nodeDsp)
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
		case row.ResourceFlags&snapDfnFlagDelete != 0:
			c.warnf("snapshot %s: marked DELETE in the source cluster — skipped", name)

			continue
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
// applied the DELETE/FAILED/SUCCESSFUL/parent/nodes skips).
func (c *converter) buildSnapshot(row *ResourceDefinitionRow, rdDsp, snapDsp, name string, nodes []string) crdv1alpha1.Snapshot {
	snap := crdv1alpha1.Snapshot{
		TypeMeta:   typeMeta("Snapshot"),
		ObjectMeta: objectMeta(name),
		Spec: crdv1alpha1.SnapshotSpec{
			ResourceDefinitionName: k8sstore.Name(rdDsp),
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
	annotateParent(&snap.ObjectMeta, k8sstore.AnnotationLinstorRDName, rdDsp)

	if ts := c.snapshotCreatedAtFor(row.ResourceName, row.SnapshotName); ts > 0 {
		snap.Annotations[crdv1alpha1.AnnotationSnapshotAdoptedCreatedAt] = strconv.FormatInt(ts, 10)
	}

	snap.Spec.VolumeDefinitions = c.snapshotVolumeRefsFor(row.ResourceName, row.SnapshotName)
	snap.Spec.LUKSKeys = c.importedSnapshotKeys(row.ResourceName, row.SnapshotName)

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

// warnNoPort reports, once per definition, a replica that neither the
// database nor the live map has a port for.
func (c *converter) warnNoPort(rdName, rdDsp string) {
	c.warnOncef("drbdport:"+rdName,
		"resource definition %s: no DRBD port in the dump or live map — blockstor will allocate a fresh port; the adopted mesh will reconnect on the new port. Capture live ports (see runbook) to avoid the blip.", rdDsp)
}

// resolveDefinitionDRBDPort is the preferred port preset on the
// definition itself: the port every replica it adopts is preset to,
// when they all share one, and none otherwise, each replica then
// carrying its own. A definition with no replica to adopt takes the
// definition-wide sources.
func (c *converter) resolveDefinitionDRBDPort(rdName string, dumpPort *int32) *int32 {
	replicas := c.liveReplicas(rdName)
	if len(replicas) == 0 {
		if live, ok := c.opts.DRBDPorts[strings.ToLower(rdName)]; ok {
			return ptr(live)
		}

		if dumpPort != nil && *dumpPort > 0 {
			return dumpPort
		}

		return nil
	}

	var shared int32

	for _, row := range replicas {
		port := c.replicaPort(row, dumpPort)
		if port == nil || (shared != 0 && *port != shared) {
			return nil
		}

		shared = *port
	}

	return ptr(shared)
}

// liveReplicas lists the replica rows of one definition that convert
// into Resources: no snapshot placements, no rows marked DELETE, and
// none on a node that did not migrate.
func (c *converter) liveReplicas(rdName string) []*ResourceRow {
	var out []*ResourceRow

	for i := range c.dump.Resources {
		row := &c.dump.Resources[i]
		if row.ResourceName == rdName && row.SnapshotName == "" &&
			row.ResourceFlags&resourceFlagDelete == 0 && c.convertedNode[row.NodeName] {
			out = append(out, row)
		}
	}

	return out
}

// mappedReplicaPort looks a replica up in one definition's per-replica
// map by its node's key name and by its display name.
func (c *converter) mappedReplicaPort(perReplica map[string]int32, nodeName string) (int32, bool) {
	for _, node := range []string{nodeName, c.displayNode(nodeName)} {
		if port, ok := perReplica[strings.ToLower(node)]; ok {
			return port, true
		}
	}

	return 0, false
}

// anyReplicaMapped reports whether the per-replica map has a port for
// at least one replica of the definition.
func (c *converter) anyReplicaMapped(rdName string, perReplica map[string]int32) bool {
	for _, row := range c.liveReplicas(rdName) {
		if _, ok := c.mappedReplicaPort(perReplica, row.NodeName); ok {
			return true
		}
	}

	return false
}

// reportUnmatchedPortNodes reports per-replica map entries that name a
// node no replica of the definition lives on. When none of a
// definition's entries match, the node names in the map most likely
// differ from LINSTOR's, and every replica will reconnect on a fresh
// port.
func (c *converter) reportUnmatchedPortNodes() {
	for i := range c.dump.ResourceDefinitions {
		row := &c.dump.ResourceDefinitions[i]
		if row.SnapshotName != "" {
			continue
		}

		perReplica := c.opts.ReplicaDRBDPorts[strings.ToLower(row.ResourceName)]
		if len(perReplica) == 0 {
			continue
		}

		// A definition that did not convert, or has no replica that
		// does, gets no port at all: nothing for the operator to fix.
		replicas := c.liveReplicas(row.ResourceName)
		if !c.convertedRD[row.ResourceName] || len(replicas) == 0 {
			continue
		}

		known := map[string]bool{}
		for _, replica := range replicas {
			known[strings.ToLower(replica.NodeName)] = true
			known[strings.ToLower(c.displayNode(replica.NodeName))] = true
		}

		var unmatched []string

		for node := range perReplica {
			if !known[node] {
				unmatched = append(unmatched, node)
			}
		}

		if len(unmatched) == 0 {
			continue
		}

		sort.Strings(unmatched)

		rdDsp := c.displayRD(row.ResourceName)
		if len(unmatched) == len(perReplica) {
			c.reportUnmatchedMap(row.ResourceName, rdDsp, replicas, unmatched)

			continue
		}

		c.warnf("resource definition %s: the live port map names nodes without a replica of it (%s) — those entries are ignored; a leftover DRBD resource on them may hold the port",
			rdDsp, strings.Join(unmatched, ", "))
	}
}

// reportUnmatchedMap reports a definition whose live-map entries all
// name nodes without a replica of it, so its ports were never checked
// against the kernel. The replicas the database has no port for get a
// fresh one.
func (c *converter) reportUnmatchedMap(rdName, rdDsp string, replicas []*ResourceRow, unmatched []string) {
	portless := 0

	for _, replica := range replicas {
		if _, ok := c.dumpReplicaPort(replica, c.drbdRD[rdName].TCPPort); !ok {
			portless++
		}
	}

	if portless == 0 {
		c.warnf("resource definition %s: no node in the live port map (%s) hosts a replica of it — check that the map uses LINSTOR's node names; the database's ports are used, unchecked against the kernel",
			rdDsp, strings.Join(unmatched, ", "))

		return
	}

	which := "any replica"
	if portless < len(replicas) {
		which = fmt.Sprintf("%d of its %d replicas", portless, len(replicas))
	}

	c.warnf("resource definition %s: no DRBD port for %s: no node in the live port map (%s) hosts a replica of it, and the database has none — check that the map uses LINSTOR's node names; each such replica will get a fresh port and reconnect on adoption",
		rdDsp, which, strings.Join(unmatched, ", "))
}

// checkPortCollisions refuses two replicas preset to one port on one
// node. The allocator honours a preset port verbatim and the field is
// set once, so a collision cannot be fixed after apply, and one of the
// two DRBD resources would fail to bind on adoption.
func checkPortCollisions(resources []crdv1alpha1.Resource) error {
	type nodePort struct {
		node string
		port int32
	}

	owner := map[nodePort]string{}

	for i := range resources {
		res := &resources[i]
		if res.Spec.DRBDPort == nil {
			continue
		}

		key := nodePort{node: naming.Key(res.Spec.NodeName), port: *res.Spec.DRBDPort}
		if prev, ok := owner[key]; ok {
			return fmt.Errorf("%w: %s and %s both listen on port %d on node %s",
				ErrPortCollision, prev, res.Name, key.port, res.Spec.NodeName)
		}

		owner[key] = res.Name
	}

	return nil
}

// resolveReplicaDRBDPort picks the port one replica listens on. The
// kernel's, from the live map, is kept, since adopting any other moves
// the endpoint under I/O; the database's is the source when the map
// has none, and a cross-check when it does. A replica with no port in
// either is reported: it gets a fresh one and reconnects on adoption.
func (c *converter) resolveReplicaDRBDPort(row *ResourceRow, dumpPort *int32, rdDsp, nodeDsp string) *int32 {
	live, hasLive := c.livePort(row)
	stored, hasStored := c.dumpReplicaPort(row, dumpPort)

	if hasLive {
		if hasStored && stored != live {
			c.warnOncef("drbdport-mismatch:"+row.ResourceName+":"+row.NodeName,
				"resource %s.%s: LINSTOR's database gives DRBD port %d, the kernel listens on %d; the kernel's port is kept. Find out why they differ before cutover",
				rdDsp, nodeDsp, stored, live)
		}

		return ptr(live)
	}

	if hasStored {
		return ptr(stored)
	}

	perReplica := c.opts.ReplicaDRBDPorts[strings.ToLower(row.ResourceName)]
	if len(perReplica) == 0 {
		c.warnNoPort(row.ResourceName, rdDsp)

		return nil
	}

	if !c.anyReplicaMapped(row.ResourceName, perReplica) {
		return nil // reportUnmatchedPortNodes names the mismatch once per definition
	}

	c.warnOncef("drbdport:"+row.ResourceName+":"+row.NodeName,
		"resource %s.%s: no DRBD port for this replica in the live map, which has ports for the other replicas of %s, nor in the database — blockstor will allocate a fresh port for it, and this replica will reconnect on adoption",
		rdDsp, nodeDsp, rdDsp)

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
// terminal readiness from a Node object that never exists. A placement
// LINSTOR had marked DELETE is dropped too: its snapshot is on the way
// out of that node, and adopting it there would bring it back.
// snapName is used for the per-exclusion warning.
func (c *converter) snapshotNodesFor(rdName, snapName, snapDsp string) []string {
	var out []string

	for i := range c.dump.Resources {
		row := &c.dump.Resources[i]
		if row.ResourceName != rdName || row.SnapshotName != snapName {
			continue
		}

		if row.ResourceFlags&snapFlagDelete != 0 {
			c.warnf("snapshot %s.%s: placement on node %s marked DELETE in the source cluster — dropped",
				c.displayRD(rdName), snapDsp, c.displayNode(row.NodeName))

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
	if emptyList(raw) {
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

// emptyList reports a list column that holds no list: empty, `[]` or `null`.
func emptyList(raw string) bool {
	return raw == "" || raw == "[]" || raw == "null"
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
// blockstor.org/linstor-name annotation when it differs.
func objectMeta(original string) metav1.ObjectMeta {
	meta := metav1.ObjectMeta{Name: k8sstore.Name(original)}
	k8sstore.SetOriginalName(&meta, original)

	return meta
}

// annotateParent keeps a parent's LINSTOR spelling beside the CR name the
// spec stores, the way the store does on Create, when the two differ.
func annotateParent(meta *metav1.ObjectMeta, key, display string) {
	if display == k8sstore.Name(display) {
		return
	}

	if meta.Annotations == nil {
		meta.Annotations = map[string]string{}
	}

	meta.Annotations[key] = display
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

// importLUKSKeys decrypts every LAYER_LUKS_VOLUMES password, live replicas
// and snapshots alike, under LINSTOR's master key and wraps it under the
// blockstor one. No plaintext password outlives this call.
//
// A given passphrase is always checked against LINSTOR's master key, LUKS
// volumes or not: a keyring made from it is the one the cluster unlocks
// with, and a typo would otherwise surface only once a volume is formatted
// under it. A passphrase with no master key to check against is refused.
func (c *converter) importLUKSKeys() error {
	if c.opts.LUKS == nil || c.opts.LUKS.MasterKey == nil {
		if len(c.dump.LayerLuksVolumes) == 0 {
			return nil
		}

		return ErrLUKSNeedsPassphrase
	}

	linstorMaster, err := LinstorMasterKey(c.dump.PropsContainers, c.opts.LUKS.LinstorPassphrase)
	if errors.Is(err, ErrLinstorNoMasterKey) && len(c.dump.LayerLuksVolumes) == 0 {
		return ErrLinstorPassphraseUnchecked
	}

	if err != nil {
		return err
	}

	defer clear(linstorMaster)

	layerByID := map[int32]LayerResourceIDRow{}
	for i := range c.dump.LayerResourceIDs {
		layerByID[c.dump.LayerResourceIDs[i].LayerResourceID] = c.dump.LayerResourceIDs[i]
	}

	for i := range c.dump.LayerLuksVolumes {
		row := &c.dump.LayerLuksVolumes[i]

		lri, ok := layerByID[row.LayerResourceID]
		if !ok || lri.LayerResourceSuffix != "" {
			continue
		}

		password, err := LinstorVolumePassword(linstorMaster, row.EncryptedPassword)
		if err != nil {
			return errors.Wrapf(err, "resource %s on node %s volume %d", lri.ResourceName, lri.NodeName, row.VlmNr)
		}

		key, err := importedLUKSKey(c.opts.LUKS.MasterKey, password)

		clear(password)

		if err != nil {
			return err
		}

		c.luksKeys[luksKeyAt{node: lri.NodeName, rd: lri.ResourceName, snap: lri.SnapshotName, vlmNr: row.VlmNr}] = key

		if lri.SnapshotName == "" {
			c.luksCount[lri.ResourceName]++
		}
	}

	return nil
}

// importedReplicaKeys is the live replica's volumes with their imported
// passwords, by volume number.
func (c *converter) importedReplicaKeys(node, rd string) []crdv1alpha1.ResourceVolumeSpec {
	var out []crdv1alpha1.ResourceVolumeSpec

	for at, key := range c.luksKeys {
		if at.node == node && at.rd == rd && at.snap == "" {
			out = append(out, crdv1alpha1.ResourceVolumeSpec{VolumeNumber: at.vlmNr, LUKSKey: key.DeepCopy()})
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].VolumeNumber < out[j].VolumeNumber })

	return out
}

// importedSnapshotKeys is the snapshot's recorded passwords, one per
// source replica's volume, keyed by the node's CR name.
func (c *converter) importedSnapshotKeys(rd, snap string) []crdv1alpha1.SnapshotLUKSKey {
	var out []crdv1alpha1.SnapshotLUKSKey

	for at, key := range c.luksKeys {
		if at.rd == rd && at.snap == snap && snap != "" {
			out = append(out, crdv1alpha1.SnapshotLUKSKey{
				NodeName: k8sstore.Name(c.displayNode(at.node)), VolumeNumber: at.vlmNr, LUKSKey: key,
			})
		}
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].NodeName != out[j].NodeName {
			return out[i].NodeName < out[j].NodeName
		}

		return out[i].VolumeNumber < out[j].VolumeNumber
	})

	return out
}
