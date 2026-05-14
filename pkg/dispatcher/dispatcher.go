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

// Package dispatcher hosts the CRD → DesiredResource translation
// the satellite c-r reconciler runs every time it observes a
// Resource event. Phase 10.6 retired the controller-side gRPC
// dispatch path; what remains is the pure-function `BuildDesired`
// + its helpers, kept in this package so the original RD → peer
// → DRBD-options walk has one home rather than being inlined
// across reconcilers.
package dispatcher

import (
	"crypto/sha256"
	"encoding/binary"
	"slices"
	"sort"
	"strconv"
	"strings"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/drbd"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// ResourceConnectionProps is a per-(HostA, HostB) DRBD tuning bag
// the REST layer collected on `linstor resource-connection
// drbd-peer-options <rd> <a> <b> --max-buffers 8192` — scenario
// 5.W04. The dispatcher folds these props into per-connection
// `drbd.Connection` entries so the satellite renderer drops the
// matching `net { … }` sub-block inside the `.res` file's
// connection mesh.
//
// The Props map keys are upstream LINSTOR-compatible:
//   - `DrbdOptions/PeerDevice/max-buffers` → emits as `max-buffers
//     <value>;` inside the connection's `net { }` block (DRBD's
//     `max-buffers` option lives on the `net` section even though
//     the LINSTOR prop key namespace puts it under `PeerDevice/`).
//   - `DrbdOptions/Net/<name>` → emits as `<name> <value>;` inside
//     `net { }` directly.
//
// Operators who patch a non-DRBD prop on the resource connection
// will see it silently dropped here — only `DrbdOptions/...` keys
// are routable into the .res file. Same convention as
// `splitDRBDOptions` on the satellite reconciler.
type ResourceConnectionProps struct {
	HostA string
	HostB string
	Props map[string]string
}

// BuildResourceConnections turns a slice of per-pair Props bags
// into the slice of `drbd.Connection` entries the .res renderer
// consumes. Order of input is preserved on output; the renderer
// matches connection entries to the mesh's (i, j) pair unordered, so
// callers don't have to canonicalise HostA / HostB.
//
// Empty Props maps are dropped — emitting an empty `net { }` sub-
// block on a connection that doesn't actually carry a tuning
// override would force a noisy `drbdadm adjust` on every reconcile.
// Connections whose `DrbdOptions/...` keys all route to unknown
// destinations (i.e. not Net / PeerDevice) emit no NetOptions and
// are likewise dropped.
func BuildResourceConnections(in []ResourceConnectionProps) []drbd.Connection {
	out := make([]drbd.Connection, 0, len(in))

	for i := range in {
		net := splitConnectionNetOptions(in[i].Props)
		if len(net) == 0 {
			continue
		}

		out = append(out, drbd.Connection{
			HostA:      in[i].HostA,
			HostB:      in[i].HostB,
			NetOptions: net,
		})
	}

	// Deterministic order so the .res renderer output stays stable
	// across reconciles — same key argument as `sortedKeys`. We sort
	// by the canonical (min, max) pair so (n1, n2) and (n2, n1)
	// don't reorder the output across reconciles.
	sort.SliceStable(out, func(i, j int) bool {
		lowI, highI := canonicalPair(out[i].HostA, out[i].HostB)
		lowJ, highJ := canonicalPair(out[j].HostA, out[j].HostB)

		if lowI != lowJ {
			return lowI < lowJ
		}

		return highI < highJ
	})

	return out
}

// canonicalPair returns the (min, max) of two host names so a
// later sort.Stable orders the output deterministically irrespective
// of the input order operators happened to PATCH the (a, b) tuple in.
func canonicalPair(a, b string) (string, string) {
	if a <= b {
		return a, b
	}

	return b, a
}

// splitConnectionNetOptions partitions a ResourceConnection props
// bag into the `net { }` sub-block emission map. Keys under
// `DrbdOptions/Net/<name>` and `DrbdOptions/PeerDevice/<name>` both
// land here — DRBD treats the connection-scope `max-buffers` /
// `ping-timeout` / `c-max-rate` knobs as net-block options, even
// though LINSTOR's `resource-connection drbd-peer-options` writes
// them under the PeerDevice/ prop namespace (upstream LINSTOR makes
// the same routing decision in ConfFileBuilder).
//
// Section names not on the allow-list are dropped — emitting them
// inside `connection { net { ... } }` would produce a syntactically
// wrong .res file (drbdadm would complain about `disk` or
// `handlers` keys in a `net` block).
func splitConnectionNetOptions(props map[string]string) map[string]string {
	if len(props) == 0 {
		return nil
	}

	out := map[string]string{}

	for key, value := range props {
		rest, ok := strings.CutPrefix(key, drbd.PropPrefix)
		if !ok {
			continue
		}

		section, rawKey, hasSection := strings.Cut(rest, "/")
		if !hasSection {
			continue
		}

		switch strings.ToLower(section) {
		case "net", "peerdevice", "peer-device":
			out[rawKey] = value
		default:
			// disk / handlers / options on a connection scope would
			// produce invalid .res output — drop them.
			continue
		}
	}

	return out
}

// drbdAddrAny is the placeholder address we put into the .res file at
// dispatch time. The satellite rewrites it to its actual pod IP when
// it renders the file (drbd doesn't accept literal 0.0.0.0).
const drbdAddrAny = "0.0.0.0"

// boolPropTrue is the canonical string-form `true` value blockstor
// stamps on drbd_options bag keys that are flag-like (auto-primary,
// peer.<n>.diskless, …). Pinning the literal avoids consumer-side
// drift between `"true"`/`"True"`/`"1"`.
const boolPropTrue = "true"

// BuildDesired translates a Resource + its same-RD peers into the
// satellite-facing DesiredResource. Port/minor/node-id assignment is
// deterministic from the RD name + sorted peer list — good enough for
// the first stand smoke; the autoplacer will replace it with a real
// allocator once we wire the IPAM hookup.
//
// nodes is consulted for each peer's `SatelliteEndpoint` prop so
// `peer.<name>.address` carries a real (pod) IP rather than a 0.0.0.0
// placeholder; drbd-9 won't replicate to 0.0.0.0.
func BuildDesired(target *blockstoriov1alpha1.Resource, peers []blockstoriov1alpha1.Resource, nodes []blockstoriov1alpha1.Node, pools []blockstoriov1alpha1.StoragePool, rd *blockstoriov1alpha1.ResourceDefinition, effectiveProps map[string]string) *intent.DesiredResource {
	// node-id and port/minor are persisted on Status by the controller
	// before Apply runs. Falling back to derive*() is a transitional
	// safety net for the case where the allocator hasn't run yet — it
	// produces the same value as the legacy behaviour, so existing
	// clusters don't see ids change on the upgrade reconcile. New
	// clusters always go through the allocator.
	port := readDRBDPort(target, peers)
	minor := readDRBDMinor(target, peers)

	// Collect every replica's (node, id) — id sourced from Status.
	// We ONLY emit peers whose id is allocated; an unallocated peer
	// is skipped this round and will reappear once the controller
	// reconciles its Status and the parent RD requeues.
	idOf := map[string]int32{}

	if id := nodeIDOf(target); id >= 0 {
		idOf[target.Spec.NodeName] = id
	}

	for i := range peers {
		if peers[i].Spec.NodeName == target.Spec.NodeName {
			continue
		}

		if id := nodeIDOf(&peers[i]); id >= 0 {
			idOf[peers[i].Spec.NodeName] = id
		}
	}

	dropped := make([]string, 0, len(idOf))

	for name := range idOf {
		if name != target.Spec.NodeName {
			dropped = append(dropped, name)
		}
	}

	slices.Sort(dropped)

	drbdOpts := map[string]string{
		"port":    strconv.Itoa(port),
		"node-id": strconv.Itoa(int(idOf[target.Spec.NodeName])),
		// PrefNic on the target's pool (storage pool prop) overrides
		// the default placeholder so DRBD binds replication to the
		// requested interface. Empty fallback → satellite picks its
		// pod IP at .res render time.
		"address": prefNicAddress(target.Spec.NodeName, targetPoolName(target), nodes, pools),
		"minor":   strconv.Itoa(minor),
	}

	// Pick a single replica to seed initial Primary. Use the lowest
	// node-id that owns a diskful replica — id is stable across
	// reconciles, so the same replica wins every time. Without this
	// every diskful brand-new RD comes up Inconsistent and stays
	// there until something opens for write. The seed flag is
	// harmless on subsequent reconciles — satellite Reconciler runs
	// primary --force only on firstActivation.
	if !slices.Contains(target.Spec.Flags, "DISKLESS") &&
		idOf[target.Spec.NodeName] == lowestDiskfulID(target, peers) {
		drbdOpts["auto-primary"] = boolPropTrue
	}

	addPeerEntries(drbdOpts, dropped, peers, nodes, pools, port, idOf)

	return assembleDesired(target, peers, rd, dropped, drbdOpts, effectiveProps)
}

// assembleDesired packages the per-replica wire payload. Pulled out
// of BuildDesired so the caller stays under the funlen budget.
func assembleDesired(target *blockstoriov1alpha1.Resource, peers []blockstoriov1alpha1.Resource, rd *blockstoriov1alpha1.ResourceDefinition, dropped []string, drbdOpts, effectiveProps map[string]string) *intent.DesiredResource {
	_ = peers // peers info already folded into drbdOpts via addPeerEntries

	wireProps := mergeEffectiveProps(target.Spec.Props, effectiveProps, drbdOpts)

	// LUKS passphrase: lift `DrbdOptions/Encryption/passphrase` from
	// the resolved options bag onto a stable `LuksPassphrase` prop the
	// satellite's LUKS layer reads. Match upstream LINSTOR's prop
	// name for compatibility with `linstor rd set-property`.
	if pass := drbdOpts[drbdEncryptionPassphraseKey]; pass != "" {
		if wireProps == nil {
			wireProps = map[string]string{}
		}

		wireProps["LuksPassphrase"] = pass

		// Drop the key from drbdOpts after lifting — splitDRBDOptions
		// on the satellite side would otherwise render it as a
		// `passphrase` line in the .res file's options block, and
		// `drbdadm create-md` rejects unknown options with
		// `expected: cpu-mask | ... but got 'passphrase'`. The
		// passphrase reaches the LUKS layer via wireProps, not the
		// .res file.
		delete(drbdOpts, drbdEncryptionPassphraseKey)
	}

	var layerStack []string
	if rd != nil {
		layerStack = rd.Spec.LayerStack
	}

	return &intent.DesiredResource{
		Name:        target.Spec.ResourceDefinitionName,
		NodeName:    target.Spec.NodeName,
		Flags:       target.Spec.Flags,
		Props:       wireProps,
		Peers:       dropped,
		Volumes:     buildVolumes(rd, target),
		DrbdOptions: drbdOpts,
		LayerStack:  layerStack,
	}
}

// drbdEncryptionPassphraseKey is the upstream LINSTOR prop key
// operators set with `linstor rd set-property <rd> Encryption/passphrase`.
//
//nolint:gosec // not a credential value, the string is a prop key name
const drbdEncryptionPassphraseKey = "DrbdOptions/Encryption/passphrase"

// mergeEffectiveProps splits the resolver's output into:
//   - DRBD options (DrbdOptions/...) → folded into drbdOpts so the
//     satellite's .res renderer drops them into the right section
//   - everything else → returned as the wire-side Props map
//
// nil effectiveProps falls back to target's own Spec.Props verbatim
// so the legacy single-scope path keeps working unchanged.
func mergeEffectiveProps(targetProps, effectiveProps, drbdOpts map[string]string) map[string]string {
	if effectiveProps == nil {
		return targetProps
	}

	wireProps := map[string]string{}

	for key, value := range effectiveProps {
		if strings.HasPrefix(key, drbd.PropPrefix) {
			drbdOpts[key] = value
		} else {
			wireProps[key] = value
		}
	}

	return wireProps
}

// nodeIDOf reads the persisted DRBD node-id off a Resource. Returns
// -1 when the controller hasn't allocated yet — the caller skips the
// replica from the wire so we never emit a stale id.
func nodeIDOf(r *blockstoriov1alpha1.Resource) int32 {
	if r.Status.DRBDNodeID == nil {
		return -1
	}

	return *r.Status.DRBDNodeID
}

// readDRBDPort returns the per-replica TCP port. Upstream LINSTOR
// allocates the port from the hosting node's range, not the RD's, so
// each replica owns its own value. Fallback to derivePort is the
// transitional safety net for in-flight reconciles before the
// controller's allocator catches up — new replicas always go through
// the per-node allocator.
func readDRBDPort(target *blockstoriov1alpha1.Resource, _ []blockstoriov1alpha1.Resource) int {
	if target.Status.DRBDPort != nil {
		return int(*target.Status.DRBDPort)
	}

	return derivePort(target.Spec.ResourceDefinitionName)
}

// readDRBDMinor mirrors readDRBDPort. Per-replica because /dev/drbd<N>
// is a local device path; two replicas on different nodes are free
// to take unrelated minors.
func readDRBDMinor(target *blockstoriov1alpha1.Resource, _ []blockstoriov1alpha1.Resource) int {
	if target.Status.DRBDMinor != nil {
		return int(*target.Status.DRBDMinor)
	}

	return deriveMinor(target.Spec.ResourceDefinitionName)
}

// peerPortOf reads the persisted DRBDPort off a peer Resource. The
// .res file's `peer.<name>.port` must reflect the port that peer
// listens on (its own allocation), not target's.
func peerPortOf(r *blockstoriov1alpha1.Resource, fallback int) int {
	if r.Status.DRBDPort != nil {
		return int(*r.Status.DRBDPort)
	}

	return fallback
}

// addPeerEntries fills in `peer.<name>.{port,node-id,address}` keys
// on the DesiredResource's drbd_options map. Pulled out of
// BuildDesired to keep the latter under the funlen budget — this
// owns the per-peer fan-out plus the port/address lookups.
func addPeerEntries(drbdOpts map[string]string, dropped []string, peers []blockstoriov1alpha1.Resource, nodes []blockstoriov1alpha1.Node, pools []blockstoriov1alpha1.StoragePool, fallbackPort int, idOf map[string]int32) {
	peerByName := make(map[string]*blockstoriov1alpha1.Resource, len(peers))
	for i := range peers {
		peerByName[peers[i].Spec.NodeName] = &peers[i]
	}

	for _, peer := range dropped {
		peerPort := fallbackPort

		peerPool := ""

		if p, ok := peerByName[peer]; ok {
			peerPort = peerPortOf(p, fallbackPort)
			peerPool = targetPoolName(p)
		}

		drbdOpts["peer."+peer+".port"] = strconv.Itoa(peerPort)
		drbdOpts["peer."+peer+".node-id"] = strconv.Itoa(int(idOf[peer]))
		drbdOpts["peer."+peer+".address"] = peerAddressWithPrefNic(peer, peerPool, nodes, pools)

		// Surface the peer's DISKLESS flag so the satellite's .res
		// renderer can emit `disk none;` instead of the diskful
		// placeholder for diskless peers.
		if p, ok := peerByName[peer]; ok && slices.Contains(p.Spec.Flags, "DISKLESS") {
			drbdOpts["peer."+peer+".diskless"] = boolPropTrue
		}
	}
}

// targetPoolName extracts the StoragePool name a Resource lives in.
// Typed Spec.StoragePool wins (Phase 10.3); legacy props["StorPoolName"]
// is the fallback for clusters mid-migration.
func targetPoolName(r *blockstoriov1alpha1.Resource) string {
	if r.Spec.StoragePool != "" {
		return r.Spec.StoragePool
	}

	return r.Spec.Props["StorPoolName"]
}

// prefNicAddress resolves the node's NetInterface address named by
// the effective PrefNic prop (pool scope > node scope), falling back
// to drbdAddrAny when nothing pins an interface. drbdAddrAny is the
// placeholder the satellite swaps for its own pod IP at .res render
// time.
func prefNicAddress(nodeName, poolName string, nodes []blockstoriov1alpha1.Node, pools []blockstoriov1alpha1.StoragePool) string {
	prefNic := lookupPrefNic(nodeName, poolName, nodes, pools)
	if prefNic == "" {
		return drbdAddrAny
	}

	addr := lookupNetInterfaceAddress(nodeName, prefNic, nodes)
	if addr == "" {
		return drbdAddrAny
	}

	return addr
}

// peerAddressWithPrefNic returns the peer node's DRBD address with
// the PrefNic override on top: when the peer's storage pool (or the
// peer's Node CRD) carries a PrefNic prop, the matching NetInterface
// address wins over the generic SatelliteEndpoint discovery. Falls
// back to peerAddress() when nothing pins it (typical single-NIC
// clusters).
func peerAddressWithPrefNic(nodeName, poolName string, nodes []blockstoriov1alpha1.Node, pools []blockstoriov1alpha1.StoragePool) string {
	prefNic := lookupPrefNic(nodeName, poolName, nodes, pools)
	if prefNic != "" {
		if addr := lookupNetInterfaceAddress(nodeName, prefNic, nodes); addr != "" {
			return addr
		}
	}

	return peerAddress(nodeName, nodes)
}

// lookupPrefNic returns the PrefNic name DRBD must bind to on
// nodeName, honouring UG9's prop-scope precedence:
//
//  1. StoragePool.Spec.Props["PrefNic"] on the (nodeName, poolName)
//     pool — most-specific scope wins, matches `linstor
//     storage-pool set-property <node> <pool> PrefNic <nic>`.
//  2. Node.Spec.Props["PrefNic"] on the node — applies to every
//     storage pool on the node, including the diskless pool. Matches
//     `linstor node set-property <node> PrefNic <nic>` per
//     scenario 3.W03.
//
// Returns "" when neither scope sets the prop; the caller treats the
// empty result as "no override" and falls through to the next address
// source (peerAddress / drbdAddrAny).
func lookupPrefNic(nodeName, poolName string, nodes []blockstoriov1alpha1.Node, pools []blockstoriov1alpha1.StoragePool) string {
	if poolName != "" {
		for i := range pools {
			if pools[i].Spec.NodeName != nodeName {
				continue
			}

			if pools[i].Spec.PoolName != poolName {
				continue
			}

			if v := pools[i].Spec.Props["PrefNic"]; v != "" {
				return v
			}

			break
		}
	}

	for i := range nodes {
		if k8s.OriginalName(&nodes[i].ObjectMeta) != nodeName {
			continue
		}

		return nodes[i].Spec.Props["PrefNic"]
	}

	return ""
}

// lookupNetInterfaceAddress finds the NetInterface named ifaceName on
// the named Node CRD and returns its Address. Empty on any miss; the
// caller uses the empty result to fall through to the next address
// source rather than 500-ing.
func lookupNetInterfaceAddress(nodeName, ifaceName string, nodes []blockstoriov1alpha1.Node) string {
	for i := range nodes {
		if k8s.OriginalName(&nodes[i].ObjectMeta) != nodeName {
			continue
		}

		for j := range nodes[i].Spec.NetInterfaces {
			if nodes[i].Spec.NetInterfaces[j].Name == ifaceName {
				return nodes[i].Spec.NetInterfaces[j].Address
			}
		}

		return ""
	}

	return ""
}

// lowestDiskfulID picks the smallest allocated node-id among the
// diskful replicas of the RD. Used to deterministically choose which
// replica seeds the initial Primary on first activation.
func lowestDiskfulID(target *blockstoriov1alpha1.Resource, peers []blockstoriov1alpha1.Resource) int32 {
	const sentinel int32 = 1 << 30

	low := sentinel

	consider := func(r *blockstoriov1alpha1.Resource) {
		if slices.Contains(r.Spec.Flags, "DISKLESS") {
			return
		}

		nodeID := nodeIDOf(r)
		if nodeID < 0 {
			return
		}

		if nodeID < low {
			low = nodeID
		}
	}

	consider(target)

	for i := range peers {
		consider(&peers[i])
	}

	return low
}

// buildVolumes turns the parent ResourceDefinition's VolumeDefinitions
// into DesiredVolumes for the satellite. DISKLESS replicas still get
// volume entries — DRBD-9 needs a `volume { ... }` block per peer in
// the .res file (with `disk none;` for the diskless ones), otherwise
// the kernel side reports `"devices": []` and `drbdsetup status` has
// no per-volume info to surface state on. The applyStorage path on
// the satellite is gated by `!diskless` so emitting Volumes here is
// safe — it only feeds the .res renderer.
//
// StoragePool name comes from the per-VD `StorPoolName` prop (most
// specific — scenario 4.W26 / UG9 §"Placing volumes of one resource
// in different storage pools"), with fallback to the Resource's
// `StorPoolName` prop (LINSTOR-compatible key) and the RD-level
// default. For DISKLESS replicas it stays empty — the .res renderer
// treats empty as "no disk on this peer".
//
// The per-VD override (scenario 4.W26) lets `vol 0` land on a fast
// NVMe pool and `vol 1` on a slow HDD pool ON THE SAME NODE. Bug 76
// still enforces same-ProviderKind ACROSS REPLICAS of one VD (the
// placer never sees per-VD pool selection — that's a satellite-side
// routing decision made AFTER the placer has already picked the
// node), but the kinds across DIFFERENT VDs of the same RD are
// orthogonal.
func buildVolumes(rd *blockstoriov1alpha1.ResourceDefinition, target *blockstoriov1alpha1.Resource) []*intent.DesiredVolume {
	if rd == nil {
		return nil
	}

	diskless := slices.Contains(target.Spec.Flags, "DISKLESS")

	// Phase 10.3 step: typed `Spec.StoragePool` wins over the legacy
	// `Spec.Props["StorPoolName"]` so we don't read stale data when
	// the controller has already migrated. RD-level fallback still
	// goes through Props since RD has no typed slot for the pool
	// name (it lives on a per-volume VG.Props in
	// ResourceGroupVolumeGroup; the RD-prop fallback is the legacy
	// shim).
	rdPool := ""

	if !diskless {
		rdPool = target.Spec.StoragePool
		if rdPool == "" {
			rdPool = target.Spec.Props["StorPoolName"]
		}

		if rdPool == "" {
			rdPool = rd.Spec.Props["StorPoolName"]
		}
	}

	// External-metadata pool (scenario 6.18, UG9
	// §"Using external DRBD metadata"). Resolves the
	// `StorPoolNameDrbdMeta` prop in the upstream-LINSTOR precedence
	// order — most-specific scope wins, terminated as soon as a
	// non-empty value is found. Diskless replicas don't carry a
	// backing disk and therefore have no metadata to route, so we
	// leave the field empty in that case.
	metaPool := ""
	if !diskless {
		metaPool = resolveMetaPool(target, rd)
	}

	// Optional clone source — set by the snapshot-restore-resource
	// REST handler on the target RD's Props. Format `<srcRD>:<snap>`.
	// When present, satellite materialises each volume via
	// Provider.RestoreVolumeFromSnapshot instead of CreateVolume so
	// the new replica starts with the snapshot's data instead of an
	// empty volume.
	const restoreFromKey = "BlockstorRestoreFromSnapshot"

	srcSnapshot := ""
	if rd.Spec.Props != nil {
		srcSnapshot = rd.Spec.Props[restoreFromKey]
	}

	out := make([]*intent.DesiredVolume, 0, len(rd.Spec.VolumeDefinitions))

	for _, vd := range rd.Spec.VolumeDefinitions {
		// Per-VD `StorPoolName` (scenario 4.W26) is the most-specific
		// scope and overrides the RD/Resource default. Empty value
		// falls through to the RD-level pool resolved above.
		// Diskless replicas still ignore per-VD pool — no backing
		// disk to provision regardless of what the prop says.
		pool := rdPool

		if !diskless {
			if v := vd.Props["StorPoolName"]; v != "" {
				pool = v
			}
		}

		out = append(out, &intent.DesiredVolume{
			VolumeNumber:   vd.VolumeNumber,
			SizeKib:        vd.SizeKib,
			StoragePool:    pool,
			SeedFromGi:     seedFromGi(target, vd.VolumeNumber),
			SourceSnapshot: srcSnapshot,
			MetaPool:       metaPool,
		})
	}

	return out
}

// StorPoolNameDrbdMetaKey is the upstream-LINSTOR prop key that
// routes DRBD activity-log + bitmap + generation-id state to a
// separate storage pool. Set by operators with e.g.
// `linstor rd set-property <rd> StorPoolNameDrbdMeta ssd-meta`.
//
// Exposed for the satellite-side reconciler which keys its meta
// volume naming on the same well-known string.
const StorPoolNameDrbdMetaKey = "StorPoolNameDrbdMeta"

// resolveMetaPool walks the upstream-LINSTOR scope precedence for
// StorPoolNameDrbdMeta — Resource → RD — returning the first non-
// empty value found. The full UG9 hierarchy (Node → RG → RD →
// Resource → VG → VD, most-specific wins) lives in
// pkg/effectiveprops; we only need the two scopes the dispatcher
// has direct objects for, because effectiveProps (already
// resolved upstream of this call) covers the Controller / RG /
// Node / VG / VD scopes via a merged map. We re-walk Resource and
// RD here because effectiveProps strips non-DrbdOptions/... keys
// into wireProps and isn't plumbed into buildVolumes today — a
// follow-up pass can unify the two readers once buildVolumes
// takes effectiveProps directly.
func resolveMetaPool(target *blockstoriov1alpha1.Resource, rd *blockstoriov1alpha1.ResourceDefinition) string {
	if v := target.Spec.Props[StorPoolNameDrbdMetaKey]; v != "" {
		return v
	}

	if rd != nil {
		if v := rd.Spec.Props[StorPoolNameDrbdMetaKey]; v != "" {
			return v
		}
	}

	return ""
}

// seedFromGi looks up the controller-allocated SeedFromGi for the
// given volume number. Empty when the controller hasn't picked a peer
// yet (fresh-cluster, no UpToDate peer to seed from); the satellite
// then skips drbdmeta seeding and pays the full initial-sync cost
// for that volume. Phase 8.1.
func seedFromGi(target *blockstoriov1alpha1.Resource, volumeNumber int32) string {
	for i := range target.Spec.Volumes {
		if target.Spec.Volumes[i].VolumeNumber == volumeNumber {
			return target.Spec.Volumes[i].SeedFromGi
		}
	}

	return ""
}

// kubeInternalNICName is the well-known NetInterface name carrying the
// corev1.Node InternalIP (a host-routable address). Populated by the
// register / label-sync path when blockstor knows the kube view of a
// node; preferred over arbitrarily-ordered NetInterfaces so we don't
// trip over piraeus-operator's habit of overwriting
// `Spec.NetInterfaces[0].Address` with the satellite **pod** IP (a
// pod-CIDR address that's only routable inside the CNI — DRBD peer
// connect fails). See Bug 48.
const kubeInternalNICName = "k8s-internal"

// defaultNICName is the upstream-LINSTOR convention for "the
// satellite endpoint" when nodes carry multiple NetInterfaces. We
// prefer it over a positional [0] read so manifest reordering doesn't
// silently flip which address gets stamped into peer blocks.
const defaultNICName = "default"

// lookupEndpoint reads the satellite endpoint from the matching Node CRD.
// Precedence:
//
//  1. typed `Spec.SatelliteEndpoint` (Phase 10.3 native field)
//  2. legacy `Spec.Props["SatelliteEndpoint"]` (partially-migrated clusters)
//  3. `Spec.NetInterfaces` by name: "k8s-internal" → "default" → first
//     non-empty entry. The named lookups exist because piraeus-operator
//     installed alongside blockstor (LinstorCluster.spec.externalController.url
//     pointing at blockstor's apiserver) overwrites
//     `Spec.NetInterfaces[0].Address` with the satellite pod IP — a
//     pod-CIDR address that isn't routable peer-to-peer. The
//     register / label-sync layer publishes the routable
//     corev1.Node InternalIP under the "k8s-internal" name so the
//     dispatcher can prefer it without doing CIDR detection here.
//     When no named interface is present we fall through to the
//     legacy "first non-empty" behaviour so single-NIC clusters keep
//     working unchanged. (Bug 48)
//
// Match is on the original LINSTOR name (annotation when slugified,
// else metadata.Name) so non-RFC1123 LINSTOR names still resolve.
func lookupEndpoint(nodeName string, nodes []blockstoriov1alpha1.Node) string {
	for i := range nodes {
		if k8s.OriginalName(&nodes[i].ObjectMeta) != nodeName {
			continue
		}

		if ep := nodes[i].Spec.SatelliteEndpoint; ep != "" {
			return ep
		}

		if ep := nodes[i].Spec.Props["SatelliteEndpoint"]; ep != "" {
			return ep
		}

		return preferredNetInterfaceAddress(nodes[i].Spec.NetInterfaces)
	}

	return ""
}

// preferredNetInterfaceAddress walks a node's NetInterfaces in the
// dispatcher's name-preference order — "k8s-internal" first (the
// host-routable corev1 InternalIP), then "default" (the upstream-
// LINSTOR convention), then the first non-empty entry as a final
// fallback for single-NIC clusters. Empty when no NetInterface
// carries an address; the caller treats that as "node not ready"
// and emits the drbdAddrAny placeholder.
func preferredNetInterfaceAddress(ifaces []blockstoriov1alpha1.NodeNetInterface) string {
	for _, name := range []string{kubeInternalNICName, defaultNICName} {
		for j := range ifaces {
			if ifaces[j].Name == name && ifaces[j].Address != "" {
				return ifaces[j].Address
			}
		}
	}

	for j := range ifaces {
		if ifaces[j].Address != "" {
			return ifaces[j].Address
		}
	}

	return ""
}

// peerAddress looks up `nodeName`'s SatelliteEndpoint and returns
// just the host part (no port). Falls back to the 0.0.0.0 placeholder
// when the node is unknown or hasn't registered yet — the satellite
// will surface a per-resource error in that case which is exactly the
// signal the controller needs to retry.
func peerAddress(nodeName string, nodes []blockstoriov1alpha1.Node) string {
	endpoint := lookupEndpoint(nodeName, nodes)
	if endpoint == "" {
		return drbdAddrAny
	}

	idx := strings.LastIndex(endpoint, ":")
	if idx <= 0 {
		return endpoint
	}

	return endpoint[:idx]
}

// DerivePort exposes derivePort to the controller's bootstrap-time
// allocation path — the controller falls back to it for the first
// replica of a fresh RD when no sibling has a persisted port yet.
// Production clusters should swap this for a TcpPortPool allocator
// (Phase 8.1) that detects collisions; deterministic hashing means
// two RDs with the same name-prefix-hash collide silently today.
func DerivePort(rd string) int { return derivePort(rd) }

// DeriveMinor mirrors DerivePort for /dev/drbd<N>.
func DeriveMinor(rd string) int { return deriveMinor(rd) }

// derivePort hashes the RD name into the drbd-9 reserved range
// 7000–7999. Matches what upstream LINSTOR's TcpPortPool does for
// fresh deployments — collisions on a real cluster are handled by
// the autoplacer, but for the smoke we live with the hash.
func derivePort(rd string) int {
	const (
		portBase  = 7000
		portRange = 1000
	)

	digest := sha256.Sum256([]byte(rd))

	return portBase + int(binary.BigEndian.Uint16(digest[:2])%portRange)
}

// deriveMinor likewise hashes into 1000–9999.
func deriveMinor(rd string) int {
	const (
		minorBase  = 1000
		minorRange = 9000
	)

	digest := sha256.Sum256([]byte(rd))

	return minorBase + int(binary.BigEndian.Uint16(digest[2:4])%minorRange)
}
