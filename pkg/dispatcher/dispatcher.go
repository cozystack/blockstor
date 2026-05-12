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
	"strconv"
	"strings"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/drbd"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

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
// the storage pool's PrefNic prop, falling back to drbdAddrAny when
// the pool / interface / prop isn't set. drbdAddrAny is the empty-
// placeholder the satellite swaps for its own pod IP at .res render
// time.
func prefNicAddress(nodeName, poolName string, nodes []blockstoriov1alpha1.Node, pools []blockstoriov1alpha1.StoragePool) string {
	if poolName == "" {
		return drbdAddrAny
	}

	prefNic := lookupPrefNic(nodeName, poolName, pools)
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
// the PrefNic override on top: when the peer's storage pool has a
// PrefNic prop, the matching NetInterface address wins over the
// generic SatelliteEndpoint discovery. Falls back to peerAddress()
// when nothing pins it (typical single-NIC clusters).
func peerAddressWithPrefNic(nodeName, poolName string, nodes []blockstoriov1alpha1.Node, pools []blockstoriov1alpha1.StoragePool) string {
	if poolName != "" {
		prefNic := lookupPrefNic(nodeName, poolName, pools)
		if prefNic != "" {
			if addr := lookupNetInterfaceAddress(nodeName, prefNic, nodes); addr != "" {
				return addr
			}
		}
	}

	return peerAddress(nodeName, nodes)
}

// lookupPrefNic reads spec.Props["PrefNic"] off the StoragePool that
// hosts (nodeName, poolName). Returns "" when the pool isn't in the
// passed slice or the prop isn't set.
func lookupPrefNic(nodeName, poolName string, pools []blockstoriov1alpha1.StoragePool) string {
	for i := range pools {
		if pools[i].Spec.NodeName != nodeName {
			continue
		}

		if pools[i].Spec.PoolName != poolName {
			continue
		}

		return pools[i].Spec.Props["PrefNic"]
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
// StoragePool name comes from the Resource's `StorPoolName` prop
// (LINSTOR-compatible key) with the RD-level fallback. For DISKLESS
// replicas it stays empty — the .res renderer treats empty as "no
// disk on this peer".
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
	pool := ""

	if !diskless {
		pool = target.Spec.StoragePool
		if pool == "" {
			pool = target.Spec.Props["StorPoolName"]
		}

		if pool == "" {
			pool = rd.Spec.Props["StorPoolName"]
		}
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
		out = append(out, &intent.DesiredVolume{
			VolumeNumber:   vd.VolumeNumber,
			SizeKib:        vd.SizeKib,
			StoragePool:    pool,
			SeedFromGi:     seedFromGi(target, vd.VolumeNumber),
			SourceSnapshot: srcSnapshot,
		})
	}

	return out
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

// lookupEndpoint reads the satellite endpoint from the matching Node CRD.
// Precedence:
//
//  1. typed `Spec.SatelliteEndpoint` (Phase 10.3 native field)
//  2. legacy `Spec.Props["SatelliteEndpoint"]` (partially-migrated clusters)
//  3. `Spec.NetInterfaces[0].Address` (first advertised interface — the
//     upstream-LINSTOR convention is "first interface is the satellite
//     endpoint"; piraeus-operator and operator-supplied manifests
//     populate this field)
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

		if len(nodes[i].Spec.NetInterfaces) > 0 && nodes[i].Spec.NetInterfaces[0].Address != "" {
			return nodes[i].Spec.NetInterfaces[0].Address
		}

		return ""
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
