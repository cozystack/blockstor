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

// Package dispatcher is the controller-side glue that turns Resource
// CRDs into satellite-side ApplyResources calls. The kubebuilder
// reconciler delegates here so the wire format and the gRPC dial /
// retry logic live in one testable place.
package dispatcher

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"slices"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/drbd"
	satellitepb "github.com/cozystack/blockstor/pkg/satellite/proto"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// drbdAddrAny is the placeholder address we put into the .res file at
// dispatch time. The satellite rewrites it to its actual pod IP when
// it renders the file (drbd doesn't accept literal 0.0.0.0).
const drbdAddrAny = "0.0.0.0"

// Dialer abstracts how we open a gRPC connection. Production wires
// the actual `grpc.NewClient`; tests inject a stub that returns a
// canned client.
type Dialer interface {
	Dial(ctx context.Context, endpoint string) (satellitepb.SatelliteClient, func() error, error)
}

// realDialer wraps grpc.NewClient with our standard insecure (cluster-
// internal) transport.
type realDialer struct{}

// NewDialer returns a production Dialer. We export it so the
// reconciler in main.go can wire it without leaking grpc imports
// across packages.
func NewDialer() Dialer {
	return realDialer{}
}

// Dial opens a connection to endpoint and returns the satellite
// client, a close func, or an error.
func (realDialer) Dial(_ context.Context, endpoint string) (satellitepb.SatelliteClient, func() error, error) {
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, errors.Wrapf(err, "dial %s", endpoint)
	}

	return satellitepb.NewSatelliteClient(conn), conn.Close, nil
}

// Dispatcher pushes a Resource's desired state to the satellite that
// hosts it.
type Dispatcher struct {
	dialer Dialer
}

// New constructs a Dispatcher with the given Dialer.
func New(dialer Dialer) *Dispatcher {
	return &Dispatcher{dialer: dialer}
}

// ApplyOptions carries everything Apply needs beyond the target's own
// spec. Kept as a struct so future fields (encryption, drbd-reactor
// hints) can land without breaking the call sites.
type ApplyOptions struct {
	// EffectiveProps is the resolved DRBD-options bag after walking
	// controller → RG → RD → Resource (see drbd.ResolveOptions).
	// nil means "use target.Spec.Props verbatim" — what the dispatch
	// did before the hierarchy resolver landed.
	EffectiveProps map[string]string

	// LayerStack is the resolved layer composition (RD → RG → default).
	// Empty falls back to RD.Spec.LayerStack inside BuildDesired so
	// older call sites that don't compute the stack keep their
	// behaviour. The satellite skips DRBD when the stack omits it.
	LayerStack []string
}

// Apply builds the DesiredResource for this Resource (looking up its
// peers from the full RD-wide list and its volumes from the parent
// RD) and sends it to the target satellite. Returns the per-resource
// result the satellite reported.
//
// nodes is the full Node CRD list — Apply uses it to resolve each
// peer's SatelliteEndpoint property. rd may be nil; when present the
// RD's VolumeDefinitions become DesiredVolumes for non-DISKLESS
// replicas (DISKLESS replicas ignore them).
func (d *Dispatcher) Apply(ctx context.Context, target *blockstoriov1alpha1.Resource, peers []blockstoriov1alpha1.Resource, nodes []blockstoriov1alpha1.Node, rd *blockstoriov1alpha1.ResourceDefinition, opts ApplyOptions) (*satellitepb.ResourceApplyResult, error) {
	endpoint := lookupEndpoint(target.Spec.NodeName, nodes)
	if endpoint == "" {
		return nil, errors.Errorf("no SatelliteEndpoint for node %q", target.Spec.NodeName)
	}

	desired := BuildDesired(target, peers, nodes, rd, opts.EffectiveProps)
	if len(opts.LayerStack) > 0 {
		desired.LayerStack = opts.LayerStack
	}

	client, closer, err := d.dialer.Dial(ctx, endpoint)
	if err != nil {
		return nil, errors.Wrapf(err, "dial %s", endpoint)
	}

	defer func() { _ = closer() }()

	resp, err := client.ApplyResources(ctx, &satellitepb.ApplyResourcesRequest{
		Resources: []*satellitepb.DesiredResource{desired},
	})
	if err != nil {
		return nil, errors.Wrap(err, "ApplyResources RPC")
	}

	if len(resp.GetResults()) == 0 {
		return nil, errors.New("empty ApplyResources response")
	}

	return resp.GetResults()[0], nil
}

// lookupEndpoint reads SatelliteEndpoint from the matching Node CRD.
// Phase 10.3: typed `Spec.SatelliteEndpoint` wins; falls back to the
// legacy `Spec.Props["SatelliteEndpoint"]` so a partially-migrated
// cluster (or a satellite that still pushes the prop in its Hello
// handshake) keeps working unchanged. Match by the original LINSTOR
// name (annotation when slugified, else metadata.Name) so
// non-RFC1123 LINSTOR names still resolve back to the right CRD.
func lookupEndpoint(nodeName string, nodes []blockstoriov1alpha1.Node) string {
	for i := range nodes {
		if k8s.OriginalName(&nodes[i].ObjectMeta) != nodeName {
			continue
		}

		if ep := nodes[i].Spec.SatelliteEndpoint; ep != "" {
			return ep
		}

		return nodes[i].Spec.Props["SatelliteEndpoint"]
	}

	return ""
}

// BuildDesired translates a Resource + its same-RD peers into the
// satellite-facing DesiredResource. Port/minor/node-id assignment is
// deterministic from the RD name + sorted peer list — good enough for
// the first stand smoke; the autoplacer will replace it with a real
// allocator once we wire the IPAM hookup.
//
// nodes is consulted for each peer's `SatelliteEndpoint` prop so
// `peer.<name>.address` carries a real (pod) IP rather than a 0.0.0.0
// placeholder; drbd-9 won't replicate to 0.0.0.0.
func BuildDesired(target *blockstoriov1alpha1.Resource, peers []blockstoriov1alpha1.Resource, nodes []blockstoriov1alpha1.Node, rd *blockstoriov1alpha1.ResourceDefinition, effectiveProps map[string]string) *satellitepb.DesiredResource {
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
		"address": drbdAddrAny, // satellite picks pod IP at .res render time
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
		drbdOpts["auto-primary"] = "true"
	}

	addPeerEntries(drbdOpts, dropped, peers, nodes, port, idOf)

	return assembleDesired(target, peers, rd, dropped, drbdOpts, effectiveProps)
}

// assembleDesired packages the per-replica wire payload. Pulled out
// of BuildDesired so the caller stays under the funlen budget.
func assembleDesired(target *blockstoriov1alpha1.Resource, peers []blockstoriov1alpha1.Resource, rd *blockstoriov1alpha1.ResourceDefinition, dropped []string, drbdOpts, effectiveProps map[string]string) *satellitepb.DesiredResource {
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
	}

	var layerStack []string
	if rd != nil {
		layerStack = rd.Spec.LayerStack
	}

	return &satellitepb.DesiredResource{
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
func addPeerEntries(drbdOpts map[string]string, dropped []string, peers []blockstoriov1alpha1.Resource, nodes []blockstoriov1alpha1.Node, fallbackPort int, idOf map[string]int32) {
	peerByName := make(map[string]*blockstoriov1alpha1.Resource, len(peers))
	for i := range peers {
		peerByName[peers[i].Spec.NodeName] = &peers[i]
	}

	for _, peer := range dropped {
		peerPort := fallbackPort
		if p, ok := peerByName[peer]; ok {
			peerPort = peerPortOf(p, fallbackPort)
		}

		drbdOpts["peer."+peer+".port"] = strconv.Itoa(peerPort)
		drbdOpts["peer."+peer+".node-id"] = strconv.Itoa(int(idOf[peer]))
		drbdOpts["peer."+peer+".address"] = peerAddress(peer, nodes)
	}
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
// into DesiredVolumes for the satellite. DISKLESS replicas get an
// empty list — they don't allocate local storage. The StoragePool name
// comes from the Resource's `StorPoolName` prop (LINSTOR-compatible
// key) with the RD-level fallback.
func buildVolumes(rd *blockstoriov1alpha1.ResourceDefinition, target *blockstoriov1alpha1.Resource) []*satellitepb.DesiredVolume {
	if rd == nil {
		return nil
	}

	if slices.Contains(target.Spec.Flags, "DISKLESS") {
		return nil
	}

	// Phase 10.3 step: typed `Spec.StoragePool` wins over the legacy
	// `Spec.Props["StorPoolName"]` so we don't read stale data when
	// the controller has already migrated. RD-level fallback still
	// goes through Props since RD has no typed slot for the pool
	// name (it lives on a per-volume VG.Props in
	// ResourceGroupVolumeGroup; the RD-prop fallback is the legacy
	// shim).
	pool := target.Spec.StoragePool
	if pool == "" {
		pool = target.Spec.Props["StorPoolName"]
	}

	if pool == "" {
		pool = rd.Spec.Props["StorPoolName"]
	}

	out := make([]*satellitepb.DesiredVolume, 0, len(rd.Spec.VolumeDefinitions))

	for _, vd := range rd.Spec.VolumeDefinitions {
		out = append(out, &satellitepb.DesiredVolume{
			VolumeNumber: vd.VolumeNumber,
			SizeKib:      vd.SizeKib,
			StoragePool:  pool,
			SeedFromGi:   seedFromGi(target, vd.VolumeNumber),
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

// DeleteResource dials the target satellite's endpoint and asks it
// to drop the resource (drbdadm down → DeleteVolume → rm .res).
// Returns the per-satellite result. Missing endpoint surfaces as a
// nil response with an error — callers retry once the Node CRD
// catches up.
func (d *Dispatcher) DeleteResource(ctx context.Context, target *blockstoriov1alpha1.Resource, rd *blockstoriov1alpha1.ResourceDefinition, nodes []blockstoriov1alpha1.Node) (*satellitepb.DeleteResourceResponse, error) {
	endpoint := lookupEndpoint(target.Spec.NodeName, nodes)
	if endpoint == "" {
		return nil, errors.Errorf("no SatelliteEndpoint for node %q", target.Spec.NodeName)
	}

	client, closer, err := d.dialer.Dial(ctx, endpoint)
	if err != nil {
		return nil, errors.Wrapf(err, "dial %s", endpoint)
	}

	defer func() { _ = closer() }()

	pool := target.Spec.StoragePool
	if pool == "" {
		pool = target.Spec.Props["StorPoolName"]
	}

	if pool == "" && rd != nil {
		pool = rd.Spec.Props["StorPoolName"]
	}

	volNumbers := make([]int32, 0)

	if rd != nil {
		for _, vd := range rd.Spec.VolumeDefinitions {
			volNumbers = append(volNumbers, vd.VolumeNumber)
		}
	}

	resp, err := client.DeleteResource(ctx, &satellitepb.DeleteResourceRequest{
		Name:          target.Spec.ResourceDefinitionName,
		StoragePool:   pool,
		VolumeNumbers: volNumbers,
	})
	if err != nil {
		return nil, errors.Wrap(err, "DeleteResource RPC")
	}

	return resp, nil
}

// CreateSnapshot dials every satellite that hosts a non-DISKLESS
// replica of `rdName` and asks it to take a snapshot. Returns the
// list of per-node results so the controller can surface granular
// status. We don't fan out concurrently — the snapshot path is
// rare and dial costs are dwarfed by the actual zfs/lvm operation.
func (d *Dispatcher) CreateSnapshot(ctx context.Context, rdName, snapName string, replicas []blockstoriov1alpha1.Resource, nodes []blockstoriov1alpha1.Node) ([]*satellitepb.CreateSnapshotResponse, error) {
	out := make([]*satellitepb.CreateSnapshotResponse, 0, len(replicas))

	for i := range replicas {
		if slices.Contains(replicas[i].Spec.Flags, "DISKLESS") {
			continue
		}

		endpoint := lookupEndpoint(replicas[i].Spec.NodeName, nodes)
		if endpoint == "" {
			out = append(out, &satellitepb.CreateSnapshotResponse{
				Ok:      false,
				Message: "no SatelliteEndpoint for node " + replicas[i].Spec.NodeName,
			})

			continue
		}

		client, closer, err := d.dialer.Dial(ctx, endpoint)
		if err != nil {
			return out, errors.Wrapf(err, "dial %s", endpoint)
		}

		resp, err := client.CreateSnapshot(ctx, &satellitepb.CreateSnapshotRequest{
			ResourceName: rdName,
			SnapshotName: snapName,
		})
		_ = closer()

		if err != nil {
			return out, errors.Wrap(err, "CreateSnapshot RPC")
		}

		out = append(out, resp)
	}

	return out, nil
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
