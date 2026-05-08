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
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	satellitepb "github.com/cozystack/blockstor/pkg/satellite/proto"
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

// Apply builds the DesiredResource for this Resource (looking up its
// peers from the full RD-wide list and its volumes from the parent
// RD) and sends it to the target satellite. Returns the per-resource
// result the satellite reported.
//
// nodes is the full Node CRD list — Apply uses it to resolve each
// peer's SatelliteEndpoint property. rd may be nil; when present the
// RD's VolumeDefinitions become DesiredVolumes for non-DISKLESS
// replicas (DISKLESS replicas ignore them).
func (d *Dispatcher) Apply(ctx context.Context, target *blockstoriov1alpha1.Resource, peers []blockstoriov1alpha1.Resource, nodes []blockstoriov1alpha1.Node, rd *blockstoriov1alpha1.ResourceDefinition) (*satellitepb.ResourceApplyResult, error) {
	endpoint := lookupEndpoint(target.Spec.NodeName, nodes)
	if endpoint == "" {
		return nil, errors.Errorf("no SatelliteEndpoint for node %q", target.Spec.NodeName)
	}

	desired := buildDesired(target, peers, nodes, rd)

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

// lookupEndpoint reads SatelliteEndpoint from the Node prop bag.
func lookupEndpoint(nodeName string, nodes []blockstoriov1alpha1.Node) string {
	for i := range nodes {
		if nodes[i].Name == nodeName {
			return nodes[i].Spec.Props["SatelliteEndpoint"]
		}
	}

	return ""
}

// buildDesired translates a Resource + its same-RD peers into the
// satellite-facing DesiredResource. Port/minor/node-id assignment is
// deterministic from the RD name + sorted peer list — good enough for
// the first stand smoke; the autoplacer will replace it with a real
// allocator once we wire the IPAM hookup.
//
// nodes is consulted for each peer's `SatelliteEndpoint` prop so
// `peer.<name>.address` carries a real (pod) IP rather than a 0.0.0.0
// placeholder; drbd-9 won't replicate to 0.0.0.0.
func buildDesired(target *blockstoriov1alpha1.Resource, peers []blockstoriov1alpha1.Resource, nodes []blockstoriov1alpha1.Node, rd *blockstoriov1alpha1.ResourceDefinition) *satellitepb.DesiredResource {
	rdName := target.Spec.ResourceDefinitionName
	port := derivePort(rdName)
	minor := deriveMinor(rdName)

	// Collect every node hosting this RD, sorted, with self at front.
	nodeNames := append([]string{}, target.Spec.NodeName)
	seen := map[string]bool{target.Spec.NodeName: true}

	for i := range peers {
		name := peers[i].Spec.NodeName
		if seen[name] {
			continue
		}

		seen[name] = true
		nodeNames = append(nodeNames, name)
	}

	// Sort all (incl. self) so node-id assignment is stable across
	// reconciles.
	sortedAll := append([]string{}, nodeNames...)
	sort.Strings(sortedAll)

	idOf := map[string]int{}
	for i, name := range sortedAll {
		idOf[name] = i
	}

	dropped := nodeNames[1:]

	drbdOpts := map[string]string{
		"port":    strconv.Itoa(port),
		"node-id": strconv.Itoa(idOf[target.Spec.NodeName]),
		"address": drbdAddrAny, // satellite picks pod IP at .res render time
		"minor":   strconv.Itoa(minor),
	}

	// Pick a single replica (lexically lowest node name) to seed
	// initial Primary. Without this every diskful brand-new RD comes
	// up Inconsistent and stays there until something opens for
	// write. The seed flag is harmless on subsequent reconciles —
	// satellite Reconciler runs primary --force only on
	// firstActivation.
	if !slices.Contains(target.Spec.Flags, "DISKLESS") &&
		target.Spec.NodeName == sortedAll[0] {
		drbdOpts["auto-primary"] = "true"
	}

	// Per-peer entries — used by ConfFileBuilder on the satellite to
	// compose the connection mesh. We resolve each peer's
	// SatelliteEndpoint prop into a real IP so drbd-9 has somewhere
	// to actually replicate to (0.0.0.0 won't work as a peer addr).
	for _, peer := range dropped {
		drbdOpts["peer."+peer+".port"] = strconv.Itoa(port)
		drbdOpts["peer."+peer+".node-id"] = strconv.Itoa(idOf[peer])
		drbdOpts["peer."+peer+".address"] = peerAddress(peer, nodes)
	}

	return &satellitepb.DesiredResource{
		Name:        rdName,
		NodeName:    target.Spec.NodeName,
		Flags:       target.Spec.Flags,
		Props:       target.Spec.Props,
		Peers:       dropped,
		Volumes:     buildVolumes(rd, target),
		DrbdOptions: drbdOpts,
	}
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

	pool := target.Spec.Props["StorPoolName"]
	if pool == "" {
		pool = rd.Spec.Props["StorPoolName"]
	}

	out := make([]*satellitepb.DesiredVolume, 0, len(rd.Spec.VolumeDefinitions))

	for _, vd := range rd.Spec.VolumeDefinitions {
		out = append(out, &satellitepb.DesiredVolume{
			VolumeNumber: vd.VolumeNumber,
			SizeKib:      vd.SizeKib,
			StoragePool:  pool,
		})
	}

	return out
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

	pool := target.Spec.Props["StorPoolName"]
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
