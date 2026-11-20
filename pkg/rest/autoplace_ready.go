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

package rest

import (
	"context"
	"slices"
	"time"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/naming"
)

// DefaultResourceReadyWait is how long LINSTOR waits for a deployed
// resource to become ready before it answers the create with a
// warning (CtrlRscCrtApiHelper.DFLT_RSC_READY_WAIT_TIME_IN_MS).
const DefaultResourceReadyWait = 15 * time.Second

// DefaultResourceDeployWait bounds how long a create or autoplace waits
// for the satellites to build the replicas it deployed, before the
// ready wait starts. LINSTOR has no such bound: its satellites answer
// every update, with an error when the apply failed. A blockstor
// satellite reports no failure to the REST layer, so the wait gives up
// and answers with the not-ready warning instead of holding the call.
const DefaultResourceDeployWait = 60 * time.Second

// resourceReadyPollInterval is how often the wait re-reads the cached
// replicas. A variable so tests can shorten it.
//
//nolint:gochecknoglobals // tunable
var resourceReadyPollInterval = 250 * time.Millisecond

// warnNotConnected is LINSTOR's WARN_NOT_CONNECTED (1002) in
// blockstor's warning band: the answer for a deployed resource whose
// satellite is not connected, given instead of waiting for it.
const warnNotConnected = maskWarn | int64(1002)

// waitDeployedReplicasReady holds a create or autoplace answer the way
// LINSTOR v1.33.1's CtrlRscCrtApiHelper.deployResources does, in two
// steps.
//
// First it waits, up to s.ResourceDeployWait, until the satellites
// have built every replica this call deployed (waitDeployedReplicasBuilt),
// as LINSTOR's updateSatellites waits for each satellite to answer the
// update before anything else. That step applies to every definition,
// whatever its layers or replica count: a local volume is answered once
// its device exists and, when it was asked for, carries its filesystem.
//
// Then it waits, up to s.ResourceReadyWait, for the replicas to become
// ready, following waitResourcesReady:
//
//   - nothing is awaited when the call deployed nothing, the definition
//     has no volume, every replica is diskless, the definition has a
//     single replica, or its layer stack has no DRBD;
//   - a deployed replica, diskful or diskless, is awaited unless it is
//     INACTIVE; one on a node that is not ONLINE is answered with
//     LINSTOR's not-connected warning instead of being awaited;
//   - ready is ResourceState.isReady: every volume reaches UpToDate
//     data, locally or through a connected UpToDate peer, and the
//     replica is connected to every diskful DRBD peer on an ONLINE node.
//
// linstor-csi relies on that answer: its ControllerPublishVolume reads
// the volume's device path right after CreateVolume returned and fails
// the attach when there is none. Answering before the satellites built
// the replicas turned that into a ControllerPublishVolume error whose
// exponential retry roughly doubled the time to attach.
//
// Replicas that are not ready within the bound get the warning LINSTOR
// gives in that case; the placement itself stands either way.
func (s *Server) waitDeployedReplicasReady(ctx context.Context, rdName string, deployed []string) []apiv1.APICallRc {
	if s.ResourceReadyWait <= 0 || len(deployed) == 0 {
		return nil
	}

	volumes := s.definitionVolumeSizes(ctx, rdName)
	if len(volumes) == 0 {
		return nil
	}

	unbuilt, done := s.waitDeployedReplicasBuilt(ctx, rdName, deployed, volumes)
	if !done {
		return nil
	}

	if len(unbuilt) > 0 {
		return notReadyWarnings(unbuilt)
	}

	if !s.definitionHasDRBD(ctx, rdName) {
		return nil
	}

	deadline := time.Now().Add(s.ResourceReadyWait)

	for {
		notConnected, pending, err := s.deployedReplicasNotReady(ctx, rdName, deployed, volumes)
		if err != nil {
			return nil
		}

		if len(pending) == 0 {
			return notConnectedWarnings(notConnected)
		}

		if !time.Now().Before(deadline) {
			return append(notConnectedWarnings(notConnected), notReadyWarnings(pending)...)
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(resourceReadyPollInterval):
		}
	}
}

// waitDeployedReplicasBuilt waits, up to s.ResourceDeployWait, until the
// satellites built every replica of rdName this call deployed, and
// returns the nodes whose replica was not built in time. done is false
// when the request was cancelled first.
//
// linstor-csi needs this before CreateVolume returns. It reads replicas
// through a cache of the resource view that it refills after its own
// writes and otherwise keeps for 30 seconds, so a CreateVolume answered
// before the device path existed has ControllerPublishVolume read a
// view without one until the cache expires, and the attacher's
// exponential retry lands the attach well after that.
//
// A zero s.ResourceDeployWait skips the step, which unit tests that run
// no satellite rely on; the binaries set DefaultResourceDeployWait.
func (s *Server) waitDeployedReplicasBuilt(ctx context.Context, rdName string, deployed []string, volumes map[int32]int64) ([]string, bool) {
	if s.ResourceDeployWait <= 0 {
		return nil, true
	}

	deadline := time.Now().Add(s.ResourceDeployWait)

	for {
		unbuilt, err := s.deployedReplicasNotBuilt(ctx, rdName, deployed, volumes)
		if err != nil || len(unbuilt) == 0 {
			return nil, true
		}

		if !time.Now().Before(deadline) {
			return unbuilt, true
		}

		select {
		case <-ctx.Done():
			return nil, false
		case <-time.After(resourceReadyPollInterval):
		}
	}
}

// deployedReplicasNotBuilt lists the deployed replicas of rdName whose
// satellite has not built them yet. A replica on a node that is not
// ONLINE is left to the ready step, which answers it with LINSTOR's
// not-connected warning, and an INACTIVE replica is never built.
func (s *Server) deployedReplicasNotBuilt(ctx context.Context, rdName string, deployed []string, volumes map[int32]int64) ([]string, error) {
	replicas, err := s.Store.Resources().ListByDefinition(ctx, rdName)
	if err != nil {
		return nil, errors.Wrap(err, "list replicas")
	}

	byNode := make(map[string]*apiv1.Resource, len(replicas))
	for i := range replicas {
		byNode[naming.Key(replicas[i].NodeName)] = &replicas[i]
	}

	var out []string

	for _, node := range deployed {
		rsc, ok := byNode[naming.Key(node)]
		if !ok || slices.Contains(rsc.Flags, apiv1.ResourceFlagInactive) || !s.nodeIsOnline(ctx, rsc.NodeName) {
			continue
		}

		if !replicaBuilt(rsc, volumes) {
			out = append(out, rsc.NodeName)
		}
	}

	return out, nil
}

// replicaBuilt reports whether the satellite built every volume of rsc:
// a diskful volume has the device path the satellite reports once the
// device is up (and formatted, when a filesystem was asked for), a
// diskless one has the disk state the satellite reads off the kernel.
func replicaBuilt(rsc *apiv1.Resource, volumes map[int32]int64) bool {
	for vn := range volumes {
		vol := reportedVolume(rsc, vn)
		if vol == nil {
			return false
		}

		if isDisklessReplica(rsc) {
			if vol.State.DiskState == "" {
				return false
			}

			continue
		}

		if vol.DevicePath == "" {
			return false
		}
	}

	return true
}

// definitionHasDRBD reports whether rdName's effective layer stack
// carries DRBD; without it there is no DRBD resource to become ready
// (LINSTOR's containsDrbdLayerData).
func (s *Server) definitionHasDRBD(ctx context.Context, rdName string) bool {
	rd, err := s.Store.ResourceDefinitions().Get(ctx, rdName)
	if err != nil {
		return false
	}

	return apiv1.LayerInStack(s.resolveAutoplaceLayerStack(ctx, &rd), apiv1.LayerKindDRBD)
}

// deployedReplicasNotReady splits the deployed replicas of rdName that
// LINSTOR would wait for into those on a node that is not ONLINE and
// those that are not ready yet.
func (s *Server) deployedReplicasNotReady(ctx context.Context, rdName string, deployed []string, volumes map[int32]int64) ([]string, []string, error) {
	replicas, err := s.Store.Resources().ListByDefinition(ctx, rdName)
	if err != nil {
		return nil, nil, errors.Wrap(err, "list replicas")
	}

	if len(replicas) <= 1 || allReplicasDiskless(replicas) {
		// The first replica has no peer to connect to, and an
		// all-diskless definition has no data to reach: LINSTOR
		// does not wait for either.
		return nil, nil, nil
	}

	byNode := make(map[string]*apiv1.Resource, len(replicas))
	online := make(map[string]bool, len(replicas))

	for i := range replicas {
		key := naming.Key(replicas[i].NodeName)
		byNode[key] = &replicas[i]
		online[key] = s.nodeIsOnline(ctx, replicas[i].NodeName)
	}

	peers := onlineDiskfulPeers(replicas, online)

	var notConnected, pending []string

	for _, node := range deployed {
		rsc, ok := byNode[naming.Key(node)]
		if !ok || slices.Contains(rsc.Flags, apiv1.ResourceFlagInactive) {
			continue
		}

		if !online[naming.Key(node)] {
			notConnected = append(notConnected, rsc.NodeName)

			continue
		}

		if !replicaReady(rsc, peers, byNode, volumes) {
			pending = append(pending, rsc.NodeName)
		}
	}

	return notConnected, pending, nil
}

// nodeIsOnline reports whether the satellite on nodeName is ONLINE,
// the only connection state LINSTOR waits on.
func (s *Server) nodeIsOnline(ctx context.Context, nodeName string) bool {
	node, err := s.Store.Nodes().Get(ctx, nodeName)
	if err != nil {
		return false
	}

	return node.ConnectionStatus == apiv1.NodeTypeOnline
}

// allReplicasDiskless is LINSTOR's allDiskless.
func allReplicasDiskless(replicas []apiv1.Resource) bool {
	for i := range replicas {
		if !isDisklessReplica(&replicas[i]) {
			return false
		}
	}

	return true
}

// onlineDiskfulPeers is LINSTOR's getOnlineNodeIds: the diskful,
// active replicas on ONLINE nodes, which a ready replica is connected
// to. Replicas being deleted are left out as well, so a create next to
// a deletion is not held for a peer that is going away.
func onlineDiskfulPeers(replicas []apiv1.Resource, online map[string]bool) []string {
	var out []string

	for i := range replicas {
		rsc := &replicas[i]

		if isDisklessReplica(rsc) || !online[naming.Key(rsc.NodeName)] ||
			slices.Contains(rsc.Flags, apiv1.ResourceFlagInactive) ||
			slices.Contains(rsc.Flags, apiv1.ResourceFlagDeleting) ||
			slices.Contains(rsc.Flags, apiv1.ResourceFlagDelete) {
			continue
		}

		out = append(out, rsc.NodeName)
	}

	return out
}

// replicaReady is LINSTOR's ResourceState.isReady for rsc against the
// expected peers. A diskful replica must also report a device path for
// every volume: LINSTOR knows the path from its layer data the moment
// the resource exists, blockstor learns it from the satellite.
func replicaReady(rsc *apiv1.Resource, peers []string, byNode map[string]*apiv1.Resource, volumes map[int32]int64) bool {
	conns := drbdConnections(rsc)

	for _, peer := range peers {
		if naming.Same(peer, rsc.NodeName) {
			continue
		}

		if !conns[naming.Key(peer)] {
			return false
		}
	}

	for vn := range volumes {
		vol := reportedVolume(rsc, vn)
		if vol == nil {
			return false
		}

		if !isDisklessReplica(rsc) && vol.DevicePath == "" {
			return false
		}

		if vol.State.DiskState != diskStateUpToDate && !reachesUpToDatePeer(conns, byNode, vn) {
			return false
		}
	}

	return true
}

// drbdConnections returns the peers rsc reports a live DRBD connection
// to, keyed by naming.Key of the peer node.
func drbdConnections(rsc *apiv1.Resource) map[string]bool {
	out := map[string]bool{}

	if rsc.LayerObject == nil || rsc.LayerObject.Drbd == nil {
		return out
	}

	for peer, conn := range rsc.LayerObject.Drbd.Connections {
		if conn.Connected {
			out[naming.Key(peer)] = true
		}
	}

	return out
}

// reportedVolume returns rsc's reported volume vn, or nil.
func reportedVolume(rsc *apiv1.Resource, vn int32) *apiv1.Volume {
	for i := range rsc.Volumes {
		if rsc.Volumes[i].VolumeNumber == vn {
			return &rsc.Volumes[i]
		}
	}

	return nil
}

// reachesUpToDatePeer reports whether one of the connected peers holds
// UpToDate data for volume vn: the way a diskless or still-syncing
// replica has access to up-to-date data.
func reachesUpToDatePeer(conns map[string]bool, byNode map[string]*apiv1.Resource, vn int32) bool {
	for peer := range conns {
		rsc, ok := byNode[peer]
		if !ok {
			continue
		}

		if vol := reportedVolume(rsc, vn); vol != nil && vol.State.DiskState == diskStateUpToDate {
			return true
		}
	}

	return false
}

// replicaNodes lists the nodes of replicas.
func replicaNodes(replicas []apiv1.Resource) []string {
	out := make([]string, 0, len(replicas))

	for i := range replicas {
		out = append(out, replicas[i].NodeName)
	}

	return out
}

// notConnectedWarnings renders LINSTOR's makeNotConnectedWarning for
// each deployed replica whose satellite is not connected.
func notConnectedWarnings(nodes []string) []apiv1.APICallRc {
	out := make([]apiv1.APICallRc, 0, len(nodes))

	for _, node := range nodes {
		out = append(out, apiv1.APICallRc{
			RetCode: warnNotConnected,
			Message: "No active connection to satellite '" + node + "'",
			Details: "The controller is trying to (re-) establish a connection to the satellite. " +
				"The controller stored the changes and as soon the satellite is connected, it will " +
				"receive this update.",
			ObjRefs: map[string]string{"Node": node},
		})
	}

	return out
}

// notReadyWarnings renders LINSTOR's per-node warning for a resource
// that did not become ready in time.
func notReadyWarnings(nodes []string) []apiv1.APICallRc {
	out := make([]apiv1.APICallRc, 0, len(nodes))

	for _, node := range nodes {
		out = append(out, apiv1.APICallRc{
			RetCode: maskWarn,
			Message: "Resource did not become ready on node '" + node +
				"' within reasonable time, check Satellite for errors.",
			ObjRefs: map[string]string{"Node": node},
		})
	}

	return out
}
