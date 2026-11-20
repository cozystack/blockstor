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

	"sigs.k8s.io/controller-runtime/pkg/log"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/naming"
	"github.com/cozystack/blockstor/pkg/store"
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
//
// The bound is sized against the callers' own deadline. piraeus-operator
// runs csi-provisioner and csi-attacher with --timeout=1m, which cancels
// CreateVolume and ControllerPublishVolume, and with them this request,
// after one minute. This bound plus DefaultResourceReadyWait leaves 15
// seconds of that minute for the definition, volume and placement
// writes, so a slow build is answered with the warning instead of being
// cut off by the sidecar and retried from scratch.
//
// The bound holds for one CreateVolume, not for one request: linstor-csi
// places a volume in two calls, the replica on the preferred node first
// and the rest after it, and both used to wait the full bound, two of
// them already the whole minute. See createBuildDeadline.
const DefaultResourceDeployWait = 30 * time.Second

// createVolumeWindow is how long one CreateVolume may run: the
// --timeout=1m of csi-provisioner. A replica of the definition created
// within it, by an earlier call, belongs to the same create.
const createVolumeWindow = time.Minute

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
// Every replica of the definition on a node that is not ONLINE gets
// LINSTOR's not-connected warning, whatever the layers or the replica
// count, as updateSatellites gives it for each satellite it cannot
// reach; neither step waits for such a replica. A deployed replica on
// such a node is warned once, where LINSTOR may warn it again from
// waitResourcesReady.
//
// First it waits, up to s.ResourceDeployWait counted from the start of
// the create this call belongs to (createBuildDeadline), until the
// satellites have built every replica this call deployed
// (waitDeployedReplicasBuilt),
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
//     INACTIVE or on a node that is not ONLINE;
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
	return s.waitReplicasReadyBy(ctx, rdName, deployed, s.createBuildDeadline(ctx, rdName, deployed))
}

// waitMadeAvailableReplicaReady is waitDeployedReplicasReady for the
// replica make-available created. That call is linstor-csi's
// ControllerPublishVolume, whose minute is its own, so the build gets
// the whole bound however recently the volume was created.
func (s *Server) waitMadeAvailableReplicaReady(ctx context.Context, rdName, node string) []apiv1.APICallRc {
	return s.waitReplicasReadyBy(ctx, rdName, []string{node}, time.Now().Add(s.ResourceDeployWait))
}

// createBuildDeadline is when a call that placed replicas of rdName
// stops waiting for the satellites to build them: s.ResourceDeployWait
// after the first replica of the same create was placed.
//
// linstor-csi's CreateVolume places a volume in two calls, the replica
// on the preferred node first and the remaining ones after it, inside
// the one minute csi-provisioner gives it. Each call waiting the whole
// bound took two bounds plus the ready wait, more than that minute
// when the satellites were slow: the sidecar cancelled the create and
// linstor-csi started deleting the volume it had half made. A replica
// of the definition placed by an earlier call less than
// createVolumeWindow ago belongs to the same create, so the bound
// counts from the earliest such replica instead of from this call.
// The calls of a create may reach different apiserver replicas, so the
// start is read off the replicas themselves.
func (s *Server) createBuildDeadline(ctx context.Context, rdName string, deployed []string) time.Time {
	now := time.Now()
	deadline := now.Add(s.ResourceDeployWait)

	if s.ResourceReadyWait <= 0 || s.ResourceDeployWait <= 0 {
		return deadline
	}

	earlier := s.earlierReplicas(ctx, rdName, deployed)

	for i := range earlier {
		rsc := &earlier[i]
		if rsc.CreateTimestamp == 0 {
			continue
		}

		created := time.UnixMilli(rsc.CreateTimestamp)
		if now.Sub(created) >= createVolumeWindow {
			continue
		}

		if shared := created.Add(s.ResourceDeployWait); shared.Before(deadline) {
			deadline = shared
		}
	}

	return deadline
}

// earlierReplicas lists the replicas of rdName on nodes other than
// deployed. When the cache lists none, it asks the API server: the
// first call of the create may have gone through a sibling apiserver
// replica whose write this one's cache has not seen yet, and missing it
// restarted the bound at a whole s.ResourceDeployWait from now.
func (s *Server) earlierReplicas(ctx context.Context, rdName string, deployed []string) []apiv1.Resource {
	others := func(replicas []apiv1.Resource) []apiv1.Resource {
		var out []apiv1.Resource

		for i := range replicas {
			if !slices.ContainsFunc(deployed, func(node string) bool { return naming.Same(node, replicas[i].NodeName) }) {
				out = append(out, replicas[i])
			}
		}

		return out
	}

	cached, err := s.Store.Resources().ListByDefinition(ctx, rdName)
	if err == nil {
		if out := others(cached); len(out) > 0 {
			return out
		}
	}

	live, err := store.LiveReplicas(ctx, s.Store, rdName)
	if err != nil {
		log.FromContext(ctx).WithName("rest").Error(err,
			"list replicas past the cache; counting the build bound from this call", "resourceDefinition", rdName)

		return nil
	}

	return others(live)
}

// waitReplicasReadyBy is waitDeployedReplicasReady with the build
// bound ending at buildDeadline.
func (s *Server) waitReplicasReadyBy(ctx context.Context, rdName string, deployed []string, buildDeadline time.Time) []apiv1.APICallRc {
	if s.ResourceReadyWait <= 0 || len(deployed) == 0 {
		return nil
	}

	view := s.newDeployedView(rdName, deployed)

	replicas, unconfirmed := view.list(ctx)
	notConnected := notConnectedWarnings(s.replicasNotConnected(ctx, append(replicaNodes(replicas), unconfirmed...)))

	volumes := s.deployedVolumeSizes(ctx, rdName)
	if len(volumes) == 0 {
		return notConnected
	}

	unbuilt, done := s.waitDeployedReplicasBuilt(ctx, view, volumes, buildDeadline)
	if !done {
		return nil
	}

	if len(unbuilt) > 0 {
		return append(notConnected, notReadyWarnings(unbuilt)...)
	}

	if !s.definitionHasDRBD(ctx, rdName) {
		return notConnected
	}

	deadline := time.Now().Add(s.ResourceReadyWait)

	for {
		pending := s.deployedReplicasNotReady(ctx, view, volumes)
		if len(pending) == 0 {
			return notConnected
		}

		if !time.Now().Before(deadline) {
			return append(notConnected, notReadyWarnings(pending)...)
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(resourceReadyPollInterval):
		}
	}
}

// waitDeployedReplicasBuilt waits, until deadline, for the satellites to
// build every replica of rdName this call deployed, and returns the
// nodes whose replica was not built in time. done is false when the
// request was cancelled first. It answers as soon as the last of them
// is reported built.
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
func (s *Server) waitDeployedReplicasBuilt(
	ctx context.Context, view *deployedView, volumes map[int32]int64, deadline time.Time,
) ([]string, bool) {
	if s.ResourceDeployWait <= 0 {
		return nil, true
	}

	for {
		unbuilt := s.deployedReplicasNotBuilt(ctx, view, volumes)
		if len(unbuilt) == 0 {
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

// deployedReplicasNotBuilt lists the deployed replicas of the view's
// definition whose satellite has not built them yet. A replica on a
// node that is not ONLINE has been answered with the not-connected
// warning instead, and an INACTIVE replica is never built. A replica
// neither the cache nor the API server could be read for counts as not
// built, so the bound decides rather than a failed read.
func (s *Server) deployedReplicasNotBuilt(ctx context.Context, view *deployedView, volumes map[int32]int64) []string {
	replicas, unconfirmed := view.list(ctx)

	byNode := make(map[string]*apiv1.Resource, len(replicas))
	for i := range replicas {
		byNode[naming.Key(replicas[i].NodeName)] = &replicas[i]
	}

	out := s.onlineNodes(ctx, unconfirmed)

	for _, node := range view.deployed {
		rsc, ok := byNode[naming.Key(node)]
		if !ok || slices.Contains(rsc.Flags, apiv1.ResourceFlagInactive) || !s.nodeIsOnline(ctx, rsc.NodeName) {
			continue
		}

		if !replicaBuilt(rsc, volumes) {
			out = append(out, rsc.NodeName)
		}
	}

	return out
}

// onlineNodes keeps the nodes of nodes whose satellite is ONLINE.
func (s *Server) onlineNodes(ctx context.Context, nodes []string) []string {
	var out []string

	for _, node := range nodes {
		if s.nodeIsOnline(ctx, node) {
			out = append(out, node)
		}
	}

	return out
}

// deployedVolumeSizes is definitionVolumeSizes for the waits: when the
// cache lists no volume, it asks the API server. linstor-csi writes the
// volume definitions right before the autoplace or create, through
// whichever apiserver replica the Service picks, and a cache that has
// not caught up made the waits answer as for a definition without
// volumes, before anything was built.
func (s *Server) deployedVolumeSizes(ctx context.Context, rdName string) map[int32]int64 {
	if sizes := s.definitionVolumeSizes(ctx, rdName); len(sizes) > 0 {
		return sizes
	}

	vds, err := store.LiveVolumes(ctx, s.Store, rdName)
	if err != nil {
		log.FromContext(ctx).WithName("rest").Error(err,
			"list volume definitions past the cache; answering without waiting", "resourceDefinition", rdName)

		return nil
	}

	out := make(map[int32]int64, len(vds))
	for i := range vds {
		out[vds[i].VolumeNumber] = vds[i].SizeKib
	}

	return out
}

// deployedView is what one wait reads the replicas of rdName through,
// for the replicas on deployed. It reads the cache, and the API server
// when the cache does not list one of them yet.
//
// The call that deployed them wrote them a few milliseconds earlier,
// and this apiserver's informer cache trails its own writes. Both waits
// skip a deployed replica they do not find, as one deleted meanwhile,
// so a cache that had not caught up answered a single local volume at
// once, before its satellite built it: linstor-csi then read a view
// without the device path and refused ControllerPublishVolume with
// "could not determine device path". LINSTOR answers from the objects
// it just created, so it never misses one.
type deployedView struct {
	s        *Server
	rdName   string
	deployed []string

	// gone holds the deployed nodes the API server listed no replica
	// on: deleted meanwhile, for instance by a linstor-csi that gave
	// up. The cache will not list them either, and asking the API
	// server again on every poll would only add load.
	gone map[string]bool

	// failed is set once a failed read past the cache was logged, so a
	// wait logs it once and not on every poll.
	failed bool
}

func (s *Server) newDeployedView(rdName string, deployed []string) *deployedView {
	return &deployedView{s: s, rdName: rdName, deployed: deployed, gone: map[string]bool{}}
}

// list returns the replicas of the definition, and the deployed nodes
// whose replica neither the cache nor the API server could be read for.
// The caller treats those as not built and not ready, so a failed read
// holds the answer until the bound instead of ending the wait as if
// there were nothing to wait for.
func (v *deployedView) list(ctx context.Context) ([]apiv1.Resource, []string) {
	replicas, cacheErr := v.s.Store.Resources().ListByDefinition(ctx, v.rdName)

	var missing []string

	for _, node := range v.deployed {
		if v.gone[naming.Key(node)] {
			continue
		}

		if cacheErr != nil || !slices.ContainsFunc(replicas, func(r apiv1.Resource) bool { return naming.Same(r.NodeName, node) }) {
			missing = append(missing, node)
		}
	}

	if len(missing) == 0 {
		return replicas, nil
	}

	live, err := store.LiveReplicas(ctx, v.s.Store, v.rdName)
	if err != nil {
		if !v.failed {
			v.failed = true
			log.FromContext(ctx).WithName("rest").Error(err,
				"list replicas past the cache; waiting for the ones the cache does not list until the bound",
				"resourceDefinition", v.rdName, "nodes", missing)
		}

		return replicas, missing
	}

	for _, node := range missing {
		if !slices.ContainsFunc(live, func(r apiv1.Resource) bool { return naming.Same(r.NodeName, node) }) {
			v.gone[naming.Key(node)] = true
		}
	}

	return live, nil
}

// replicaBuilt reports whether the satellite built every volume of rsc:
// each has the device path the satellite reports once the device is up
// (and formatted, when a filesystem was asked for). That holds for a
// diskless replica too, whose path is its DRBD device: the path is what
// linstor-csi's ControllerPublishVolume reads, and a disk state alone
// let make-available answer before the view had one.
func replicaBuilt(rsc *apiv1.Resource, volumes map[int32]int64) bool {
	for vn := range volumes {
		vol := reportedVolume(rsc, vn)
		if vol == nil || vol.DevicePath == "" {
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

// replicasNotConnected lists, in order, those of the replica nodes
// whose satellite is not ONLINE: those LINSTOR's updateSatellites
// cannot reach and answers with the not-connected warning. A replica on
// an EVICTED node is left out, as updateSatellites skips it. The nodes
// come from the wait's deployedView, so a replica the cache does not
// list yet is warned like the others.
func (s *Server) replicasNotConnected(ctx context.Context, nodes []string) []string {
	var out []string

	for _, name := range nodes {
		node, err := s.Store.Nodes().Get(ctx, name)
		if err != nil || slices.Contains(node.Flags, apiv1.NodeFlagEvicted) {
			continue
		}

		if node.ConnectionStatus != apiv1.NodeTypeOnline {
			out = append(out, name)
		}
	}

	slices.Sort(out)

	return out
}

// deployedReplicasNotReady lists the deployed replicas of the view's
// definition that LINSTOR would wait for and that are not ready yet. A
// replica on a node that is not ONLINE is not waited for: it has been
// answered with the not-connected warning. A deployed replica that could
// not be read is counted as not ready, as in deployedReplicasNotBuilt.
func (s *Server) deployedReplicasNotReady(ctx context.Context, view *deployedView, volumes map[int32]int64) []string {
	replicas, unconfirmed := view.list(ctx)

	if len(replicas)+len(unconfirmed) <= 1 || (len(unconfirmed) == 0 && allReplicasDiskless(replicas)) {
		// The first replica has no peer to connect to, and an
		// all-diskless definition has no data to reach: LINSTOR
		// does not wait for either.
		return nil
	}

	byNode := make(map[string]*apiv1.Resource, len(replicas))
	online := make(map[string]bool, len(replicas))

	for i := range replicas {
		key := naming.Key(replicas[i].NodeName)
		byNode[key] = &replicas[i]
		online[key] = s.nodeIsOnline(ctx, replicas[i].NodeName)
	}

	peers := onlineDiskfulPeers(replicas, online)

	pending := s.onlineNodes(ctx, unconfirmed)

	for _, node := range view.deployed {
		rsc, ok := byNode[naming.Key(node)]
		if !ok || slices.Contains(rsc.Flags, apiv1.ResourceFlagInactive) || !online[naming.Key(node)] {
			continue
		}

		if !replicaReady(rsc, peers, byNode, volumes) {
			pending = append(pending, rsc.NodeName)
		}
	}

	return pending
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
// expected peers. The replica must also report a device path for every
// volume: LINSTOR knows the path from its layer data the moment the
// resource exists, blockstor learns it from the satellite. After the
// build step that always holds already; the check matters only when
// ResourceDeployWait is zero and the build step is skipped.
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

		if vol.DevicePath == "" {
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
