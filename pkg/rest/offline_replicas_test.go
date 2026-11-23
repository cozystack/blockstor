// SPDX-License-Identifier: Apache-2.0

package rest

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// seedOfflinePeer stores a two-replica definition whose second node is
// OFFLINE while the store still holds that satellite's last report: a
// connected UpToDate Secondary, what a hard reset leaves behind.
func seedOfflinePeer(t *testing.T) store.Store {
	t.Helper()

	backend := store.NewInMemory()
	ctx := t.Context()

	nodes := []apiv1.Node{
		{Name: "n1", ConnectionStatus: apiv1.NodeTypeOnline},
		{Name: "n2", ConnectionStatus: apiv1.NodeTypeOffline},
	}
	for i := range nodes {
		if err := backend.Nodes().Create(ctx, &nodes[i]); err != nil {
			t.Fatalf("seed node: %v", err)
		}
	}

	if err := backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-x"}); err != nil {
		t.Fatalf("seed definition: %v", err)
	}

	for _, pair := range [][2]string{{"n1", "n2"}, {"n2", "n1"}} {
		seedReportedReplica(ctx, t, backend, pair[0], pair[1])
	}

	return backend
}

func seedReportedReplica(ctx context.Context, t *testing.T, backend store.Store, node, peer string) {
	t.Helper()

	unused := false

	err := backend.Resources().Create(ctx, &apiv1.Resource{
		Name: "pvc-x", NodeName: node,
		State:   apiv1.ResourceState{InUse: &unused, DrbdState: "UpToDate"},
		Volumes: []apiv1.Volume{{VolumeNumber: 0, State: apiv1.VolumeState{DiskState: "UpToDate"}}},
		LayerObject: &apiv1.ResourceLayer{
			Type: apiv1.LayerKindDRBD,
			Drbd: &apiv1.DrbdResourceLayer{
				Connections: map[string]apiv1.DrbdConnection{peer: {Connected: true, Message: "Connected"}},
				DrbdVolumes: []apiv1.DrbdVolume{{VolumeNumber: 0, DiskState: "UpToDate"}},
			},
		},
	})
	if err != nil {
		t.Fatalf("seed replica on %s: %v", node, err)
	}
}

func decodeReplicas(t *testing.T, url string) map[string]apiv1.ResourceWithVolumes {
	t.Helper()

	resp := httpGet(t, url)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}

	var list []apiv1.ResourceWithVolumes
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		t.Fatalf("decode: %v", err)
	}

	out := make(map[string]apiv1.ResourceWithVolumes, len(list))
	for i := range list {
		out[list[i].NodeName] = list[i]
	}

	return out
}

func assertNoSatelliteState(t *testing.T, where string, rsc *apiv1.ResourceWithVolumes) {
	t.Helper()

	if rsc.State.InUse != nil || rsc.State.DrbdState != "" {
		t.Errorf("%s: replica on the OFFLINE node keeps its role: %+v", where, rsc.State)
	}

	for _, vol := range rsc.Volumes {
		if vol.State.DiskState != "" {
			t.Errorf("%s: volume %d keeps the stale disk state %q", where, vol.VolumeNumber, vol.State.DiskState)
		}
	}

	if rsc.LayerObject != nil && rsc.LayerObject.Drbd != nil {
		if len(rsc.LayerObject.Drbd.Connections) > 0 {
			t.Errorf("%s: replica on the OFFLINE node keeps its peer links: %+v", where, rsc.LayerObject.Drbd.Connections)
		}
	}
}

func assertSatelliteState(t *testing.T, where string, rsc *apiv1.ResourceWithVolumes) {
	t.Helper()

	if rsc.State.InUse == nil || len(rsc.Volumes) == 0 || rsc.Volumes[0].State.DiskState != "UpToDate" {
		t.Errorf("%s: replica on the ONLINE node lost its state: %+v %+v", where, rsc.State, rsc.Volumes)
	}
}

// LINSTOR answers the replicas of a node whose satellite is offline with
// no satellite state, which linstor-client shows as Unknown and lists
// under `--faulty`. Every read of a replica gives that answer; the
// replica on the node that is up keeps its own.
func TestOfflineNodeReplicasCarryNoSatelliteState(t *testing.T) {
	base, stop := startServerWithStore(t, seedOfflinePeer(t))
	defer stop()

	for _, path := range []string{"/v1/view/resources", "/v1/resource-definitions/pvc-x/resources"} {
		got := decodeReplicas(t, base+path)

		offline := got["n2"]
		assertNoSatelliteState(t, path, &offline)

		online := got["n1"]
		assertSatelliteState(t, path, &online)
	}

	resp := httpGet(t, base+"/v1/resource-definitions/pvc-x/resources/n2")
	defer func() { _ = resp.Body.Close() }()

	var one apiv1.ResourceWithVolumes
	if err := json.NewDecoder(resp.Body).Decode(&one); err != nil {
		t.Fatalf("decode single replica: %v", err)
	}

	assertNoSatelliteState(t, "single replica", &one)
}

// The per-replica volume reads answer from the same replica and must
// agree with the view: LINSTOR fills `volumes[].state` from the
// satellite's state on these endpoints too, so an OFFLINE node's
// volume carries none.
func TestOfflineNodeVolumesCarryNoSatelliteState(t *testing.T) {
	base, stop := startServerWithStore(t, seedOfflinePeer(t))
	defer stop()

	decode := func(url string, into any) {
		t.Helper()

		resp := httpGet(t, url)
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s: status = %d", url, resp.StatusCode)
		}

		if err := json.NewDecoder(resp.Body).Decode(into); err != nil {
			t.Fatalf("%s: decode: %v", url, err)
		}
	}

	var offlineList []apiv1.Volume
	decode(base+"/v1/resource-definitions/pvc-x/resources/n2/volumes", &offlineList)

	if len(offlineList) != 1 || offlineList[0].State.DiskState != "" {
		t.Errorf("volume list of the OFFLINE node's replica = %+v, want one volume with no disk state", offlineList)
	}

	var offlineOne apiv1.Volume
	decode(base+"/v1/resource-definitions/pvc-x/resources/n2/volumes/0", &offlineOne)

	if offlineOne.State.DiskState != "" {
		t.Errorf("volume 0 of the OFFLINE node's replica keeps disk state %q", offlineOne.State.DiskState)
	}

	var onlineOne apiv1.Volume
	decode(base+"/v1/resource-definitions/pvc-x/resources/n1/volumes/0", &onlineOne)

	if onlineOne.State.DiskState != "UpToDate" {
		t.Errorf("volume 0 of the ONLINE node's replica = %q, want UpToDate", onlineOne.State.DiskState)
	}
}

// A node back ONLINE after a reboot runs a new satellite, which has
// not reported the replica yet: the stored state is the one from before
// the crash, and the reads answer it with no state, as LINSTOR does
// until the reconnected satellite reports it.
func TestReplicaNotReportedSinceRestartCarriesNoSatelliteState(t *testing.T) {
	backend := seedOfflinePeer(t)
	ctx := t.Context()

	node, err := backend.Nodes().Get(ctx, "n2")
	if err != nil {
		t.Fatalf("get node: %v", err)
	}

	node.ConnectionStatus = apiv1.NodeTypeOnline
	node.SatelliteIncarnation = "inc-2"

	if err := backend.Nodes().Update(ctx, &node); err != nil {
		t.Fatalf("bring node back: %v", err)
	}

	stale, err := backend.Resources().Get(ctx, "pvc-x", "n2")
	if err != nil {
		t.Fatalf("get replica: %v", err)
	}

	stale.SatelliteIncarnation = "inc-1"

	if err := backend.Resources().Update(ctx, &stale); err != nil {
		t.Fatalf("stamp the old incarnation: %v", err)
	}

	base, stop := startServerWithStore(t, backend)
	defer stop()

	got := decodeReplicas(t, base+"/v1/view/resources")

	before := got["n2"]
	assertNoSatelliteState(t, "replica reported before the restart", &before)

	online := got["n1"]
	assertSatelliteState(t, "replica on the other node", &online)
}

// The view answers an OFFLINE node's replica with no state, which
// linstor-client and the blockstor CLI read as Unknown and list under
// `--faulty`; `?faulty=true` passed over it for carrying no disk state.
func TestOfflineNodeReplicaIsFaulty(t *testing.T) {
	base, stop := startServerWithStore(t, seedOfflinePeer(t))
	defer stop()

	faulty := decodeReplicas(t, base+"/v1/view/resources?faulty=true")
	if _, listed := faulty["n2"]; !listed {
		t.Errorf("?faulty=true does not list the OFFLINE node's replica: %v", faulty)
	}
}

// seedDestroyedKernelResource stores a two-replica definition on two
// ONLINE nodes whose second replica lost its DRBD kernel resource
// (`drbdsetup down`, or a bring-up that keeps failing): the satellite
// left a cleared status that still reads Secondary, and the
// KernelLoaded Condition False.
func seedDestroyedKernelResource(t *testing.T) store.Store {
	t.Helper()

	backend := seedOfflinePeer(t)
	ctx := t.Context()

	node, err := backend.Nodes().Get(ctx, "n2")
	if err != nil {
		t.Fatalf("get node: %v", err)
	}

	node.ConnectionStatus = apiv1.NodeTypeOnline

	if err := backend.Nodes().Update(ctx, &node); err != nil {
		t.Fatalf("bring node up: %v", err)
	}

	down, err := backend.Resources().Get(ctx, "pvc-x", "n2")
	if err != nil {
		t.Fatalf("get replica: %v", err)
	}

	unused := false
	down.State = apiv1.ResourceState{InUse: &unused}
	down.Volumes = nil
	down.LayerObject.Drbd.Connections = nil
	down.LayerObject.Drbd.DrbdVolumes = nil
	down.KernelSlotDestroyed = true

	if err := backend.Resources().Update(ctx, &down); err != nil {
		t.Fatalf("store the departed replica: %v", err)
	}

	return backend
}

// LINSTOR answers a replica whose kernel resource is gone with no
// state, linstor-client's Unknown; the view answered in_use false,
// which reads as a healthy unused replica, and `?faulty=true` passed
// over it because it carried no disk state, though it holds no data
// and no peer link.
func TestDestroyedKernelResourceReadsUnknownAndFaulty(t *testing.T) {
	base, stop := startServerWithStore(t, seedDestroyedKernelResource(t))
	defer stop()

	got := decodeReplicas(t, base+"/v1/view/resources")

	down := got["n2"]
	assertNoSatelliteState(t, "replica with no kernel resource", &down)

	up := got["n1"]
	assertSatelliteState(t, "replica on the other node", &up)

	faulty := decodeReplicas(t, base+"/v1/view/resources?faulty=true")
	if _, listed := faulty["n2"]; !listed {
		t.Errorf("?faulty=true does not list the replica with no kernel resource: %v", faulty)
	}
}
