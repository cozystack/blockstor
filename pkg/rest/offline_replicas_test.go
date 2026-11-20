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

		for _, vol := range rsc.LayerObject.Drbd.DrbdVolumes {
			if vol.DiskState != "" {
				t.Errorf("%s: DRBD layer volume %d keeps %q", where, vol.VolumeNumber, vol.DiskState)
			}
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
