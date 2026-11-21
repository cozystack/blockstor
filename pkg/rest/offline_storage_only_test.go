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
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// seedOfflineStorageOnly is a STORAGE-only definition whose replica on
// n2 sits on an OFFLINE node. Both replicas carry what the store
// projects for them: the default DRBD chain with the last reported
// peer links. Only the definition names the real stack.
func seedOfflineStorageOnly(t *testing.T) store.Store {
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

	err := backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: "pvc-local", LayerStack: []string{apiv1.LayerKindStorage},
	})
	if err != nil {
		t.Fatalf("seed definition: %v", err)
	}

	for _, pair := range [][2]string{{"n1", "n2"}, {"n2", "n1"}} {
		err := backend.Resources().Create(ctx, &apiv1.Resource{
			Name: "pvc-local", NodeName: pair[0],
			State:   apiv1.ResourceState{DrbdState: "UpToDate"},
			Volumes: []apiv1.Volume{{VolumeNumber: 0, State: apiv1.VolumeState{DiskState: "UpToDate"}}},
			LayerObject: &apiv1.ResourceLayer{
				Type: apiv1.LayerKindDRBD,
				Drbd: &apiv1.DrbdResourceLayer{
					Connections: map[string]apiv1.DrbdConnection{pair[1]: {Connected: true, Message: "Connected"}},
				},
			},
		})
		if err != nil {
			t.Fatalf("seed replica on %s: %v", pair[0], err)
		}
	}

	return backend
}

// linstorClientRow is what linstor-client v1.27.1 prints in the State
// and Conns cells of `r l` for a replica, reduced to the inputs it reads
// for a replica with volumes: a volume whose top layer is not DRBD
// reads Created before any state is looked at
// (linstor_client/commands/vlm_cmds.py), and a replica with no DRBD
// layer has Conns Ok (linstor_client/commands/rsc_cmds.py). Anything
// else falls to the disk state, which is not this test's concern.
func linstorClientRow(rsc *apiv1.ResourceWithVolumes) (string, string) {
	state := "disk state"

	if len(rsc.Volumes) > 0 {
		layers := rsc.Volumes[0].LayerDataList
		if len(layers) > 0 && !strings.EqualFold(layers[0].Type, apiv1.LayerKindDRBD) {
			state = "Created"
		}
	}

	conns := "Ok"

	for layer := rsc.LayerObject; layer != nil; {
		if strings.EqualFold(layer.Type, apiv1.LayerKindDRBD) {
			conns = "peer links"

			break
		}

		if len(layer.Children) == 0 {
			break
		}

		layer = &layer.Children[0]
	}

	return state, conns
}

// A STORAGE-only replica on an OFFLINE node is answered with its
// volumes and its STORAGE layer data, as LINSTOR answers it, so
// linstor-client shows it Created with Conns Ok and leaves it out of
// `--faulty`. Forgetting the dead satellite's state must not take
// either away, or the row turns into the Unknown a DRBD replica on
// that node reads.
func TestOfflineNodeStorageOnlyReplicaReadsCreated(t *testing.T) {
	base, stop := startServerWithStore(t, seedOfflineStorageOnly(t))
	defer stop()

	check := func(where string, rsc *apiv1.ResourceWithVolumes) {
		t.Helper()

		if len(rsc.Volumes) == 0 {
			t.Fatalf("%s: replica on the OFFLINE node is answered without volumes: %+v", where, rsc)
		}

		if state, conns := linstorClientRow(rsc); state != "Created" || conns != "Ok" {
			t.Errorf("%s: linstor-client would show State %q, Conns %q, want Created, Ok: %+v",
				where, state, conns, rsc)
		}
	}

	for _, path := range []string{"/v1/view/resources", "/v1/resource-definitions/pvc-local/resources"} {
		got := decodeReplicas(t, base+path)

		for _, node := range []string{"n1", "n2"} {
			rsc, ok := got[node]
			if !ok {
				t.Fatalf("%s: no replica on %s", path, node)
			}

			check(path+" "+node, &rsc)
		}
	}

	resp := httpGet(t, base+"/v1/resource-definitions/pvc-local/resources/n2")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("single replica: status = %d", resp.StatusCode)
	}

	var one apiv1.ResourceWithVolumes
	if err := json.NewDecoder(resp.Body).Decode(&one); err != nil {
		t.Fatalf("decode single replica: %v", err)
	}

	check("single replica", &one)
}
