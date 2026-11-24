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
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// createRDForPeerSlots posts an RD create and returns the peer slots the
// stored definition got.
func createRDForPeerSlots(t *testing.T, st store.Store, body string) int32 {
	t.Helper()

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/resource-definitions", []byte(body))
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		got, _ := io.ReadAll(resp.Body)
		t.Fatalf("create: status %d, body %s", resp.StatusCode, got)
	}

	rd, err := st.ResourceDefinitions().Get(t.Context(), "pvc-slots")
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	return rd.PeerSlots
}

// A new definition gets its peer slots the way LINSTOR resolves them when
// it creates the definition's DRBD layer: the request, the group's
// peer_slots, PeerSlotsNewResource on the definition, the group and the
// controller, else 7.
func TestRDCreateResolvesPeerSlotsLikeLinstor(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		body      string
		group     apiv1.ResourceGroup
		ctrlProps map[string]string
		want      int32
	}{
		"default": {
			body: `{"resource_definition":{"name":"pvc-slots","resource_group_name":"rg"}}`,
			want: 7,
		},
		"request": {
			body:      `{"resource_definition":{"name":"pvc-slots","resource_group_name":"rg"},"drbd_peer_slots":3}`,
			group:     apiv1.ResourceGroup{PeerSlots: 9, Props: map[string]string{"PeerSlotsNewResource": "11"}},
			ctrlProps: map[string]string{"PeerSlotsNewResource": "12"},
			want:      3,
		},
		"group peer_slots": {
			body:      `{"resource_definition":{"name":"pvc-slots","resource_group_name":"rg","props":{"PeerSlotsNewResource":"10"}}}`,
			group:     apiv1.ResourceGroup{PeerSlots: 9},
			ctrlProps: map[string]string{"PeerSlotsNewResource": "12"},
			want:      9,
		},
		"definition prop": {
			body:      `{"resource_definition":{"name":"pvc-slots","resource_group_name":"rg","props":{"PeerSlotsNewResource":"10"}}}`,
			group:     apiv1.ResourceGroup{Props: map[string]string{"PeerSlotsNewResource": "11"}},
			ctrlProps: map[string]string{"PeerSlotsNewResource": "12"},
			want:      10,
		},
		"group prop": {
			body:      `{"resource_definition":{"name":"pvc-slots","resource_group_name":"rg"}}`,
			group:     apiv1.ResourceGroup{Props: map[string]string{"PeerSlotsNewResource": "11"}},
			ctrlProps: map[string]string{"PeerSlotsNewResource": "12"},
			want:      11,
		},
		"controller prop": {
			body:      `{"resource_definition":{"name":"pvc-slots","resource_group_name":"rg"}}`,
			ctrlProps: map[string]string{"PeerSlotsNewResource": "12"},
			want:      12,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			st := store.NewInMemory()
			group := tc.group
			group.Name = "rg"

			if err := st.ResourceGroups().Create(t.Context(), &group); err != nil {
				t.Fatalf("seed rg: %v", err)
			}

			if tc.ctrlProps != nil {
				if err := st.ControllerProps().Set(t.Context(), tc.ctrlProps); err != nil {
					t.Fatalf("seed controller props: %v", err)
				}
			}

			if got := createRDForPeerSlots(t, st, tc.body); got != tc.want {
				t.Errorf("peer slots: got %d, want %d", got, tc.want)
			}
		})
	}
}

// A count no superblock holds is refused with LINSTOR's words, and nothing
// is created.
func TestRDCreateRefusesTooManyPeerSlots(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/resource-definitions",
		[]byte(`{"resource_definition":{"name":"pvc-slots"},"drbd_peer_slots":32}`))
	defer func() { _ = resp.Body.Close() }()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusBadRequest || !strings.Contains(string(body), "given peer_slots is larger then allowed: 32") {
		t.Fatalf("status %d, body %s; want 400 naming the limit", resp.StatusCode, body)
	}

	if _, err := st.ResourceDefinitions().Get(t.Context(), "pvc-slots"); err == nil {
		t.Errorf("a refused definition was created")
	}
}

// LINSTOR shows a definition's peer slots in the data of its DRBD layer;
// a definition without a recorded count shows the 15 it has.
func TestRDGetReportsPeerSlotsOnTheDRBDLayer(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	for name, slots := range map[string]int32{"rd-seven": 7, "rd-legacy": 0} {
		if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: name, PeerSlots: slots}); err != nil {
			t.Fatalf("seed %s: %v", name, err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	for name, want := range map[string]float64{"rd-seven": 7, "rd-legacy": 15} {
		resp := httpGet(t, base+"/v1/resource-definitions/"+name)

		var rd struct {
			LayerData []struct {
				Type string         `json:"type"`
				Data map[string]any `json:"data"`
			} `json:"layer_data"`
		}

		err := json.NewDecoder(resp.Body).Decode(&rd)
		_ = resp.Body.Close()

		if err != nil || len(rd.LayerData) == 0 || rd.LayerData[0].Type != "DRBD" {
			t.Fatalf("%s: layer_data %+v, %v", name, rd.LayerData, err)
		}

		if got := rd.LayerData[0].Data["peer_slots"]; got != want {
			t.Errorf("%s: peer_slots %v, want %v", name, got, want)
		}

		if rd.LayerData[1].Data != nil {
			t.Errorf("%s: STORAGE layer carries data %v", name, rd.LayerData[1].Data)
		}
	}
}

// seedDefinitionWithReplicas stores a definition with the given count and
// diskful replicas on n0..n(diskful-1), each on an online node with a pool,
// plus an online node "new" with a pool and no replica.
func seedDefinitionWithReplicas(t *testing.T, st store.Store, slots int32, diskful int) {
	t.Helper()

	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: "pvc-slots", PeerSlots: slots, LayerStack: []string{"DRBD", "STORAGE"},
	}); err != nil {
		t.Fatalf("seed rd: %v", err)
	}

	for i := range diskful + 1 {
		node := fmt.Sprintf("n%d", i)
		if i == diskful {
			node = "new"
		}

		if err := st.Nodes().Create(ctx, &apiv1.Node{
			Name: node, Type: apiv1.NodeTypeSatellite, ConnectionStatus: "ONLINE",
			NetInterfaces: []apiv1.NetInterface{{Name: DefaultNetInterfaceName, Address: fmt.Sprintf("10.0.0.%d", i+1)}},
		}); err != nil {
			t.Fatalf("seed node %s: %v", node, err)
		}

		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			NodeName: node, StoragePoolName: "lvm-thin", ProviderKind: apiv1.StoragePoolKindLVMThin,
		}); err != nil {
			t.Fatalf("seed pool on %s: %v", node, err)
		}

		if i == diskful {
			continue
		}

		if err := st.Resources().Create(ctx, &apiv1.Resource{
			Name: "pvc-slots", NodeName: node, Props: map[string]string{"StorPoolName": "lvm-thin"},
		}); err != nil {
			t.Fatalf("seed replica on %s: %v", node, err)
		}
	}
}

// `rd modify --peer-slots` changes the count for replicas created
// afterwards, and is refused, as in LINSTOR, when the definition already
// has more replicas with a disk than the count leaves room for.
func TestRDModifyPeerSlots(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		slots   int32
		refused bool
	}{
		"raised":            {slots: 12},
		"lowered, fits":     {slots: 3},
		"lowered too far":   {slots: 2, refused: true},
		"more than a block": {slots: 40, refused: true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			st := store.NewInMemory()
			seedDefinitionWithReplicas(t, st, 7, 5)

			base, stop := startServerWithStore(t, st)
			defer stop()

			resp := httpPut(t, base+"/v1/resource-definitions/pvc-slots",
				[]byte(fmt.Sprintf(`{"drbd_peer_slots":%d}`, tc.slots)))
			defer func() { _ = resp.Body.Close() }()

			rd, err := st.ResourceDefinitions().Get(t.Context(), "pvc-slots")
			if err != nil {
				t.Fatalf("get: %v", err)
			}

			if tc.refused {
				if resp.StatusCode != http.StatusBadRequest || rd.PeerSlots != 7 {
					t.Errorf("status %d, peer slots %d; want 400 and 7 kept", resp.StatusCode, rd.PeerSlots)
				}

				return
			}

			if resp.StatusCode != http.StatusOK || rd.PeerSlots != tc.slots {
				t.Errorf("status %d, peer slots %d; want 200 and %d", resp.StatusCode, rd.PeerSlots, tc.slots)
			}
		})
	}
}

// A modify that names no count, like `rd set-property`, keeps it.
func TestRDModifyWithoutPeerSlotsKeepsThem(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedDefinitionWithReplicas(t, st, 7, 1)

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPut(t, base+"/v1/resource-definitions/pvc-slots", []byte(`{"override_props":{"Aux/a":"b"}}`))
	_ = resp.Body.Close()

	rd, err := st.ResourceDefinitions().Get(t.Context(), "pvc-slots")
	if err != nil || rd.PeerSlots != 7 {
		t.Errorf("peer slots after set-property: %d, %v; want 7", rd.PeerSlots, err)
	}
}

// A spawn takes its peer slots from the spawn's peer_slots, else the
// group's, as LINSTOR's spawn does.
func TestSpawnResolvesPeerSlots(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		body string
		want int32
	}{
		"spawn's": {body: `{"resource_definition_name":"pvc-slots","volume_sizes":[1048576],"definitions_only":true,"peer_slots":4}`, want: 4},
		"group's": {body: `{"resource_definition_name":"pvc-slots","volume_sizes":[1048576],"definitions_only":true}`, want: 9},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			st := store.NewInMemory()
			if err := st.ResourceGroups().Create(t.Context(), &apiv1.ResourceGroup{Name: "rg", PeerSlots: 9}); err != nil {
				t.Fatalf("seed rg: %v", err)
			}

			base, stop := startServerWithStore(t, st)
			defer stop()

			resp := httpPost(t, base+"/v1/resource-groups/rg/spawn", []byte(tc.body))
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != http.StatusCreated {
				body, _ := io.ReadAll(resp.Body)
				t.Fatalf("spawn: status %d, body %s", resp.StatusCode, body)
			}

			rd, err := st.ResourceDefinitions().Get(t.Context(), "pvc-slots")
			if err != nil || rd.PeerSlots != tc.want {
				t.Errorf("peer slots: %d, %v; want %d", rd.PeerSlots, err, tc.want)
			}
		})
	}
}

// A group's peer_slots is refused past what a superblock holds, and `rg
// modify --peer-slots` changes it, as in LINSTOR.
func TestRGPeerSlots(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/resource-groups", []byte(`{"name":"rg-big","peer_slots":32}`))
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("rg c with 32 peer slots: status %d, want 400", resp.StatusCode)
	}

	resp = httpPost(t, base+"/v1/resource-groups", []byte(`{"name":"rg","peer_slots":9}`))
	_ = resp.Body.Close()

	resp = httpPut(t, base+"/v1/resource-groups/rg", []byte(`{"peer_slots":40}`))
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("rg modify to 40 peer slots: status %d, want 400", resp.StatusCode)
	}

	resp = httpPut(t, base+"/v1/resource-groups/rg", []byte(`{"peer_slots":4}`))
	_ = resp.Body.Close()

	rg, err := st.ResourceGroups().Get(t.Context(), "rg")
	if err != nil || rg.PeerSlots != 4 {
		t.Errorf("rg after modify: peer slots %d, %v; want 4", rg.PeerSlots, err)
	}
}
