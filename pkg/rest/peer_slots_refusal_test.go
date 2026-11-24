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
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Seven peer slots hold eight replicas with a disk. A ninth is refused
// with FAIL_INSUFFICIENT_PEER_SLOTS, as LINSTOR refuses it, instead of
// being created and failing in DRBD; a diskless one still goes in.
func TestResourceCreateRefusesAReplicaThePeerSlotsCannotHold(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		diskful int
		body    string
		refused bool
	}{
		"eighth":          {diskful: 7, body: `{"resource":{"props":{"StorPoolName":"lvm-thin"}}}`},
		"ninth":           {diskful: 8, body: `{"resource":{"props":{"StorPoolName":"lvm-thin"}}}`, refused: true},
		"ninth, diskless": {diskful: 8, body: `{"resource":{"flags":["DISKLESS"]}}`},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			st := store.NewInMemory()
			seedDefinitionWithReplicas(t, st, 7, tc.diskful)

			base, stop := startServerWithStore(t, st)
			defer stop()

			resp := httpPost(t, base+"/v1/resource-definitions/pvc-slots/resources/new", []byte(tc.body))
			defer func() { _ = resp.Body.Close() }()

			_, getErr := st.Resources().Get(t.Context(), "pvc-slots", "new")

			if !tc.refused {
				if resp.StatusCode >= http.StatusBadRequest || getErr != nil {
					body, _ := io.ReadAll(resp.Body)
					t.Fatalf("status %d, body %s, replica %v; want it created", resp.StatusCode, body, getErr)
				}

				return
			}

			rcs := decodeRCs(t, resp)
			if resp.StatusCode != http.StatusBadRequest ||
				rcs[0].RetCode != apiCallRcError|apiCallRcFailInsufficientPeerSlots ||
				!strings.Contains(rcs[0].Message, "insufficient peer slots") {
				t.Fatalf("status %d, envelope %+v; want FAIL_INSUFFICIENT_PEER_SLOTS", resp.StatusCode, rcs)
			}

			if getErr == nil {
				t.Errorf("the refused replica was created")
			}
		})
	}
}

// Turning a diskless replica diskful takes a peer slot too, and is refused
// the same way when none is left.
func TestToggleDiskRefusesADiskThePeerSlotsCannotHold(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedDefinitionWithReplicas(t, st, 1, 2)

	if err := st.Resources().Create(t.Context(), &apiv1.Resource{
		Name: "pvc-slots", NodeName: "new", Flags: []string{apiv1.ResourceFlagDiskless},
	}); err != nil {
		t.Fatalf("seed diskless: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPut(t, base+"/v1/resource-definitions/pvc-slots/resources/new/toggle-disk/diskful/lvm-thin", nil)
	defer func() { _ = resp.Body.Close() }()

	rcs := decodeRCs(t, resp)
	if rcs[0].RetCode != apiCallRcError|apiCallRcFailInsufficientPeerSlots {
		t.Fatalf("status %d, envelope %+v; want FAIL_INSUFFICIENT_PEER_SLOTS", resp.StatusCode, rcs)
	}

	res, err := st.Resources().Get(t.Context(), "pvc-slots", "new")
	if err != nil || !isDisklessReplica(&res) {
		t.Errorf("replica after the refusal: %+v, %v; want it still diskless", res.Flags, err)
	}
}

// An autoplace stops at the replica the peer slots cannot hold, and says
// why, as LINSTOR's autoplace does through its resource create.
func TestAutoplaceRefusesReplicasBeyondThePeerSlots(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedDefinitionWithReplicas(t, st, 1, 2)

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-slots/autoplace",
		[]byte(`{"select_filter":{"place_count":3,"storage_pool":"lvm-thin"}}`))
	defer func() { _ = resp.Body.Close() }()

	rcs := decodeRCs(t, resp)
	if rcs[0].RetCode != apiCallRcError|apiCallRcFailInsufficientPeerSlots {
		t.Fatalf("status %d, envelope %+v; want FAIL_INSUFFICIENT_PEER_SLOTS", resp.StatusCode, rcs)
	}

	if _, err := st.Resources().Get(t.Context(), "pvc-slots", "new"); err == nil {
		t.Errorf("the third replica of a one-slot definition was placed")
	}
}

// seedPeerSlotNodes seeds three online nodes with an lvm-thin pool each.
func seedPeerSlotNodes(t *testing.T, st store.Store) {
	t.Helper()

	for i := range 3 {
		node := fmt.Sprintf("n%d", i)

		if err := st.Nodes().Create(t.Context(), &apiv1.Node{
			Name: node, Type: apiv1.NodeTypeSatellite, ConnectionStatus: "ONLINE",
			NetInterfaces: []apiv1.NetInterface{{Name: DefaultNetInterfaceName, Address: fmt.Sprintf("10.0.0.%d", i+1)}},
		}); err != nil {
			t.Fatalf("seed node %s: %v", node, err)
		}

		if err := st.StoragePools().Create(t.Context(), &apiv1.StoragePool{
			NodeName: node, StoragePoolName: "lvm-thin", ProviderKind: apiv1.StoragePoolKindLVMThin,
			FreeCapacity: 1 << 30, TotalCapacity: 1 << 30,
		}); err != nil {
			t.Fatalf("seed pool on %s: %v", node, err)
		}
	}
}

// assertRefusedWithNoReplica checks a request was refused with
// FAIL_INSUFFICIENT_PEER_SLOTS and left the definition without a replica.
func assertRefusedWithNoReplica(t *testing.T, st store.Store, resp *http.Response, rdName string) {
	t.Helper()

	rcs := decodeRCs(t, resp)
	if resp.StatusCode != http.StatusBadRequest || len(rcs) == 0 ||
		rcs[0].RetCode != apiCallRcError|apiCallRcFailInsufficientPeerSlots {
		t.Fatalf("status %d, envelope %+v; want FAIL_INSUFFICIENT_PEER_SLOTS", resp.StatusCode, rcs)
	}

	replicas, err := st.Resources().ListByDefinition(t.Context(), rdName)
	if err != nil {
		t.Fatalf("list replicas: %v", err)
	}

	if len(replicas) != 0 {
		t.Errorf("%d replicas created by a refused request; LINSTOR creates none", len(replicas))
	}
}

// A spawn that places more replicas with a disk than one more than the
// peer slots is refused whole, as LINSTOR refuses it: LINSTOR creates the
// definition and its replicas in one transaction, each replica through
// the check a single create runs, so nothing is left behind. Accepted, the
// replicas were created and never attached, their satellites failing
// `drbdadm adjust` with (162) on every pass.
func TestSpawnRefusesAPlacementThePeerSlotsCannotHold(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedPeerSlotNodes(t, st)

	if err := st.ResourceGroups().Create(t.Context(), &apiv1.ResourceGroup{
		Name:         "rg-slots",
		SelectFilter: apiv1.AutoSelectFilter{PlaceCount: 3, StoragePool: "lvm-thin"},
		Props:        map[string]string{"PeerSlotsNewResource": "1"},
	}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/resource-groups/rg-slots/spawn",
		[]byte(`{"resource_definition_name":"pvc-spawned","volume_sizes":[65536]}`))
	defer func() { _ = resp.Body.Close() }()

	assertRefusedWithNoReplica(t, st, resp, "pvc-spawned")

	if _, err := st.ResourceDefinitions().Get(t.Context(), "pvc-spawned"); err == nil {
		t.Errorf("the refused spawn left its definition behind")
	}
}

// An autoplace of more replicas than the peer slots hold creates none of
// them, where checking each replica against the ones before it created
// the first ones and stopped at the one that did not fit.
func TestAutoplaceRefusesAPlacementThePeerSlotsCannotHoldWhole(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedPeerSlotNodes(t, st)

	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{
		Name: "pvc-slots", PeerSlots: 1, LayerStack: []string{"DRBD", "STORAGE"},
	}); err != nil {
		t.Fatalf("seed rd: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-slots/autoplace",
		[]byte(`{"select_filter":{"place_count":3,"storage_pool":"lvm-thin"}}`))
	defer func() { _ = resp.Body.Close() }()

	assertRefusedWithNoReplica(t, st, resp, "pvc-slots")
}

// A resource create naming several nodes is refused whole when the peer
// slots cannot hold all of its replicas with a disk.
func TestResourceCreateOnSeveralNodesRefusesWhatThePeerSlotsCannotHold(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedPeerSlotNodes(t, st)

	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{
		Name: "pvc-slots", PeerSlots: 1, LayerStack: []string{"DRBD", "STORAGE"},
	}); err != nil {
		t.Fatalf("seed rd: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-slots/resources", []byte(`[`+
		`{"resource":{"node_name":"n0","props":{"StorPoolName":"lvm-thin"}}},`+
		`{"resource":{"node_name":"n1","props":{"StorPoolName":"lvm-thin"}}},`+
		`{"resource":{"node_name":"n2","props":{"StorPoolName":"lvm-thin"}}}]`))
	defer func() { _ = resp.Body.Close() }()

	assertRefusedWithNoReplica(t, st, resp, "pvc-slots")
}
