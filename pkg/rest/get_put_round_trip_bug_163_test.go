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
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Bug 163 (P0) — GET→PUT round-trip broken on RD / Resource / Node /
// StoragePool after Bug 161 (`DisallowUnknownFields`).
//
// The modify-body structs declared on the PUT endpoints don't declare
// every read-side key the corresponding GET emits (e.g. `effective_props`,
// `name`, `node_name`, `storage_pool_name`). Operators piping
// `GET | jq edits | PUT` now hit 400 unknown-field for fields the
// handler is happy to ignore.
//
// Each test below performs the exact `curl GET | curl PUT` operator
// workflow:
//   1. Create the object via the store.
//   2. GET it back through the REST server.
//   3. Decode the body into a generic map, optionally tweak a property.
//   4. Re-encode and PUT it back.
//   5. Expect 200 — not 400 unknown-field.

// TestBug163RDGetPutRoundTrip: GET a ResourceDefinition, PUT it back
// unchanged. Read-side keys (`name`, `effective_props`, `volume_definitions`,
// `props`, `flags`, `layer_data`, `layer_stack`, `uuid`, `external_name`,
// `resource_group_name`, `annotations`) must all be tolerated on PUT.
func TestBug163RDGetPutRoundTrip(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:              "rd163",
		ResourceGroupName: "DfltRscGrp",
		Props:             map[string]string{"DrbdOptions/Net/protocol": "C"},
	}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	getResp := httpGet(t, base+"/v1/resource-definitions/rd163")

	getBody := mustReadBody(t, getResp)

	_ = getResp.Body.Close()

	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("GET status: got %d, want 200. Body: %s", getResp.StatusCode, getBody)
	}

	// Decode + re-encode through map[string]any to ensure the wire-shape
	// bytes (including read-side keys) survive the round-trip unchanged.
	var raw map[string]any
	if err := json.Unmarshal(getBody, &raw); err != nil {
		t.Fatalf("decode GET: %v", err)
	}

	reencoded, err := json.Marshal(raw)
	if err != nil {
		t.Fatalf("re-encode: %v", err)
	}

	putResp := httpPut(t, base+"/v1/resource-definitions/rd163", reencoded)

	putBody := mustReadBody(t, putResp)

	_ = putResp.Body.Close()

	if putResp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status: got %d, want 200 (Bug 163 round-trip).\nGET body: %s\nPUT body: %s",
			putResp.StatusCode, getBody, putBody)
	}
}

// TestBug163ResourceGetPutRoundTrip: GET a Resource, PUT it back. The
// resource modify handler decodes into `apiv1.GenericPropsModify`, which
// declares only override_props / delete_props / delete_namespaces — every
// read-side key (`name`, `node_name`, `props`, `flags`, `state`, `uuid`,
// `layer_object`, `volumes`, `effective_props`, ...) is currently unknown.
func TestBug163ResourceGetPutRoundTrip(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "rd163res"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n1", Type: apiv1.NodeTypeSatellite}); err != nil {
		t.Fatalf("seed Node: %v", err)
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name:     "rd163res",
		NodeName: "n1",
		Props:    map[string]string{"Aux/owner": "team-a"},
	}); err != nil {
		t.Fatalf("seed Resource: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	getResp := httpGet(t, base+"/v1/resource-definitions/rd163res/resources/n1")

	getBody := mustReadBody(t, getResp)

	_ = getResp.Body.Close()

	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("GET status: got %d, want 200. Body: %s", getResp.StatusCode, getBody)
	}

	var raw map[string]any
	if err := json.Unmarshal(getBody, &raw); err != nil {
		t.Fatalf("decode GET: %v", err)
	}

	reencoded, err := json.Marshal(raw)
	if err != nil {
		t.Fatalf("re-encode: %v", err)
	}

	putResp := httpPut(t, base+"/v1/resource-definitions/rd163res/resources/n1", reencoded)

	putBody := mustReadBody(t, putResp)

	_ = putResp.Body.Close()

	if putResp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status: got %d, want 200 (Bug 163 round-trip).\nGET body: %s\nPUT body: %s",
			putResp.StatusCode, getBody, putBody)
	}
}

// TestBug163NodeGetPutRoundTrip: GET a Node, PUT it back. NodeModify
// already accepts most read-side keys post-Bug-158/161, but `props` was
// not declared on the modify struct (only override_props / delete_props
// / delete_namespaces from the embedded GenericPropsModify).
func TestBug163NodeGetPutRoundTrip(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{
		Name:  "n163",
		Type:  apiv1.NodeTypeSatellite,
		Props: map[string]string{"Aux/zone": "us-east-1a"},
	}); err != nil {
		t.Fatalf("seed Node: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	getResp := httpGet(t, base+"/v1/nodes/n163")

	getBody := mustReadBody(t, getResp)

	_ = getResp.Body.Close()

	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("GET status: got %d, want 200. Body: %s", getResp.StatusCode, getBody)
	}

	var raw map[string]any
	if err := json.Unmarshal(getBody, &raw); err != nil {
		t.Fatalf("decode GET: %v", err)
	}

	reencoded, err := json.Marshal(raw)
	if err != nil {
		t.Fatalf("re-encode: %v", err)
	}

	putResp := httpPut(t, base+"/v1/nodes/n163", reencoded)

	putBody := mustReadBody(t, putResp)

	_ = putResp.Body.Close()

	if putResp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status: got %d, want 200 (Bug 163 round-trip).\nGET body: %s\nPUT body: %s",
			putResp.StatusCode, getBody, putBody)
	}
}

// TestBug163StoragePoolGetPutRoundTrip: GET a StoragePool, PUT it back.
// The SP modify handler decodes into `apiv1.GenericPropsModify` — every
// read-side key (`storage_pool_name`, `node_name`, `provider_kind`,
// `props`, `static_traits`, `free_capacity`, `total_capacity`,
// `free_space_mgr_name`, `shared_space`, `reports`, `supports_snapshots`,
// `external_locking`, `uuid`, `state`) is currently unknown.
func TestBug163StoragePoolGetPutRoundTrip(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n163sp", Type: apiv1.NodeTypeSatellite}); err != nil {
		t.Fatalf("seed Node: %v", err)
	}

	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		StoragePoolName: "p163",
		NodeName:        "n163sp",
		ProviderKind:    apiv1.StoragePoolKindLVMThin,
		Props:           map[string]string{"StorDriver/LvmVg": "vg1"},
	}); err != nil {
		t.Fatalf("seed StoragePool: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	getResp := httpGet(t, base+"/v1/nodes/n163sp/storage-pools/p163")

	getBody := mustReadBody(t, getResp)

	_ = getResp.Body.Close()

	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("GET status: got %d, want 200. Body: %s", getResp.StatusCode, getBody)
	}

	var raw map[string]any
	if err := json.Unmarshal(getBody, &raw); err != nil {
		t.Fatalf("decode GET: %v", err)
	}

	reencoded, err := json.Marshal(raw)
	if err != nil {
		t.Fatalf("re-encode: %v", err)
	}

	putResp := httpPut(t, base+"/v1/nodes/n163sp/storage-pools/p163", reencoded)

	putBody := mustReadBody(t, putResp)

	_ = putResp.Body.Close()

	if putResp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status: got %d, want 200 (Bug 163 round-trip).\nGET body: %s\nPUT body: %s",
			putResp.StatusCode, getBody, putBody)
	}
}
