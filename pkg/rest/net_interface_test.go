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

// TestNetInterfaceCreateAppendsToNode adds a NetInterface to a Node
// without disturbing the rest of the spec. The interface lives
// inline on Node.Spec.NetInterfaces[]; we don't mint a separate CRD.
func TestNetInterfaceCreateAppendsToNode(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{
		Name: "n1",
		Type: apiv1.NodeTypeSatellite,
		NetInterfaces: []apiv1.NetInterface{
			{Name: "default", Address: "10.0.0.1"},
		},
	}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.NetInterface{
		Name: "replication", Address: "10.10.0.1", SatellitePort: 7000,
	})

	resp := httpPost(t, base+"/v1/nodes/n1/net-interfaces", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.Nodes().Get(ctx, "n1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if len(got.NetInterfaces) != 2 {
		t.Fatalf("interface count: got %d, want 2", len(got.NetInterfaces))
	}

	names := map[string]bool{}
	for _, n := range got.NetInterfaces {
		names[n.Name] = true
	}

	if !names["default"] || !names["replication"] {
		t.Errorf("expected both default + replication; got %v", got.NetInterfaces)
	}
}

// TestNetInterfaceCreateReplacesSameName: idempotent — second create
// with the same name overwrites in place rather than duplicating.
func TestNetInterfaceCreateReplacesSameName(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	_ = st.Nodes().Create(ctx, &apiv1.Node{
		Name: "n1",
		Type: apiv1.NodeTypeSatellite,
		NetInterfaces: []apiv1.NetInterface{
			{Name: "default", Address: "10.0.0.1"},
		},
	})

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.NetInterface{Name: "default", Address: "192.168.1.5"})

	resp := httpPost(t, base+"/v1/nodes/n1/net-interfaces", body)
	_ = resp.Body.Close()

	got, _ := st.Nodes().Get(ctx, "n1")
	if len(got.NetInterfaces) != 1 {
		t.Errorf("idempotent create grew the list: %v", got.NetInterfaces)
	}

	if got.NetInterfaces[0].Address != "192.168.1.5" {
		t.Errorf("address not overwritten: %v", got.NetInterfaces[0])
	}
}

// TestNetInterfaceUpdatePathNameWins: the URL's {name} is the
// authoritative interface identifier; a name in the body is ignored.
func TestNetInterfaceUpdatePathNameWins(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	_ = st.Nodes().Create(ctx, &apiv1.Node{
		Name: "n1",
		Type: apiv1.NodeTypeSatellite,
		NetInterfaces: []apiv1.NetInterface{
			{Name: "replication", Address: "10.10.0.1"},
		},
	})

	base, stop := startServerWithStore(t, st)
	defer stop()

	// Send body with a different name to prove the path wins.
	body, _ := json.Marshal(apiv1.NetInterface{Name: "evil", Address: "10.10.0.99"})

	resp := httpPut(t, base+"/v1/nodes/n1/net-interfaces/replication", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	got, _ := st.Nodes().Get(ctx, "n1")
	if len(got.NetInterfaces) != 1 {
		t.Fatalf("count: got %d, want 1; interfaces=%v", len(got.NetInterfaces), got.NetInterfaces)
	}

	if got.NetInterfaces[0].Name != "replication" {
		t.Errorf("name overridden by body: %v", got.NetInterfaces[0])
	}

	if got.NetInterfaces[0].Address != "10.10.0.99" {
		t.Errorf("address not updated: %v", got.NetInterfaces[0])
	}
}

// TestNetInterfaceDelete drops the interface; subsequent delete is a
// no-op (idempotent).
func TestNetInterfaceDelete(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	_ = st.Nodes().Create(ctx, &apiv1.Node{
		Name: "n1",
		Type: apiv1.NodeTypeSatellite,
		NetInterfaces: []apiv1.NetInterface{
			{Name: "default", Address: "10.0.0.1"},
			{Name: "replication", Address: "10.10.0.1"},
		},
	})

	base, stop := startServerWithStore(t, st)
	defer stop()

	for range 2 {
		resp := httpDelete(t, base+"/v1/nodes/n1/net-interfaces/replication")
		_ = resp.Body.Close()

		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("delete status: got %d, want 204", resp.StatusCode)
		}
	}

	got, _ := st.Nodes().Get(ctx, "n1")
	if len(got.NetInterfaces) != 1 {
		t.Errorf("count: got %d, want 1; interfaces=%v", len(got.NetInterfaces), got.NetInterfaces)
	}

	if got.NetInterfaces[0].Name != "default" {
		t.Errorf("wrong interface survived: %v", got.NetInterfaces[0])
	}
}

// TestNetInterfaceCreateBadJSON pins the 400 branch on a malformed
// NetInterface body. Without this, a satellite-bootstrap script that
// posts truncated JSON would silently get 500 (or worse, leak the
// raw JSON error to the client) — the contract is "client error,
// body identifies the problem".
func TestNetInterfaceCreateBadJSON(t *testing.T) {
	st := store.NewInMemory()
	if err := st.Nodes().Create(t.Context(), &apiv1.Node{Name: "n1", Type: apiv1.NodeTypeSatellite}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/nodes/n1/net-interfaces", []byte("{not-json"))
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400", resp.StatusCode)
	}
}

// TestNetInterfaceCreateMissingName pins the 400 branch when neither
// the URL path nor the body supplies an interface name. Both create
// (no path-name) and update (path-name) share the same decoder, so
// the validator must accept body-name on POST and reject only when
// both sources are empty.
func TestNetInterfaceCreateMissingName(t *testing.T) {
	st := store.NewInMemory()
	if err := st.Nodes().Create(t.Context(), &apiv1.Node{Name: "n1", Type: apiv1.NodeTypeSatellite}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.NetInterface{Address: "10.0.0.1"}) // Name omitted
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/nodes/n1/net-interfaces", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400 (missing name)", resp.StatusCode)
	}
}

// TestNetInterfaceCreateUnknownNode pins the 404 branch when the
// {node} pathvar doesn't resolve. linstor-csi's bootstrap retries on
// 404 (waiting for the node CRD to appear) but treats 5xx as fatal —
// a regression that returned 500 here would deadlock satellite
// initialisation behind csi.
func TestNetInterfaceCreateUnknownNode(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	body, err := json.Marshal(apiv1.NetInterface{Name: "default", Address: "10.0.0.1"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/nodes/ghost/net-interfaces", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", resp.StatusCode)
	}
}

// TestNetInterfaceUpdateCreatesOnMissing pins the upstream LINSTOR
// "PUT-creates" semantic: a PUT to /v1/nodes/{node}/net-interfaces/{name}
// on a name the node doesn't have yet must CREATE the interface
// rather than 404. This matches `linstor n interface modify` in
// upstream — the CLI is "fire and forget", caller doesn't need to
// know whether the interface already exists.
func TestNetInterfaceUpdateCreatesOnMissing(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{
		Name: "n1",
		Type: apiv1.NodeTypeSatellite,
		NetInterfaces: []apiv1.NetInterface{
			{Name: "default", Address: "10.0.0.1"},
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.NetInterface{Address: "10.10.0.5"})

	resp := httpPut(t, base+"/v1/nodes/n1/net-interfaces/replication", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	got, _ := st.Nodes().Get(ctx, "n1")
	if len(got.NetInterfaces) != 2 {
		t.Fatalf("count: got %d, want 2 (PUT-creates contract); ifaces=%v",
			len(got.NetInterfaces), got.NetInterfaces)
	}

	var found *apiv1.NetInterface
	for i := range got.NetInterfaces {
		if got.NetInterfaces[i].Name == "replication" {
			found = &got.NetInterfaces[i]

			break
		}
	}

	if found == nil {
		t.Fatalf("replication iface not appended; got %v", got.NetInterfaces)
	}

	if found.Address != "10.10.0.5" {
		t.Errorf("address: got %q, want 10.10.0.5", found.Address)
	}
}

// TestNetInterfaceDeleteUnknownNode pins the 404 branch on
// handleNetInterfaceDelete (was 76.5%): DELETE against an unknown
// {node} pathvar must surface a clean 404 rather than 500. Operator
// scripts that idempotently delete interfaces on node teardown
// expect 404 when the node is already gone.
func TestNetInterfaceDeleteUnknownNode(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpDelete(t, base+"/v1/nodes/ghost/net-interfaces/default")
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", resp.StatusCode)
	}
}

// TestNetInterfaceDeleteMissingIsIdempotent pins that DELETE on an
// interface name the node doesn't have is a no-op (returns 204
// without modifying the iface list). Symmetric with the DELETE-on-
// already-deleted path that TestNetInterfaceDelete already covers
// for the same name re-fired.
func TestNetInterfaceDeleteMissingIsIdempotent(t *testing.T) {
	st := store.NewInMemory()
	if err := st.Nodes().Create(t.Context(), &apiv1.Node{
		Name: "n1",
		Type: apiv1.NodeTypeSatellite,
		NetInterfaces: []apiv1.NetInterface{
			{Name: "default", Address: "10.0.0.1"},
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t, base+"/v1/nodes/n1/net-interfaces/never-existed")
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("status: got %d, want 204 (idempotent delete)", resp.StatusCode)
	}

	got, _ := st.Nodes().Get(t.Context(), "n1")
	if len(got.NetInterfaces) != 1 || got.NetInterfaces[0].Name != "default" {
		t.Errorf("existing interface lost: %v", got.NetInterfaces)
	}
}
