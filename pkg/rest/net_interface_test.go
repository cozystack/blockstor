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

	// Upstream LINSTOR returns 201 for the per-interface POST + an
	// ApiCallRc envelope body. blockstor matches that wire shape.
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201", resp.StatusCode)
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

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("delete status: got %d, want 200", resp.StatusCode)
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

// TestNetInterfaceListReturnsArray pins the GET /v1/nodes/{n}/net-interfaces
// shape: a JSON array of NetInterface objects. golinstor's
// `Nodes.GetNetInterfaces(...)` parses this into []client.NetInterface;
// without an explicit empty-array branch a fresh node would deserialise
// `null` into a nil slice instead of an empty one.
func TestNetInterfaceListReturnsArray(t *testing.T) {
	st := store.NewInMemory()
	if err := st.Nodes().Create(t.Context(), &apiv1.Node{
		Name: "n1",
		Type: apiv1.NodeTypeSatellite,
		NetInterfaces: []apiv1.NetInterface{
			{Name: "default", Address: "10.0.0.1", SatellitePort: 3366},
			{Name: "repl", Address: "10.10.0.1", SatellitePort: 7000},
		},
	}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/nodes/n1/net-interfaces")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got []apiv1.NetInterface

	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(got) != 2 {
		t.Errorf("count: got %d, want 2", len(got))
	}
}

// TestNetInterfaceGetByName pins the GET /v1/nodes/{n}/net-interfaces/{name}
// happy path and the 404 branch. The single-fetch endpoint is what
// `linstor n interface modify` uses to read the existing config
// before patching it.
func TestNetInterfaceGetByName(t *testing.T) {
	st := store.NewInMemory()
	if err := st.Nodes().Create(t.Context(), &apiv1.Node{
		Name: "n1",
		Type: apiv1.NodeTypeSatellite,
		NetInterfaces: []apiv1.NetInterface{
			{Name: "default", Address: "10.0.0.1", SatellitePort: 3366},
		},
	}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/nodes/n1/net-interfaces/default")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got apiv1.NetInterface
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if got.Name != "default" || got.Address != "10.0.0.1" {
		t.Errorf("body mismatch: %+v", got)
	}

	missing := httpGet(t, base+"/v1/nodes/n1/net-interfaces/nope")
	_ = missing.Body.Close()

	if missing.StatusCode != http.StatusNotFound {
		t.Errorf("missing status: got %d, want 404", missing.StatusCode)
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

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status: got %d, want 200 (idempotent delete)", resp.StatusCode)
	}

	got, _ := st.Nodes().Get(t.Context(), "n1")
	if len(got.NetInterfaces) != 1 || got.NetInterfaces[0].Name != "default" {
		t.Errorf("existing interface lost: %v", got.NetInterfaces)
	}
}

// TestNetInterfaceModifyActiveIsPresentationOnly: scenario 3.W02
// (cross-listed wave1 3.10).
//
// Upstream LINSTOR's `linstor n interface modify <node> <nic> --active`
// flips `StltConn/0/Active=true` on the named NIC, and the satellite
// then re-dials the controller via that interface. Phase 10.6 of
// blockstor retired the satellite→controller wire (satellites talk
// only to kube-apiserver via `ctrl.GetConfig()`), so the wire-level
// `is_active` flag is presentation-only: synthesised by
// `DefaultNetInterfaceFields` as `i == 0`, irrespective of whatever
// the caller PUT in the request body.
//
// This test pins the deferred contract so a future regression that
// silently starts honouring `is_active=true` in the body (without
// also wiring the satellite re-dial path) trips here loudly:
//
//  1. PATCH (PUT with `is_active=true`) on the *second* interface
//     does NOT make it active on read — position [0] still wins.
//  2. The only way to switch which NIC the `linstor n interface list`
//     CLI renders as `Active` is to reorder the slice (DELETE the
//     old [0], PUT the new entry, then PUT the previous [0] back so
//     it falls to [1]). After that reorder, the formerly-second
//     interface synthesises `IsActive=true`.
//  3. The presentation-only contract holds across both `inmemory`
//     and (by virtue of `DefaultNetInterfaceFields`) the K8s store.
//
// If anyone later implements scenario 3.10 Outcome A (resurrect a
// custom satellite→controller wire that re-dials on `is_active`
// changes), this test must be deleted alongside `redial_spec_test.go`
// and replaced with a positive re-dial assertion. Until then the
// REST surface MUST behave as documented.
func TestNetInterfaceModifyActiveIsPresentationOnly(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{
		Name: "n1",
		Type: apiv1.NodeTypeSatellite,
		NetInterfaces: []apiv1.NetInterface{
			{Name: "default", Address: "10.0.0.1"},
			{Name: "satconn_1G", Address: "192.168.43.10"},
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// Step 1: GET shows synthesized IsActive = (i == 0). The first
	// interface (default) is active; the second (satconn_1G) is not.
	preResp := httpGet(t, base+"/v1/nodes/n1/net-interfaces")

	var pre []apiv1.NetInterface

	if err := json.NewDecoder(preResp.Body).Decode(&pre); err != nil {
		_ = preResp.Body.Close()
		t.Fatalf("decode pre: %v", err)
	}

	_ = preResp.Body.Close()

	if len(pre) != 2 {
		t.Fatalf("pre len: got %d, want 2", len(pre))
	}

	if pre[0].Name != "default" || !pre[0].IsActive {
		t.Errorf("pre[0]: want default+IsActive, got %+v", pre[0])
	}

	if pre[1].Name != "satconn_1G" || pre[1].IsActive {
		t.Errorf("pre[1]: want satconn_1G+!IsActive, got %+v", pre[1])
	}

	// Step 2: PATCH the second interface with is_active=true via PUT.
	// The wire decoder accepts the field (round-trip-safe — clients
	// MUST be able to send the upstream payload without 400), but the
	// synthesised value on read MUST still reflect position, not body.
	body, _ := json.Marshal(apiv1.NetInterface{
		Name: "satconn_1G", Address: "192.168.43.10", IsActive: true,
	})

	resp := httpPut(t, base+"/v1/nodes/n1/net-interfaces/satconn_1G", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status: got %d, want 200", resp.StatusCode)
	}

	postResp := httpGet(t, base+"/v1/nodes/n1/net-interfaces")

	var post []apiv1.NetInterface

	if err := json.NewDecoder(postResp.Body).Decode(&post); err != nil {
		_ = postResp.Body.Close()
		t.Fatalf("decode post: %v", err)
	}

	_ = postResp.Body.Close()

	if len(post) != 2 {
		t.Fatalf("post len: got %d, want 2", len(post))
	}

	// Order preserved: default still at [0], satconn_1G still at [1].
	// IsActive synthesised as (i == 0), unchanged from pre.
	if post[0].Name != "default" || !post[0].IsActive {
		t.Errorf("post[0]: PATCH leaked into ordering — want default+IsActive, got %+v; "+
			"if this fires, scenario 3.10 Outcome A is being implemented — "+
			"replace this test with a positive re-dial assertion", post[0])
	}

	if post[1].Name != "satconn_1G" || post[1].IsActive {
		t.Errorf("post[1]: is_active=true in body was honoured on read — want satconn_1G+!IsActive, got %+v; "+
			"if this fires, the synthesiser stopped enforcing i==0 — see scenario 3.W02 deferred contract", post[1])
	}

	// Step 3: the documented switch path — DELETE old [0], re-PUT it
	// so it lands at the end. After that reorder, satconn_1G is at
	// position [0] and synthesises IsActive=true.
	delResp := httpDelete(t, base+"/v1/nodes/n1/net-interfaces/default")
	_ = delResp.Body.Close()

	if delResp.StatusCode != http.StatusOK {
		t.Fatalf("DELETE default: got %d, want 200", delResp.StatusCode)
	}

	rebody, _ := json.Marshal(apiv1.NetInterface{Name: "default", Address: "10.0.0.1"})

	recreate := httpPost(t, base+"/v1/nodes/n1/net-interfaces", rebody)
	_ = recreate.Body.Close()

	if recreate.StatusCode != http.StatusCreated {
		t.Fatalf("POST default: got %d, want 201", recreate.StatusCode)
	}

	switchedResp := httpGet(t, base+"/v1/nodes/n1/net-interfaces")

	var switched []apiv1.NetInterface

	if err := json.NewDecoder(switchedResp.Body).Decode(&switched); err != nil {
		_ = switchedResp.Body.Close()
		t.Fatalf("decode switched: %v", err)
	}

	_ = switchedResp.Body.Close()

	if len(switched) != 2 {
		t.Fatalf("switched len: got %d, want 2; ifaces=%v", len(switched), switched)
	}

	// satconn_1G migrated to position [0] → now synthesises IsActive.
	if switched[0].Name != "satconn_1G" || !switched[0].IsActive {
		t.Errorf("switched[0]: reorder did not promote satconn_1G — got %+v", switched[0])
	}

	if switched[1].Name != "default" || switched[1].IsActive {
		t.Errorf("switched[1]: re-added default did not demote — got %+v", switched[1])
	}
}

// TestNetInterfaceModifyActiveBodyRoundTrips: scenario 3.W02
// belt-and-braces. The PUT body MUST decode `is_active` cleanly even
// though the value is presentation-only on read — upstream golinstor
// always sends the field, and rejecting it with 400 would break every
// `linstor n interface modify` call from the official CLI.
//
// We pin the wire contract (POST accepts `is_active`; PUT accepts
// `is_active`; both round-trip to 200/201) so a future stricter
// decoder doesn't accidentally start 400-ing on the field name alone.
func TestNetInterfaceModifyActiveBodyRoundTrips(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{
		Name: "n1",
		Type: apiv1.NodeTypeSatellite,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// POST with is_active=true: upstream's create-flow on a node with
	// a single NIC sends the field set; we must accept it.
	postBody, _ := json.Marshal(apiv1.NetInterface{
		Name: "default", Address: "10.0.0.1", IsActive: true,
	})

	postResp := httpPost(t, base+"/v1/nodes/n1/net-interfaces", postBody)
	_ = postResp.Body.Close()

	if postResp.StatusCode != http.StatusCreated {
		t.Errorf("POST is_active=true: got %d, want 201", postResp.StatusCode)
	}

	// PUT with is_active=true: the modify-active path.
	putBody, _ := json.Marshal(apiv1.NetInterface{Address: "10.0.0.2", IsActive: true})

	putResp := httpPut(t, base+"/v1/nodes/n1/net-interfaces/default", putBody)
	_ = putResp.Body.Close()

	if putResp.StatusCode != http.StatusOK {
		t.Errorf("PUT is_active=true: got %d, want 200", putResp.StatusCode)
	}
}
