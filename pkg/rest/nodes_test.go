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
	"bytes"
	"encoding/json"
	"maps"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"testing"

	lapi "github.com/LINBIT/golinstor/client"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// startServerWithStore is sugar for tests that need a pre-populated
// store. Wires a fake controller-runtime client so the Secret-backed
// passphrase path and ControllerConfig-backed controller-properties
// path work end-to-end without an envtest harness.
func startServerWithStore(t *testing.T, st store.Store) (string, func()) {
	t.Helper()

	return startServerCustom(t, &Server{
		Addr:      pickFreeAddr(t),
		Store:     st,
		Client:    newFakeRESTClient(t),
		Namespace: testRESTNamespace,
	})
}

// TestNodesListEmpty: empty store returns "[]" not null.
func TestNodesListEmpty(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	c := newClient(t, base)

	got, err := c.Nodes.GetAll(t.Context())
	if err != nil {
		t.Fatalf("GetAll: %v", err)
	}

	if got == nil {
		t.Errorf("GetAll returned nil, want empty slice")
	}

	if len(got) != 0 {
		t.Errorf("len: got %d, want 0", len(got))
	}
}

// TestNodesCreateRoundTrip: POST a node, then GetAll/Get see it.
func TestNodesCreateRoundTrip(t *testing.T) {
	st := store.NewInMemory()

	base, stop := startServerWithStore(t, st)
	defer stop()

	c := newClient(t, base)

	if err := c.Nodes.Create(t.Context(), lapi.Node{
		Name: "alpha",
		Type: apiv1.NodeTypeSatellite,
		NetInterfaces: []lapi.NetInterface{
			{Name: "default", Address: net.ParseIP("10.0.0.5")},
		},
	}); err != nil {
		t.Fatalf("Create: %v", err)
	}

	got, err := c.Nodes.Get(t.Context(), "alpha")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.Name != "alpha" {
		t.Errorf("Name: got %q, want alpha", got.Name)
	}

	if got.Type != apiv1.NodeTypeSatellite {
		t.Errorf("Type: got %q, want %q", got.Type, apiv1.NodeTypeSatellite)
	}

	all, err := c.Nodes.GetAll(t.Context())
	if err != nil {
		t.Fatalf("GetAll: %v", err)
	}

	if len(all) != 1 {
		t.Fatalf("len: got %d, want 1", len(all))
	}

	if all[0].Name != "alpha" {
		t.Errorf("all[0].Name: got %q, want alpha", all[0].Name)
	}
}

// TestNodesGetMissing: 404 from REST, not 500. golinstor turns this into
// ErrNotFound; we just check the HTTP code so we are independent of golinstor's
// translation table.
func TestNodesGetMissing(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpGet(t, base+"/v1/nodes/ghost")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", resp.StatusCode)
	}
}

// TestNodesCreateConflict: POST with a duplicate name returns 409.
func TestNodesCreateConflict(t *testing.T) {
	st := store.NewInMemory()
	if err := st.Nodes().Create(t.Context(), &apiv1.Node{Name: "n1"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.Node{Name: "n1", Type: apiv1.NodeTypeSatellite})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/nodes", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusConflict {
		t.Errorf("status: got %d, want 409", resp.StatusCode)
	}
}

// TestNodesCreateBadJSON: malformed body returns 400, not 500.
func TestNodesCreateBadJSON(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpPost(t, base+"/v1/nodes", []byte("{not-json"))
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400", resp.StatusCode)
	}
}

// TestNodesDeleteMissing: DELETE of an absent node folds into 200 +
// warn-mask ApiCallRc envelope (Bug 66). Cozystack's evacuation
// playbook retries `linstor n d` per node; the previous 404 crashed
// the python CLI's XML decoder fallback on the second pass.
func TestNodesDeleteMissing(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpDelete(t, base+"/v1/nodes/ghost")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status: got %d, want 200", resp.StatusCode)
	}
}

// TestNodesUpdate: PUT /v1/nodes/{node} round-trips a Props change
// onto the existing Node. Path-derived name wins over body so callers
// can omit it.
func TestNodesUpdate(t *testing.T) {
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

	body, _ := json.Marshal(apiv1.NodeModify{
		GenericPropsModify: apiv1.GenericPropsModify{
			OverrideProps: map[string]string{"Aux/zone": "us-east-1a"},
		},
	})

	resp := httpPut(t, base+"/v1/nodes/n1", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.Nodes().Get(ctx, "n1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.Props["Aux/zone"] != "us-east-1a" {
		t.Errorf("Props after PUT: got %v, want Aux/zone=us-east-1a", got.Props)
	}

	// Existing Type field must survive — the PUT body didn't
	// specify it (node_type omitted), so the handler must NOT
	// reset it to "".
	if got.Type != apiv1.NodeTypeSatellite {
		t.Errorf("Type clobbered: got %q, want SATELLITE", got.Type)
	}
}

// TestNodesUpdateMissing: PUT against a non-existent node → 404.
func TestNodesUpdateMissing(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	body, _ := json.Marshal(apiv1.Node{Type: apiv1.NodeTypeSatellite})
	resp := httpPut(t, base+"/v1/nodes/ghost", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", resp.StatusCode)
	}
}

// TestNodesUpdateBadJSON: invalid JSON → 400 before we touch the
// store. Pins the per-handler decode-error path.
func TestNodesUpdateBadJSON(t *testing.T) {
	st := store.NewInMemory()
	if err := st.Nodes().Create(t.Context(), &apiv1.Node{Name: "n1"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPut(t, base+"/v1/nodes/n1", []byte("{not json"))
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400", resp.StatusCode)
	}
}

// TestNodesDeleteOK: DELETE existing node, then it disappears.
func TestNodesDeleteOK(t *testing.T) {
	st := store.NewInMemory()
	if err := st.Nodes().Create(t.Context(), &apiv1.Node{Name: "n1"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	c := newClient(t, base)
	if err := c.Nodes.Delete(t.Context(), "n1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	all, err := c.Nodes.GetAll(t.Context())
	if err != nil {
		t.Fatalf("GetAll: %v", err)
	}

	if len(all) != 0 {
		t.Errorf("after Delete, len=%d, want 0", len(all))
	}
}

// TestNodeListIncludesNetInterfaceAddressAndPort pins Bug 59 / CLI
// parity audit row #1: `linstor n l` Addresses column renders
// `<address>:<port> (<TYPE>)`. When a Node is created with bare
// address-only NetInterfaces (the common case — piraeus operator,
// linstor-csi, and golinstor all let satellite_port/encryption_type
// default), GET /v1/nodes MUST surface the upstream defaults
// (port=3366, type=PLAIN) and IsActive=true on the first interface
// so the CLI renders a non-empty Addresses column.
func TestNodeListIncludesNetInterfaceAddressAndPort(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	// Seed via the store directly so the assertion isolates the
	// read-path defaulting from any handler-side normalisation —
	// the in-memory store stamps defaults on Get/List, mirroring
	// the K8s backend's crdToWireNode.
	if err := st.Nodes().Create(ctx, &apiv1.Node{
		Name: "n1",
		Type: apiv1.NodeTypeSatellite,
		NetInterfaces: []apiv1.NetInterface{
			{Name: "default", Address: "10.0.0.5"},
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/nodes/n1")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got apiv1.Node
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(got.NetInterfaces) != 1 {
		t.Fatalf("NetInterfaces: got %d, want 1", len(got.NetInterfaces))
	}

	iface := got.NetInterfaces[0]
	if iface.Address != "10.0.0.5" {
		t.Errorf("Address: got %q, want 10.0.0.5", iface.Address)
	}

	if iface.SatellitePort != apiv1.DefaultSatellitePort {
		t.Errorf("SatellitePort: got %d, want %d (upstream LINSTOR default)",
			iface.SatellitePort, apiv1.DefaultSatellitePort)
	}

	if iface.SatelliteEncryptionType != apiv1.DefaultSatelliteEncryptionType {
		t.Errorf("SatelliteEncryptionType: got %q, want %q",
			iface.SatelliteEncryptionType, apiv1.DefaultSatelliteEncryptionType)
	}

	if !iface.IsActive {
		t.Errorf("IsActive: first interface must be marked active")
	}
}

// TestNodeListIncludesNetInterfaceUUID covers CLI parity audit row #1
// (F1): upstream LINSTOR's NetInterface DTO carries a `uuid` field
// that some tooling uses to diff interface state across reconciles.
// Bug 59 plugged port/encryption/is_active; this pins UUID on both
// Get and List so the wire shape is complete.
func TestNodeListIncludesNetInterfaceUUID(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{
		Name: "n1",
		Type: apiv1.NodeTypeSatellite,
		NetInterfaces: []apiv1.NetInterface{
			{Name: "default", Address: "10.0.0.5"},
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// Get path.
	getResp := httpGet(t, base+"/v1/nodes/n1")
	defer func() { _ = getResp.Body.Close() }()

	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("Get status: got %d, want 200", getResp.StatusCode)
	}

	var got apiv1.Node
	if err := json.NewDecoder(getResp.Body).Decode(&got); err != nil {
		t.Fatalf("Get decode: %v", err)
	}

	if len(got.NetInterfaces) != 1 {
		t.Fatalf("Get NetInterfaces: got %d, want 1", len(got.NetInterfaces))
	}

	if got.NetInterfaces[0].UUID == "" {
		t.Errorf("Get: NetInterface.UUID is empty; upstream LINSTOR always populates this field")
	}

	// List path — same node should surface the same shape.
	listResp := httpGet(t, base+"/v1/nodes")
	defer func() { _ = listResp.Body.Close() }()

	if listResp.StatusCode != http.StatusOK {
		t.Fatalf("List status: got %d, want 200", listResp.StatusCode)
	}

	var listed []apiv1.Node
	if err := json.NewDecoder(listResp.Body).Decode(&listed); err != nil {
		t.Fatalf("List decode: %v", err)
	}

	if len(listed) != 1 || len(listed[0].NetInterfaces) != 1 {
		t.Fatalf("List shape: got %#v", listed)
	}

	if listed[0].NetInterfaces[0].UUID == "" {
		t.Errorf("List: NetInterface.UUID is empty")
	}
}

// TestNodeNetInterfaceUUIDIsStable: two reads of the same (node, ifname)
// must return the same UUID. Stability across reconciles is the whole
// point of surfacing a UUID — a value that changes each request would
// be worse than nothing for diffing tooling.
func TestNodeNetInterfaceUUIDIsStable(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{
		Name: "n1",
		Type: apiv1.NodeTypeSatellite,
		NetInterfaces: []apiv1.NetInterface{
			{Name: "default", Address: "10.0.0.5"},
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	read := func() string {
		t.Helper()

		resp := httpGet(t, base+"/v1/nodes/n1")
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status: got %d, want 200", resp.StatusCode)
		}

		var node apiv1.Node
		if err := json.NewDecoder(resp.Body).Decode(&node); err != nil {
			t.Fatalf("decode: %v", err)
		}

		if len(node.NetInterfaces) != 1 {
			t.Fatalf("NetInterfaces: got %d, want 1", len(node.NetInterfaces))
		}

		return node.NetInterfaces[0].UUID
	}

	first := read()
	second := read()

	if first == "" {
		t.Fatalf("UUID empty on first read")
	}

	if first != second {
		t.Errorf("UUID drifted between reads: first=%q second=%q", first, second)
	}
}

// TestNodeNetInterfaceUUIDDiffersByInterface: two interfaces on the
// same node must get distinct UUIDs. Derivation rule is `(node,
// ifname)` so collisions would silently break diffing whenever a node
// ran multiple replication paths.
func TestNodeNetInterfaceUUIDDiffersByInterface(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{
		Name: "n1",
		Type: apiv1.NodeTypeSatellite,
		NetInterfaces: []apiv1.NetInterface{
			{Name: "default", Address: "10.0.0.5"},
			{Name: "replication", Address: "10.10.0.5"},
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/nodes/n1")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got apiv1.Node
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(got.NetInterfaces) != 2 {
		t.Fatalf("NetInterfaces: got %d, want 2", len(got.NetInterfaces))
	}

	a := got.NetInterfaces[0].UUID
	b := got.NetInterfaces[1].UUID

	if a == "" || b == "" {
		t.Fatalf("empty UUID: a=%q b=%q", a, b)
	}

	if a == b {
		t.Errorf("UUIDs collide for distinct interfaces: %q", a)
	}
}

// TestNodeCreateAutoCreatesDfltDisklessStorPool pins Bug 59 / parity
// audit row #3: upstream LINSTOR provisions a `DfltDisklessStorPool`
// per satellite at node-register time so the autoplacer's
// DisklessOnRemaining path and `linstor sp l` show one diskless row
// per node. The synthesis is the controller-side fix for the audit's
// "BS never auto-creates DfltDisklessStorPool" delta.
func TestNodeCreateAutoCreatesDfltDisklessStorPool(t *testing.T) {
	st := store.NewInMemory()

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.Node{
		Name: "n1",
		Type: apiv1.NodeTypeSatellite,
	})

	resp := httpPost(t, base+"/v1/nodes", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("POST /v1/nodes status: got %d, want 201", resp.StatusCode)
	}

	// /v1/view/storage-pools is the call linstor-csi and the
	// linstor CLI make to enumerate placement candidates. The new
	// node MUST surface a DISKLESS pool entry.
	viewResp := httpGet(t, base+"/v1/view/storage-pools?nodes=n1")
	defer func() { _ = viewResp.Body.Close() }()

	if viewResp.StatusCode != http.StatusOK {
		t.Fatalf("GET storage-pools status: got %d, want 200", viewResp.StatusCode)
	}

	var pools []apiv1.StoragePool
	if err := json.NewDecoder(viewResp.Body).Decode(&pools); err != nil {
		t.Fatalf("decode: %v", err)
	}

	var found bool

	for i := range pools {
		if pools[i].NodeName == "n1" &&
			pools[i].StoragePoolName == DfltDisklessStorPoolName &&
			pools[i].ProviderKind == apiv1.StoragePoolKindDiskless {
			found = true

			break
		}
	}

	if !found {
		t.Errorf("DfltDisklessStorPool not auto-created on node 'n1'; got pools=%+v", pools)
	}
}

// TestNodeCreateReturnsConnectionWarning pins cli-parity-audit row #40:
// upstream LINSTOR's `n c` returns a 2-entry ApiCallRc envelope on
// the wire — a SUCCESS for the controller-side record, then a WARNING
// "No active connection to satellite '<name>'" so the operator knows
// the satellite daemon still needs to come up.
func TestNodeCreateReturnsConnectionWarning(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	body, _ := json.Marshal(apiv1.Node{
		Name: "parity-fake",
		Type: apiv1.NodeTypeSatellite,
	})

	resp := httpPost(t, base+"/v1/nodes", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201", resp.StatusCode)
	}

	var rcs []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rcs); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}

	if len(rcs) != 2 {
		t.Fatalf("envelope entries: got %d, want 2 (success + connection warning); got=%+v", len(rcs), rcs)
	}

	// Entry 0: the canonical success line. Python CLI and golinstor
	// both dereference replies[0]; flipping the order would break
	// every existing caller.
	if rcs[0].RetCode&maskInfo == 0 {
		t.Errorf("entry 0 ret_code = %x, want maskInfo bit set", rcs[0].RetCode)
	}

	if rcs[0].RetCode&maskWarn != 0 {
		t.Errorf("entry 0 ret_code = %x, leaked warn-mask bit onto success", rcs[0].RetCode)
	}

	// Entry 1: the connection advisory. Must carry the warn-mask bit
	// so the contract normalizer bins it into the <warn> bucket and so
	// the Python CLI prints it as WARNING, not as plain info.
	if rcs[1].RetCode&maskWarn == 0 {
		t.Errorf("entry 1 ret_code = %x, want warn-mask bit set", rcs[1].RetCode)
	}

	// Message must contain the exact upstream phrasing so log greppers
	// and operator runbooks already built against upstream don't have
	// to learn a second pattern.
	wantSubstring := "No active connection to satellite 'parity-fake'"
	if rcs[1].Message != wantSubstring {
		t.Errorf("entry 1 message: got %q, want %q", rcs[1].Message, wantSubstring)
	}
}

// TestNodeCreateNoWarningWhenSatelliteAlreadyConnected pins the
// negative case: a node POSTed with ConnectionStatus="ONLINE" — the
// "adopt an already-running satellite" path — must NOT emit the
// no-connection warning. The mask flip would otherwise look like a
// permanent regression on every reconcile-time re-POST.
func TestNodeCreateNoWarningWhenSatelliteAlreadyConnected(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	body, _ := json.Marshal(apiv1.Node{
		Name:             "already-up",
		Type:             apiv1.NodeTypeSatellite,
		ConnectionStatus: apiv1.NodeTypeOnline,
	})

	resp := httpPost(t, base+"/v1/nodes", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201", resp.StatusCode)
	}

	var rcs []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rcs); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}

	if len(rcs) != 1 {
		t.Fatalf("envelope entries on pre-connected node: got %d, want 1 (success only); got=%+v", len(rcs), rcs)
	}

	if rcs[0].RetCode&maskWarn != 0 {
		t.Errorf("success entry leaked warn-mask bit on pre-connected path: ret_code=%x", rcs[0].RetCode)
	}
}

// TestNodesEndpointsWithoutStore: when Store is nil, every endpoint that
// needs persistence returns 503 Service Unavailable. The version endpoint
// continues to work.
func TestNodesEndpointsWithoutStore(t *testing.T) {
	base, stop := startServerCustom(t, &Server{Addr: pickFreeAddr(t), Store: nil})
	defer stop()

	resp := httpGet(t, base+"/v1/nodes")
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status: got %d, want 503", resp.StatusCode)
	}

	resp2 := httpGet(t, base+"/v1/controller/version")
	_ = resp2.Body.Close()

	if resp2.StatusCode != http.StatusOK {
		t.Errorf("version status: got %d, want 200", resp2.StatusCode)
	}
}

// --- helpers ---

func newClient(t *testing.T, base string) *lapi.Client {
	t.Helper()

	u, err := url.Parse(base)
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}

	c, err := lapi.NewClient(lapi.BaseURL(u))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	return c
}

func httpPost(t *testing.T, addr string, body []byte) *http.Response {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, addr, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}

	return resp
}

func httpDelete(t *testing.T, addr string) *http.Response {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodDelete, addr, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}

	return resp
}

func httpPatch(t *testing.T, addr string, body []byte) *http.Response {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPatch, addr, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}

	return resp
}

func httpPut(t *testing.T, addr string, body []byte) *http.Response {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPut, addr, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}

	return resp
}

// TestNodeDeleteUnknownReturns200Warning pins the Bug 66 idempotence
// contract for `DELETE /v1/nodes/{node}`. Cozystack's node-evacuation
// playbook calls `linstor n d` in a retry loop after a node finishes
// draining; a 404 on the second pass crashed the python CLI's XML
// decoder fallback. The handler must fold NotFound into 200 + an
// ApiCallRc with the WARN bit and an "already absent" message that
// names the node.
func TestNodeDeleteUnknownReturns200Warning(t *testing.T) {
	t.Parallel()

	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpDelete(t, base+"/v1/nodes/ghost-node")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var rc []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rc); err != nil {
		t.Fatalf("decode ApiCallRc envelope: %v", err)
	}

	if len(rc) == 0 {
		t.Fatalf("ApiCallRc envelope: got empty, want one entry")
	}

	if rc[0].RetCode&maskWarn == 0 {
		t.Errorf("ret_code: got %#x, want WARN bit (%#x) set", rc[0].RetCode, maskWarn)
	}

	if !strings.Contains(rc[0].Message, "already absent") {
		t.Errorf("message: got %q, want 'already absent' marker", rc[0].Message)
	}

	if !strings.Contains(rc[0].Message, "ghost-node") {
		t.Errorf("message: got %q, want it to name ghost-node", rc[0].Message)
	}
}

// seedNodeForF2 is a tiny helper used by the F2 capability-synthesis
// tests below. They all need the same shape (one Satellite-typed node
// with no props/layers/UUID at seed time) so the assertion focuses
// on what the read-path adds.
func seedNodeForF2(t *testing.T, st store.Store, name string) {
	t.Helper()

	if err := st.Nodes().Create(t.Context(), &apiv1.Node{
		Name: name,
		Type: apiv1.NodeTypeSatellite,
	}); err != nil {
		t.Fatalf("seed node %q: %v", name, err)
	}
}

// fetchNodeForF2 GETs a node through the REST surface and returns
// the decoded wire-shape value. Centralises the boilerplate the F2
// tests share.
func fetchNodeForF2(t *testing.T, base, name string) apiv1.Node {
	t.Helper()

	resp := httpGet(t, base+"/v1/nodes/"+name)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/nodes/%s status: got %d, want 200", name, resp.StatusCode)
	}

	var got apiv1.Node
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode node %q: %v", name, err)
	}

	return got
}

// TestNodeListIncludesUUIDStable pins F2/audit row #1: upstream
// LINSTOR's Node DTO carries a top-level `uuid` field, and operators
// script `linstor n l --pastable` against it. The UUID MUST be
// non-empty AND stable across separate reads (same name → same UUID
// on every GET, every controller restart, every replica of the
// apiserver). Re-derive in the test via the same helper to also pin
// the namespace constant so a future refactor doesn't silently
// re-roll every operator's cached UUID.
func TestNodeListIncludesUUIDStable(t *testing.T) {
	st := store.NewInMemory()
	seedNodeForF2(t, st, "n1")

	base, stop := startServerWithStore(t, st)
	defer stop()

	first := fetchNodeForF2(t, base, "n1")
	if first.UUID == "" {
		t.Fatalf("UUID: got empty, want non-empty UUID v5")
	}

	want := apiv1.StableNodeUUID("n1")
	if first.UUID != want {
		t.Errorf("UUID: got %q, want %q (deterministic SHA1 namespace)", first.UUID, want)
	}

	// Stability across reads — re-GET and confirm the same value
	// comes back. If it didn't, operators' tooling that caches UUID
	// would break on every reconcile.
	second := fetchNodeForF2(t, base, "n1")
	if first.UUID != second.UUID {
		t.Errorf("UUID not stable across reads: got %q then %q", first.UUID, second.UUID)
	}
}

// TestNodeListSurfacesSupportedLayers pins F2/audit row #1:
// upstream's `Node.resource_layers` array advertises which LINSTOR
// layer types this satellite implements. `linstor advise` and the
// CLI's layer-stack picker read this list. Blockstor's satellite
// implements DRBD, STORAGE, and LUKS — the response MUST surface
// them in upstream-name form.
func TestNodeListSurfacesSupportedLayers(t *testing.T) {
	st := store.NewInMemory()
	seedNodeForF2(t, st, "n1")

	base, stop := startServerWithStore(t, st)
	defer stop()

	got := fetchNodeForF2(t, base, "n1")

	wantLayers := []string{"DRBD", "STORAGE", "LUKS"}
	for _, want := range wantLayers {
		var found bool

		for _, have := range got.ResourceLayers {
			if have == want {
				found = true

				break
			}
		}

		if !found {
			t.Errorf("ResourceLayers missing %q; got %v", want, got.ResourceLayers)
		}
	}
}

// TestNodeListSurfacesSupportedProviders pins F2/audit row #1:
// upstream's `Node.storage_providers` advertises which provider
// kinds `linstor sp c` will accept on this node. The list MUST
// match what `pkg/satellite/factory.go::NewProviderFromKind`
// actually instantiates — operators rely on it to validate
// StorageClass `linstor.csi.linbit.com/storagePool` references.
func TestNodeListSurfacesSupportedProviders(t *testing.T) {
	st := store.NewInMemory()
	seedNodeForF2(t, st, "n1")

	base, stop := startServerWithStore(t, st)
	defer stop()

	got := fetchNodeForF2(t, base, "n1")

	// LVM_THIN / ZFS_THIN / FILE_THIN / DISKLESS are the four that
	// linstor-csi exercises most heavily in piraeus-operator
	// deployments — pin them explicitly so a future trim doesn't
	// silently break CSI provisioning.
	wantProviders := []string{"LVM_THIN", "ZFS_THIN", "FILE_THIN", "DISKLESS"}
	for _, want := range wantProviders {
		var found bool

		for _, have := range got.StorageProviders {
			if have == want {
				found = true

				break
			}
		}

		if !found {
			t.Errorf("StorageProviders missing %q; got %v", want, got.StorageProviders)
		}
	}
}

// TestNodeListExposesUnsupportedSets pins F2/audit row #1:
// upstream's `Node.unsupported_layers` and `unsupported_providers`
// are `map[string][]string` where the key names the
// missing-layer/provider and the value carries one-or-more reason
// strings. The CLI surfaces these in `linstor n l --pastable`'s
// SupportInfo column. Blockstor explicitly scopes out CACHE /
// WRITECACHE / NVME layers and the OPENFLEX_TARGET / REMOTE_SPDK /
// SPDK / STORAGE_SPACES{,_THIN} / EBS_TARGET / EBS_INIT providers
// — the wire MUST advertise this so `linstor advise` doesn't
// suggest unreachable configurations.
func TestNodeListExposesUnsupportedSets(t *testing.T) {
	st := store.NewInMemory()
	seedNodeForF2(t, st, "n1")

	base, stop := startServerWithStore(t, st)
	defer stop()

	got := fetchNodeForF2(t, base, "n1")

	wantLayers := []string{"CACHE", "WRITECACHE", "NVME"}
	for _, want := range wantLayers {
		reasons, ok := got.UnsupportedLayers[want]
		if !ok {
			t.Errorf("UnsupportedLayers missing %q; got keys=%v", want, mapKeys(got.UnsupportedLayers))

			continue
		}

		if len(reasons) == 0 {
			t.Errorf("UnsupportedLayers[%q]: got empty reason list, want at least one", want)
		}
	}

	wantProviders := []string{
		"OPENFLEX_TARGET", "REMOTE_SPDK", "SPDK",
		"STORAGE_SPACES", "STORAGE_SPACES_THIN",
		"EBS_TARGET", "EBS_INIT",
	}
	for _, want := range wantProviders {
		reasons, ok := got.UnsupportedProviders[want]
		if !ok {
			t.Errorf("UnsupportedProviders missing %q; got keys=%v", want, mapKeys(got.UnsupportedProviders))

			continue
		}

		if len(reasons) == 0 {
			t.Errorf("UnsupportedProviders[%q]: got empty reason list, want at least one", want)
		}
	}
}

// TestNodeListPropsNodeUname pins F2/audit row #1: upstream LINSTOR
// stamps `props.NodeUname` (the value `uname -n` would have returned
// on the satellite host) and `props.CurStltConnName` ("default" for
// single-connection deployments). Both surface in `linstor n l
// --show-props NodeUname,CurStltConnName`. Blockstor synthesises
// NodeUname from the node name (Piraeus convention) and pins
// CurStltConnName=default since there's exactly one connection per
// node in this architecture.
func TestNodeListPropsNodeUname(t *testing.T) {
	st := store.NewInMemory()
	seedNodeForF2(t, st, "worker-1")

	base, stop := startServerWithStore(t, st)
	defer stop()

	got := fetchNodeForF2(t, base, "worker-1")

	if got.Props == nil {
		t.Fatalf("Props: got nil, want NodeUname/CurStltConnName populated")
	}

	if got.Props["NodeUname"] != "worker-1" {
		t.Errorf("Props[NodeUname]: got %q, want %q", got.Props["NodeUname"], "worker-1")
	}

	if got.Props["CurStltConnName"] != "default" {
		t.Errorf("Props[CurStltConnName]: got %q, want %q",
			got.Props["CurStltConnName"], "default")
	}
}

// TestNodeListPropertiesRoundTripAllNamespaces pins scenario 1.W01
// (P0, unit) for the Node scope: `linstor node list-properties` is
// served client-side from the `props` field of `GET /v1/nodes/{node}`,
// so the REST round-trip must preserve every known LINSTOR namespace
// (`DrbdOptions/`, `Aux/`, `FileSystem/`, `StorDriver/`) verbatim —
// no key rewriting, no value re-encoding. A regression that lower-
// cased keys or stripped namespace prefixes would break golinstor's
// per-key lookups in the recovery-copilot's diagnostic playbooks.
func TestNodeListPropertiesRoundTripAllNamespaces(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	seed := map[string]string{
		"DrbdOptions/Net/protocol":     "C",
		"Aux/rack-id":                  "rack-7",
		"FileSystem/Type":              "ext4",
		"StorDriver/StorPoolName":      "blockstor-zfs",
		"Aux/cozystack.io/tenant-uuid": "9f3c-aabb",
	}

	if err := st.Nodes().Create(ctx, &apiv1.Node{
		Name:  "worker-1",
		Type:  apiv1.NodeTypeSatellite,
		Props: maps.Clone(seed),
	}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	got := fetchNodeForF2(t, base, "worker-1")

	if got.Props == nil {
		t.Fatalf("Props: got nil, want a map (scenario 1.W01: empty scope returns empty map, not nil)")
	}

	for k, want := range seed {
		if got.Props[k] != want {
			t.Errorf("Props[%q]: got %q, want %q (namespace round-trip drift)", k, got.Props[k], want)
		}
	}
}

// TestNodeListPropertiesUnknownNodeReturns404 pins the unknown-scope
// half of scenario 1.W01: a `list-properties` call against a node
// that does not exist must surface a 404 (which golinstor's CLI maps
// to "Node 'ghost' not found"). A 500 here would mask a typo as a
// transient server error and silently retry forever.
func TestNodeListPropertiesUnknownNodeReturns404(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpGet(t, base+"/v1/nodes/ghost")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", resp.StatusCode)
	}
}

// TestNodeListPropertiesEmptySeedNotNil pins the "empty scope returns
// empty map (not nil)" clause of scenario 1.W01. A satellite-typed
// node seeded with no Props at all must still emit a usable map
// after the F2 synthesis: NodeUname + CurStltConnName are stamped
// unconditionally, so an operator running `linstor n list-properties`
// on a fresh node never sees a "no properties" CLI panic from an
// indexed access on a nil map.
func TestNodeListPropertiesEmptySeedNotNil(t *testing.T) {
	st := store.NewInMemory()
	seedNodeForF2(t, st, "fresh-node")

	base, stop := startServerWithStore(t, st)
	defer stop()

	got := fetchNodeForF2(t, base, "fresh-node")

	if got.Props == nil {
		t.Fatalf("Props: got nil, want non-nil map (F2 synthesises NodeUname + CurStltConnName)")
	}

	if len(got.Props) == 0 {
		t.Errorf("Props: got empty map, want at least NodeUname + CurStltConnName")
	}
}

// mapKeys is a tiny helper that returns the keys of a
// `map[string][]string` as a sorted slice — used in failure messages
// for the F2 tests so a regression shows up as `got keys=[...]`
// rather than as a Go map literal's randomised order.
func mapKeys(m map[string][]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}

	sort.Strings(out)

	return out
}
