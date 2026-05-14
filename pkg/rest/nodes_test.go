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
	"net"
	"net/http"
	"net/url"
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
