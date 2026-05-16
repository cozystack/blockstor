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
	"net/http"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// nodeConnectionPutBody is the upstream `NodeConnectionModify` wire
// shape. python-linstor 1.27.1's `set_property` codepath sends
// override_props as a JSON object; we marshal the same shape via a
// typed struct so the tests pin the on-wire field tags.
type nodeConnectionPutBody struct {
	OverrideProps    map[string]string `json:"override_props,omitempty"`
	DeleteProps      []string          `json:"delete_props,omitempty"`
	DeleteNamespaces []string          `json:"delete_namespaces,omitempty"`
}

// seedNodeConnectionEndpoints returns a fresh in-memory store with the
// named Node CRDs pre-seeded. After Bug 133 wired the node-existence
// gate on PUT /v1/node-connections/{a}/{b}, every test that exercises
// the persistence / list surface must seed both endpoints; the wire
// handler now 404s on bogus names. The helper keeps the test bodies
// focused on the property-modify shape rather than CRD seeding.
func seedNodeConnectionEndpoints(t *testing.T, names ...string) store.Store {
	t.Helper()

	st := store.NewInMemory()

	for _, name := range names {
		if err := st.Nodes().Create(t.Context(), &apiv1.Node{Name: name}); err != nil {
			t.Fatalf("seed node %q: %v", name, err)
		}
	}

	return st
}

// TestNodeConnectionPutReturnsEnvelope is Bug 101's primary
// regression: PUT /v1/node-connections/{a}/{b} with a non-empty
// override_props body must return 200 + the LINSTOR `[]APICallRc`
// envelope, NOT 204 with an empty body. python-linstor's
// `set_property` codepath crashed on 204+empty with
//
//	Unable to parse REST json data: Expecting value: line 1 column 1 (char 0)
//
// because the empty body trips `json.loads("")`. The fix is the
// 200+envelope wire shape; the success ret_code must include the
// MASK_INFO bit (`maskInfo`) so python-linstor's `rc.is_success()`
// branch fires and the operator's `linstor node-connection
// set-property` exits 0.
func TestNodeConnectionPutReturnsEnvelope(t *testing.T) {
	base, stop := startServerWithStore(t, seedNodeConnectionEndpoints(t, "alpha", "beta"))
	defer stop()

	body, err := json.Marshal(nodeConnectionPutBody{
		OverrideProps: map[string]string{"Sites/Site": "some-site"},
	})
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPut,
		base+"/v1/node-connections/alpha/beta", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}

	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	// The decoder fails when the body is empty — that's the exact
	// python-linstor crash mode this regression test pins. Any
	// future "optimization" that drops the body without dropping
	// the 200 status code would still fail here.
	var rcs []apiv1.APICallRc
	if decodeErr := json.NewDecoder(resp.Body).Decode(&rcs); decodeErr != nil {
		t.Fatalf("decode envelope: %v", decodeErr)
	}

	if len(rcs) == 0 {
		t.Fatal("envelope: got 0 entries, want at least 1")
	}

	// MASK_INFO (0x0001_0000_0000) must be set — that's how the
	// python CLI distinguishes "success" from "error/warn".
	if rcs[0].RetCode&maskInfo == 0 {
		t.Errorf("ret_code: got %#x, want MASK_INFO bit set", rcs[0].RetCode)
	}

	if rcs[0].Message == "" {
		t.Error("message: got empty, want operator-visible text")
	}
}

// TestNodeConnectionPutPersists is the persistence half of Bug 101:
// after a successful PUT, the property MUST come back through GET on
// the same (a, b) pair. The pre-fix handler returned a vacuous
// success and stored nothing; `linstor node-connection list` then
// showed an empty matrix even after the operator's set-property
// "succeeded".
func TestNodeConnectionPutPersists(t *testing.T) {
	base, stop := startServerWithStore(t, seedNodeConnectionEndpoints(t, "alpha", "beta"))
	defer stop()

	body, err := json.Marshal(nodeConnectionPutBody{
		OverrideProps: map[string]string{
			"Sites/Site":                   "some-site",
			"DrbdOptions/Net/ping-timeout": "100",
		},
	})
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPut,
		base+"/v1/node-connections/alpha/beta", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}

	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status: got %d, want 200", resp.StatusCode)
	}

	// GET on the same pair must reflect what was just PUT.
	getResp := httpGet(t, base+"/v1/node-connections/alpha/beta")
	defer func() { _ = getResp.Body.Close() }()

	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("GET status: got %d, want 200", getResp.StatusCode)
	}

	var got nodeConnectionWire
	if decodeErr := json.NewDecoder(getResp.Body).Decode(&got); decodeErr != nil {
		t.Fatalf("decode GET: %v", decodeErr)
	}

	if got.NodeA != "alpha" || got.NodeB != "beta" {
		t.Errorf("nodes: got (%q,%q), want (alpha,beta)", got.NodeA, got.NodeB)
	}

	if got.Props["Sites/Site"] != "some-site" {
		t.Errorf("Sites/Site: got %q, want some-site", got.Props["Sites/Site"])
	}

	if got.Props["DrbdOptions/Net/ping-timeout"] != "100" {
		t.Errorf("ping-timeout: got %q, want 100", got.Props["DrbdOptions/Net/ping-timeout"])
	}
}

// TestNodeConnectionPairOrderIndependent pins the canonical-key
// invariant: a write against (alpha, beta) MUST be visible to a read
// against (beta, alpha). Without canonicalisation the same pair would
// live under two separate records depending on argument order, and
// `linstor node-connection list <node-a> <node-b>` vs
// `linstor node-connection list <node-b> <node-a>` would render
// different rows — confusing operators and breaking idempotent
// reconciler patterns that don't track a canonical name order.
func TestNodeConnectionPairOrderIndependent(t *testing.T) {
	base, stop := startServerWithStore(t, seedNodeConnectionEndpoints(t, "alpha", "beta"))
	defer stop()

	body, err := json.Marshal(nodeConnectionPutBody{
		OverrideProps: map[string]string{"Sites/Site": "site-x"},
	})
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}

	// Write A->B.
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPut,
		base+"/v1/node-connections/alpha/beta", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}

	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status: got %d, want 200", resp.StatusCode)
	}

	// Read B->A.
	getResp := httpGet(t, base+"/v1/node-connections/beta/alpha")
	defer func() { _ = getResp.Body.Close() }()

	var got nodeConnectionWire
	if decodeErr := json.NewDecoder(getResp.Body).Decode(&got); decodeErr != nil {
		t.Fatalf("decode: %v", decodeErr)
	}

	if got.Props["Sites/Site"] != "site-x" {
		t.Errorf("Sites/Site under reversed (B,A): got %q, want site-x", got.Props["Sites/Site"])
	}
}

// TestNodeConnectionListReturnsArray pins the matrix-list wire shape:
// 200 + JSON array of {node_a, node_b, props}. After at least one
// PUT, the list MUST include that pair. The pre-fix handler always
// returned `[]` regardless of what had been written, so operators
// running `linstor node-connection list` saw an empty table even
// after a "successful" set-property.
func TestNodeConnectionListReturnsArray(t *testing.T) {
	base, stop := startServerWithStore(t, seedNodeConnectionEndpoints(t, "n1", "n2"))
	defer stop()

	body, err := json.Marshal(nodeConnectionPutBody{
		OverrideProps: map[string]string{"Sites/Site": "us-east"},
	})
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPut,
		base+"/v1/node-connections/n1/n2", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}

	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status: got %d, want 200", resp.StatusCode)
	}

	listResp := httpGet(t, base+"/v1/node-connections")
	defer func() { _ = listResp.Body.Close() }()

	if listResp.StatusCode != http.StatusOK {
		t.Fatalf("LIST status: got %d, want 200", listResp.StatusCode)
	}

	var pairs []nodeConnectionWire
	if decodeErr := json.NewDecoder(listResp.Body).Decode(&pairs); decodeErr != nil {
		t.Fatalf("decode: %v", decodeErr)
	}

	if len(pairs) != 1 {
		t.Fatalf("len: got %d entries, want 1", len(pairs))
	}

	if pairs[0].Props["Sites/Site"] != "us-east" {
		t.Errorf("Sites/Site: got %q, want us-east", pairs[0].Props["Sites/Site"])
	}
}

// TestNodeConnectionListFilterByNode pins the optional-node filter
// path used by `linstor node-connection list <node>`: returns only
// the pairs that have <node> as either endpoint. Required so an
// operator running `linstor node-connection list dev-kvaps-worker-1`
// doesn't get a flood of unrelated rows from large clusters.
func TestNodeConnectionListFilterByNode(t *testing.T) {
	base, stop := startServerWithStore(t,
		seedNodeConnectionEndpoints(t, "alpha", "beta", "gamma", "delta"))
	defer stop()

	cases := []struct {
		path  string
		site  string
		count int
	}{
		// (alpha, beta): site-x
		{"/v1/node-connections/alpha/beta", "site-x", 0},
		// (gamma, delta): site-y — NOT touching alpha
		{"/v1/node-connections/gamma/delta", "site-y", 0},
	}

	for _, tc := range cases {
		body, err := json.Marshal(nodeConnectionPutBody{
			OverrideProps: map[string]string{"Sites/Site": tc.site},
		})
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}

		req, err := http.NewRequestWithContext(t.Context(), http.MethodPut,
			base+tc.path, bytes.NewReader(body))
		if err != nil {
			t.Fatalf("new request: %v", err)
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("do request: %v", err)
		}

		_ = resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("PUT %s: status %d, want 200", tc.path, resp.StatusCode)
		}
	}

	listResp := httpGet(t, base+"/v1/node-connections/alpha")
	defer func() { _ = listResp.Body.Close() }()

	var pairs []nodeConnectionWire
	if decodeErr := json.NewDecoder(listResp.Body).Decode(&pairs); decodeErr != nil {
		t.Fatalf("decode: %v", decodeErr)
	}

	if len(pairs) != 1 {
		t.Fatalf("filter alpha: got %d pairs, want 1 (alpha,beta only)", len(pairs))
	}

	if pairs[0].Props["Sites/Site"] != "site-x" {
		t.Errorf("filtered Sites/Site: got %q, want site-x", pairs[0].Props["Sites/Site"])
	}
}

// TestNodeConnectionPutEmptyBodyIs4xx pins the bad-input path: PUT
// with an empty / malformed body must return a 4xx wrapped in the
// `[]APICallRc` envelope — NOT a 500, and NOT a 204 / empty. The
// python CLI's failure-classification looks at the envelope's
// MASK_ERROR bit, not the HTTP status alone; surfacing as a typed
// error here keeps the operator-facing diagnostic actionable.
func TestNodeConnectionPutEmptyBodyIs4xx(t *testing.T) {
	base, stop := startServerWithStore(t, seedNodeConnectionEndpoints(t, "alpha", "beta"))
	defer stop()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPut,
		base+"/v1/node-connections/alpha/beta", bytes.NewBufferString(""))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}

	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 400 || resp.StatusCode >= 500 {
		t.Fatalf("status: got %d, want 4xx", resp.StatusCode)
	}

	var rcs []apiv1.APICallRc
	if decodeErr := json.NewDecoder(resp.Body).Decode(&rcs); decodeErr != nil {
		t.Fatalf("decode envelope: %v (body must be `[]APICallRc`, not bare error)", decodeErr)
	}

	if len(rcs) == 0 {
		t.Fatal("envelope: got 0 entries, want at least 1")
	}
}

// TestNodeConnectionDeleteProp pins the delete-prop half: PUT with
// `delete_props: ["Sites/Site"]` removes the named key. After the
// only remaining key is gone, the pair MUST drop out of the list
// response entirely (no zero-props ghost rows).
func TestNodeConnectionDeleteProp(t *testing.T) {
	base, stop := startServerWithStore(t, seedNodeConnectionEndpoints(t, "n1", "n2"))
	defer stop()

	// First, set.
	setBody, err := json.Marshal(nodeConnectionPutBody{
		OverrideProps: map[string]string{"Sites/Site": "x"},
	})
	if err != nil {
		t.Fatalf("marshal set body: %v", err)
	}

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPut,
		base+"/v1/node-connections/n1/n2", bytes.NewReader(setBody))
	if err != nil {
		t.Fatalf("new set request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do set request: %v", err)
	}

	_ = resp.Body.Close()

	// Then, delete that one key.
	delBody, err := json.Marshal(nodeConnectionPutBody{
		DeleteProps: []string{"Sites/Site"},
	})
	if err != nil {
		t.Fatalf("marshal del body: %v", err)
	}

	req, err = http.NewRequestWithContext(t.Context(), http.MethodPut,
		base+"/v1/node-connections/n1/n2", bytes.NewReader(delBody))
	if err != nil {
		t.Fatalf("new del request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do del request: %v", err)
	}

	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DELETE-prop status: got %d, want 200", resp.StatusCode)
	}

	// List must be empty — emptied pair drops out.
	listResp := httpGet(t, base+"/v1/node-connections")
	defer func() { _ = listResp.Body.Close() }()

	var pairs []nodeConnectionWire
	if decodeErr := json.NewDecoder(listResp.Body).Decode(&pairs); decodeErr != nil {
		t.Fatalf("decode list: %v", decodeErr)
	}

	if len(pairs) != 0 {
		t.Errorf("post-delete list: got %d pairs, want 0 (emptied pair must drop out)", len(pairs))
	}
}
