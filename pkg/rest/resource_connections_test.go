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

	"github.com/cozystack/blockstor/pkg/store"
)

// TestResourceConnectionDRBDPeerOptionsW04 pins scenario 5.W04: a
// PATCH to
// `/v1/resource-definitions/{rd}/resource-connections/{a}/{b}/drbd-peer-options`
// carrying `override_props: {"DrbdOptions/PeerDevice/max-buffers":
// "8192"}` must persist the prop on the (rd, a, b) ResourceConnection
// — i.e. a subsequent GET of the same pair returns the prop in
// `props`, while a GET of a DIFFERENT (rd, a, b) triple (different
// rd OR different pair) returns an empty `props` map.
//
// The differentiation from 5.W03 (node-connection scope) is the
// reason both pinning directions matter: a stray RD-keyed write
// landing on every connection of every RD would silently break the
// `--max-buffers` knob's promised scope.
func TestResourceConnectionDRBDPeerOptionsW04(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	const (
		rdHot  = "pvc-hot"
		rdCold = "pvc-cold"
		nodeA  = "n1"
		nodeB  = "n2"
		nodeC  = "n3"

		maxBuffersKey = "DrbdOptions/PeerDevice/max-buffers"
		maxBuffersVal = "8192"
	)

	// PATCH the hot RD's (n1, n2) connection only.
	patchBody := `{"override_props":{"` + maxBuffersKey + `":"` + maxBuffersVal + `"}}`
	patchURL := base + "/v1/resource-definitions/" + rdHot +
		"/resource-connections/" + nodeA + "/" + nodeB + "/drbd-peer-options"

	resp := doRequest(t, http.MethodPatch, patchURL, patchBody)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("PATCH status: got %d, want 204", resp.StatusCode)
	}

	// GET the same pair — must surface the prop.
	gotProps := getResourceConnectionProps(t, base, rdHot, nodeA, nodeB)
	if gotProps[maxBuffersKey] != maxBuffersVal {
		t.Errorf("hot (n1, n2) props[%s]=%q want %q (full props: %+v)",
			maxBuffersKey, gotProps[maxBuffersKey], maxBuffersVal, gotProps)
	}

	// Scope-pinning #1: different pair on the SAME rd must stay clean.
	// `n1<->n2` is patched; `n1<->n3` is not.
	gotOther := getResourceConnectionProps(t, base, rdHot, nodeA, nodeC)
	if v, ok := gotOther[maxBuffersKey]; ok {
		t.Errorf("scope leaked across pairs: (n1, n3) sees %s=%q", maxBuffersKey, v)
	}

	// Scope-pinning #2: same pair on a DIFFERENT rd must stay clean.
	// `(rdHot, n1, n2)` is patched; `(rdCold, n1, n2)` is not — the
	// per-(rd, a, b) scope is the whole point of 5.W04 vs. 5.W03.
	gotCold := getResourceConnectionProps(t, base, rdCold, nodeA, nodeB)
	if v, ok := gotCold[maxBuffersKey]; ok {
		t.Errorf("scope leaked across RDs: (rdCold, n1, n2) sees %s=%q", maxBuffersKey, v)
	}
}

// TestResourceConnectionPairOrderIndependent pins that the registry
// keys on the SORTED pair so `linstor resource-connection ... n1 n2`
// and `... n2 n1` reach the same connection — operators on the CLI
// don't reliably remember which side is HostA.
func TestResourceConnectionPairOrderIndependent(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	const (
		rd            = "pvc-1"
		maxBuffersKey = "DrbdOptions/PeerDevice/max-buffers"
		maxBuffersVal = "8192"
	)

	// PATCH via (n2, n1) ordering.
	patchURL := base + "/v1/resource-definitions/" + rd +
		"/resource-connections/n2/n1/drbd-peer-options"
	patchBody := `{"override_props":{"` + maxBuffersKey + `":"` + maxBuffersVal + `"}}`

	resp := doRequest(t, http.MethodPatch, patchURL, patchBody)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("PATCH status: got %d, want 204", resp.StatusCode)
	}

	// GET via (n1, n2) ordering — must still see the prop.
	gotProps := getResourceConnectionProps(t, base, rd, "n1", "n2")
	if gotProps[maxBuffersKey] != maxBuffersVal {
		t.Errorf("unordered match failed: (n1, n2) GET sees %+v", gotProps)
	}
}

// TestResourceConnectionDeletePropsW02 pins the `--unset-max-buffers`
// path (scenario 5.W02 syntax, applied to the resource-connection
// scope): a PATCH with `delete_props` drops the key from the
// connection props bag without touching unrelated keys.
func TestResourceConnectionDeletePropsW02(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	const (
		rd            = "pvc-1"
		nodeA         = "n1"
		nodeB         = "n2"
		maxBuffersKey = "DrbdOptions/PeerDevice/max-buffers"
		pingKey       = "DrbdOptions/Net/ping-timeout"
	)

	patchURL := base + "/v1/resource-definitions/" + rd +
		"/resource-connections/" + nodeA + "/" + nodeB + "/drbd-peer-options"

	// Set both keys.
	resp := doRequest(t, http.MethodPatch, patchURL,
		`{"override_props":{"`+maxBuffersKey+`":"8192","`+pingKey+`":"100"}}`)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("PATCH (set) status: got %d, want 204", resp.StatusCode)
	}

	// Now unset only max-buffers.
	resp = doRequest(t, http.MethodPatch, patchURL,
		`{"delete_props":["`+maxBuffersKey+`"]}`)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("PATCH (unset) status: got %d, want 204", resp.StatusCode)
	}

	props := getResourceConnectionProps(t, base, rd, nodeA, nodeB)
	if _, ok := props[maxBuffersKey]; ok {
		t.Errorf("delete_props did not remove key: %+v", props)
	}

	if props[pingKey] != "100" {
		t.Errorf("delete_props clobbered unrelated key: %+v", props)
	}
}

// getResourceConnectionProps GETs (rd, a, b) and returns the props
// sub-map. Empty map when the pair has never been PATCHed.
func getResourceConnectionProps(t *testing.T, base, rd, nodeA, nodeB string) map[string]string {
	t.Helper()

	url := base + "/v1/resource-definitions/" + rd +
		"/resource-connections/" + nodeA + "/" + nodeB

	resp := httpGet(t, url)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET status for (%s, %s, %s): got %d, want 200", rd, nodeA, nodeB, resp.StatusCode)
	}

	var body struct {
		NodeA string            `json:"node_a"`
		NodeB string            `json:"node_b"`
		Props map[string]string `json:"props"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode GET (%s, %s, %s): %v", rd, nodeA, nodeB, err)
	}

	if body.Props == nil {
		return map[string]string{}
	}

	return body.Props
}

// doRequest is a tiny wrapper to PATCH/PUT/POST with a JSON body —
// kept local to this test file rather than promoted into a shared
// helper to keep test deltas isolated.
func doRequest(t *testing.T, method, url, body string) *http.Response {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), method, url, bytes.NewBufferString(body))
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
