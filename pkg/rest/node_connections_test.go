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

// TestNodeConnectionsListReturnsEmpty pins the contract on the
// matrix-list endpoint: 200 with `[]`. golinstor's polling loop
// logs an error for any non-200, so the empty-but-present shape
// keeps the controller log clean in cozystack's flat-L2 setup.
func TestNodeConnectionsListReturnsEmpty(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpGet(t, base+"/v1/node-connections")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got []map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(got) != 0 {
		t.Errorf("body: got %d entries, want 0", len(got))
	}
}

// TestNodeConnectionGetReturnsEmptyObject pins the per-pair GET
// shape: 200 with `{node_a, node_b, properties:{}}`. golinstor
// expects a NodeConnection object (not a list) here — returning
// a list would silently mis-decode into an empty zero-value
// struct without surfacing the wire-shape mismatch.
func TestNodeConnectionGetReturnsEmptyObject(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpGet(t, base+"/v1/node-connections/n1/n2")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if got["node_a"] != "n1" || got["node_b"] != "n2" {
		t.Errorf("node fields: got %+v", got)
	}

	props, ok := got["properties"].(map[string]any)
	if !ok {
		t.Fatalf("properties not a map: %T %+v", got["properties"], got["properties"])
	}

	if len(props) != 0 {
		t.Errorf("properties: got %d entries, want 0", len(props))
	}
}

// TestNodeConnectionPutDeleteNoContent pins the write surface:
// PUT/POST/PATCH/DELETE all accept the request and return 204.
// Returning 4xx would break `linstor node-connection set-property`
// even though we don't persist anything — operators experimenting
// with the CLI command would see an error where Java LINSTOR
// silently accepts.
func TestNodeConnectionPutDeleteNoContent(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	client := &http.Client{}

	cases := []struct {
		method string
		body   string
	}{
		{http.MethodPut, `{"override_props":{"DrbdOptions/Net/ping-timeout":"100"}}`},
		{http.MethodPost, `{"override_props":{"DrbdOptions/Net/ping-timeout":"100"}}`},
		{http.MethodPatch, `{"delete_props":["DrbdOptions/Net/ping-timeout"]}`},
		{http.MethodDelete, ""},
	}

	for _, tc := range cases {
		req, err := http.NewRequestWithContext(t.Context(), tc.method,
			base+"/v1/node-connections/n1/n2", bytes.NewBufferString(tc.body))
		if err != nil {
			t.Fatalf("%s: %v", tc.method, err)
		}

		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("%s: %v", tc.method, err)
		}

		_ = resp.Body.Close()

		if resp.StatusCode != http.StatusNoContent {
			t.Errorf("%s status: got %d, want 204", tc.method, resp.StatusCode)
		}
	}
}
