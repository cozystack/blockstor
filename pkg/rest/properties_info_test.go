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
	"net/http"
	"testing"

	"github.com/cozystack/blockstor/pkg/store"
)

// Finding 8 (P2): the `/v1/{kind}/properties/info` endpoints were
// unwired. The nodes variant fell into the `/v1/nodes/{node}/info`
// handler with "node 'properties': object not found"; the other
// three (RD/RG/SPDfn) 404'd as not-implemented. The tests below pin:
//
//   - All four routes return 200 with a non-empty catalogue.
//   - Each catalogue entry carries the `name` + `info` shape the
//     python CLI keys on (`linstor n lp -i` etc.).
//   - The nodes variant correctly beats `handleNodeInfo` — its
//     response is the PropsInfo array, not the nodeInfo struct.

// runPropsInfoExpect asserts the endpoint returns a non-empty
// PropsInfo array with entries carrying at least `name`. Shared by
// the four per-kind subtests so the wire contract is enforced
// uniformly.
func runPropsInfoExpect(t *testing.T, base, path string) {
	t.Helper()

	resp := httpGet(t, base+path)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("%s status: got %d, want 200", path, resp.StatusCode)
	}

	var entries []map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&entries); err != nil {
		t.Fatalf("%s decode: %v", path, err)
	}

	if len(entries) == 0 {
		t.Fatalf("%s returned empty catalogue; python CLI crashes on len=0", path)
	}

	for i, e := range entries {
		if _, ok := e["name"]; !ok {
			t.Errorf("%s entry[%d] missing `name`: %+v", path, i, e)
		}

		if _, ok := e["info"]; !ok {
			t.Errorf("%s entry[%d] missing `info`: %+v", path, i, e)
		}
	}
}

// TestPropertiesInfoNodes pins that the literal node-properties path
// beats the `/v1/nodes/{node}/info` wildcard router and serves the
// catalogue. Before the fix, the wildcard handler treated "properties"
// as the node name and returned 404 "node 'properties': object not
// found" — surfaced as the misleading routing trap in Finding 8.
func TestPropertiesInfoNodes(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	runPropsInfoExpect(t, base, "/v1/nodes/properties/info")
}

// TestPropertiesInfoResourceDefinitions pins the RD catalogue.
func TestPropertiesInfoResourceDefinitions(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	runPropsInfoExpect(t, base, "/v1/resource-definitions/properties/info")
}

// TestPropertiesInfoResourceGroups pins the RG catalogue.
func TestPropertiesInfoResourceGroups(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	runPropsInfoExpect(t, base, "/v1/resource-groups/properties/info")
}

// TestPropertiesInfoStoragePoolDefinitions pins the SPDfn catalogue.
func TestPropertiesInfoStoragePoolDefinitions(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	runPropsInfoExpect(t, base, "/v1/storage-pool-definitions/properties/info")
}
