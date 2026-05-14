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
	"context"
	"encoding/json"
	"net/http"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// seedRDForConnections seeds a single RD so the resource-connection
// path handler has something to attach paths to. The RD-attach is the
// design choice that keeps multi-path state on the parent RD rather
// than introducing a fresh ResourceConnection CRD — the operator-
// visible REST surface still looks identical to upstream LINSTOR.
func seedRDForConnections(t *testing.T, st store.Store, name string) {
	t.Helper()

	err := st.ResourceDefinitions().Create(context.Background(), &apiv1.ResourceDefinition{
		Name: name,
	})
	if err != nil {
		t.Fatalf("seed RD %q: %v", name, err)
	}
}

// TestResourceConnectionPathPostRoundTrip pins scenario 3.7: two
// successive POSTs add two distinct paths; the subsequent GET returns
// both in declaration order.
func TestResourceConnectionPathPostRoundTrip(t *testing.T) {
	st := store.NewInMemory()
	seedRDForConnections(t, st, "pvc-1")

	base, stop := startServerWithStore(t, st)
	defer stop()

	for _, body := range []string{
		`{"name":"path1","node_a_address":"10.1.1.5","node_b_address":"10.1.1.6"}`,
		`{"name":"path2","node_a_address":"10.2.2.5","node_b_address":"10.2.2.6"}`,
	} {
		resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/resource-connections/n1/n2/paths", []byte(body))

		_ = resp.Body.Close()

		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("POST %s: status %d, want 201", body, resp.StatusCode)
		}
	}

	resp := httpGet(t, base+"/v1/resource-definitions/pvc-1/resource-connections/n1/n2/paths")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET status: got %d, want 200", resp.StatusCode)
	}

	var got []map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode GET: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("paths: got %d entries, want 2: %+v", len(got), got)
	}

	if got[0]["name"] != "path1" || got[0]["node_a_address"] != "10.1.1.5" || got[0]["node_b_address"] != "10.1.1.6" {
		t.Errorf("paths[0]: got %+v", got[0])
	}

	if got[1]["name"] != "path2" || got[1]["node_a_address"] != "10.2.2.5" || got[1]["node_b_address"] != "10.2.2.6" {
		t.Errorf("paths[1]: got %+v", got[1])
	}
}

// TestResourceConnectionPathPostIdempotentOnName: re-POSTing the same
// path-name UPSERTs the addresses without creating a duplicate entry.
// The CLI / golinstor expect this (Java LINSTOR's REST POST on
// resource-connection paths is documented as an UPSERT on name).
func TestResourceConnectionPathPostIdempotentOnName(t *testing.T) {
	st := store.NewInMemory()
	seedRDForConnections(t, st, "pvc-1")

	base, stop := startServerWithStore(t, st)
	defer stop()

	first := `{"name":"path1","node_a_address":"10.1.1.5","node_b_address":"10.1.1.6"}`
	second := `{"name":"path1","node_a_address":"10.9.9.5","node_b_address":"10.9.9.6"}`

	for _, body := range []string{first, second} {
		resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/resource-connections/n1/n2/paths", []byte(body))

		_ = resp.Body.Close()

		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("POST %s: status %d, want 201", body, resp.StatusCode)
		}
	}

	resp := httpGet(t, base+"/v1/resource-definitions/pvc-1/resource-connections/n1/n2/paths")
	defer func() { _ = resp.Body.Close() }()

	var got []map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode GET: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("paths: got %d entries after re-POST, want 1: %+v", len(got), got)
	}

	if got[0]["node_a_address"] != "10.9.9.5" || got[0]["node_b_address"] != "10.9.9.6" {
		t.Errorf("UPSERT failed: got %+v, want addresses replaced", got[0])
	}
}

// TestResourceConnectionPathDeleteLeavesOthers: DELETE path1 removes
// only that path; path2 stays.
func TestResourceConnectionPathDeleteLeavesOthers(t *testing.T) {
	st := store.NewInMemory()
	seedRDForConnections(t, st, "pvc-1")

	base, stop := startServerWithStore(t, st)
	defer stop()

	for _, body := range []string{
		`{"name":"path1","node_a_address":"10.1.1.5","node_b_address":"10.1.1.6"}`,
		`{"name":"path2","node_a_address":"10.2.2.5","node_b_address":"10.2.2.6"}`,
	} {
		resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/resource-connections/n1/n2/paths", []byte(body))

		_ = resp.Body.Close()
	}

	resp := httpDelete(t, base+"/v1/resource-definitions/pvc-1/resource-connections/n1/n2/paths/path1")

	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE status: got %d, want 204", resp.StatusCode)
	}

	getResp := httpGet(t, base+"/v1/resource-definitions/pvc-1/resource-connections/n1/n2/paths")
	defer func() { _ = getResp.Body.Close() }()

	var got []map[string]string
	if err := json.NewDecoder(getResp.Body).Decode(&got); err != nil {
		t.Fatalf("decode GET: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("paths after DELETE: got %d entries, want 1: %+v", len(got), got)
	}

	if got[0]["name"] != "path2" {
		t.Errorf("kept the wrong path: %+v", got[0])
	}
}

// TestResourceConnectionPathPostUnknownRD: POSTing to a non-existent
// RD must surface as 404, not as a silent no-op. golinstor reads the
// ApiCallRc envelope and surfaces the error to the operator.
func TestResourceConnectionPathPostUnknownRD(t *testing.T) {
	st := store.NewInMemory()

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t,
		base+"/v1/resource-definitions/ghost/resource-connections/n1/n2/paths",
		[]byte(`{"name":"path1","node_a_address":"10.1.1.5","node_b_address":"10.1.1.6"}`))
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", resp.StatusCode)
	}
}

// TestResourceConnectionPathOrderIsSymmetric: paths posted under
// (n1, n2) are retrievable under (n2, n1) with the A/B addresses
// swapped. The endpoint is logically symmetric — drbd-9 doesn't
// distinguish "first" and "second" host within a connection.
func TestResourceConnectionPathOrderIsSymmetric(t *testing.T) {
	st := store.NewInMemory()
	seedRDForConnections(t, st, "pvc-1")

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/resource-connections/n1/n2/paths",
		[]byte(`{"name":"path1","node_a_address":"10.1.1.5","node_b_address":"10.1.1.6"}`))

	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("seed POST: status %d, want 201", resp.StatusCode)
	}

	getResp := httpGet(t, base+"/v1/resource-definitions/pvc-1/resource-connections/n2/n1/paths")
	defer func() { _ = getResp.Body.Close() }()

	var got []map[string]string
	if err := json.NewDecoder(getResp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("paths: got %d, want 1: %+v", len(got), got)
	}

	// When queried with (n2, n1), n2's address must appear as A and
	// n1's as B — that's the contract operators rely on when the
	// REST surface is the source of truth for what the satellite
	// will render.
	if got[0]["node_a_address"] != "10.1.1.6" || got[0]["node_b_address"] != "10.1.1.5" {
		t.Errorf("swapped query did not swap addresses: %+v", got[0])
	}
}
