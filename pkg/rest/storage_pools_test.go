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

// /v1/view/storage-pools is the aggregate view linstor-csi calls in its
// node-registration loop. It MUST return all pools across all nodes (no
// implicit per-node filter), and clients rely on the JSON shape.
func TestViewStoragePoolsEmpty(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpGet(t, base+"/v1/view/storage-pools")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got []apiv1.StoragePool

	err := json.NewDecoder(resp.Body).Decode(&got)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if got == nil || len(got) != 0 {
		t.Errorf("body: got %v, want empty (non-nil)", got)
	}
}

// TestViewStoragePoolsAllNodes: pools from every node show up.
func TestViewStoragePoolsAllNodes(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	pools := []apiv1.StoragePool{
		{StoragePoolName: "p1", NodeName: "n1", ProviderKind: apiv1.StoragePoolKindLVMThin},
		{StoragePoolName: "p1", NodeName: "n2", ProviderKind: apiv1.StoragePoolKindLVMThin},
		{StoragePoolName: "p2", NodeName: "n2", ProviderKind: apiv1.StoragePoolKindZFSThin},
	}

	for i := range pools {
		if err := st.StoragePools().Create(ctx, &pools[i]); err != nil {
			t.Fatalf("seed[%d]: %v", i, err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/view/storage-pools")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got []apiv1.StoragePool
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(got) != 3 {
		t.Fatalf("len: got %d, want 3", len(got))
	}
}

// TestViewStoragePoolsNodeFilter pins ?nodes=N1 returning only that
// node's pools, case-insensitively. linstor-csi's NodeRegister loop
// expects the same set-membership semantics Java LINSTOR has.
func TestViewStoragePoolsNodeFilter(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	for _, sp := range []apiv1.StoragePool{
		{StoragePoolName: "p1", NodeName: "n1", ProviderKind: apiv1.StoragePoolKindLVMThin},
		{StoragePoolName: "p1", NodeName: "n2", ProviderKind: apiv1.StoragePoolKindLVMThin},
		{StoragePoolName: "p2", NodeName: "n2", ProviderKind: apiv1.StoragePoolKindZFSThin},
	} {
		if err := st.StoragePools().Create(ctx, &sp); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/view/storage-pools?nodes=N1")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got []apiv1.StoragePool
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(got) != 1 || got[0].NodeName != "n1" {
		t.Errorf("filter: got %v, want one entry on n1", got)
	}
}

// TestViewStoragePoolsPoolFilter pins ?storage_pools=p2 returning only
// pools named "p2", regardless of node.
func TestViewStoragePoolsPoolFilter(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	for _, sp := range []apiv1.StoragePool{
		{StoragePoolName: "p1", NodeName: "n1", ProviderKind: apiv1.StoragePoolKindLVMThin},
		{StoragePoolName: "p2", NodeName: "n1", ProviderKind: apiv1.StoragePoolKindZFSThin},
		{StoragePoolName: "p2", NodeName: "n2", ProviderKind: apiv1.StoragePoolKindZFSThin},
	} {
		if err := st.StoragePools().Create(ctx, &sp); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/view/storage-pools?storage_pools=p2")
	defer func() { _ = resp.Body.Close() }()

	var got []apiv1.StoragePool
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(got) != 2 {
		t.Errorf("filter: got %d entries, want 2; entries=%v", len(got), got)
	}

	for i := range got {
		if got[i].StoragePoolName != "p2" {
			t.Errorf("filter leaked: got pool %q, want p2", got[i].StoragePoolName)
		}
	}
}

// TestNodeStoragePoolsList: GET /v1/nodes/{node}/storage-pools returns only
// that node's pools.
func TestNodeStoragePoolsList(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	for _, sp := range []apiv1.StoragePool{
		{StoragePoolName: "p1", NodeName: "n1"},
		{StoragePoolName: "p2", NodeName: "n1"},
		{StoragePoolName: "p1", NodeName: "n2"},
	} {
		if err := st.StoragePools().Create(ctx, &sp); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/nodes/n1/storage-pools")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got []apiv1.StoragePool
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(got) != 2 {
		t.Errorf("len: got %d, want 2 (n1 has p1+p2)", len(got))
	}

	for _, sp := range got {
		if sp.NodeName != "n1" {
			t.Errorf("returned pool from wrong node %q", sp.NodeName)
		}
	}
}

// TestNodeStoragePoolGet: 200/404/golden body shape.
func TestNodeStoragePoolGet(t *testing.T) {
	st := store.NewInMemory()
	if err := st.StoragePools().Create(t.Context(), &apiv1.StoragePool{
		StoragePoolName: "p1",
		NodeName:        "n1",
		ProviderKind:    apiv1.StoragePoolKindFileThin,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/nodes/n1/storage-pools/p1")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got apiv1.StoragePool
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if got.StoragePoolName != "p1" || got.NodeName != "n1" {
		t.Errorf("got %s/%s, want n1/p1", got.NodeName, got.StoragePoolName)
	}

	// Missing pool → 404.
	missingResp := httpGet(t, base+"/v1/nodes/n1/storage-pools/ghost")
	_ = missingResp.Body.Close()

	if missingResp.StatusCode != http.StatusNotFound {
		t.Errorf("missing: got %d, want 404", missingResp.StatusCode)
	}
}

// TestStoragePoolsPerNodeListReturnsPools pins the per-node listing against
// the regression linstor-csi hit in Bug 24: pools were visible on
// /v1/view/storage-pools (so the controller knew about them) but
// /v1/nodes/{n}/storage-pools returned []. linstor-csi's CreateVolume
// probes each node via /v1/nodes/{n}/storage-pools, so an empty per-node
// response caused autoplace to fail with ResourceExhausted and PVCs
// stayed Pending forever. The per-node count must equal the view's
// per-node-filtered count.
func TestStoragePoolsPerNodeListReturnsPools(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	seed := []apiv1.StoragePool{
		{StoragePoolName: "pa", NodeName: "n1", ProviderKind: apiv1.StoragePoolKindLVMThin},
		{StoragePoolName: "pb", NodeName: "n1", ProviderKind: apiv1.StoragePoolKindZFSThin},
		{StoragePoolName: "pc", NodeName: "n1", ProviderKind: apiv1.StoragePoolKindFileThin},
		{StoragePoolName: "pa", NodeName: "n2", ProviderKind: apiv1.StoragePoolKindLVMThin},
		{StoragePoolName: "pd", NodeName: "n2", ProviderKind: apiv1.StoragePoolKindZFSThin},
	}

	for i := range seed {
		if err := st.StoragePools().Create(ctx, &seed[i]); err != nil {
			t.Fatalf("seed[%d]: %v", i, err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/nodes/n1/storage-pools")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got []apiv1.StoragePool
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(got) != 3 {
		t.Fatalf("len: got %d, want 3 (n1 has pa+pb+pc); entries=%v", len(got), got)
	}

	for _, sp := range got {
		if sp.NodeName != "n1" {
			t.Errorf("returned pool from wrong node %q (want n1)", sp.NodeName)
		}
	}
}

// TestStoragePoolsPerNodeListEmptyOnUnknownNode: an unknown node yields
// an empty 200 body, mirroring upstream LINSTOR (NOT 404). linstor-csi
// treats 404 on this path as a hard transport error; an empty list is
// the expected "this node has no pools" signal.
func TestStoragePoolsPerNodeListEmptyOnUnknownNode(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	// Seed a pool on a different node so the store isn't empty (rules
	// out the trivial "empty store returns empty body" case).
	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		StoragePoolName: "p1",
		NodeName:        "n1",
		ProviderKind:    apiv1.StoragePoolKindLVMThin,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/nodes/ghost/storage-pools")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200 (LINSTOR returns empty list, not 404, for unknown nodes)", resp.StatusCode)
	}

	var got []apiv1.StoragePool
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if got == nil {
		t.Errorf("body: got nil, want empty (non-nil) slice — linstor-csi rejects null JSON")
	}

	if len(got) != 0 {
		t.Errorf("body: got %v, want empty", got)
	}
}

// TestStoragePoolsPerNodeListMatchesViewFiltering pins the parity
// invariant: GET /v1/nodes/{n}/storage-pools must return exactly what
// GET /v1/view/storage-pools?nodes={n} returns. Bug 24 was a drift —
// the two endpoints went through different store methods and the
// per-node path returned [] while the view returned the full set.
// This test prevents the two from diverging again.
func TestStoragePoolsPerNodeListMatchesViewFiltering(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	for _, sp := range []apiv1.StoragePool{
		{StoragePoolName: "p1", NodeName: "n1", ProviderKind: apiv1.StoragePoolKindLVMThin},
		{StoragePoolName: "p2", NodeName: "n1", ProviderKind: apiv1.StoragePoolKindZFSThin},
		{StoragePoolName: "p1", NodeName: "n2", ProviderKind: apiv1.StoragePoolKindLVMThin},
		{StoragePoolName: "p3", NodeName: "n3", ProviderKind: apiv1.StoragePoolKindFileThin},
	} {
		if err := st.StoragePools().Create(ctx, &sp); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	for _, node := range []string{"n1", "n2", "n3", "ghost"} {
		perNodeResp := httpGet(t, base+"/v1/nodes/"+node+"/storage-pools")

		var perNode []apiv1.StoragePool

		if err := json.NewDecoder(perNodeResp.Body).Decode(&perNode); err != nil {
			_ = perNodeResp.Body.Close()
			t.Fatalf("per-node decode for %q: %v", node, err)
		}

		_ = perNodeResp.Body.Close()

		viewResp := httpGet(t, base+"/v1/view/storage-pools?nodes="+node)

		var viewFiltered []apiv1.StoragePool

		if err := json.NewDecoder(viewResp.Body).Decode(&viewFiltered); err != nil {
			_ = viewResp.Body.Close()
			t.Fatalf("view decode for %q: %v", node, err)
		}

		_ = viewResp.Body.Close()

		if len(perNode) != len(viewFiltered) {
			t.Errorf("drift on node %q: per-node len=%d, view-filtered len=%d", node, len(perNode), len(viewFiltered))

			continue
		}

		// Same elements in same order — both endpoints sort the
		// underlying slice consistently (List sorts by (node,pool)
		// and matchAnyFold preserves order).
		for i := range perNode {
			if perNode[i].NodeName != viewFiltered[i].NodeName ||
				perNode[i].StoragePoolName != viewFiltered[i].StoragePoolName {
				t.Errorf("drift on node %q index %d: per-node=%s/%s, view=%s/%s",
					node, i,
					perNode[i].NodeName, perNode[i].StoragePoolName,
					viewFiltered[i].NodeName, viewFiltered[i].StoragePoolName)
			}
		}
	}
}

// Without a Store, pool endpoints also return 503.
func TestStoragePoolEndpointsWithoutStore(t *testing.T) {
	base, stop := startServerCustom(t, &Server{Addr: pickFreeAddr(t), Store: nil})
	defer stop()

	for _, path := range []string{
		"/v1/view/storage-pools",
		"/v1/nodes/n1/storage-pools",
		"/v1/nodes/n1/storage-pools/p1",
	} {
		resp := httpGet(t, base+path)
		_ = resp.Body.Close()

		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Errorf("%s: got %d, want 503", path, resp.StatusCode)
		}
	}

	// POST /v1/nodes/{n}/storage-pools must also gate on the store —
	// without it the requireStore middleware should short-circuit
	// before the handler validates the body.
	postResp := httpPost(t, base+"/v1/nodes/n1/storage-pools",
		[]byte(`{"storage_pool_name":"p1","provider_kind":"LVM_THIN"}`))
	_ = postResp.Body.Close()

	if postResp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("POST without store: got %d, want 503", postResp.StatusCode)
	}
}

// TestPerNodeStoragePoolPostCreatesPool pins Bug 31: the satellite
// Hello/heartbeat loop registers each local pool with POST
// /v1/nodes/{node}/storage-pools. Before the handler was wired the
// route returned 405 and the satellite spun retrying forever; this
// test pins the happy path returning 201 with the upstream-shaped
// `[]ApiCallRc` envelope and the new pool landing in the store.
func TestPerNodeStoragePoolPostCreatesPool(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n1", Type: apiv1.NodeTypeSatellite}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.StoragePool{
		StoragePoolName: "p1",
		ProviderKind:    apiv1.StoragePoolKindLVMThin,
		Props:           map[string]string{"StorDriver/LvmVg": "vg1", "StorDriver/ThinPool": "thin"},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/nodes/n1/storage-pools", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201", resp.StatusCode)
	}

	var env []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}

	if len(env) == 0 {
		t.Fatalf("envelope: got empty, want at least one ApiCallRc")
	}

	got, err := st.StoragePools().Get(ctx, "n1", "p1")
	if err != nil {
		t.Fatalf("store Get after POST: %v", err)
	}

	if got.NodeName != "n1" || got.StoragePoolName != "p1" ||
		got.ProviderKind != apiv1.StoragePoolKindLVMThin {
		t.Errorf("stored pool: got %+v, want n1/p1 LVM_THIN", got)
	}

	if got.Props["StorDriver/LvmVg"] != "vg1" {
		t.Errorf("stored props lost: got %v", got.Props)
	}
}

// TestPerNodeStoragePoolPostIdempotent pins the upsert semantics: a
// satellite that re-announces the same (node, pool) every heartbeat
// must not see 409/error — the registration loop relies on
// re-POST being a no-op-success. We also assert that an updated
// ProviderKind / Props on the second POST overwrites the stored row,
// so operator-driven `linstor storage-pool create` re-runs with
// corrected config actually take effect.
func TestPerNodeStoragePoolPostIdempotent(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n1", Type: apiv1.NodeTypeSatellite}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	first, err := json.Marshal(apiv1.StoragePool{
		StoragePoolName: "p1",
		ProviderKind:    apiv1.StoragePoolKindLVMThin,
		Props:           map[string]string{"StorDriver/LvmVg": "vg1"},
	})
	if err != nil {
		t.Fatalf("marshal first: %v", err)
	}

	resp1 := httpPost(t, base+"/v1/nodes/n1/storage-pools", first)
	_ = resp1.Body.Close()

	if resp1.StatusCode != http.StatusCreated {
		t.Fatalf("first POST: got %d, want 201", resp1.StatusCode)
	}

	// Re-POST same (node, name) with a different ProviderKind/Props.
	// Must succeed, must overwrite Spec fields, must NOT 409.
	second, err := json.Marshal(apiv1.StoragePool{
		StoragePoolName: "p1",
		ProviderKind:    apiv1.StoragePoolKindZFSThin,
		Props:           map[string]string{"StorDriver/ZPool": "zp1"},
	})
	if err != nil {
		t.Fatalf("marshal second: %v", err)
	}

	resp2 := httpPost(t, base+"/v1/nodes/n1/storage-pools", second)
	defer func() { _ = resp2.Body.Close() }()

	if resp2.StatusCode == http.StatusConflict {
		t.Fatalf("idempotency broken: got 409 on re-POST")
	}

	if resp2.StatusCode != http.StatusCreated {
		t.Fatalf("second POST: got %d, want 201", resp2.StatusCode)
	}

	got, err := st.StoragePools().Get(ctx, "n1", "p1")
	if err != nil {
		t.Fatalf("store Get after second POST: %v", err)
	}

	if got.ProviderKind != apiv1.StoragePoolKindZFSThin {
		t.Errorf("provider_kind not updated: got %q, want ZFS_THIN", got.ProviderKind)
	}

	if got.Props["StorDriver/ZPool"] != "zp1" || got.Props["StorDriver/LvmVg"] != "" {
		t.Errorf("props not replaced: got %v, want only StorDriver/ZPool=zp1", got.Props)
	}
}

// TestPerNodeStoragePoolPostNodeUnknown pins that POSTing a pool on a
// node the controller doesn't know returns a clean 404 (not 500).
// Letting the store-level create succeed without a parent Node would
// leave the controller with an orphan pool the satellite can never
// own.
func TestPerNodeStoragePoolPostNodeUnknown(t *testing.T) {
	st := store.NewInMemory()

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.StoragePool{
		StoragePoolName: "p1",
		ProviderKind:    apiv1.StoragePoolKindLVMThin,
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/nodes/ghost/storage-pools", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", resp.StatusCode)
	}
}

// TestPerNodeStoragePoolPostMissingFields pins that an empty / partial
// body is a clean 400 — not a 5xx and not a silent create with zero
// fields. The validation has to fire on the REST side because the
// underlying store accepts whatever you hand it (Bug 31 follow-up).
func TestPerNodeStoragePoolPostMissingFields(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n1", Type: apiv1.NodeTypeSatellite}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	cases := []struct {
		name string
		body []byte
	}{
		{name: "empty object", body: []byte(`{}`)},
		{name: "missing provider_kind", body: []byte(`{"storage_pool_name":"p1"}`)},
		{name: "missing pool name", body: []byte(`{"provider_kind":"LVM_THIN"}`)},
		{name: "unknown provider_kind", body: []byte(`{"storage_pool_name":"p1","provider_kind":"BOGUS"}`)},
		{name: "garbage json", body: []byte(`not-json`)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := httpPost(t, base+"/v1/nodes/n1/storage-pools", tc.body)
			_ = resp.Body.Close()

			if resp.StatusCode != http.StatusBadRequest {
				t.Errorf("status: got %d, want 400", resp.StatusCode)
			}
		})
	}
}
