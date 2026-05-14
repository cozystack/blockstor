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
	"maps"
	"net/http"
	"strings"
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

// TestPoolCreateRejectsNonCanonicalName pins that a POST body whose
// storage_pool_name carries a '.' (which would shift the
// `<pool>.<node>` boundary the CRD's CEL rule enforces) returns a
// 400 with the convention message — not a 5xx or a silent create
// that the apiserver would later reject with a hard-to-trace 422.
//
// The wire body has no `metadata.name` field (the REST server sets
// the CRD name via `crdName(node, pool)`), so the only failure mode
// is a pool name that breaks the canonical encoding. We test that
// path explicitly so a future convention drift doesn't go unnoticed.
func TestPoolCreateRejectsNonCanonicalName(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n1", Type: apiv1.NodeTypeSatellite}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.StoragePool{
		StoragePoolName: "thin.evil", // contains '.', would corrupt <pool>.<node>
		ProviderKind:    apiv1.StoragePoolKindLVMThin,
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/nodes/n1/storage-pools", body)

	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status: got %d, want 400", resp.StatusCode)
	}

	bodyBuf := make([]byte, 1<<10)

	n, _ := resp.Body.Read(bodyBuf)
	if !strings.Contains(string(bodyBuf[:n]), "metadata.name must equal") {
		t.Errorf("body: got %q, want substring \"metadata.name must equal\"", string(bodyBuf[:n]))
	}
}

// TestSPListIncludesFreeSpaceMgrName pins Bug 59 / CLI parity audit
// row #3: `linstor sp l` SharedName column reads from
// `free_space_mgr_name`. Upstream LINSTOR sets it to `<node>:<pool>`
// for local pools and to the shared-space identifier for shared ones,
// and the Python CLI's `':' not in free_space_mgr_name` check crashes
// with TypeError when the field is null. Both store backends MUST
// surface the field on read.
func TestSPListIncludesFreeSpaceMgrName(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	// Local pool (no shared space) → expect `<node>:<pool>`.
	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		StoragePoolName: "zfs-thin",
		NodeName:        "dev-kvaps-worker-1",
		ProviderKind:    apiv1.StoragePoolKindZFSThin,
	}); err != nil {
		t.Fatalf("seed local: %v", err)
	}

	// Shared pool → expect the shared-space identifier verbatim.
	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		StoragePoolName: "san-pool",
		NodeName:        "dev-kvaps-worker-2",
		ProviderKind:    apiv1.StoragePoolKindLVM,
		SharedSpaceID:   "shared-san-A",
	}); err != nil {
		t.Fatalf("seed shared: %v", err)
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

	want := map[string]string{
		"dev-kvaps-worker-1:zfs-thin": "dev-kvaps-worker-1:zfs-thin",
		"dev-kvaps-worker-2:san-pool": "shared-san-A",
	}

	for i := range got {
		key := got[i].NodeName + ":" + got[i].StoragePoolName

		expect, ok := want[key]
		if !ok {
			continue
		}

		if got[i].FreeSpaceMgrName != expect {
			t.Errorf("FreeSpaceMgrName for %s: got %q, want %q",
				key, got[i].FreeSpaceMgrName, expect)
		}

		delete(want, key)
	}

	if len(want) != 0 {
		t.Errorf("missing pools in response: %v", want)
	}
}

// TestPoolCreateProducesCanonicalCRDName pins that a normal POST
// stores a pool the (node, pool) key resolves and the round-trip
// preserves both halves of the canonical name. The InMemory store
// keys on the wire tuple directly, so this test would still pass
// against a regression in the k8s store's `crdName`; the CEL test
// in `pkg/store/k8s` covers that path. Together they pin both
// sides of the encoding.
func TestPoolCreateProducesCanonicalCRDName(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "w1", Type: apiv1.NodeTypeSatellite}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.StoragePool{
		StoragePoolName: "zfs-thin",
		ProviderKind:    apiv1.StoragePoolKindZFSThin,
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/nodes/w1/storage-pools", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201", resp.StatusCode)
	}

	got, err := st.StoragePools().Get(ctx, "w1", "zfs-thin")
	if err != nil {
		t.Fatalf("store Get: %v", err)
	}

	// Round-trip on (node, pool) — the k8s store maps this to
	// `metadata.name = <pool>.<node>` via `crdName`. The InMemory
	// store keys on the wire tuple directly; either way both fields
	// must come back populated for the canonical name to be derivable
	// downstream.
	if got.NodeName != "w1" || got.StoragePoolName != "zfs-thin" {
		t.Errorf("round-trip: got (node=%q, pool=%q), want (w1, zfs-thin)",
			got.NodeName, got.StoragePoolName)
	}
}

// postPoolForExpandTest is a small helper that wires up a node, posts
// the given pool body, and returns the stored row. Bug 63's expand-alias
// tests all follow the same shape — extracted to keep each test focused
// on its props-table expectation rather than HTTP plumbing.
func postPoolForExpandTest(t *testing.T, body apiv1.StoragePool) apiv1.StoragePool {
	t.Helper()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n1", Type: apiv1.NodeTypeSatellite}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/nodes/n1/storage-pools", raw)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201", resp.StatusCode)
	}

	got, err := st.StoragePools().Get(ctx, "n1", body.StoragePoolName)
	if err != nil {
		t.Fatalf("store Get: %v", err)
	}

	return got
}

// TestSPCreateExpandsLVMThinStorPoolName pins Bug 63 for LVM_THIN: the
// linstor-client CLI's `--pool-name <vg>/<thin>` payload arrives as
// `StorDriver/StorPoolName="vg/thin"` with no kind-specific keys. The
// REST handler must split the alias into `StorDriver/LvmVg=vg` plus
// `StorDriver/ThinPool=thin` so the satellite's NewProviderFromKind
// can register the pool; the original StorPoolName is retained for
// upstream-CLI display parity.
func TestSPCreateExpandsLVMThinStorPoolName(t *testing.T) {
	got := postPoolForExpandTest(t, apiv1.StoragePool{
		StoragePoolName: "p1",
		ProviderKind:    apiv1.StoragePoolKindLVMThin,
		Props:           map[string]string{"StorDriver/StorPoolName": "vg/thin"},
	})

	if got.Props["StorDriver/LvmVg"] != "vg" {
		t.Errorf("LvmVg: got %q, want %q (props=%v)", got.Props["StorDriver/LvmVg"], "vg", got.Props)
	}

	if got.Props["StorDriver/ThinPool"] != "thin" {
		t.Errorf("ThinPool: got %q, want %q (props=%v)", got.Props["StorDriver/ThinPool"], "thin", got.Props)
	}

	if got.Props["StorDriver/StorPoolName"] != "vg/thin" {
		t.Errorf("StorPoolName retained: got %q, want %q", got.Props["StorDriver/StorPoolName"], "vg/thin")
	}
}

// TestSPCreateExpandsZFSThinStorPoolName: same Bug 63 path for ZFS_THIN.
// The CLI emits `--pool-name <zpool>`; the REST handler must mirror it
// into `StorDriver/ZPoolThin` (NOT `StorDriver/ZPool`, which is the
// thick-only key the satellite's newZFS reads for non-thin pools).
func TestSPCreateExpandsZFSThinStorPoolName(t *testing.T) {
	got := postPoolForExpandTest(t, apiv1.StoragePool{
		StoragePoolName: "p1",
		ProviderKind:    apiv1.StoragePoolKindZFSThin,
		Props:           map[string]string{"StorDriver/StorPoolName": "blockstor-zfs"},
	})

	if got.Props["StorDriver/ZPoolThin"] != "blockstor-zfs" {
		t.Errorf("ZPoolThin: got %q, want %q (props=%v)",
			got.Props["StorDriver/ZPoolThin"], "blockstor-zfs", got.Props)
	}

	if got.Props["StorDriver/ZPool"] != "" {
		t.Errorf("ZPool must stay empty for ZFS_THIN: got %q", got.Props["StorDriver/ZPool"])
	}

	if got.Props["StorDriver/StorPoolName"] != "blockstor-zfs" {
		t.Errorf("StorPoolName retained: got %q, want %q",
			got.Props["StorDriver/StorPoolName"], "blockstor-zfs")
	}
}

// TestSPCreateExpandsFileThinStorPoolName: FILE_THIN's kind-specific
// key is `StorDriver/FileDir` (same as FILE — thinness only changes
// allocation policy, not the on-disk layout). Bug 63 expansion must
// populate FileDir so the satellite's newFile can resolve the
// backing directory.
func TestSPCreateExpandsFileThinStorPoolName(t *testing.T) {
	got := postPoolForExpandTest(t, apiv1.StoragePool{
		StoragePoolName: "p1",
		ProviderKind:    apiv1.StoragePoolKindFileThin,
		Props:           map[string]string{"StorDriver/StorPoolName": "/var/lib/blockstor/file1"},
	})

	if got.Props["StorDriver/FileDir"] != "/var/lib/blockstor/file1" {
		t.Errorf("FileDir: got %q, want %q (props=%v)",
			got.Props["StorDriver/FileDir"], "/var/lib/blockstor/file1", got.Props)
	}

	if got.Props["StorDriver/StorPoolName"] != "/var/lib/blockstor/file1" {
		t.Errorf("StorPoolName retained: got %q, want %q",
			got.Props["StorDriver/StorPoolName"], "/var/lib/blockstor/file1")
	}
}

// TestSPCreateExplicitKeyTakesPrecedence pins the "explicit > implicit"
// precedence Bug 63's normalization promises: when both StorPoolName
// and the kind-specific key are supplied, the explicit kind-specific
// value wins and the alias does NOT overwrite it. This matters for
// operator-managed pools that pre-fill `LvmVg` and tack on a stale
// StorPoolName from an older CLI invocation — the alias must not
// silently rewrite the volume group.
func TestSPCreateExplicitKeyTakesPrecedence(t *testing.T) {
	got := postPoolForExpandTest(t, apiv1.StoragePool{
		StoragePoolName: "p1",
		ProviderKind:    apiv1.StoragePoolKindLVM,
		Props: map[string]string{
			"StorDriver/StorPoolName": "alias-vg",
			"StorDriver/LvmVg":        "explicit-vg",
		},
	})

	if got.Props["StorDriver/LvmVg"] != "explicit-vg" {
		t.Errorf("LvmVg: got %q, want %q (alias overwrote explicit value)",
			got.Props["StorDriver/LvmVg"], "explicit-vg")
	}

	if got.Props["StorDriver/StorPoolName"] != "alias-vg" {
		t.Errorf("StorPoolName retained: got %q, want %q",
			got.Props["StorDriver/StorPoolName"], "alias-vg")
	}
}

// TestSPCreateNoNormalizationNeeded pins the pass-through path: a
// payload that already carries the kind-specific key and no alias
// must not gain spurious props from the expansion logic. Piraeus
// emits this shape on every heartbeat; a noisy diff here would
// trigger the controller's Update path on every reconcile.
func TestSPCreateNoNormalizationNeeded(t *testing.T) {
	got := postPoolForExpandTest(t, apiv1.StoragePool{
		StoragePoolName: "p1",
		ProviderKind:    apiv1.StoragePoolKindZFS,
		Props:           map[string]string{"StorDriver/ZPool": "tank"},
	})

	if got.Props["StorDriver/ZPool"] != "tank" {
		t.Errorf("ZPool: got %q, want %q", got.Props["StorDriver/ZPool"], "tank")
	}

	// Spurious keys would indicate the expansion logic wrote to a
	// kind-specific slot it shouldn't touch when no alias is set.
	for _, k := range []string{
		"StorDriver/StorPoolName",
		"StorDriver/LvmVg",
		"StorDriver/ThinPool",
		"StorDriver/ZPoolThin",
		"StorDriver/FileDir",
	} {
		if got.Props[k] != "" {
			t.Errorf("unexpected key %q populated: got %q", k, got.Props[k])
		}
	}
}

// TestSPCreateDisklessNoProps pins scenario 6.W05: `sp create diskless
// <node> <pool>` must succeed without any `StorDriver/*` props. DISKLESS
// pools own no underlying storage — they exist purely as allocator
// targets for the autoplacer's `DisklessOnRemaining` path and the
// `AutoAddQuorumTiebreaker` reconciler. The contract this test pins:
//
//  1. POST /v1/nodes/{node}/storage-pools with
//     `{"storage_pool_name":"<p>","provider_kind":"DISKLESS"}` and NO
//     Props returns 201 — the kind validator must accept DISKLESS, and
//     the expand-StorPoolName-alias normaliser must not trip on the
//     missing-Props bag (Bug 63 path is a no-op for DISKLESS by design).
//  2. The stored CRD has `FreeCapacity == 0` and `TotalCapacity == 0`.
//     DISKLESS pools report zero capacity to the placer (the in-memory
//     replica consumes no local space); a non-zero default would skew
//     the MaxFreeSpace scoring. The fields are int64 with omitempty so
//     "absent in body" must round-trip as zero, not as "preserve previous".
//  3. The pool surfaces on the aggregate /v1/view/storage-pools call —
//     that's the path linstor-csi and the autoplacer use to enumerate
//     diskless targets for the DisklessOnRemaining path. End-to-end the
//     satellite's NewProviderFromKind returns (nil, nil) for DISKLESS
//     and the StoragePoolReconciler's `RegisterProvider(nil)` path is
//     a no-op deregister — a regression that required Props would fail
//     this REST-level test first.
//
// Cross-listed with wave1 6.5 (auto-creation at Node Hello time): the
// existing `TestNodeCreateAutoCreatesDfltDisklessStorPool` pins the
// auto-create variant; this test pins the explicit-create path the
// operator uses to add extra diskless pools beyond `DfltDisklessStorPool`.
func TestSPCreateDisklessNoProps(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n1", Type: apiv1.NodeTypeSatellite}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// Wire body: no Props, no FreeCapacity, no TotalCapacity. The
	// CLI's `linstor sp c diskless <node> <pool>` emits exactly this
	// — DISKLESS has no backing-storage knob to fill in.
	body, err := json.Marshal(apiv1.StoragePool{
		StoragePoolName: "diskless-extra",
		ProviderKind:    apiv1.StoragePoolKindDiskless,
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/nodes/n1/storage-pools", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201 (DISKLESS must not require Props)", resp.StatusCode)
	}

	got, err := st.StoragePools().Get(ctx, "n1", "diskless-extra")
	if err != nil {
		t.Fatalf("store Get after POST: %v", err)
	}

	if got.ProviderKind != apiv1.StoragePoolKindDiskless {
		t.Errorf("ProviderKind: got %q, want %q",
			got.ProviderKind, apiv1.StoragePoolKindDiskless)
	}

	if got.FreeCapacity != 0 {
		t.Errorf("FreeCapacity: got %d, want 0 (DISKLESS has no backing storage)", got.FreeCapacity)
	}

	if got.TotalCapacity != 0 {
		t.Errorf("TotalCapacity: got %d, want 0 (DISKLESS has no backing storage)", got.TotalCapacity)
	}

	// Props bag must remain absent / empty — the expand-alias path
	// must not synthesise spurious keys when no StorPoolName alias is
	// present and the kind has no canonical prop key.
	for k, v := range got.Props {
		t.Errorf("DISKLESS pool gained spurious prop %q=%q (Props must stay empty)", k, v)
	}

	// And the pool must surface on the aggregate view — that's the
	// call linstor-csi and the autoplacer make to enumerate diskless
	// targets for the DisklessOnRemaining path.
	viewResp := httpGet(t, base+"/v1/view/storage-pools?nodes=n1")
	defer func() { _ = viewResp.Body.Close() }()

	if viewResp.StatusCode != http.StatusOK {
		t.Fatalf("view status: got %d, want 200", viewResp.StatusCode)
	}

	var pools []apiv1.StoragePool
	if err := json.NewDecoder(viewResp.Body).Decode(&pools); err != nil {
		t.Fatalf("decode view: %v", err)
	}

	var found bool

	for i := range pools {
		if pools[i].NodeName == "n1" &&
			pools[i].StoragePoolName == "diskless-extra" &&
			pools[i].ProviderKind == apiv1.StoragePoolKindDiskless {
			found = true

			break
		}
	}

	if !found {
		t.Errorf("created DISKLESS pool not visible in /v1/view/storage-pools; got pools=%+v", pools)
	}
}

// TestSPCreateDisklessIgnoresStorPoolNameAlias pins that the Bug 63
// expand-StorPoolName-alias normaliser is a no-op for DISKLESS. A
// caller that copy-pastes `StorDriver/StorPoolName=foo` from an LVM
// example onto a DISKLESS create must NOT gain spurious kind-specific
// keys (LvmVg, ZPool, FileDir, …). Without the no-op, a misconfigured
// alias would land in the props bag and trip the satellite's
// per-kind config validator the next time the operator switched the
// kind to LVM with a real VG.
func TestSPCreateDisklessIgnoresStorPoolNameAlias(t *testing.T) {
	got := postPoolForExpandTest(t, apiv1.StoragePool{
		StoragePoolName: "p1",
		ProviderKind:    apiv1.StoragePoolKindDiskless,
		Props:           map[string]string{"StorDriver/StorPoolName": "stray-alias"},
	})

	// StorPoolName is retained for CLI-display parity (matches the
	// "explicit alias retained" semantics of the other expand tests).
	if got.Props["StorDriver/StorPoolName"] != "stray-alias" {
		t.Errorf("StorPoolName retained: got %q, want %q",
			got.Props["StorDriver/StorPoolName"], "stray-alias")
	}

	// But no kind-specific keys may be synthesised — DISKLESS has
	// no backing-storage knob and the satellite's NewProviderFromKind
	// short-circuits to (nil, nil) without reading Props at all.
	for _, k := range []string{
		"StorDriver/LvmVg",
		"StorDriver/ThinPool",
		"StorDriver/ZPool",
		"StorDriver/ZPoolThin",
		"StorDriver/FileDir",
	} {
		if got.Props[k] != "" {
			t.Errorf("DISKLESS gained spurious kind-specific prop %q=%q", k, got.Props[k])
		}
	}
}

// TestSPDeleteUnknownUsesWarnMaskNotInfo is the Bug 66 alignment
// guard for `DELETE /v1/nodes/{node}/storage-pools/{pool}`. The
// handler pre-existed Bug 66 (Bug 52, 93d104163) and already folded
// NotFound into a 200, but tagged it with maskInfo — making the
// "already absent" reply indistinguishable from a real drop in audit
// logs. Bug 66 promotes the no-op path into the warn band so the
// envelope shape matches every other delete handler in this package.
//
// A regression that reverted to maskInfo (silent) would still pass
// existing 200-status assertions, so this test pins the WARN bit
// itself.
func TestSPDeleteUnknownUsesWarnMaskNotInfo(t *testing.T) {
	t.Parallel()

	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpDelete(t, base+"/v1/nodes/ghost-node/storage-pools/ghost-pool")
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
		t.Errorf("ret_code: got %#x, want WARN bit (%#x) set on no-op replay", rc[0].RetCode, maskWarn)
	}

	if !strings.Contains(rc[0].Message, "already absent") {
		t.Errorf("message: got %q, want 'already absent' marker", rc[0].Message)
	}

	if !strings.Contains(rc[0].Message, "ghost-pool") || !strings.Contains(rc[0].Message, "ghost-node") {
		t.Errorf("message: got %q, want it to name both ghost-pool and ghost-node", rc[0].Message)
	}
}

// TestSPDeleteRefusesWhenReferencedByResource pins scenario 6.W06
// (cross-listed with Bug 52): `linstor sp d <node> <pool>` MUST
// refuse with 409 + FAIL_IN_USE when at least one Resource replica
// on `(node, pool)` still references the pool via a Volume whose
// `StoragePool` matches. The pool CRD stays in the store; the
// operator drops the referencing replicas first, then retries.
//
// Without this pin a regression that turned the delete into a
// cascade (drop referencing replicas under the hood) would silently
// throw away on-disk data on every `sp d` — upstream LINSTOR refuses
// for the same reason in `CtrlStorPoolApiCallHandler` and we must
// match.
func TestSPDeleteRefusesWhenReferencedByResource(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n1", Type: apiv1.NodeTypeSatellite}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		NodeName:        "n1",
		StoragePoolName: "p1",
		ProviderKind:    apiv1.StoragePoolKindLVMThin,
		Props:           map[string]string{"StorDriver/LvmVg": "vg1", "StorDriver/ThinPool": "thin"},
	}); err != nil {
		t.Fatalf("seed pool: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-ref"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	// Replica with a Volume bound to (n1, p1) — the satellite has
	// already reported the per-volume observation, so the wire-side
	// view of the Resource carries an unambiguous reference to the
	// pool we're trying to drop.
	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name:     "pvc-ref",
		NodeName: "n1",
		Volumes: []apiv1.Volume{
			{VolumeNumber: 0, StoragePool: "p1"},
		},
	}); err != nil {
		t.Fatalf("seed replica: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t, base+"/v1/nodes/n1/storage-pools/p1")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status: got %d, want 409 (still-referenced refusal)", resp.StatusCode)
	}

	var rc []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rc); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}

	if len(rc) == 0 {
		t.Fatalf("envelope: got empty, want one entry")
	}

	// FAIL_IN_USE sub-code 997 OR'd with MASK_ERROR (high bit set).
	// Python CLI matches on the sub-code; the MASK_ERROR makes the
	// CLI surface the line as an error rather than informational.
	if rc[0].RetCode&0xFFFF != apiCallRcFailInUse {
		t.Errorf("ret_code sub-code: got %#x, want FAIL_IN_USE (%d)",
			rc[0].RetCode&0xFFFF, apiCallRcFailInUse)
	}

	if rc[0].RetCode >= 0 {
		t.Errorf("ret_code: got %#x, want MASK_ERROR (negative) bit set", rc[0].RetCode)
	}

	// Operator-facing message must name both the pool and the node
	// so `linstor sp d` exit-error tells the operator exactly which
	// pair is wedged. The referencing replica name belongs in
	// Details (matches upstream's wire shape — Message stays terse).
	if !strings.Contains(rc[0].Message, "p1") || !strings.Contains(rc[0].Message, "n1") {
		t.Errorf("message: got %q, want it to name pool 'p1' and node 'n1'", rc[0].Message)
	}

	if !strings.Contains(rc[0].Details, "pvc-ref") {
		t.Errorf("details: got %q, want it to name the referencing replica 'pvc-ref'", rc[0].Details)
	}

	// CRITICAL: the pool CRD must still exist — a refused delete
	// that nevertheless dropped the pool would leave the cluster
	// in a half-deleted state (CRD gone, on-disk VG still there,
	// replica's StoragePool reference dangling). Without this
	// assertion a regression that swapped the order of the
	// refusal-check and the store Delete would silently pass.
	if _, err := st.StoragePools().Get(ctx, "n1", "p1"); err != nil {
		t.Errorf("pool removed despite 409 refusal: %v", err)
	}
}

// TestSPDeleteIgnoresUnobservedReplica pins the tri-state semantic
// on the refusal check: a Resource replica that has NOT yet had its
// Volumes populated by the satellite observer MUST NOT count as
// "using" the pool. Without this carve-out a fresh `n d`-evacuated
// node could never have its empty pool dropped — the controller's
// view of the replicas would carry the old `(node, pool)` pair
// until the satellite reconciler caught up, and `sp d` would
// permanently 409.
//
// Matches upstream LINSTOR which iterates per-volume provider
// objects (a replica with no provider object yet is invisible to
// the refusal walk).
func TestSPDeleteIgnoresUnobservedReplica(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n1", Type: apiv1.NodeTypeSatellite}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		NodeName:        "n1",
		StoragePoolName: "p1",
		ProviderKind:    apiv1.StoragePoolKindLVMThin,
		Props:           map[string]string{"StorDriver/LvmVg": "vg1", "StorDriver/ThinPool": "thin"},
	}); err != nil {
		t.Fatalf("seed pool: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-unobs"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	// Replica without Volumes — satellite hasn't reported yet,
	// the controller can't prove the pool is referenced.
	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name:     "pvc-unobs",
		NodeName: "n1",
	}); err != nil {
		t.Fatalf("seed replica: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t, base+"/v1/nodes/n1/storage-pools/p1")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200 (unobserved replica ignored)", resp.StatusCode)
	}

	if _, err := st.StoragePools().Get(ctx, "n1", "p1"); err == nil {
		t.Errorf("pool kept despite no observed reference")
	}
}

// TestSPDeleteScopedToNode pins the per-node scope of the refusal:
// a Resource replica that references the same pool name on a
// DIFFERENT node MUST NOT block the delete on (node, pool). Pool
// names are not globally unique in upstream LINSTOR — `(node, pool)`
// is the composite key, and operators routinely re-use the same
// pool name across nodes (`DfltStorPool` is the most common case).
// Without the per-node carve-out, dropping a single node's
// `DfltStorPool` would refuse forever as long as ANY other node had
// a replica bound to its own `DfltStorPool`.
func TestSPDeleteScopedToNode(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	for _, n := range []string{"n1", "n2"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{Name: n, Type: apiv1.NodeTypeSatellite}); err != nil {
			t.Fatalf("seed node %s: %v", n, err)
		}

		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			NodeName:        n,
			StoragePoolName: "p1",
			ProviderKind:    apiv1.StoragePoolKindLVMThin,
			Props:           map[string]string{"StorDriver/LvmVg": "vg1", "StorDriver/ThinPool": "thin"},
		}); err != nil {
			t.Fatalf("seed pool on %s: %v", n, err)
		}
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-other"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	// Replica references (n2, p1) — must NOT block delete of (n1, p1).
	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name:     "pvc-other",
		NodeName: "n2",
		Volumes: []apiv1.Volume{
			{VolumeNumber: 0, StoragePool: "p1"},
		},
	}); err != nil {
		t.Fatalf("seed replica: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t, base+"/v1/nodes/n1/storage-pools/p1")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200 (other-node reference ignored)", resp.StatusCode)
	}

	if _, err := st.StoragePools().Get(ctx, "n1", "p1"); err == nil {
		t.Errorf("(n1, p1) kept despite no on-node reference")
	}

	// (n2, p1) must still exist — we only deleted (n1, p1).
	if _, err := st.StoragePools().Get(ctx, "n2", "p1"); err != nil {
		t.Errorf("(n2, p1) removed by (n1, p1) delete: %v", err)
	}
}

// TestStoragePoolListPropertiesRoundTripAllNamespaces pins scenario
// 1.W01 (P0, unit) for the StoragePool scope: `linstor storage-pool
// list-properties` reads the `props` field of `GET
// /v1/nodes/{node}/storage-pools/{pool}`. Every LINSTOR-known
// namespace (`DrbdOptions/`, `Aux/`, `FileSystem/`, `StorDriver/`)
// must round-trip verbatim — Bug 63's StorPoolName alias expansion
// is the closest we get to normalisation, and it only adds keys, it
// must not rewrite the namespaces themselves.
func TestStoragePoolListPropertiesRoundTripAllNamespaces(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n1", Type: apiv1.NodeTypeSatellite}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	seed := map[string]string{
		"StorDriver/LvmVg":           "vg1",
		"StorDriver/ThinPool":        "thin",
		"DrbdOptions/AutoVerifyAlgo": "crc32c",
		"Aux/cozystack.io/pool-tier": "premium",
		"FileSystem/MkfsOptions":     "-K -E lazy_itable_init=0",
	}

	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		NodeName:        "n1",
		StoragePoolName: "p1",
		ProviderKind:    apiv1.StoragePoolKindLVMThin,
		Props:           maps.Clone(seed),
	}); err != nil {
		t.Fatalf("seed pool: %v", err)
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

	if got.Props == nil {
		t.Fatalf("Props: got nil, want a {Key,Value} map")
	}

	for k, want := range seed {
		if got.Props[k] != want {
			t.Errorf("Props[%q]: got %q, want %q (namespace round-trip drift)", k, got.Props[k], want)
		}
	}
}

// TestStoragePoolListPropertiesUnknownPoolReturns404 pins the
// unknown-scope half of scenario 1.W01 for storage pools: an absent
// (node, pool) pair must 404, not return an empty-Props 200 — that
// would hide an operator typo.
func TestStoragePoolListPropertiesUnknownPoolReturns404(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpGet(t, base+"/v1/nodes/ghost-node/storage-pools/ghost-pool")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", resp.StatusCode)
	}
}

// TestStoragePoolListPropertiesEmptyDecodes pins the "empty scope
// returns empty map (not nil)" clause of scenario 1.W01 for the
// StoragePool scope: an SP seeded with no Props must still decode
// into a usable `props` field — golinstor's
// `linstor sp list-properties` indexes into the map directly, so
// nil would panic the CLI with a "nil map dereference" on the
// first `--show-props` access.
func TestStoragePoolListPropertiesEmptyDecodes(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n1", Type: apiv1.NodeTypeSatellite}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		NodeName:        "n1",
		StoragePoolName: "p-empty",
		ProviderKind:    apiv1.StoragePoolKindDiskless,
	}); err != nil {
		t.Fatalf("seed pool: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/nodes/n1/storage-pools/p-empty")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got apiv1.StoragePool
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	// `props,omitempty` legitimately omits the field on the wire when
	// the seed has nothing in the bag, so Props may decode to nil.
	// The scenario's "empty map (not nil)" contract is satisfied at
	// the LINSTOR-CLI level by ranging over a (potentially nil) map
	// — that is the no-panic check we pin here so a future refactor
	// that swaps the map for a pointer-typed wrapper trips the test.
	for k, v := range got.Props {
		t.Errorf("Props: unexpected entry %q=%q on an empty seed", k, v)
	}
}
