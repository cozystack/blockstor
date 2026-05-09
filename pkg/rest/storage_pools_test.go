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
}
