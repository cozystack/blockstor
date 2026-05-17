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

// Bug 229 (P3) — `GET /v1/storage-pool-definitions/{name}` single-
// item lookup was missing; only the list endpoint was wired. Upstream
// Java `controller/.../StoragePoolDefinitions.java` overloads the
// same handler for both list and per-name lookup — clients hit the
// single-item form to verify a definition exists without paging the
// full list. Pre-fix any per-name fetch 404s.

// TestBug229SPDefinitionSingleGetByName: with two SPDs derived from
// the StoragePool table, GET .../storage-pool-definitions/main must
// return ONLY the "main" entry, filtered by name.
func TestBug229SPDefinitionSingleGetByName(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	pools := []apiv1.StoragePool{
		{StoragePoolName: "main", NodeName: "n1", ProviderKind: apiv1.StoragePoolKindLVM},
		{StoragePoolName: "alt", NodeName: "n2", ProviderKind: apiv1.StoragePoolKindZFS},
	}
	for i := range pools {
		if err := st.StoragePools().Create(ctx, &pools[i]); err != nil {
			t.Fatalf("seed pool %s: %v", pools[i].StoragePoolName, err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/storage-pool-definitions/main")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	// Upstream returns the same list-shaped envelope as the
	// collection endpoint, filtered to the requested name (matches
	// the Java handler's `listStoragePoolDefinitions(storagePoolName)`
	// shape). The python CLI iterates over the slice.
	var got []struct {
		StoragePoolName string `json:"storage_pool_name"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode body: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("filtered SPDs: got %d, want 1", len(got))
	}

	if got[0].StoragePoolName != "main" {
		t.Errorf("filtered SPD name: got %q, want %q", got[0].StoragePoolName, "main")
	}
}

// TestBug229SPDefinitionSingleUnknown: requesting an unknown SPD
// name must 404 — the python CLI surfaces this as "no such
// storage-pool-definition" instead of an empty list confusion.
func TestBug229SPDefinitionSingleUnknown(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		StoragePoolName: "main", NodeName: "n1", ProviderKind: apiv1.StoragePoolKindLVM,
	}); err != nil {
		t.Fatalf("seed pool: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/storage-pool-definitions/ghost")
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", resp.StatusCode)
	}
}
