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

// TestAdviseRD recommends the top-N pools by free capacity. Read-
// only — no Resources are created.
func TestAdviseRD(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "rg",
		SelectFilter: apiv1.AutoSelectFilter{
			PlaceCount:  2,
			StoragePool: "pool",
		},
	}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:              "pvc-advise",
		ResourceGroupName: "rg",
	}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	for i, free := range []int64{100, 300, 200} {
		_ = st.StoragePools().Create(ctx, &apiv1.StoragePool{
			StoragePoolName: "pool",
			NodeName:        []string{"n1", "n2", "n3"}[i],
			ProviderKind:    apiv1.StoragePoolKindLVMThin,
			FreeCapacity:    free,
		})
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/resource-definitions/pvc-advise/advise")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got adviceEntry
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if got.Name != "pvc-advise" {
		t.Errorf("name: got %q, want pvc-advise", got.Name)
	}

	if len(got.Suggestions) != 2 {
		t.Fatalf("suggestions: got %d, want 2; entry=%+v", len(got.Suggestions), got)
	}

	// First suggestion is the highest free (n2 with 300), second is
	// n3 with 200. n1 (100) doesn't make the cut.
	if got.Suggestions[0].NodeName != "n2" {
		t.Errorf("first pick: got %q, want n2 (300 free)", got.Suggestions[0].NodeName)
	}

	if got.Suggestions[1].NodeName != "n3" {
		t.Errorf("second pick: got %q, want n3 (200 free)", got.Suggestions[1].NodeName)
	}

	if got.Conflict != "" {
		t.Errorf("conflict: got %q, want empty (2 pools satisfy place_count=2)", got.Conflict)
	}

	// Read-only: no Resource was created.
	resList, _ := st.Resources().ListByDefinition(ctx, "pvc-advise")
	if len(resList) != 0 {
		t.Errorf("advise must not create Resources; got %v", resList)
	}
}

// TestAdviseRDInsufficient: place_count > available pools surfaces a
// non-empty Conflict.
func TestAdviseRDInsufficient(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	_ = st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name:         "rg",
		SelectFilter: apiv1.AutoSelectFilter{PlaceCount: 3, StoragePool: "pool"},
	})
	_ = st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:              "pvc-tight",
		ResourceGroupName: "rg",
	})
	_ = st.StoragePools().Create(ctx, &apiv1.StoragePool{
		StoragePoolName: "pool",
		NodeName:        "n1",
		ProviderKind:    apiv1.StoragePoolKindLVMThin,
		FreeCapacity:    1024,
	})

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/resource-definitions/pvc-tight/advise")
	defer func() { _ = resp.Body.Close() }()

	var got adviceEntry

	_ = json.NewDecoder(resp.Body).Decode(&got)

	if got.Conflict == "" {
		t.Errorf("expected non-empty conflict for under-capacity advice; got %+v", got)
	}
}
