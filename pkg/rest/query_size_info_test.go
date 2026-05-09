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

// TestQuerySizeInfo: max_vlm_size_in_kib equals the FreeCapacity of
// the n-th-largest pool (where n = place_count). Two replicas across
// pools with 100/200/300 KiB free → max volume 200 KiB (the smaller
// of the two we'd pick).
func TestQuerySizeInfo(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "rg-cap",
		SelectFilter: apiv1.AutoSelectFilter{
			PlaceCount:  2,
			StoragePool: "pool",
		},
	}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	for i, free := range []int64{100, 200, 300} {
		nodeName := []string{"n1", "n2", "n3"}[i]
		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			StoragePoolName: "pool",
			NodeName:        nodeName,
			ProviderKind:    apiv1.StoragePoolKindLVMThin,
			FreeCapacity:    free,
			TotalCapacity:   free * 2,
		}); err != nil {
			t.Fatalf("seed pool: %v", err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/resource-groups/rg-cap/query-size-info", []byte(`{}`))
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got querySizeInfoResponse
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if got.SpaceInfo.MaxVlmSizeInKib != 200 {
		t.Errorf("max vol: got %d KiB, want 200 (2nd largest free across 3 pools)",
			got.SpaceInfo.MaxVlmSizeInKib)
	}

	if got.SpaceInfo.CapacityInKib != 1200 {
		t.Errorf("capacity: got %d, want 1200 (sum of 200+400+600)", got.SpaceInfo.CapacityInKib)
	}

	if got.SpaceInfo.AvailableSizeInKib != 600 {
		t.Errorf("available: got %d, want 600 (sum of free)", got.SpaceInfo.AvailableSizeInKib)
	}
}

// TestQuerySizeInfo_Insufficient: place_count exceeds candidate
// pools → max_vlm_size_in_kib is 0. golinstor uses this signal to
// fail the resource-create pre-flight cleanly.
func TestQuerySizeInfo_Insufficient(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	_ = st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "rg-tight",
		SelectFilter: apiv1.AutoSelectFilter{
			PlaceCount:  3,
			StoragePool: "pool",
		},
	})

	_ = st.StoragePools().Create(ctx, &apiv1.StoragePool{
		StoragePoolName: "pool",
		NodeName:        "n1",
		ProviderKind:    apiv1.StoragePoolKindLVMThin,
		FreeCapacity:    1024,
	})

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/resource-groups/rg-tight/query-size-info", []byte(`{}`))
	defer func() { _ = resp.Body.Close() }()

	var got querySizeInfoResponse

	_ = json.NewDecoder(resp.Body).Decode(&got)

	if got.SpaceInfo.MaxVlmSizeInKib != 0 {
		t.Errorf("max vol: got %d, want 0 (place_count=3 > 1 pool)",
			got.SpaceInfo.MaxVlmSizeInKib)
	}
}

// TestQueryAllSizeInfo answers per-RG capacity for every RG in one
// response.
func TestQueryAllSizeInfo(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	_ = st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name:         "rg-1",
		SelectFilter: apiv1.AutoSelectFilter{PlaceCount: 1, StoragePool: "pool"},
	})
	_ = st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name:         "rg-2",
		SelectFilter: apiv1.AutoSelectFilter{PlaceCount: 2, StoragePool: "pool"},
	})

	for _, n := range []string{"n1", "n2"} {
		_ = st.StoragePools().Create(ctx, &apiv1.StoragePool{
			StoragePoolName: "pool",
			NodeName:        n,
			ProviderKind:    apiv1.StoragePoolKindLVMThin,
			FreeCapacity:    1024,
		})
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/query-all-size-info", []byte(`{}`))
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got queryAllSizeInfoResponse
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(got.Result) != 2 {
		t.Errorf("RG count: got %d, want 2", len(got.Result))
	}

	if got.Result["rg-1"].SpaceInfo.MaxVlmSizeInKib != 1024 {
		t.Errorf("rg-1 max: got %d, want 1024", got.Result["rg-1"].SpaceInfo.MaxVlmSizeInKib)
	}

	if got.Result["rg-2"].SpaceInfo.MaxVlmSizeInKib != 1024 {
		t.Errorf("rg-2 max: got %d, want 1024 (both pools fit)", got.Result["rg-2"].SpaceInfo.MaxVlmSizeInKib)
	}
}
