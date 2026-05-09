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

// TestAutoplacePicksRequestedCount: with 3 candidate pools and place_count=2,
// exactly 2 Resources land.
func TestAutoplacePicksRequestedCount(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	for _, n := range []string{"n1", "n2", "n3"} {
		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			StoragePoolName: "pool",
			NodeName:        n,
			ProviderKind:    apiv1.StoragePoolKindLVMThin,
		}); err != nil {
			t.Fatalf("seed pool %s: %v", n, err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.AutoPlaceRequest{
		SelectFilter: apiv1.AutoSelectFilter{PlaceCount: 2, StoragePool: "pool"},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/autoplace", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.Resources().ListByDefinition(ctx, "pvc-1")
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	if len(got) != 2 {
		t.Errorf("placed: got %d, want 2", len(got))
	}
}

// TestAutoplaceConflictWhenInsufficient: place_count=3 but only 1 pool — 409.
func TestAutoplaceConflictWhenInsufficient(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		StoragePoolName: "pool",
		NodeName:        "n1",
		ProviderKind:    apiv1.StoragePoolKindLVMThin,
	}); err != nil {
		t.Fatalf("seed pool: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.AutoPlaceRequest{
		SelectFilter: apiv1.AutoSelectFilter{PlaceCount: 3, StoragePool: "pool"},
	})

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/autoplace", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusConflict {
		t.Errorf("status: got %d, want 409", resp.StatusCode)
	}
}

// TestAutoplaceMissingRD: 404 if the RD is unknown.
func TestAutoplaceMissingRD(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	body, _ := json.Marshal(apiv1.AutoPlaceRequest{
		SelectFilter: apiv1.AutoSelectFilter{PlaceCount: 1},
	})

	resp := httpPost(t, base+"/v1/resource-definitions/ghost/autoplace", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", resp.StatusCode)
	}
}

// TestAutoplaceInheritsRGFilter: when the request filter is empty, place
// count comes from the parent RG's select_filter.
func TestAutoplaceInheritsRGFilter(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name:         "rg-1",
		SelectFilter: apiv1.AutoSelectFilter{PlaceCount: 2, StoragePool: "pool"},
	}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:              "pvc-1",
		ResourceGroupName: "rg-1",
	}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	for _, n := range []string{"n1", "n2"} {
		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			StoragePoolName: "pool", NodeName: n, ProviderKind: apiv1.StoragePoolKindLVMThin,
		}); err != nil {
			t.Fatalf("seed pool: %v", err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.AutoPlaceRequest{}) // empty — inherit everything

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/autoplace", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	got, _ := st.Resources().ListByDefinition(ctx, "pvc-1")
	if len(got) != 2 {
		t.Errorf("len: got %d, want 2 (inherited from RG)", len(got))
	}
}

// TestAutoplacePrefersFreestPool: with three same-kind pools but different
// free_capacity, the placer picks the highest-free pool first. Production
// workloads quickly skew capacity across nodes; without weighting, naive
// first-N placement starves a single pool faster than the others.
func TestAutoplacePrefersFreestPool(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	pools := []apiv1.StoragePool{
		{StoragePoolName: "pool", NodeName: "n1", ProviderKind: apiv1.StoragePoolKindLVMThin, FreeCapacity: 1000},
		{StoragePoolName: "pool", NodeName: "n2", ProviderKind: apiv1.StoragePoolKindLVMThin, FreeCapacity: 5000},
		{StoragePoolName: "pool", NodeName: "n3", ProviderKind: apiv1.StoragePoolKindLVMThin, FreeCapacity: 3000},
	}

	for i := range pools {
		if err := st.StoragePools().Create(ctx, &pools[i]); err != nil {
			t.Fatalf("seed pool: %v", err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.AutoPlaceRequest{
		SelectFilter: apiv1.AutoSelectFilter{PlaceCount: 1, StoragePool: "pool"},
	})

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/autoplace", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d", resp.StatusCode)
	}

	got, _ := st.Resources().ListByDefinition(ctx, "pvc-1")
	if len(got) != 1 {
		t.Fatalf("placed: got %d, want 1", len(got))
	}

	if got[0].NodeName != "n2" {
		t.Errorf("expected placement on n2 (most free); got %s", got[0].NodeName)
	}
}

// TestAutoplaceSkipsEvictedNodes: a node flagged EVICTED is excluded
// from the candidate pool so autoplace does not undo an eviction
// the operator just initiated.
func TestAutoplaceSkipsEvictedNodes(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	for _, n := range []string{"n1", "n2", "n3"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{Name: n, Type: apiv1.NodeTypeSatellite}); err != nil {
			t.Fatalf("seed node %s: %v", n, err)
		}
		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			StoragePoolName: "pool",
			NodeName:        n,
			ProviderKind:    apiv1.StoragePoolKindLVMThin,
		}); err != nil {
			t.Fatalf("seed pool %s: %v", n, err)
		}
	}

	if err := st.Nodes().Update(ctx, &apiv1.Node{
		Name:  "n2",
		Type:  apiv1.NodeTypeSatellite,
		Flags: []string{apiv1.NodeFlagEvicted},
	}); err != nil {
		t.Fatalf("evict n2: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.AutoPlaceRequest{
		SelectFilter: apiv1.AutoSelectFilter{PlaceCount: 2, StoragePool: "pool"},
	})

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/autoplace", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.Resources().ListByDefinition(ctx, "pvc-1")
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	for _, r := range got {
		if r.NodeName == "n2" {
			t.Errorf("autoplace landed on EVICTED node n2; replicas=%v", got)
		}
	}

	if len(got) != 2 {
		t.Errorf("placed: got %d, want 2", len(got))
	}
}

// TestResourceListAndGet: GET /v1/resource-definitions/{rd}/resources
// returns all replicas wrapped as ResourceWithVolumes; the per-node
// GET returns one entry or 404 when missing. linstor-csi's reconciler
// hits these on every CreateVolume / ControllerPublishVolume call so
// the contract has to stay tight.
func TestResourceListAndGet(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	for _, n := range []string{"n1", "n2"} {
		if err := st.Resources().Create(ctx, &apiv1.Resource{Name: "pvc-1", NodeName: n}); err != nil {
			t.Fatalf("seed res %s: %v", n, err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	listResp := httpGet(t, base+"/v1/resource-definitions/pvc-1/resources")
	defer func() { _ = listResp.Body.Close() }()

	if listResp.StatusCode != http.StatusOK {
		t.Fatalf("list status: got %d, want 200", listResp.StatusCode)
	}

	var listGot []apiv1.ResourceWithVolumes
	if err := json.NewDecoder(listResp.Body).Decode(&listGot); err != nil {
		t.Fatalf("list decode: %v", err)
	}

	if len(listGot) != 2 {
		t.Errorf("list len: got %d, want 2", len(listGot))
	}

	getResp := httpGet(t, base+"/v1/resource-definitions/pvc-1/resources/n1")
	defer func() { _ = getResp.Body.Close() }()

	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("get status: got %d, want 200", getResp.StatusCode)
	}

	var getGot apiv1.ResourceWithVolumes
	if err := json.NewDecoder(getResp.Body).Decode(&getGot); err != nil {
		t.Fatalf("get decode: %v", err)
	}

	if getGot.NodeName != "n1" {
		t.Errorf("get node: got %q, want n1", getGot.NodeName)
	}

	missingResp := httpGet(t, base+"/v1/resource-definitions/pvc-1/resources/n9")
	defer func() { _ = missingResp.Body.Close() }()

	if missingResp.StatusCode != http.StatusNotFound {
		t.Errorf("missing-node status: got %d, want 404", missingResp.StatusCode)
	}
}

// TestResourceListMissingRD: 404 when the parent RD doesn't exist.
func TestResourceListMissingRD(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpGet(t, base+"/v1/resource-definitions/ghost/resources")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", resp.StatusCode)
	}
}

// TestResourceCreateAndDelete: explicit single-replica placement via REST.
func TestResourceCreateAndDelete(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.ResourceCreate{
		Resource: apiv1.Resource{NodeName: "n1"},
	})

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/resources", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create: got %d, want 201", resp.StatusCode)
	}

	delResp := httpDelete(t, base+"/v1/resource-definitions/pvc-1/resources/n1")
	_ = delResp.Body.Close()

	if delResp.StatusCode != http.StatusNoContent {
		t.Errorf("delete: got %d, want 204", delResp.StatusCode)
	}
}
