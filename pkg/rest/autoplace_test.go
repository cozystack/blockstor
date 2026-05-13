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
	"slices"
	"testing"
	"time"

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

	// TotalCapacity is uniform across pools so the weighted scorer's
	// MaxFreeSpace strategy (Free/Total ratio) ends up driven by Free
	// alone, preserving the legacy "biggest-free-first" ordering this
	// test was written for under the new scenario 2.17 scorer.
	pools := []apiv1.StoragePool{
		{StoragePoolName: "pool", NodeName: "n1", ProviderKind: apiv1.StoragePoolKindLVMThin, FreeCapacity: 1000, TotalCapacity: 10000},
		{StoragePoolName: "pool", NodeName: "n2", ProviderKind: apiv1.StoragePoolKindLVMThin, FreeCapacity: 5000, TotalCapacity: 10000},
		{StoragePoolName: "pool", NodeName: "n3", ProviderKind: apiv1.StoragePoolKindLVMThin, FreeCapacity: 3000, TotalCapacity: 10000},
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

// TestAutoplaceSuccessReturnsApiCallRc verifies the response body shape
// matches Java LINSTOR's: a `[]ApiCallRc` with MASK_INFO set and a
// non-zero ret_code. Pinned because golinstor proxies and the linstor
// CLI both surface the message — silent regressions back to "200 with
// empty body" turn that diagnostic channel off.
func TestAutoplaceSuccessReturnsApiCallRc(t *testing.T) {
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
		SelectFilter: apiv1.AutoSelectFilter{PlaceCount: 1, StoragePool: "pool"},
	})

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/autoplace", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("got %d entries, want 1", len(got))
	}

	const maskInfo int64 = 0x0040_0000_0000_0000
	if got[0].RetCode&maskInfo == 0 {
		t.Errorf("ret_code missing MASK_INFO: %x", got[0].RetCode)
	}

	if got[0].Message == "" {
		t.Errorf("expected non-empty Message in success entry")
	}
}

// TestAutoplaceReplicasOnDifferent enforces anti-affinity over a
// topology key on the Node CRD. Two replicas in the same zone must
// NEVER both end up placed when `replicas_on_different=["zone"]` is
// set — that's the whole point of anti-affinity in production.
func TestAutoplaceReplicasOnDifferent(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-anti"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	// Three nodes, only TWO distinct zones — n1 and n2 share zone-A.
	// place_count=3 must fail (only two zones available); place_count=2
	// must spread across distinct zones.
	for _, spec := range []struct {
		name, zone string
	}{
		{"n1", "zone-a"},
		{"n2", "zone-a"},
		{"n3", "zone-b"},
	} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{
			Name:  spec.name,
			Type:  apiv1.NodeTypeSatellite,
			Props: map[string]string{"Aux/zone": spec.zone},
		}); err != nil {
			t.Fatalf("seed node %s: %v", spec.name, err)
		}

		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			StoragePoolName: "pool",
			NodeName:        spec.name,
			ProviderKind:    apiv1.StoragePoolKindLVMThin,
		}); err != nil {
			t.Fatalf("seed pool %s: %v", spec.name, err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.AutoPlaceRequest{
		SelectFilter: apiv1.AutoSelectFilter{
			PlaceCount:          2,
			StoragePool:         "pool",
			ReplicasOnDifferent: []string{"zone"},
		},
	})

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-anti/autoplace", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.Resources().ListByDefinition(ctx, "pvc-anti")
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("placed: got %d, want 2", len(got))
	}

	zones := map[string]string{"n1": "zone-a", "n2": "zone-a", "n3": "zone-b"}

	seen := map[string]string{}

	for _, r := range got {
		zone := zones[r.NodeName]
		if other, dup := seen[zone]; dup {
			t.Errorf("anti-affinity violated: %s and %s both in zone %q", other, r.NodeName, zone)
		}

		seen[zone] = r.NodeName
	}
}

// TestAutoplaceReplicasOnDifferentExhausted: place_count exceeds the
// number of distinct zones → 409 Conflict.
func TestAutoplaceReplicasOnDifferentExhausted(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-anti"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	for _, spec := range []struct{ name, zone string }{
		{"n1", "zone-a"},
		{"n2", "zone-a"},
	} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{
			Name:  spec.name,
			Type:  apiv1.NodeTypeSatellite,
			Props: map[string]string{"Aux/zone": spec.zone},
		}); err != nil {
			t.Fatalf("seed node %s: %v", spec.name, err)
		}

		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			StoragePoolName: "pool",
			NodeName:        spec.name,
			ProviderKind:    apiv1.StoragePoolKindLVMThin,
		}); err != nil {
			t.Fatalf("seed pool %s: %v", spec.name, err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.AutoPlaceRequest{
		SelectFilter: apiv1.AutoSelectFilter{
			PlaceCount:          2,
			StoragePool:         "pool",
			ReplicasOnDifferent: []string{"zone"},
		},
	})

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-anti/autoplace", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusConflict {
		t.Errorf("status: got %d, want 409 (only one zone available)", resp.StatusCode)
	}
}

// TestAutoplaceReplicasOnSame: replicas_on_same forces every replica
// to share the topology value of the first one.
func TestAutoplaceReplicasOnSame(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-same"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	// Two zones, but one of them has only one node. With
	// replicas_on_same+place_count=2 the placer must pick the zone
	// that has 2+ nodes, never split across zones.
	for _, spec := range []struct{ name, zone string }{
		{"n1", "zone-a"},
		{"n2", "zone-b"},
		{"n3", "zone-b"},
	} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{
			Name:  spec.name,
			Type:  apiv1.NodeTypeSatellite,
			Props: map[string]string{"Aux/zone": spec.zone},
		}); err != nil {
			t.Fatalf("seed node %s: %v", spec.name, err)
		}

		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			StoragePoolName: "pool",
			NodeName:        spec.name,
			ProviderKind:    apiv1.StoragePoolKindLVMThin,
		}); err != nil {
			t.Fatalf("seed pool %s: %v", spec.name, err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.AutoPlaceRequest{
		SelectFilter: apiv1.AutoSelectFilter{
			PlaceCount:     2,
			StoragePool:    "pool",
			ReplicasOnSame: []string{"zone"},
		},
	})

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-same/autoplace", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.Resources().ListByDefinition(ctx, "pvc-same")
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("placed: got %d, want 2", len(got))
	}

	zones := map[string]string{"n1": "zone-a", "n2": "zone-b", "n3": "zone-b"}

	first := zones[got[0].NodeName]
	second := zones[got[1].NodeName]

	if first != second {
		t.Errorf("replicas_on_same violated: %s in %q vs %s in %q",
			got[0].NodeName, first, got[1].NodeName, second)
	}

	if first != "zone-b" {
		t.Errorf("expected zone-b (the only zone with 2 nodes); got %q", first)
	}
}

// TestAutoplaceDisklessOnRemaining: with the flag set + 2 diskful
// replicas placed, every other healthy node gains a DISKLESS replica
// so consumers can mount the PVC anywhere.
func TestAutoplaceDisklessOnRemaining(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-don"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	for _, n := range []string{"n1", "n2", "n3", "n4"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{Name: n, Type: apiv1.NodeTypeSatellite}); err != nil {
			t.Fatalf("seed node: %v", err)
		}

		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			StoragePoolName: "pool",
			NodeName:        n,
			ProviderKind:    apiv1.StoragePoolKindLVMThin,
		}); err != nil {
			t.Fatalf("seed pool: %v", err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.AutoPlaceRequest{
		SelectFilter: apiv1.AutoSelectFilter{
			PlaceCount:          2,
			StoragePool:         "pool",
			DisklessOnRemaining: true,
		},
	})

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-don/autoplace", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.Resources().ListByDefinition(ctx, "pvc-don")
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	// 4 nodes total: 2 diskful + 2 diskless witnesses.
	if len(got) != 4 {
		t.Fatalf("replica count: got %d, want 4 (2 diskful + 2 diskless); entries=%v", len(got), got)
	}

	diskful := 0
	diskless := 0

	for _, r := range got {
		if slices.Contains(r.Flags, "DISKLESS") {
			diskless++
		} else {
			diskful++
		}
	}

	if diskful != 2 || diskless != 2 {
		t.Errorf("split: got %d diskful + %d diskless, want 2/2", diskful, diskless)
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

// TestAutoplaceSharedLUNAntiAffinity: two pools share the same backing
// LUN (SharedSpaceID="exos-lun-42"); a 2-replica autoplace must never
// land both replicas on those pools, even though they live on
// distinct nodes. Real-world rationale: at the physical layer the
// LUN is the same disk, so both replicas would sit on the same
// failure domain — defeating the redundancy a 2-replica RD promises.
func TestAutoplaceSharedLUNAntiAffinity(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-shared"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	for _, n := range []string{"n1", "n2", "n3"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{Name: n, Type: apiv1.NodeTypeSatellite}); err != nil {
			t.Fatalf("seed node %s: %v", n, err)
		}
	}

	// n1 + n2 each see the same EXOS LUN; n3 has its own local pool.
	pools := []apiv1.StoragePool{
		{StoragePoolName: "pool", NodeName: "n1", ProviderKind: apiv1.StoragePoolKindLVMThin, SharedSpaceID: "exos-lun-42", FreeCapacity: 9000},
		{StoragePoolName: "pool", NodeName: "n2", ProviderKind: apiv1.StoragePoolKindLVMThin, SharedSpaceID: "exos-lun-42", FreeCapacity: 9000},
		{StoragePoolName: "pool", NodeName: "n3", ProviderKind: apiv1.StoragePoolKindLVMThin, FreeCapacity: 5000},
	}
	for i := range pools {
		if err := st.StoragePools().Create(ctx, &pools[i]); err != nil {
			t.Fatalf("seed pool: %v", err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.AutoPlaceRequest{
		SelectFilter: apiv1.AutoSelectFilter{PlaceCount: 2, StoragePool: "pool"},
	})

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-shared/autoplace", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.Resources().ListByDefinition(ctx, "pvc-shared")
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("placed: got %d, want 2", len(got))
	}

	// Exactly one of {n1, n2} can be picked; n3 (local) MUST be the other.
	nodes := []string{got[0].NodeName, got[1].NodeName}
	if !slices.Contains(nodes, "n3") {
		t.Errorf("expected one replica on n3 (the non-shared node); got %v", nodes)
	}

	sharedHit := 0
	for _, n := range nodes {
		if n == "n1" || n == "n2" {
			sharedHit++
		}
	}

	if sharedHit > 1 {
		t.Errorf("both replicas on the same shared-LUN pool group: %v", nodes)
	}
}

// TestAutoplaceSharedLUNExhausted: 2-replica RD against two pools
// sharing one LUN — only one replica fits, the other has no
// candidate and the request must 409. Pins the conflict path.
func TestAutoplaceSharedLUNExhausted(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-shared-2"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	for _, n := range []string{"n1", "n2"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{Name: n, Type: apiv1.NodeTypeSatellite}); err != nil {
			t.Fatalf("seed node %s: %v", n, err)
		}

		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			StoragePoolName: "pool",
			NodeName:        n,
			ProviderKind:    apiv1.StoragePoolKindLVMThin,
			SharedSpaceID:   "exos-lun-42",
		}); err != nil {
			t.Fatalf("seed pool %s: %v", n, err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.AutoPlaceRequest{
		SelectFilter: apiv1.AutoSelectFilter{PlaceCount: 2, StoragePool: "pool"},
	})

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-shared-2/autoplace", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusConflict {
		t.Errorf("status: got %d, want 409 (only one shared-LUN slot fits)", resp.StatusCode)
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

	// Upstream LINSTOR replies with HTTP 200 + ApiCallRc envelope; the
	// `linstor` CLI rejects a bare 204 No Content.
	if delResp.StatusCode != http.StatusOK {
		t.Errorf("delete: got %d, want 200", delResp.StatusCode)
	}
}

// TestAutoplacePersistsLayerListOntoRD: linstor-csi (and piraeus-operator's
// LinstorSatelliteConfiguration.spec.storageClasses[*].layerList) sets
// layer_list on the autoplace request rather than on RD create. The REST
// handler must persist that onto the RD's LayerStack so the dispatcher /
// satellite chain sees the right composition.
func TestAutoplacePersistsLayerListOntoRD(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-csi"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	for _, n := range []string{"n1", "n2"} {
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
		LayerList:    []string{apiv1.LayerKindDRBD, apiv1.LayerKindLUKS, apiv1.LayerKindStorage},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-csi/autoplace", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.ResourceDefinitions().Get(ctx, "pvc-csi")
	if err != nil {
		t.Fatalf("get rd: %v", err)
	}

	want := []string{apiv1.LayerKindDRBD, apiv1.LayerKindLUKS, apiv1.LayerKindStorage}
	if !slices.Equal(got.LayerStack, want) {
		t.Errorf("LayerStack: got %v, want %v", got.LayerStack, want)
	}
}

// TestAutoplaceLayerListDoesNotOverwriteExistingStack: an RD that already
// has a LayerStack (operator-supplied via REST POST or CRD create) wins over
// any layer_list the autoplace request smuggles in. Otherwise CSI clients
// could silently flip an explicitly-set composition on a re-place.
func TestAutoplaceLayerListDoesNotOverwriteExistingStack(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	existing := []string{apiv1.LayerKindStorage} // single-replica local
	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:       "pvc-fixed",
		LayerStack: existing,
	}); err != nil {
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
		SelectFilter: apiv1.AutoSelectFilter{PlaceCount: 1, StoragePool: "pool"},
		LayerList:    []string{apiv1.LayerKindDRBD, apiv1.LayerKindStorage},
	})

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-fixed/autoplace", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.ResourceDefinitions().Get(ctx, "pvc-fixed")
	if err != nil {
		t.Fatalf("get rd: %v", err)
	}

	if !slices.Equal(got.LayerStack, existing) {
		t.Errorf("LayerStack: got %v, want unchanged %v", got.LayerStack, existing)
	}
}

// TestResourceCreatePersistsLayerList: same pass-through but on the
// per-node resource-create path linstor-csi uses for explicit placement.
func TestResourceCreatePersistsLayerList(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-csi-explicit"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.ResourceCreate{
		Resource: apiv1.Resource{
			Name:     "pvc-csi-explicit",
			NodeName: "n1",
		},
		LayerList: []string{apiv1.LayerKindLUKS, apiv1.LayerKindStorage},
	})

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-csi-explicit/resources", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201", resp.StatusCode)
	}

	got, err := st.ResourceDefinitions().Get(ctx, "pvc-csi-explicit")
	if err != nil {
		t.Fatalf("get rd: %v", err)
	}

	want := []string{apiv1.LayerKindLUKS, apiv1.LayerKindStorage}
	if !slices.Equal(got.LayerStack, want) {
		t.Errorf("LayerStack: got %v, want %v", got.LayerStack, want)
	}
}

// TestResourceCreateBadJSON: malformed body → 400.
func TestResourceCreateBadJSON(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/resources", []byte("{not-json"))
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400", resp.StatusCode)
	}
}

// TestResourceCreateMissingNodeName: POST with empty node_name → 400.
// Pinned because linstor-csi calls this for explicit-placement
// requests where node selection is operator-driven; an empty node
// in the body must not silently land as "wherever" — the satellite
// reconciler relies on a definite NodeName to apply the resource.
func TestResourceCreateMissingNodeName(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.ResourceCreate{
		Resource: apiv1.Resource{}, // NodeName omitted
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/resources", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400 (missing node_name)", resp.StatusCode)
	}
}

// TestResourceDeleteMissing: DELETE on a non-existent (RD, node) →
// 404. Pinned because linstor-csi performs idempotent replica
// removal during volume teardown; the 404 must surface cleanly so
// csi treats it as "already gone" rather than retrying forever.
func TestResourceDeleteMissing(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpDelete(t, base+"/v1/resource-definitions/ghost/resources/n1")
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", resp.StatusCode)
	}
}

// TestAutoplaceBadJSON: malformed body → 400. Pinned because
// linstor-csi calls this on every CreateVolume; a regression that
// flipped a decoder error to 500 would loop the csi retry path.
func TestAutoplaceBadJSON(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/autoplace", []byte("{not-json"))
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400", resp.StatusCode)
	}
}

// TestResourceDeleteTieBreakerStampsSuppression: deleting a
// TIE_BREAKER-flagged replica writes the auto-tiebreaker
// suppression annotation on the parent RD with a future RFC3339
// deadline. The RD-side reconciler honours that annotation to
// avoid immediately re-stamping a fresh witness — without this,
// `linstor r d <tiebreaker-node> <rd>` would silently get undone
// within milliseconds by the next reconcile.
//
// Regression guard for Bug 4.
func TestResourceDeleteTieBreakerStampsSuppression(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-tb"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name:     "pvc-tb",
		NodeName: "n3",
		Flags:    []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker},
	}); err != nil {
		t.Fatalf("seed replica: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t, base+"/v1/resource-definitions/pvc-tb/resources/n3")
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("delete status: got %d, want 200", resp.StatusCode)
	}

	rd, err := st.ResourceDefinitions().Get(ctx, "pvc-tb")
	if err != nil {
		t.Fatalf("get RD: %v", err)
	}

	raw, ok := rd.Annotations[AutoTiebreakerSuppressedUntilAnnotation]
	if !ok || raw == "" {
		t.Fatalf("suppression annotation missing; annotations=%v", rd.Annotations)
	}

	deadline, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		t.Fatalf("annotation value %q is not RFC3339: %v", raw, err)
	}

	if !deadline.After(time.Now()) {
		t.Errorf("annotation deadline %v is not in the future", deadline)
	}
}

// TestResourceDeleteRegularReplicaSkipsSuppression: deleting a
// regular diskful replica must NOT stamp the suppression
// annotation. Only TIE_BREAKER deletes carry operator intent that
// should pause the auto-witness loop — otherwise every `linstor r
// d` would silently disable the invariant for 5 minutes.
func TestResourceDeleteRegularReplicaSkipsSuppression(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-reg"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name: "pvc-reg", NodeName: "n1",
	}); err != nil {
		t.Fatalf("seed replica: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t, base+"/v1/resource-definitions/pvc-reg/resources/n1")
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("delete status: got %d, want 200", resp.StatusCode)
	}

	rd, err := st.ResourceDefinitions().Get(ctx, "pvc-reg")
	if err != nil {
		t.Fatalf("get RD: %v", err)
	}

	if _, ok := rd.Annotations[AutoTiebreakerSuppressedUntilAnnotation]; ok {
		t.Errorf("suppression annotation must not appear on a regular-replica delete; got %v", rd.Annotations)
	}
}

// TestMakeAvailablePromotesTiebreakerWitness pins the CSI
// ControllerPublishVolume happy path: linstor-csi calls
// `POST .../resources/{node}/make-available` with `{diskful:false}`
// against a node that already carries a [DISKLESS, TIE_BREAKER]
// witness. The witness must lose TIE_BREAKER (and keep DISKLESS) so
// the satellite reconciler exposes a real DRBD device — without
// this the Pod stays in ContainerCreating with "could not determine
// device path".
func TestMakeAvailablePromotesTiebreakerWitness(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	witness := &apiv1.Resource{
		Name:     "pvc-1",
		NodeName: "n3",
		Flags:    []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker},
	}
	if err := st.Resources().Create(ctx, witness); err != nil {
		t.Fatalf("seed tiebreaker witness: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.ResourceMakeAvailable{Diskful: false})

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/resources/n3/make-available", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.Resources().Get(ctx, "pvc-1", "n3")
	if err != nil {
		t.Fatalf("get promoted: %v", err)
	}

	if !slices.Contains(got.Flags, apiv1.ResourceFlagDiskless) {
		t.Errorf("DISKLESS flag missing after promote: %v", got.Flags)
	}

	if slices.Contains(got.Flags, apiv1.ResourceFlagTieBreaker) {
		t.Errorf("TIE_BREAKER must be stripped after CSI attach: %v", got.Flags)
	}
}

// TestMakeAvailableCreatesDisklessOnEmptyNode covers the second
// half of the CSI promote semantics — when no replica exists on the
// target node, make-available creates a plain DISKLESS one. Without
// this the manual fallback path in linstor-csi's Attach has to fire
// a second REST call and racily collide with the controller's
// tiebreaker placer.
func TestMakeAvailableCreatesDisklessOnEmptyNode(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.ResourceMakeAvailable{Diskful: false})

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/resources/n2/make-available", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.Resources().Get(ctx, "pvc-1", "n2")
	if err != nil {
		t.Fatalf("get created: %v", err)
	}

	if !slices.Contains(got.Flags, apiv1.ResourceFlagDiskless) {
		t.Errorf("DISKLESS flag missing on freshly-created replica: %v", got.Flags)
	}

	if slices.Contains(got.Flags, apiv1.ResourceFlagTieBreaker) {
		t.Errorf("freshly-created CSI replica must not carry TIE_BREAKER: %v", got.Flags)
	}
}

// TestMakeAvailableMissingRDReturns404: linstor-csi treats 404 here
// as "no such volume → fail attach"; any other status code would
// loop the csi retry path. Match upstream LINSTOR exactly.
func TestMakeAvailableMissingRDReturns404(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	body, _ := json.Marshal(apiv1.ResourceMakeAvailable{Diskful: false})

	resp := httpPost(t, base+"/v1/resource-definitions/ghost/resources/n1/make-available", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", resp.StatusCode)
	}
}

// TestMakeAvailableDiskfulPromotesWitnessToDiskful exercises the
// less-common `{diskful:true}` branch — the witness loses both
// DISKLESS and TIE_BREAKER so the reconciler attaches storage on
// that node. Mirrors the upstream `linstor resource toggle-disk
// --diskful` semantics.
func TestMakeAvailableDiskfulPromotesWitnessToDiskful(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	witness := &apiv1.Resource{
		Name:     "pvc-1",
		NodeName: "n3",
		Flags:    []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker},
	}
	if err := st.Resources().Create(ctx, witness); err != nil {
		t.Fatalf("seed witness: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.ResourceMakeAvailable{Diskful: true})

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/resources/n3/make-available", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.Resources().Get(ctx, "pvc-1", "n3")
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if slices.Contains(got.Flags, apiv1.ResourceFlagDiskless) ||
		slices.Contains(got.Flags, apiv1.ResourceFlagTieBreaker) {
		t.Errorf("diskful promote should strip both DISKLESS and TIE_BREAKER, got %v", got.Flags)
	}
}

// TestMakeAvailableDiskfulReplicaIsNoOp: calling make-available on a
// node that already has a diskful replica returns 200 with no flag
// mutation. Idempotent by design — csi may issue the call on every
// ControllerPublishVolume retry.
func TestMakeAvailableDiskfulReplicaIsNoOp(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	diskful := &apiv1.Resource{
		Name:     "pvc-1",
		NodeName: "n1",
		Props:    map[string]string{"StorPoolName": "pool"},
	}
	if err := st.Resources().Create(ctx, diskful); err != nil {
		t.Fatalf("seed diskful: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.ResourceMakeAvailable{Diskful: false})

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/resources/n1/make-available", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.Resources().Get(ctx, "pvc-1", "n1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if len(got.Flags) != 0 {
		t.Errorf("no-op make-available must not add flags to a diskful replica, got %v", got.Flags)
	}
}

// TestMakeAvailablePersistsLayerListOntoRD: linstor-csi may smuggle a
// layer_list on the make-available call (matches the
// `autoplace`/`resources` POST behaviour). Persist onto RD.LayerStack
// only when the RD doesn't already carry one — operator-set wins.
func TestMakeAvailablePersistsLayerListOntoRD(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.ResourceMakeAvailable{
		LayerList: []string{apiv1.LayerKindDRBD, apiv1.LayerKindStorage},
	})

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/resources/n1/make-available", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	rd, err := st.ResourceDefinitions().Get(ctx, "pvc-1")
	if err != nil {
		t.Fatalf("get rd: %v", err)
	}

	want := []string{apiv1.LayerKindDRBD, apiv1.LayerKindStorage}
	if !slices.Equal(rd.LayerStack, want) {
		t.Errorf("LayerStack: got %v, want %v", rd.LayerStack, want)
	}
}

// TestResourceCreatePromotesTiebreakerWithDisklessFlag covers the
// linstor-csi fallback path: after make-available returns 404 the
// client retries with `POST .../resources` carrying
// `Flags: [DISKLESS]` and NO StorPoolName. The promote branch must
// fire on that bare-diskless envelope too, otherwise the second
// call collides with the existing TIE_BREAKER witness.
func TestResourceCreatePromotesTiebreakerWithDisklessFlag(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	witness := &apiv1.Resource{
		Name:     "pvc-1",
		NodeName: "n3",
		Flags:    []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker},
	}
	if err := st.Resources().Create(ctx, witness); err != nil {
		t.Fatalf("seed witness: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.ResourceCreate{
		Resource: apiv1.Resource{
			NodeName: "n3",
			Flags:    []string{apiv1.ResourceFlagDiskless},
		},
	})

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/resources", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201", resp.StatusCode)
	}

	got, err := st.Resources().Get(ctx, "pvc-1", "n3")
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if !slices.Contains(got.Flags, apiv1.ResourceFlagDiskless) {
		t.Errorf("DISKLESS must remain: %v", got.Flags)
	}

	if slices.Contains(got.Flags, apiv1.ResourceFlagTieBreaker) {
		t.Errorf("TIE_BREAKER must be stripped: %v", got.Flags)
	}
}
