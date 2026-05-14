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
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestQuerySizeInfoNextSpawnResult pins the "preview" half of
// `linstor rg query-size-info` (wave2 scenario 1.W04): the response
// must surface the N pool-on-node tuples the next spawn would land
// on, sorted by per-pool MaxVolumeSize descending. Without this,
// `df`-style operator preview before a spawn is impossible — the
// only signal is `max_vlm_size_in_kib`, which collapses the entire
// placement decision to a single scalar.
//
// Setup: three LVM pools (free 100/200/300) with place-count 2 →
// top two are 300 and 200 (nodes "n3" and "n2"). Thick provider,
// so no oversubscription ratios populated on the preview rows.
func TestQuerySizeInfoNextSpawnResult(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "rg-preview",
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
			ProviderKind:    apiv1.StoragePoolKindLVM,
			FreeCapacity:    free,
			TotalCapacity:   free * 2,
		}); err != nil {
			t.Fatalf("seed pool: %v", err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/resource-groups/rg-preview/query-size-info", []byte(`{}`))
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got querySizeInfoResponse
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(got.SpaceInfo.NextSpawnResult) != 2 {
		t.Fatalf("next_spawn_result len: got %d, want 2 (place_count=2)",
			len(got.SpaceInfo.NextSpawnResult))
	}

	if got.SpaceInfo.NextSpawnResult[0].NodeName != "n3" {
		t.Errorf("row 0 node: got %q, want n3 (largest free)",
			got.SpaceInfo.NextSpawnResult[0].NodeName)
	}

	if got.SpaceInfo.NextSpawnResult[1].NodeName != "n2" {
		t.Errorf("row 1 node: got %q, want n2 (2nd largest free)",
			got.SpaceInfo.NextSpawnResult[1].NodeName)
	}

	for i, row := range got.SpaceInfo.NextSpawnResult {
		if row.StorPoolName != "pool" {
			t.Errorf("row %d pool: got %q, want %q", i, row.StorPoolName, "pool")
		}

		// Thick provider: ratios must stay zero so they JSON-omit;
		// emitting 1.0 here would mislead operators into thinking
		// the gate is engaged when LVM/ZFS physically can't honour it.
		if row.StorPoolOversubscriptionRatio != 0 {
			t.Errorf("row %d overall ratio: got %v, want 0 (thick pool)",
				i, row.StorPoolOversubscriptionRatio)
		}
	}

	if len(got.Reports) != 0 {
		t.Errorf("reports: got %d entries, want 0 (RG satisfiable)", len(got.Reports))
	}
}

// TestQuerySizeInfoNextSpawnResultThinRatios pins that thin pools
// have their per-row oversubscription ratios populated so an
// operator inspecting the preview can see the gate that produced
// each tuple's cap. Free=10, total=100, free-ratio=4, total-ratio=2
// → row.free_ratio=4, row.total_ratio=2, overall=min(4,2)=2.
func TestQuerySizeInfoNextSpawnResultThinRatios(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "rg-thin-preview",
		SelectFilter: apiv1.AutoSelectFilter{
			PlaceCount:  1,
			StoragePool: "pool",
		},
	}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		StoragePoolName: "pool",
		NodeName:        "n1",
		ProviderKind:    apiv1.StoragePoolKindLVMThin,
		Props: map[string]string{
			"MaxFreeCapacityOversubscriptionRatio":  "4",
			"MaxTotalCapacityOversubscriptionRatio": "2",
		},
		FreeCapacity:  10,
		TotalCapacity: 100,
	}); err != nil {
		t.Fatalf("seed pool: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/resource-groups/rg-thin-preview/query-size-info", []byte(`{}`))
	defer func() { _ = resp.Body.Close() }()

	var got querySizeInfoResponse
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(got.SpaceInfo.NextSpawnResult) != 1 {
		t.Fatalf("next_spawn_result len: got %d, want 1", len(got.SpaceInfo.NextSpawnResult))
	}

	row := got.SpaceInfo.NextSpawnResult[0]
	if row.StorPoolFreeCapacityOversubscriptionRatio != 4 {
		t.Errorf("free ratio: got %v, want 4", row.StorPoolFreeCapacityOversubscriptionRatio)
	}

	if row.StorPoolTotalCapacityOversubscriptionRatio != 2 {
		t.Errorf("total ratio: got %v, want 2", row.StorPoolTotalCapacityOversubscriptionRatio)
	}

	if row.StorPoolOversubscriptionRatio != 2 {
		t.Errorf("overall ratio: got %v, want 2 (min(free, total))",
			row.StorPoolOversubscriptionRatio)
	}
}

// TestQuerySizeInfoUnsatisfiableReports pins the constraint-impossible
// signal: place-count exceeds available pools → `reports` carries an
// info-band ApiCallRc explaining why, AND next_spawn_result is empty.
// Operators need the *why*, not just a silent zero — that's the only
// way `linstor rg query-size-info` can tell "no pools" from "wrong
// filter" from "all nodes evicted".
func TestQuerySizeInfoUnsatisfiableReports(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	_ = st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "rg-bad",
		SelectFilter: apiv1.AutoSelectFilter{
			PlaceCount:  3,
			StoragePool: "pool",
		},
	})

	_ = st.StoragePools().Create(ctx, &apiv1.StoragePool{
		StoragePoolName: "pool",
		NodeName:        "n1",
		ProviderKind:    apiv1.StoragePoolKindLVM,
		FreeCapacity:    1024,
	})

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/resource-groups/rg-bad/query-size-info", []byte(`{}`))
	defer func() { _ = resp.Body.Close() }()

	var got querySizeInfoResponse

	_ = json.NewDecoder(resp.Body).Decode(&got)

	if len(got.SpaceInfo.NextSpawnResult) != 0 {
		t.Errorf("next_spawn_result: got %d, want 0 (cannot satisfy place_count=3)",
			len(got.SpaceInfo.NextSpawnResult))
	}

	if len(got.Reports) == 0 {
		t.Fatalf("reports: got 0 entries, want 1 (RG unsatisfiable)")
	}

	if !strings.Contains(got.Reports[0].Message, "place-count=3") {
		t.Errorf("reports[0].message: got %q, want it to mention place-count=3",
			got.Reports[0].Message)
	}

	if got.Reports[0].RetCode != maskInfo {
		t.Errorf("reports[0].ret_code: got %d, want maskInfo (%d)",
			got.Reports[0].RetCode, maskInfo)
	}
}

// TestQuerySizeInfoNoEligiblePools pins the all-pools-disabled path
// (filter matches nothing). Distinguishes "no candidate pools" from
// "fewer pools than place-count" — both produce empty preview rows
// but the reason line is different. Operators rely on this to debug
// over-restrictive RG.SelectFilter.StoragePool typos.
func TestQuerySizeInfoNoEligiblePools(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	_ = st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "rg-typo",
		SelectFilter: apiv1.AutoSelectFilter{
			PlaceCount:  1,
			StoragePool: "wrong-pool-name",
		},
	})

	_ = st.StoragePools().Create(ctx, &apiv1.StoragePool{
		StoragePoolName: "actual-pool",
		NodeName:        "n1",
		ProviderKind:    apiv1.StoragePoolKindLVM,
		FreeCapacity:    1024,
	})

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/resource-groups/rg-typo/query-size-info", []byte(`{}`))
	defer func() { _ = resp.Body.Close() }()

	var got querySizeInfoResponse

	_ = json.NewDecoder(resp.Body).Decode(&got)

	if len(got.Reports) == 0 {
		t.Fatalf("reports: got 0 entries, want 1 (no eligible pools)")
	}

	if !strings.Contains(got.Reports[0].Message, "wrong-pool-name") {
		t.Errorf("reports[0].message: got %q, want it to mention the typo filter",
			got.Reports[0].Message)
	}
}

// TestQuerySizeInfoSatisfiableNoReports pins the non-degenerate
// path: RG that can be satisfied → `reports` MUST be empty. A
// regression that always emitted a report would clutter the
// operator's preview output with stale lines on every healthy run.
func TestQuerySizeInfoSatisfiableNoReports(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	_ = st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "rg-good",
		SelectFilter: apiv1.AutoSelectFilter{
			PlaceCount:  1,
			StoragePool: "pool",
		},
	})

	_ = st.StoragePools().Create(ctx, &apiv1.StoragePool{
		StoragePoolName: "pool",
		NodeName:        "n1",
		ProviderKind:    apiv1.StoragePoolKindLVM,
		FreeCapacity:    1024,
		TotalCapacity:   2048,
	})

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/resource-groups/rg-good/query-size-info", []byte(`{}`))
	defer func() { _ = resp.Body.Close() }()

	var got querySizeInfoResponse

	_ = json.NewDecoder(resp.Body).Decode(&got)

	if len(got.Reports) != 0 {
		t.Errorf("reports: got %d entries, want 0 on healthy RG; msg=%v",
			len(got.Reports), got.Reports)
	}

	if len(got.SpaceInfo.NextSpawnResult) != 1 {
		t.Errorf("next_spawn_result: got %d, want 1", len(got.SpaceInfo.NextSpawnResult))
	}
}
