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
			ProviderKind:    apiv1.StoragePoolKindLVM,
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
		ProviderKind:    apiv1.StoragePoolKindLVM,
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
			ProviderKind:    apiv1.StoragePoolKindLVM,
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

// TestQuerySizeInfoSharedLUN: pools sharing a backing LUN must
// contribute their capacity once, not summed. Without dedup, two
// pools each "seeing" 1000 KiB of the same LUN would report 2000
// KiB available — and `linstor advise` / golinstor's pre-flight
// would happily admit a 1500-KiB request that physically can't fit.
func TestQuerySizeInfoSharedLUN(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	_ = st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "rg-shared",
		SelectFilter: apiv1.AutoSelectFilter{
			PlaceCount:  2,
			StoragePool: "pool",
		},
	})

	// Two pools sharing the same LUN, plus a third independent pool.
	pools := []apiv1.StoragePool{
		{StoragePoolName: "pool", NodeName: "n1", ProviderKind: apiv1.StoragePoolKindLVM, SharedSpaceID: "lun-1", FreeCapacity: 1000, TotalCapacity: 2000},
		{StoragePoolName: "pool", NodeName: "n2", ProviderKind: apiv1.StoragePoolKindLVM, SharedSpaceID: "lun-1", FreeCapacity: 1000, TotalCapacity: 2000},
		{StoragePoolName: "pool", NodeName: "n3", ProviderKind: apiv1.StoragePoolKindLVM, FreeCapacity: 700, TotalCapacity: 1400},
	}
	for i := range pools {
		_ = st.StoragePools().Create(ctx, &pools[i])
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/resource-groups/rg-shared/query-size-info", []byte(`{}`))
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got querySizeInfoResponse

	_ = json.NewDecoder(resp.Body).Decode(&got)

	// After dedup, candidates are {n1, n3} (n2 collapsed into n1's
	// shared LUN). place_count=2 → max-vol is the smaller free, 700.
	if got.SpaceInfo.MaxVlmSizeInKib != 700 {
		t.Errorf("max vol: got %d, want 700 (n3 = smaller free of the two surviving slots)",
			got.SpaceInfo.MaxVlmSizeInKib)
	}

	// Available capacity: shared LUN counted once (1000) + n3 (700)
	// = 1700, NOT 2700 (which would be the un-deduped sum).
	if got.SpaceInfo.AvailableSizeInKib != 1700 {
		t.Errorf("available: got %d, want 1700 (shared LUN counted once)",
			got.SpaceInfo.AvailableSizeInKib)
	}
}

// TestDisabledNodes pins which node-flag values cause a node to be
// excluded from the query-size-info / advise capacity rollup. Both
// EVICTED and LOST must surface in the disabled set; any other flag
// (or no flags at all) leaves the node in the available pool.
//
// A regression that dropped one of the two flags would silently
// over-count free capacity — operators rely on this to keep an
// EVICTED node's pools out of the autoplace candidate list, and to
// stop linstor-csi from sizing volumes against capacity that is
// definitionally unreachable.
func TestDisabledNodes(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	for _, n := range []apiv1.Node{
		{Name: "healthy", Flags: nil},
		{Name: "online-with-other-flag", Flags: []string{"SOME_OTHER_FLAG"}},
		{Name: "evicted", Flags: []string{apiv1.NodeFlagEvicted}},
		{Name: "lost", Flags: []string{apiv1.NodeFlagLost}},
		{Name: "evicted-and-lost", Flags: []string{apiv1.NodeFlagEvicted, apiv1.NodeFlagLost}},
	} {
		if err := st.Nodes().Create(ctx, &n); err != nil {
			t.Fatalf("seed %s: %v", n.Name, err)
		}
	}

	srv := &Server{Store: st}

	got, err := srv.disabledNodes(ctx)
	if err != nil {
		t.Fatalf("disabledNodes: %v", err)
	}

	want := map[string]struct{}{
		"evicted":          {},
		"lost":             {},
		"evicted-and-lost": {},
	}

	if len(got) != len(want) {
		t.Fatalf("got %d disabled, want %d; got=%v", len(got), len(want), got)
	}

	for name := range want {
		if _, ok := got[name]; !ok {
			t.Errorf("missing %q from disabled set; got=%v", name, got)
		}
	}

	for _, ok := range []string{"healthy", "online-with-other-flag"} {
		if _, bad := got[ok]; bad {
			t.Errorf("%q must NOT be disabled (has no EVICTED/LOST flag)", ok)
		}
	}
}

// TestReplicaCount pins the autoplace-default-1 fallback. Three
// branches: nil filter (RG with no AutoSelectFilter set), zero
// PlaceCount, negative PlaceCount — all collapse to 1, matching
// upstream autoplacer's "1-replica until told otherwise" default.
//
// A regression that returned 0 here would make query-size-info
// claim infinite available volume size (the n-th-largest pool of
// zero pools is 0 → divide by zero in capacity rollup) and cause
// linstor-csi's CreateVolume sizing logic to either OOM or oversize.
func TestReplicaCount(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		f    *apiv1.AutoSelectFilter
		want int
	}{
		{"nil", nil, 1},
		{"zero", &apiv1.AutoSelectFilter{PlaceCount: 0}, 1},
		{"negative", &apiv1.AutoSelectFilter{PlaceCount: -3}, 1},
		{"explicit 1", &apiv1.AutoSelectFilter{PlaceCount: 1}, 1},
		{"explicit 3", &apiv1.AutoSelectFilter{PlaceCount: 3}, 3},
	}

	for _, c := range cases {
		got := replicaCount(c.f)
		if got != c.want {
			t.Errorf("%s: got %d, want %d", c.name, got, c.want)
		}
	}
}

// TestQuerySizeInfoMissingRG: POST /v1/resource-groups/{rg}/query-size-info
// with an unknown RG → 404. linstor-csi calls this on every CreateVolume
// to gate sizing; a regression that returned 5xx would flip golinstor's
// retry classification (5xx retryable, 4xx fatal) and bury operator typos
// in the RG name behind infinite retries.
func TestQuerySizeInfoMissingRG(t *testing.T) {
	t.Parallel()

	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpPost(t, base+"/v1/resource-groups/ghost/query-size-info", []byte(`{}`))
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", resp.StatusCode)
	}
}

// TestQueryAllSizeInfoEmpty: POST /v1/query-all-size-info on a cluster
// with no RGs → 200 + empty result map. Pinned because golinstor's
// CLI auto-completion runs this every keystroke; an empty cluster
// must produce a usable JSON response, not a 500.
func TestQueryAllSizeInfoEmpty(t *testing.T) {
	t.Parallel()

	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpPost(t, base+"/v1/query-all-size-info", []byte(`{}`))
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status: got %d, want 200", resp.StatusCode)
	}

	var got queryAllSizeInfoResponse
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if got.Result == nil {
		t.Errorf("Result: got nil map; want empty map (golinstor expects a JSON object, not null)")
	}
}

// TestQuerySizeInfoFreeCapacityRatioGate pins the
// `MaxFreeCapacityOversubscriptionRatio` gate: ZFS_THIN pool with
// 10 KiB free, ratio=2 → MaxVolumeSize=20 KiB (free × ratio).
//
// The pool prop overrides the controller default — that's the
// precedence upstream LINSTOR uses in PriorityProps. Without this,
// a 10 KiB ZFS_THIN pool would (a) silently advertise 200 KiB at the
// upstream default ratio of 20, and (b) ignore an operator's pool-
// scoped override entirely. Both are P1 capacity-guard failures.
func TestQuerySizeInfoFreeCapacityRatioGate(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	_ = st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "rg-thin",
		SelectFilter: apiv1.AutoSelectFilter{
			PlaceCount:  1,
			StoragePool: "pool",
		},
	})

	_ = st.StoragePools().Create(ctx, &apiv1.StoragePool{
		StoragePoolName: "pool",
		NodeName:        "n1",
		ProviderKind:    apiv1.StoragePoolKindZFSThin,
		Props: map[string]string{
			// Pool-level override of the free-capacity gate. Set the
			// total-capacity ratio too so it doesn't squash the free
			// gate (totalCap=20 × 2 = 40 > 20, so min(20, 40) = 20).
			"MaxFreeCapacityOversubscriptionRatio":  "2",
			"MaxTotalCapacityOversubscriptionRatio": "2",
		},
		FreeCapacity:  10,
		TotalCapacity: 20,
	})

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/resource-groups/rg-thin/query-size-info", []byte(`{}`))
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got querySizeInfoResponse

	_ = json.NewDecoder(resp.Body).Decode(&got)

	if got.SpaceInfo.MaxVlmSizeInKib != 20 {
		t.Errorf("max vol: got %d KiB, want 20 (free=10 × ratio=2)",
			got.SpaceInfo.MaxVlmSizeInKib)
	}
}

// TestQuerySizeInfoTotalCapacityRatioGate pins the
// `MaxTotalCapacityOversubscriptionRatio` clamp: a generous
// free-capacity ratio is overridden by a tighter total-capacity
// cap. Pool: total=100, free=80, ratios (free=10, total=2) →
// freeCap = 800, totalCap = 200 → min = 200.
//
// Without this clamp, a near-empty pool with a high free ratio
// would advertise wildly more than the underlying device can
// physically hold, and the gate would devolve to "free * ratio"
// alone.
func TestQuerySizeInfoTotalCapacityRatioGate(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	_ = st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "rg-total",
		SelectFilter: apiv1.AutoSelectFilter{
			PlaceCount:  1,
			StoragePool: "pool",
		},
	})

	_ = st.StoragePools().Create(ctx, &apiv1.StoragePool{
		StoragePoolName: "pool",
		NodeName:        "n1",
		ProviderKind:    apiv1.StoragePoolKindLVMThin,
		Props: map[string]string{
			"MaxFreeCapacityOversubscriptionRatio":  "10",
			"MaxTotalCapacityOversubscriptionRatio": "2",
		},
		FreeCapacity:  80,
		TotalCapacity: 100,
	})

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/resource-groups/rg-total/query-size-info", []byte(`{}`))
	defer func() { _ = resp.Body.Close() }()

	var got querySizeInfoResponse

	_ = json.NewDecoder(resp.Body).Decode(&got)

	if got.SpaceInfo.MaxVlmSizeInKib != 200 {
		t.Errorf("max vol: got %d KiB, want 200 (min(80×10, 100×2))",
			got.SpaceInfo.MaxVlmSizeInKib)
	}
}

// TestQuerySizeInfoOverallRatioFallback pins the
// `MaxOversubscriptionRatio` umbrella fallback: when neither
// MaxFreeCapacity... nor MaxTotalCapacity... is set, both fall
// back to the umbrella ratio. ratio=3 + free=10, total=100 →
// min(30, 300) = 30.
//
// This is the operator's single-knob "tune the whole cluster"
// path — without the fallback, setting just MaxOversubscriptionRatio
// would silently do nothing and operators would (rightly) declare
// the gate broken.
func TestQuerySizeInfoOverallRatioFallback(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	_ = st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "rg-overall",
		SelectFilter: apiv1.AutoSelectFilter{
			PlaceCount:  1,
			StoragePool: "pool",
		},
	})

	_ = st.StoragePools().Create(ctx, &apiv1.StoragePool{
		StoragePoolName: "pool",
		NodeName:        "n1",
		ProviderKind:    apiv1.StoragePoolKindLVMThin,
		Props: map[string]string{
			"MaxOversubscriptionRatio": "3",
		},
		FreeCapacity:  10,
		TotalCapacity: 100,
	})

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/resource-groups/rg-overall/query-size-info", []byte(`{}`))
	defer func() { _ = resp.Body.Close() }()

	var got querySizeInfoResponse

	_ = json.NewDecoder(resp.Body).Decode(&got)

	if got.SpaceInfo.MaxVlmSizeInKib != 30 {
		t.Errorf("max vol: got %d KiB, want 30 (10 × MaxOversubscriptionRatio=3)",
			got.SpaceInfo.MaxVlmSizeInKib)
	}
}

// TestQuerySizeInfoThickProviderIgnoresRatios pins that raw LVM /
// ZFS pools never oversubscribe — even if an operator (mistakenly)
// sets the ratio props, MaxVolumeSize stays at FreeCapacity.
// Storage backends that allocate physically can't honour the gate,
// so applying it would advertise space that doesn't exist.
func TestQuerySizeInfoThickProviderIgnoresRatios(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	_ = st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "rg-thick",
		SelectFilter: apiv1.AutoSelectFilter{
			PlaceCount:  1,
			StoragePool: "pool",
		},
	})

	_ = st.StoragePools().Create(ctx, &apiv1.StoragePool{
		StoragePoolName: "pool",
		NodeName:        "n1",
		ProviderKind:    apiv1.StoragePoolKindZFS, // thick
		Props: map[string]string{
			"MaxOversubscriptionRatio": "20",
		},
		FreeCapacity:  10,
		TotalCapacity: 20,
	})

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/resource-groups/rg-thick/query-size-info", []byte(`{}`))
	defer func() { _ = resp.Body.Close() }()

	var got querySizeInfoResponse

	_ = json.NewDecoder(resp.Body).Decode(&got)

	if got.SpaceInfo.MaxVlmSizeInKib != 10 {
		t.Errorf("max vol: got %d KiB, want 10 (thick pool: ratio ignored)",
			got.SpaceInfo.MaxVlmSizeInKib)
	}
}

// TestQuerySizeInfoThinPoolDefaultRatio pins the implicit default
// of 20 (LINSTOR's documented default in `properties.json`). Free=5,
// no overrides → cap = 5 × 20 = 100. This is what a fresh cluster
// will report; any operator-visible regression here means the
// gate has lost its default and would be the de-facto "free space"
// behaviour again.
func TestQuerySizeInfoThinPoolDefaultRatio(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	_ = st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "rg-default",
		SelectFilter: apiv1.AutoSelectFilter{
			PlaceCount:  1,
			StoragePool: "pool",
		},
	})

	_ = st.StoragePools().Create(ctx, &apiv1.StoragePool{
		StoragePoolName: "pool",
		NodeName:        "n1",
		ProviderKind:    apiv1.StoragePoolKindZFSThin,
		FreeCapacity:    5,
		TotalCapacity:   1000, // big enough that the total gate doesn't clamp
	})

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/resource-groups/rg-default/query-size-info", []byte(`{}`))
	defer func() { _ = resp.Body.Close() }()

	var got querySizeInfoResponse

	_ = json.NewDecoder(resp.Body).Decode(&got)

	if got.SpaceInfo.MaxVlmSizeInKib != 100 {
		t.Errorf("max vol: got %d KiB, want 100 (default ratio 20 × free 5)",
			got.SpaceInfo.MaxVlmSizeInKib)
	}
}

// TestQuerySizeInfoControllerRatioFallback: pool-level prop is
// absent, controller prop carries the gate, the pool should
// inherit it. Pins the precedence path (pool > controller >
// default) on the controller-side branch — the missing piece for
// "set it once cluster-wide" operator workflow.
func TestQuerySizeInfoControllerRatioFallback(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	_ = st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "rg-ctrl",
		SelectFilter: apiv1.AutoSelectFilter{
			PlaceCount:  1,
			StoragePool: "pool",
		},
	})

	_ = st.StoragePools().Create(ctx, &apiv1.StoragePool{
		StoragePoolName: "pool",
		NodeName:        "n1",
		ProviderKind:    apiv1.StoragePoolKindLVMThin,
		FreeCapacity:    10,
		TotalCapacity:   1000,
	})

	srv := &Server{
		Addr:      pickFreeAddr(t),
		Store:     st,
		Client:    newFakeRESTClient(t),
		Namespace: testRESTNamespace,
	}
	// Seed a controller-level MaxFreeCapacityOversubscriptionRatio.
	if err := applyControllerProps(ctx, srv.Client, &apiv1.GenericPropsModify{
		OverrideProps: map[string]string{
			"MaxFreeCapacityOversubscriptionRatio": "4",
		},
	}); err != nil {
		t.Fatalf("seed ctrl props: %v", err)
	}

	base, stop := startServerCustom(t, srv)
	defer stop()

	resp := httpPost(t, base+"/v1/resource-groups/rg-ctrl/query-size-info", []byte(`{}`))
	defer func() { _ = resp.Body.Close() }()

	var got querySizeInfoResponse

	_ = json.NewDecoder(resp.Body).Decode(&got)

	if got.SpaceInfo.MaxVlmSizeInKib != 40 {
		t.Errorf("max vol: got %d KiB, want 40 (controller ratio 4 × free 10)",
			got.SpaceInfo.MaxVlmSizeInKib)
	}
}

// TestQuerySizeInfoPoolPropOverridesController pins the precedence
// rule — when both layers carry the same key, the pool-level value
// wins. Same shape as upstream's PriorityProps walk.
func TestQuerySizeInfoPoolPropOverridesController(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	_ = st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "rg-prec",
		SelectFilter: apiv1.AutoSelectFilter{
			PlaceCount:  1,
			StoragePool: "pool",
		},
	})

	_ = st.StoragePools().Create(ctx, &apiv1.StoragePool{
		StoragePoolName: "pool",
		NodeName:        "n1",
		ProviderKind:    apiv1.StoragePoolKindLVMThin,
		Props: map[string]string{
			"MaxFreeCapacityOversubscriptionRatio": "5",
		},
		FreeCapacity:  10,
		TotalCapacity: 1000,
	})

	srv := &Server{
		Addr:      pickFreeAddr(t),
		Store:     st,
		Client:    newFakeRESTClient(t),
		Namespace: testRESTNamespace,
	}
	if err := applyControllerProps(ctx, srv.Client, &apiv1.GenericPropsModify{
		OverrideProps: map[string]string{
			// Controller says 100; pool says 5. Pool wins → cap 50.
			"MaxFreeCapacityOversubscriptionRatio": "100",
		},
	}); err != nil {
		t.Fatalf("seed ctrl props: %v", err)
	}

	base, stop := startServerCustom(t, srv)
	defer stop()

	resp := httpPost(t, base+"/v1/resource-groups/rg-prec/query-size-info", []byte(`{}`))
	defer func() { _ = resp.Body.Close() }()

	var got querySizeInfoResponse

	_ = json.NewDecoder(resp.Body).Decode(&got)

	if got.SpaceInfo.MaxVlmSizeInKib != 50 {
		t.Errorf("max vol: got %d KiB, want 50 (pool ratio 5 wins over controller 100)",
			got.SpaceInfo.MaxVlmSizeInKib)
	}
}
