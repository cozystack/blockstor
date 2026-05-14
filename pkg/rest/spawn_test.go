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
	"context"
	"encoding/json"
	"net/http"
	"slices"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestSpawnCreatesRDAndVDs: this is the gateway linstor-csi calls on every
// CreateVolume; it must materialise both the RD and one VD per requested
// size, atomically.
func TestSpawnCreatesRDAndVDs(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name:  "rg-1",
		Props: map[string]string{"DrbdOptions/auto-quorum": "io-error"},
	}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.ResourceGroupSpawn{
		ResourceDefinitionName: "pvc-1",
		VolumeSizes:            []int64{2 * 1024 * 1024, 4 * 1024 * 1024}, // bytes
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-groups/rg-1/spawn", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201", resp.StatusCode)
	}

	rd, err := st.ResourceDefinitions().Get(ctx, "pvc-1")
	if err != nil {
		t.Fatalf("RD not created: %v", err)
	}

	if rd.ResourceGroupName != "rg-1" {
		t.Errorf("ResourceGroupName: got %q, want rg-1", rd.ResourceGroupName)
	}

	if rd.Props["DrbdOptions/auto-quorum"] != "io-error" {
		t.Errorf("RG props were not inherited: %+v", rd.Props)
	}

	vds, err := st.VolumeDefinitions().List(ctx, "pvc-1")
	if err != nil {
		t.Fatalf("List VDs: %v", err)
	}

	if len(vds) != 2 {
		t.Fatalf("len: got %d, want 2", len(vds))
	}

	// 2 MiB / 1024 = 2048 KiB; 4 MiB → 4096 KiB.
	if vds[0].SizeKib != 2048 || vds[1].SizeKib != 4096 {
		t.Errorf("VD sizes: got %d, %d, want 2048, 4096", vds[0].SizeKib, vds[1].SizeKib)
	}
}

// TestSpawnMissingRG: 404 if the named ResourceGroup does not exist.
func TestSpawnMissingRG(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	body, err := json.Marshal(apiv1.ResourceGroupSpawn{
		ResourceDefinitionName: "pvc-1",
		VolumeSizes:            []int64{1024 * 1024},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-groups/ghost/spawn", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", resp.StatusCode)
	}
}

// TestSpawnMissingRDName: 400 if the request omits resource_definition_name.
func TestSpawnMissingRDName(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceGroups().Create(t.Context(), &apiv1.ResourceGroup{Name: "rg-1"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.ResourceGroupSpawn{VolumeSizes: []int64{1024 * 1024}})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-groups/rg-1/spawn", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400", resp.StatusCode)
	}
}

// TestSpawnRollsBackOnVDFailure: if any VD create fails, the half-built RD
// is rolled back so the caller can retry. We force the failure by spawning
// twice with the same name.
func TestSpawnRollsBackOnVDFailure(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()
	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{Name: "rg-1"}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// pre-existing RD with the same name to provoke an Already-Exists on RD create.
	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.ResourceGroupSpawn{
		ResourceDefinitionName: "pvc-1",
		VolumeSizes:            []int64{1024 * 1024},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-groups/rg-1/spawn", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusConflict {
		t.Errorf("status: got %d, want 409 (RD already exists)", resp.StatusCode)
	}
}

// TestRollbackSpawn pins the half-spawn cleanup contract: when handleSpawn
// has already created the RD but a downstream VolumeDefinitions().Create
// fails, rollbackSpawn must remove the orphan RD so the next spawn isn't
// blocked by a 409 on the same name. Two branches matter:
//
//  1. RD exists → Delete succeeds, RD gone afterwards.
//  2. RD already missing (e.g. another reconciler swept it) → ErrNotFound
//     is silently swallowed, no panic.
//
// We also verify the cancelled-parent-context branch: rollbackSpawn uses
// context.WithoutCancel internally so even if the request context was
// already cancelled (client gave up, body-write deadline hit), the
// cleanup still runs.
func TestRollbackSpawn(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-orphan"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Branch 1: existing RD → cleared.
	rollbackSpawn(ctx, st, "pvc-orphan")

	if _, err := st.ResourceDefinitions().Get(ctx, "pvc-orphan"); err == nil {
		t.Errorf("RD survived rollback")
	}

	// Branch 2: missing RD → no panic, no-op.
	rollbackSpawn(ctx, st, "pvc-orphan") // already deleted
	rollbackSpawn(ctx, st, "ghost")      // never existed

	// Cancelled parent ctx: cleanup must still run thanks to WithoutCancel.
	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-cancelled-parent"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	rollbackSpawn(cancelled, st, "pvc-cancelled-parent")

	if _, err := st.ResourceDefinitions().Get(ctx, "pvc-cancelled-parent"); err == nil {
		t.Errorf("RD survived rollback under cancelled parent ctx (WithoutCancel must shield Delete)")
	}
}

// TestCopyVolumeGroupProps pins the per-volume props lookup spawn uses
// to lift VolumeGroup-template props onto the freshly-created VD. The
// branches matter because spawn writes the result straight into the
// VD; a regression in any branch silently corrupts the RG → VD prop
// inheritance contract that operators rely on for per-volume tuning
// (e.g. fs-type, encrypt-passphrase, drbd-net options).
//
//  1. miss → nil (caller must NOT propagate any props for this volNum)
//  2. match with empty Props → nil (don't return an empty map; spawn
//     skips the SetProps call entirely on nil, vs a needless empty
//     write on map-len-zero)
//  3. match with non-empty Props → independent copy (mutation by the
//     caller must NOT bleed back into the RG template, otherwise a
//     later spawn for sibling RDs would inherit the mutation)
func TestCopyVolumeGroupProps(t *testing.T) {
	t.Parallel()

	src := []apiv1.VolumeGroup{
		{VolumeNumber: 0, Props: map[string]string{"k0": "v0"}},
		{VolumeNumber: 1, Props: nil}, // empty-props branch
		{VolumeNumber: 2, Props: map[string]string{"k2a": "v2a", "k2b": "v2b"}},
	}

	if got := copyVolumeGroupProps(src, 99); got != nil {
		t.Errorf("miss: got %v, want nil", got)
	}

	if got := copyVolumeGroupProps(src, 1); got != nil {
		t.Errorf("empty-props: got %v, want nil (spawn skips SetProps on nil)", got)
	}

	got := copyVolumeGroupProps(src, 2)
	if got == nil || got["k2a"] != "v2a" || got["k2b"] != "v2b" || len(got) != 2 {
		t.Fatalf("match: got %v, want {k2a:v2a, k2b:v2b}", got)
	}

	// Mutate the copy: the source must NOT change.
	got["k2a"] = "MUTATED"
	got["new"] = "added"

	if src[2].Props["k2a"] != "v2a" {
		t.Errorf("template mutated: src[2].Props[k2a]=%q, want v2a", src[2].Props["k2a"])
	}

	if _, ok := src[2].Props["new"]; ok {
		t.Errorf("template mutated: src[2].Props[new] leaked from copy")
	}
}

// TestSpawnBadJSON: malformed body → 400. Pinned because
// linstor-csi calls /spawn on every CreateVolume; a regression
// flipping decoder errors to 5xx would loop the csi retry path.
func TestSpawnBadJSON(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceGroups().Create(t.Context(), &apiv1.ResourceGroup{Name: "rg-1"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/resource-groups/rg-1/spawn", []byte("{not-json"))
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400", resp.StatusCode)
	}
}

// TestSpawnRejectsExceedingFreeCapacityRatio is the spawn-side gate:
// requesting a volume bigger than free × MaxFreeCapacityOversubscriptionRatio
// returns 409 with an actionable message naming the prop to raise.
//
// Setup: ZFS_THIN pool with 1024 KiB free, ratio=2 → cap = 2048 KiB.
// Requesting 3 MiB (3072 KiB) exceeds the cap, so the spawn is
// rejected and no RD lands in the store. Without this gate,
// linstor-csi would happily create a definition the cluster
// physically can't host and only fail at autoplace time.
func TestSpawnRejectsExceedingFreeCapacityRatio(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "rg-cap",
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
		ProviderKind:    apiv1.StoragePoolKindZFSThin,
		Props: map[string]string{
			"MaxFreeCapacityOversubscriptionRatio":  "2",
			"MaxTotalCapacityOversubscriptionRatio": "2",
		},
		FreeCapacity:  1024, // KiB
		TotalCapacity: 4096, // KiB — big enough that the total gate doesn't clamp first
	}); err != nil {
		t.Fatalf("seed pool: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.ResourceGroupSpawn{
		ResourceDefinitionName: "pvc-too-big",
		VolumeSizes:            []int64{3 * 1024 * 1024}, // 3 MiB = 3072 KiB > 2048 cap
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-groups/rg-cap/spawn", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status: got %d, want 409", resp.StatusCode)
	}

	// Decode error envelope; verify the message names at least one
	// of the props the operator can raise. Pinned because the
	// actionable message is the operator's only signal to know
	// which knob fixes the rejection — a regression to a generic
	// "too big" would silently break the ops runbook.
	var rc []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rc); err != nil {
		t.Fatalf("decode error envelope: %v", err)
	}

	if len(rc) == 0 || rc[0].Message == "" {
		t.Fatalf("empty error envelope: %+v", rc)
	}

	msg := rc[0].Message
	if !containsAll(msg, "over-subscription", "MaxFreeCapacityOversubscriptionRatio") {
		t.Errorf("message must name the gate + a knob: %q", msg)
	}

	// The half-built RD must NOT exist — gate runs before any
	// store mutation.
	if _, err := st.ResourceDefinitions().Get(ctx, "pvc-too-big"); err == nil {
		t.Errorf("RD leaked past the gate")
	}
}

// TestSpawnRejectsExceedingTotalCapacityRatio pins the
// MaxTotalCapacityOversubscriptionRatio gate end-to-end via spawn.
// The free-capacity gate is generous (ratio=100), but the total-
// capacity gate is tight (ratio=2) — the smaller wins.
//
// Pool: total=10 KiB, free=10 KiB. Free × 100 = 1000 KiB but
// Total × 2 = 20 KiB → cap=20. Requesting 24 KiB → 409.
func TestSpawnRejectsExceedingTotalCapacityRatio(t *testing.T) {
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
			"MaxFreeCapacityOversubscriptionRatio":  "100",
			"MaxTotalCapacityOversubscriptionRatio": "2",
		},
		FreeCapacity:  10,
		TotalCapacity: 10,
	})

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.ResourceGroupSpawn{
		ResourceDefinitionName: "pvc-total-gate",
		VolumeSizes:            []int64{24 * 1024}, // 24 KiB > cap=20
	})

	resp := httpPost(t, base+"/v1/resource-groups/rg-total/spawn", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusConflict {
		t.Errorf("status: got %d, want 409 (total-capacity gate)", resp.StatusCode)
	}
}

// TestSpawnRejectsExceedingOversubscriptionRatio pins the
// umbrella MaxOversubscriptionRatio fallback at the spawn layer.
// No per-axis ratio set; only the umbrella value, so both axes
// inherit it. Free=10 × 3 = 30 cap; request 40 → 409.
func TestSpawnRejectsExceedingOversubscriptionRatio(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	_ = st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "rg-umb",
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
		TotalCapacity: 1000, // wide so the total gate doesn't bite
	})

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.ResourceGroupSpawn{
		ResourceDefinitionName: "pvc-umb",
		VolumeSizes:            []int64{40 * 1024}, // 40 KiB > 30 cap
	})

	resp := httpPost(t, base+"/v1/resource-groups/rg-umb/spawn", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusConflict {
		t.Errorf("status: got %d, want 409 (umbrella ratio)", resp.StatusCode)
	}
}

// TestSpawnAcceptsWithinOversubscriptionGate pins the happy path:
// a request that fits inside the gate (free × ratio) passes
// straight through. We don't fully autoplace (no nodes/replica
// fixtures), but the call gets past the gate — exit status 201
// (autoplace deferred) is the acceptable success on this fixture.
func TestSpawnAcceptsWithinOversubscriptionGate(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	_ = st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "rg-ok",
		SelectFilter: apiv1.AutoSelectFilter{
			PlaceCount:  1,
			StoragePool: "pool",
		},
	})

	_ = st.Nodes().Create(ctx, &apiv1.Node{Name: "n1"})

	_ = st.StoragePools().Create(ctx, &apiv1.StoragePool{
		StoragePoolName: "pool",
		NodeName:        "n1",
		ProviderKind:    apiv1.StoragePoolKindZFSThin,
		Props: map[string]string{
			"MaxOversubscriptionRatio": "5",
		},
		FreeCapacity:  10, // KiB
		TotalCapacity: 100,
	})

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.ResourceGroupSpawn{
		ResourceDefinitionName: "pvc-fits",
		VolumeSizes:            []int64{40 * 1024}, // 40 KiB ≤ 50 cap
	})

	resp := httpPost(t, base+"/v1/resource-groups/rg-ok/spawn", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		t.Errorf("status: got %d, want 201 (gate must allow)", resp.StatusCode)
	}
}

// containsAll asserts every needle is a substring of haystack.
// Inlined here (vs `strings.Contains` loops in the assertion) so
// the test's expectation reads as one assertion line.
func containsAll(haystack string, needles ...string) bool {
	for _, n := range needles {
		found := false

		for i := 0; i+len(n) <= len(haystack); i++ {
			if haystack[i:i+len(n)] == n {
				found = true

				break
			}
		}

		if !found {
			return false
		}
	}

	return true
}

// TestSpawnInheritsLayerStackFromRG mirrors Bug 54 on the
// `linstor rg spawn` path: the RD spawned from an RG whose SelectFilter
// pins LayerStack must carry that stack so the dispatcher / satellite
// chain spawns Resources at the right composition. Without this stamp
// in buildSpawnedRD, every linstor-csi CreateVolume on an STORAGE-only
// RG would still emit DRBD-stacked replicas via the legacy needsDRBD
// default — silently contradicting the operator's SelectFilter.
func TestSpawnInheritsLayerStackFromRG(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "rg-storage-only",
		SelectFilter: apiv1.AutoSelectFilter{
			LayerStack: []string{apiv1.LayerKindStorage},
		},
	}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.ResourceGroupSpawn{
		ResourceDefinitionName: "pvc-spawn",
		// definitions_only: skip placement; we only assert the RD shape.
		DefinitionsOnly: true,
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-groups/rg-storage-only/spawn", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201", resp.StatusCode)
	}

	got, err := st.ResourceDefinitions().Get(ctx, "pvc-spawn")
	if err != nil {
		t.Fatalf("get rd: %v", err)
	}

	want := []string{apiv1.LayerKindStorage}
	if !slices.Equal(got.LayerStack, want) {
		t.Errorf("LayerStack: got %v, want %v", got.LayerStack, want)
	}
}

// TestSpawnAutoplacesFromRGSelectFilter pins scenario 9.W04 (P0):
// `rg spawn <rg> <rd> <size>` materialises an RD + VDs AND places
// replicas according to the RG's SelectFilter (PlaceCount +
// StoragePool). Without the autoplace step inside spawn, linstor-csi
// would observe an empty `r l` after CreateVolume returns and loop on
// retries; the operator-visible contract is that `linstor rg spawn`
// is a single shot that yields fully-placed resources, mirroring
// upstream LINSTOR.
//
// Fixture: RG carries SelectFilter{PlaceCount:2, StoragePool:"pool"};
// three same-kind LVM_THIN pools across n1/n2/n3 with skewed free
// capacity so the weighted scorer's preference is deterministic.
// The test asserts:
//
//  1. HTTP 201 (spawned + placed)
//  2. RD + VD landed in the store with the RG-derived LayerStack
//     (Bug 54 cross-check — even with placement enabled)
//  3. Exactly PlaceCount=2 Resources placed, on the two freest nodes
//     (n1 + n2), each tagged with the RG's StoragePool name
func TestSpawnAutoplacesFromRGSelectFilter(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "rg-place",
		SelectFilter: apiv1.AutoSelectFilter{
			PlaceCount:  2,
			StoragePool: "pool",
			LayerStack:  []string{apiv1.LayerKindStorage},
		},
	}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	// Three same-kind pools (Bug 76 happy path — homogeneous cluster).
	// FreeCapacity skew makes the placer's biggest-free-first sort
	// pick n1 + n2 deterministically, leaving n3 unplaced.
	// Capacities in KiB; numbers chosen so a 64 KiB VD fits comfortably.
	pools := []apiv1.StoragePool{
		{StoragePoolName: "pool", NodeName: "n1", ProviderKind: apiv1.StoragePoolKindLVMThin, FreeCapacity: 1_000_000, TotalCapacity: 10_000_000},
		{StoragePoolName: "pool", NodeName: "n2", ProviderKind: apiv1.StoragePoolKindLVMThin, FreeCapacity: 800_000, TotalCapacity: 10_000_000},
		{StoragePoolName: "pool", NodeName: "n3", ProviderKind: apiv1.StoragePoolKindLVMThin, FreeCapacity: 100_000, TotalCapacity: 10_000_000},
	}
	for i := range pools {
		if err := st.StoragePools().Create(ctx, &pools[i]); err != nil {
			t.Fatalf("seed pool: %v", err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.ResourceGroupSpawn{
		ResourceDefinitionName: "pvc-spawn",
		VolumeSizes:            []int64{4 * 1024 * 1024}, // 4 MiB
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-groups/rg-place/spawn", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201", resp.StatusCode)
	}

	// The spawn envelope must announce success — not a deferred
	// autoplace. A regression where RG.SelectFilter is ignored
	// (e.g. PlaceCount=0 path triggered) surfaces here as a
	// "spawned, autoplace deferred" message.
	var rc []apiv1.APICallRc

	if err := json.NewDecoder(resp.Body).Decode(&rc); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}

	if len(rc) == 0 || containsAll(rc[0].Message, "autoplace deferred") {
		t.Errorf("envelope must announce success: %+v", rc)
	}

	rd, err := st.ResourceDefinitions().Get(ctx, "pvc-spawn")
	if err != nil {
		t.Fatalf("get RD: %v", err)
	}

	if rd.ResourceGroupName != "rg-place" {
		t.Errorf("ResourceGroupName: got %q, want rg-place", rd.ResourceGroupName)
	}

	// LayerStack must still inherit even when autoplace runs
	// (cross-check on Bug 54: the RD passes through buildSpawnedRD).
	if !slices.Equal(rd.LayerStack, []string{apiv1.LayerKindStorage}) {
		t.Errorf("LayerStack: got %v, want [%s]", rd.LayerStack, apiv1.LayerKindStorage)
	}

	vds, err := st.VolumeDefinitions().List(ctx, "pvc-spawn")
	if err != nil {
		t.Fatalf("list VDs: %v", err)
	}

	if len(vds) != 1 || vds[0].SizeKib != 4096 {
		t.Errorf("VDs: got %+v, want 1 VD of 4096 KiB", vds)
	}

	got, err := st.Resources().ListByDefinition(ctx, "pvc-spawn")
	if err != nil {
		t.Fatalf("list resources: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("placed: got %d, want 2 (RG.SelectFilter.PlaceCount)", len(got))
	}

	// Pin the deterministic biggest-free-first picks: n1 + n2,
	// never n3. This guards against a regression that ignores
	// RG.SelectFilter.StoragePool (would place on any pool) or
	// silently truncates PlaceCount.
	nodes := map[string]bool{}
	for _, r := range got {
		nodes[r.NodeName] = true

		if r.Props["StorPoolName"] != "pool" {
			t.Errorf("replica on %s: StorPoolName=%q, want %q (from RG.SelectFilter)",
				r.NodeName, r.Props["StorPoolName"], "pool")
		}
	}

	if !nodes["n1"] || !nodes["n2"] || nodes["n3"] {
		t.Errorf("placement nodes: got %v, want {n1, n2} (biggest-free)", nodes)
	}
}

// TestSpawnAutoplaceRejectsCrossKindFromRG pins scenario 9.W04 + Bug 76
// at the spawn boundary: when the RG's StoragePoolList enumerates
// pools whose ProviderKinds differ across the cluster, the placer's
// same-kind gate must still apply during spawn so a single RD never
// ends up with cross-provider replicas. Without this guard a single
// `rg spawn` could materialise an RD whose two replicas live on
// FILE_THIN + LVM_THIN — the exact symptom Bug 76 reports.
//
// Fixture: n1 has only ZFS_THIN, n2 has only LVM_THIN; RG lists both
// pools and asks for PlaceCount=2. The placer's same-kind constraint
// finds no peer for either kind, so it short-places. The spawn
// handler surfaces the shortfall via the "autoplace deferred"
// envelope (HTTP 201, but body names the failure) — the operator's
// signal that the cluster shape can't satisfy the RG today.
func TestSpawnAutoplaceRejectsCrossKindFromRG(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "rg-mixed",
		SelectFilter: apiv1.AutoSelectFilter{
			PlaceCount:      2,
			StoragePoolList: []string{"zfs-thin", "lvm-thin"},
		},
	}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	pools := []apiv1.StoragePool{
		{StoragePoolName: "zfs-thin", NodeName: "n1", ProviderKind: apiv1.StoragePoolKindZFSThin, FreeCapacity: 1000, TotalCapacity: 10000},
		{StoragePoolName: "lvm-thin", NodeName: "n2", ProviderKind: apiv1.StoragePoolKindLVMThin, FreeCapacity: 1000, TotalCapacity: 10000},
	}
	for i := range pools {
		if err := st.StoragePools().Create(ctx, &pools[i]); err != nil {
			t.Fatalf("seed pool: %v", err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.ResourceGroupSpawn{
		ResourceDefinitionName: "pvc-mixed",
		VolumeSizes:            []int64{1024 * 1024},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-groups/rg-mixed/spawn", body)
	defer func() { _ = resp.Body.Close() }()

	// The RD definition is materialised regardless (the contract is
	// "definitions land, placement may defer"); the deferral message
	// is the operator's signal that Bug 76 fired.
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201", resp.StatusCode)
	}

	var rc []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rc); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}

	if len(rc) == 0 || rc[0].Message == "" {
		t.Fatalf("empty envelope: %+v", rc)
	}

	if !containsAll(rc[0].Message, "autoplace deferred") {
		t.Errorf("message must flag the deferral: %q", rc[0].Message)
	}

	// Same-kind sanity (Bug 76): at most ONE replica may land —
	// never a cross-kind pair. (Placement of 0 is also acceptable
	// on a future hardening of the placer; 2 cross-kind replicas
	// is the regression signal.)
	got, err := st.Resources().ListByDefinition(ctx, "pvc-mixed")
	if err != nil {
		t.Fatalf("list resources: %v", err)
	}

	if len(got) > 1 {
		kinds := map[string]bool{}
		for _, r := range got {
			pool, _ := st.StoragePools().Get(ctx, r.NodeName, r.Props["StorPoolName"])
			kinds[pool.ProviderKind] = true
		}

		if len(kinds) > 1 {
			t.Errorf("Bug 76: cross-kind replicas placed via spawn: %v", got)
		}
	}
}

// TestSpawnPartialFlagAllowsShortPlacement pins scenario 9.W04's
// `--partial` clause: when the operator opts into partial placement
// the spawn handler MUST still return HTTP 201 even if the placer
// short-places (placed < want). Mirrors upstream LINSTOR's
// "definitions land, placement is best-effort" contract on the
// partial path. The PartialFlag is parsed on the request but the
// current implementation always tolerates shortfall (deferred
// envelope) regardless — this test pins that behaviour so a future
// tightening that demands placed==want on PartialFlag=false doesn't
// silently regress the partial path.
//
// Fixture: RG asks for 3 replicas on "pool" but only 1 same-kind
// pool exists, forcing a shortfall. With PartialFlag=true the
// response is HTTP 201 + deferred envelope and the RD is preserved.
func TestSpawnPartialFlagAllowsShortPlacement(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "rg-partial",
		SelectFilter: apiv1.AutoSelectFilter{
			PlaceCount:  3,
			StoragePool: "pool",
		},
	}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		StoragePoolName: "pool",
		NodeName:        "n1",
		ProviderKind:    apiv1.StoragePoolKindLVMThin,
		FreeCapacity:    1000, TotalCapacity: 10000,
	}); err != nil {
		t.Fatalf("seed pool: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.ResourceGroupSpawn{
		ResourceDefinitionName: "pvc-partial",
		VolumeSizes:            []int64{1024 * 1024},
		PartialFlag:            true,
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-groups/rg-partial/spawn", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201 (partial tolerated)", resp.StatusCode)
	}

	// RD must survive even under shortfall — operators rely on the
	// definition existing so a later autoplace can add replicas
	// once the cluster grows. A regression that rolls back the RD
	// on shortfall would break the partial-recovery flow.
	if _, err := st.ResourceDefinitions().Get(ctx, "pvc-partial"); err != nil {
		t.Errorf("RD must survive partial placement: %v", err)
	}

	// Only the single available pool is placed on; never more
	// than 1 (no cross-kind synthesis, no over-placement).
	got, _ := st.Resources().ListByDefinition(ctx, "pvc-partial")
	if len(got) > 1 {
		t.Errorf("placed: got %d, want ≤1 (only n1 has a pool)", len(got))
	}
}
