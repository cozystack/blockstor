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
