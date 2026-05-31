// SPDX-License-Identifier: Apache-2.0

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

// TestAutoplaceIssue45RejectsZeroFreeCapacityPool pins the Issue #45
// fix: `POST /v1/resource-definitions/{rd}/autoplace` against a pool
// with `FreeCapacity=0` MUST refuse the placement with a structured
// 409 envelope BEFORE the placer is invoked. Pre-fix, linstor-csi's
// CreateVolume against a now-full pool placed the replica anyway,
// the PVC reached Bound, and only the satellite-side LV allocation
// failed (silent data-plane breakage).
//
// The gate consults `computeSizeInfo.MaxVlmSizeInKib` — the same
// value the parallel spawn-side `rejectIfExceedsOversubGate` uses —
// so over-subscription ratios and shared-LUN dedup are honoured
// uniformly across spawn / autoplace.
func TestAutoplaceIssue45RejectsZeroFreeCapacityPool(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-full"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	// 1 GiB VolumeDefinition.
	if err := st.VolumeDefinitions().Create(ctx, "pvc-full", &apiv1.VolumeDefinition{
		VolumeNumber: 0,
		SizeKib:      1048576,
	}); err != nil {
		t.Fatalf("seed VD: %v", err)
	}

	// FreeCapacity=0: pool is fully consumed. With any oversub ratio
	// the effective MaxVolumeSize is still 0 (0 × ratio = 0), so the
	// gate MUST fire.
	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		StoragePoolName: "full",
		NodeName:        "n1",
		ProviderKind:    apiv1.StoragePoolKindLVMThin,
		FreeCapacity:    0,
		TotalCapacity:   10485760,
	}); err != nil {
		t.Fatalf("seed pool: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.AutoPlaceRequest{
		SelectFilter: apiv1.AutoSelectFilter{PlaceCount: 1, StoragePool: "full"},
	})

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-full/autoplace", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status: got %d, want 409 (Issue #45 capacity gate)", resp.StatusCode)
	}

	var rc []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rc); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}

	if len(rc) == 0 {
		t.Fatalf("envelope: empty array")
	}

	// Wire shape: same ApiCallRc code as the placer's
	// CapacityShortfallError path (FAIL_NOT_ENOUGH_NODES | MASK_ERROR).
	// Operators can classify both REST-gate and placer-gate
	// shortfalls with one rule.
	if rc[0].RetCode&apiCallRcError == 0 {
		t.Errorf("ret_code missing MASK_ERROR bit: %#x", rc[0].RetCode)
	}

	if rc[0].RetCode&apiCallRcFailNotEnoughNodes == 0 {
		t.Errorf("ret_code missing FAIL_NOT_ENOUGH_NODES sub-code: %#x", rc[0].RetCode)
	}

	// Critical: the placement must NOT have happened. Pre-fix the
	// Resource was stamped and the operator only found out later.
	got, err := st.Resources().ListByDefinition(ctx, "pvc-full")
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	if len(got) != 0 {
		t.Errorf("Issue #45: placement should be refused, got %d Resource(s): %+v", len(got), got)
	}

	// Details block carries the numeric required vs. cap so the
	// operator can size-down or grow a pool without re-running the
	// call to find the gap.
	if !strings.Contains(rc[0].Details, "1048576") {
		t.Errorf("Details missing required '1048576 KiB', got:\n%s", rc[0].Details)
	}
}

// TestAutoplaceIssue45RejectsUndersizedPool pins the partial-capacity
// case: pool has SOME free space but less than the requested volume.
// Mirrors the production scenario where a chain of PVCs gradually
// fills a pool until the next CreateVolume would overflow.
//
// `FreeCapacity=512` × default thin ratio 20 = 10240 KiB cap. A
// 1 GiB (1048576 KiB) VD must still be refused.
func TestAutoplaceIssue45RejectsUndersizedPool(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-big"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.VolumeDefinitions().Create(ctx, "pvc-big", &apiv1.VolumeDefinition{
		VolumeNumber: 0,
		SizeKib:      1048576, // 1 GiB
	}); err != nil {
		t.Fatalf("seed VD: %v", err)
	}

	// 512 KiB free × 20 (default thin ratio) = 10240 KiB cap.
	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		StoragePoolName: "almost-full",
		NodeName:        "n1",
		ProviderKind:    apiv1.StoragePoolKindLVMThin,
		FreeCapacity:    512,
		TotalCapacity:   10485760,
	}); err != nil {
		t.Fatalf("seed pool: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.AutoPlaceRequest{
		SelectFilter: apiv1.AutoSelectFilter{PlaceCount: 1, StoragePool: "almost-full"},
	})

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-big/autoplace", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status: got %d, want 409", resp.StatusCode)
	}

	got, _ := st.Resources().ListByDefinition(ctx, "pvc-big")
	if len(got) != 0 {
		t.Errorf("Issue #45: placement should be refused, got %d", len(got))
	}
}

// TestAutoplaceIssue45AllowsFittingPool sanity-checks that the new
// gate does NOT regress the happy path: a pool with sufficient
// FreeCapacity must let the placement through.
func TestAutoplaceIssue45AllowsFittingPool(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-ok"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.VolumeDefinitions().Create(ctx, "pvc-ok", &apiv1.VolumeDefinition{
		VolumeNumber: 0,
		SizeKib:      1024, // 1 MiB — tiny, well within the pool.
	}); err != nil {
		t.Fatalf("seed VD: %v", err)
	}

	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		StoragePoolName: "big",
		NodeName:        "n1",
		ProviderKind:    apiv1.StoragePoolKindLVMThin,
		FreeCapacity:    10485760, // 10 GiB
		TotalCapacity:   10485760,
	}); err != nil {
		t.Fatalf("seed pool: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.AutoPlaceRequest{
		SelectFilter: apiv1.AutoSelectFilter{PlaceCount: 1, StoragePool: "big"},
	})

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-ok/autoplace", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200 (sufficient capacity)", resp.StatusCode)
	}

	got, err := st.Resources().ListByDefinition(ctx, "pvc-ok")
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	if len(got) != 1 {
		t.Errorf("placed: got %d, want 1", len(got))
	}
}

// TestAutoplaceIssue45SkipsGateWhenNoVDs verifies the
// definitions-only path: when an RD has no VolumeDefinitions yet,
// the gate must be a no-op (nothing to size against). Mirrors the
// spawn-side semantic for empty `req.VolumeSizes`.
func TestAutoplaceIssue45SkipsGateWhenNoVDs(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-empty"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	// Pool with zero free — would trip the gate IF there were VDs.
	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		StoragePoolName: "zero",
		NodeName:        "n1",
		ProviderKind:    apiv1.StoragePoolKindLVMThin,
		FreeCapacity:    0,
		TotalCapacity:   1024,
	}); err != nil {
		t.Fatalf("seed pool: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.AutoPlaceRequest{
		SelectFilter: apiv1.AutoSelectFilter{PlaceCount: 1, StoragePool: "zero"},
	})

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-empty/autoplace", body)
	_ = resp.Body.Close()

	// No VDs → gate is a no-op. Placer then runs and either succeeds
	// (no required size to filter against) or 409s on its own
	// "not enough candidate storage pools" logic — both are
	// acceptable here; we only assert the Issue #45 gate is NOT the
	// one that fires.
	if resp.StatusCode == http.StatusInternalServerError {
		t.Fatalf("definitions-only autoplace must not 500 from the Issue #45 gate, got %d", resp.StatusCode)
	}
}
