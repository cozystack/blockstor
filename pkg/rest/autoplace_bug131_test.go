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

// File: bug 131 (P1) — autoplace silently drops
// `select_filter.provider_list`.
//
// Per the v3 report: when CSI / CLI submits
//
//	POST /v1/resource-definitions/<rd>/autoplace
//	{"select_filter":{"provider_list":["ZFS"],"place_count":1}}
//
// the placer DOES NOT enforce the constraint — replicas land on any
// matching SP regardless of provider kind. The decode path in
// `pkg/api/v1.AutoSelectFilter` is fine, the placer's
// `matchesPoolFilter` already implements ProviderList enforcement,
// but `mergeAutoplaceFilter` in `pkg/rest/autoplace.go` never copied
// req.ProviderList onto the merged filter — so the placer received
// an empty list and treated every pool as eligible.
//
// Tests below pin the four observable contracts:
//
//   - happy path: provider_list=["ZFS"] with 1 LVM_THIN + 1 ZFS pool
//     lands the replica on the ZFS pool.
//   - hard refusal: provider_list=["INVALID_PROVIDER"] with no
//     matching pool yields 409 + an envelope naming the operator-
//     supplied allow-list value.
//   - multi-value: provider_list=["ZFS","LVM_THIN"] accepts either.
//   - regression: empty provider_list preserves the old behaviour
//     (any kind eligible).

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestBug131ProviderListFiltersOutNonMatchingSP: with one LVM_THIN
// pool on n1 and one ZFS pool on n2, an autoplace call carrying
// `provider_list=["ZFS"]` and `place_count=1` MUST place the single
// replica on n2's ZFS pool. Pre-fix the placer received an empty
// ProviderList from mergeAutoplaceFilter and would land on either
// pool depending on the weighted scorer.
func TestBug131ProviderListFiltersOutNonMatchingSP(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	pools := []apiv1.StoragePool{
		{StoragePoolName: "lvmpool", NodeName: "n1", ProviderKind: apiv1.StoragePoolKindLVMThin, FreeCapacity: 9000, TotalCapacity: 10000},
		{StoragePoolName: "zfspool", NodeName: "n2", ProviderKind: apiv1.StoragePoolKindZFS, FreeCapacity: 1000, TotalCapacity: 10000},
	}
	for i := range pools {
		if err := st.StoragePools().Create(ctx, &pools[i]); err != nil {
			t.Fatalf("seed pool: %v", err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.AutoPlaceRequest{
		SelectFilter: apiv1.AutoSelectFilter{
			PlaceCount:   1,
			ProviderList: []string{apiv1.StoragePoolKindZFS},
		},
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

	if len(got) != 1 {
		t.Fatalf("placed: got %d, want 1", len(got))
	}

	// The placement MUST be on the ZFS pool's node. Pre-fix the scorer
	// preferred n1 (9000 free vs 1000 free) so this test FAILS on main:
	// it lands on n1's LVM_THIN pool even though provider_list=["ZFS"].
	if got[0].NodeName != "n2" {
		t.Errorf("placement node: got %s, want n2 (the ZFS pool)", got[0].NodeName)
	}

	if pool := got[0].Props["StorPoolName"]; pool != "zfspool" {
		t.Errorf("placement pool: got %q, want %q", pool, "zfspool")
	}
}

// TestBug131ProviderListWithNoMatchingSPRefuses: provider_list points
// at a kind that no candidate pool advertises. Placer MUST refuse
// (409 — "Not enough available nodes" envelope) instead of silently
// landing on the wrong-kind pool. The rendered envelope MUST mention
// the operator-supplied allow-list value so the operator can tell
// "no candidate" from "you typo'd the provider name".
func TestBug131ProviderListWithNoMatchingSPRefuses(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		StoragePoolName: "lvmpool",
		NodeName:        "n1",
		ProviderKind:    apiv1.StoragePoolKindLVMThin,
		FreeCapacity:    9000,
		TotalCapacity:   10000,
	}); err != nil {
		t.Fatalf("seed pool: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.AutoPlaceRequest{
		SelectFilter: apiv1.AutoSelectFilter{
			PlaceCount:   1,
			ProviderList: []string{"INVALID_PROVIDER"},
		},
	})

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/autoplace", body)
	defer resp.Body.Close()

	// Pre-fix this returns 200 — the constraint is silently dropped.
	if resp.StatusCode != http.StatusConflict {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: got %d, want 409. body: %s", resp.StatusCode, string(raw))
	}

	// Envelope MUST name the operator-supplied provider so the CLI
	// renders an actionable diagnostic instead of a generic shortfall.
	raw, _ := io.ReadAll(resp.Body)

	var rcs []apiv1.APICallRc
	if err := json.Unmarshal(raw, &rcs); err != nil {
		t.Fatalf("decode envelope: %v. body: %s", err, string(raw))
	}

	if len(rcs) == 0 {
		t.Fatalf("envelope empty: %s", string(raw))
	}

	if !strings.Contains(rcs[0].Details, "INVALID_PROVIDER") {
		t.Errorf("envelope details should mention INVALID_PROVIDER; got: %s", rcs[0].Details)
	}

	// And no Resource may have been created.
	got, _ := st.Resources().ListByDefinition(ctx, "pvc-1")
	if len(got) != 0 {
		t.Errorf("placed resources on bogus provider_list: got %d, want 0", len(got))
	}
}

// TestBug131ProviderListMultipleValuesAllowsAny: provider_list with
// two entries means "pool may be either kind". With one ZFS pool and
// one LVM_THIN pool, place_count=1 must succeed (and the chosen pool's
// kind must be in the allow-list).
func TestBug131ProviderListMultipleValuesAllowsAny(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	pools := []apiv1.StoragePool{
		{StoragePoolName: "lvmpool", NodeName: "n1", ProviderKind: apiv1.StoragePoolKindLVMThin, FreeCapacity: 9000, TotalCapacity: 10000},
		{StoragePoolName: "zfspool", NodeName: "n2", ProviderKind: apiv1.StoragePoolKindZFS, FreeCapacity: 5000, TotalCapacity: 10000},
	}
	for i := range pools {
		if err := st.StoragePools().Create(ctx, &pools[i]); err != nil {
			t.Fatalf("seed pool: %v", err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.AutoPlaceRequest{
		SelectFilter: apiv1.AutoSelectFilter{
			PlaceCount:   1,
			ProviderList: []string{apiv1.StoragePoolKindZFS, apiv1.StoragePoolKindLVMThin},
		},
	})

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/autoplace", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	got, _ := st.Resources().ListByDefinition(ctx, "pvc-1")
	if len(got) != 1 {
		t.Fatalf("placed: got %d, want 1", len(got))
	}

	// The chosen pool's kind MUST be one of the allowed ones.
	chosenPool := got[0].Props["StorPoolName"]

	pool, err := st.StoragePools().Get(ctx, got[0].NodeName, chosenPool)
	if err != nil {
		t.Fatalf("lookup chosen pool: %v", err)
	}

	if pool.ProviderKind != apiv1.StoragePoolKindZFS && pool.ProviderKind != apiv1.StoragePoolKindLVMThin {
		t.Errorf("chosen kind %q not in allow-list", pool.ProviderKind)
	}
}

// TestBug131NoProviderListNoFilter: empty / omitted provider_list
// preserves the old behaviour — every kind is eligible. Regression
// guard so a future "default to refuse unknown kinds" change doesn't
// break the path CSI uses for unconstrained autoplace.
func TestBug131NoProviderListNoFilter(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	// Only an LVM_THIN pool — without the regression guard a buggy
	// fix that defaulted ProviderList to ["ZFS"] would mis-refuse this.
	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		StoragePoolName: "lvmpool",
		NodeName:        "n1",
		ProviderKind:    apiv1.StoragePoolKindLVMThin,
		FreeCapacity:    9000,
		TotalCapacity:   10000,
	}); err != nil {
		t.Fatalf("seed pool: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.AutoPlaceRequest{
		SelectFilter: apiv1.AutoSelectFilter{PlaceCount: 1},
	})

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/autoplace", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	got, _ := st.Resources().ListByDefinition(ctx, "pvc-1")
	if len(got) != 1 {
		t.Errorf("placed: got %d, want 1 (LVM_THIN allowed when provider_list empty)", len(got))
	}
}
