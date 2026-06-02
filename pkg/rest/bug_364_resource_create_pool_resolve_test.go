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
	"net/http"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Bug 364 (P1, hunt-caught 2026-06-02): `linstor r c <node> <rd>`
// without `--storage-pool` against an RG that pins its default via
// `select_filter.storage_pool_list` (not `select_filter.storage_pool`)
// created a Resource with empty `Props["StorPoolName"]`. The satellite
// reconciler then had no pool to bind to and the replica wedged at
// "Provisioning" — visible to the operator only as a phantom replica
// that never reached UpToDate.
//
// Bug-hunt v7 (2026-06-02) reproduced on a live dev stand:
//
//   $ curl -sS -X PUT .../v1/resource-groups/testrg \
//       -d '{"select_filter":{"storage_pool":"","storage_pool_list":["lvm-thin"]}}'
//   200
//   $ linstor r c dev-worker-1 testlist
//   SUCCESS: resource(s) created on resource-definition: testlist
//   $ curl -sS .../v1/resource-definitions/testlist/resources \
//       | jq '.[].props'
//   null   # <-- empty: StorPoolName missing
//
// linstor-csi posts no body to the per-node resource-create endpoint
// and relies on RG-side propagation for the pool name. When the
// StorageClass sets `linstor.csi.linbit.com/storagePool: <p>`,
// linstor-csi's RGCreate path lands the value under
// SelectFilter.StoragePoolList[0] (not .StoragePool), so this is the
// canonical CSI shape — every Cozystack volume hits this path.
//
// The fix extends `resolveTakeoverStorPool` to also walk the RG's
// `StoragePoolList[0]` (mirroring the existing `resolveGatePoolName`
// fallback chain — the per-pool capacity gate already tolerates that
// tier). 1 line of intent, 3 lines of fence.

// TestBug364ResourceCreatePicksUpStoragePoolList pins the canonical
// reproducer: an RG with only `storage_pool_list` (no
// `storage_pool`) must drive `resolveStorPoolForFreshCreate` to stamp
// the first entry onto `res.Props["StorPoolName"]`.
func TestBug364ResourceCreatePicksUpStoragePoolList(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "csirg",
		SelectFilter: apiv1.AutoSelectFilter{
			PlaceCount:      1,
			StoragePoolList: []string{"lvm-thin"},
		},
	}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:              "rd1",
		ResourceGroupName: "csirg",
	}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Nodes().Create(ctx, &apiv1.Node{
		Name: "n1",
		Type: apiv1.NodeTypeSatellite,
		NetInterfaces: []apiv1.NetInterface{
			{Name: DefaultNetInterfaceName, Address: "10.0.0.1"},
		},
	}); err != nil {
		t.Fatalf("seed Node: %v", err)
	}

	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		NodeName:        "n1",
		StoragePoolName: "lvm-thin",
		ProviderKind:    apiv1.StoragePoolKindLVMThin,
	}); err != nil {
		t.Fatalf("seed SP: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// linstor-csi shape: empty body, path-only intent.
	resp := httpPost(t, base+"/v1/resource-definitions/rd1/resources/n1",
		[]byte(""))
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		got, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 201. Body: %s", resp.StatusCode, got)
	}

	res, err := st.Resources().Get(ctx, "rd1", "n1")
	if err != nil {
		t.Fatalf("re-fetch resource: %v", err)
	}

	if got, want := res.Props["StorPoolName"], "lvm-thin"; got != want {
		t.Errorf("StorPoolName: got %q, want %q (Bug 364: storage_pool_list[0] must seed fresh-create)",
			got, want)
	}
}

// TestBug364ResolveTakeoverStorPoolFromList exercises the helper
// directly so a future refactor of the create-pipeline doesn't lose
// the tier-4 fallback semantics.
func TestBug364ResolveTakeoverStorPoolFromList(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "rg",
		SelectFilter: apiv1.AutoSelectFilter{
			StoragePoolList: []string{"pool-a", "pool-b"},
		},
	}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:              "rd",
		ResourceGroupName: "rg",
	}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	s := &Server{Store: st}

	got, err := s.resolveTakeoverStorPool(ctx, "rd", "n1")
	if err != nil {
		t.Fatalf("resolveTakeoverStorPool: %v", err)
	}

	if want := "pool-a"; got != want {
		t.Errorf("resolveTakeoverStorPool: got %q, want %q (must return StoragePoolList[0])",
			got, want)
	}
}

// TestBug364StoragePoolWinsOverList pins the precedence: when both
// SelectFilter.StoragePool and SelectFilter.StoragePoolList are set,
// the single .StoragePool takes priority (matches
// `resolveGatePoolName`'s tier ordering and upstream LINSTOR's
// CtrlRscCrtApiHelper).
func TestBug364StoragePoolWinsOverList(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "rg",
		SelectFilter: apiv1.AutoSelectFilter{
			StoragePool:     "wins",
			StoragePoolList: []string{"loses"},
		},
	}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:              "rd",
		ResourceGroupName: "rg",
	}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	s := &Server{Store: st}

	got, err := s.resolveTakeoverStorPool(ctx, "rd", "n1")
	if err != nil {
		t.Fatalf("resolveTakeoverStorPool: %v", err)
	}

	if want := "wins"; got != want {
		t.Errorf("resolveTakeoverStorPool: got %q, want %q (single .StoragePool must beat list)",
			got, want)
	}
}

// TestBug364NoListNoStoragePoolReturnsEmpty pins the "no pool
// anywhere" case — empty string with no error, so the downstream
// flow falls through to diskless / 404 paths as before.
func TestBug364NoListNoStoragePoolReturnsEmpty(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name:         "rg",
		SelectFilter: apiv1.AutoSelectFilter{},
	}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:              "rd",
		ResourceGroupName: "rg",
	}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	s := &Server{Store: st}

	got, err := s.resolveTakeoverStorPool(ctx, "rd", "n1")
	if err != nil {
		t.Fatalf("resolveTakeoverStorPool: %v", err)
	}

	if got != "" {
		t.Errorf("resolveTakeoverStorPool: got %q, want %q (no pool config → empty)",
			got, "")
	}
}
