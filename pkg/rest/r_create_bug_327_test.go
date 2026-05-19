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
	"slices"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Bug 327 (P1, recurring, user-reported 5×) — `linstor r c <node> <rd>`
// without `--diskless` must produce a DISKFUL replica, even when a
// TIE_BREAKER witness already lives on ANOTHER node.
//
// Reproduction from the e2e2 stand:
//
//	$ linstor r l                                        # before
//	test  e2e2-worker-1  …  UpToDate
//	test  e2e2-worker-2  …  UpToDate
//	test  e2e2-worker-3  …  TieBreaker
//
//	$ linstor r d e2e2-worker-2 test                     # delete
//	$ linstor r c e2e2-worker-2 test                     # create (no --diskless)
//
//	$ linstor r l                                        # after
//	test  e2e2-worker-1  …  UpToDate
//	test  e2e2-worker-2  …  Diskless          ← WRONG: should be UpToDate
//	test  e2e2-worker-3  …  TieBreaker
//
// Root cause: the REST handler `createOneResource` persists the wire
// body verbatim. A bare `r c` from the LINSTOR CLI carries no flags
// AND no `StorPoolName` — upstream LINSTOR's CtrlRscCrtApiHelper
// resolves the pool from the parent RG's `SelectFilter.StoragePool`
// before staging. The pre-fix blockstor handler skipped that step,
// so the satellite reconciler dispatched the new replica with an
// empty pool → no backing device → the slot came up as Diskless on
// the DRBD layer.
//
// The fix must mirror upstream: when the request omits a pool AND
// the new replica is not explicitly DISKLESS, resolve the pool from
// the parent RG (`SelectFilter.StoragePool`) — or from a sibling
// diskful replica when the RG default is absent — and stamp it onto
// the persisted Resource. The presence of a TIE_BREAKER peer on
// another node MUST NOT leak its DISKLESS / TIE_BREAKER flags into
// the new spawn.

// TestBug327ResourceCreateOnNodeWithExistingTieBreakerProducesDiskful
// is the primary regression test for Bug 327. Pins the contract that
// `r c <node> <rd>` without `--diskless` produces a diskful replica
// (no DISKLESS / TIE_BREAKER flag, with a resolved StoragePool) when
// an existing TIE_BREAKER witness lives on a different node.
//
// User has shown this bug 5 times. The test MUST fail on the broken
// code path and pass after the resolver+stamp fix lands.
func TestBug327ResourceCreateOnNodeWithExistingTieBreakerProducesDiskful(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	const (
		rdName = "test"
		rgName = "rg-default"
		pool   = "stand"
	)

	// Parent RG with a default storage pool — the resolution source
	// every `linstor r c <node> <rd>` (no --storage-pool) relies on
	// per upstream LINSTOR semantics.
	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: rgName,
		SelectFilter: apiv1.AutoSelectFilter{
			StoragePool: pool,
		},
	}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:              rdName,
		ResourceGroupName: rgName,
	}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	for _, n := range []string{"worker-1", "worker-2", "worker-3"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{
			Name: n, Type: apiv1.NodeTypeSatellite,
		}); err != nil {
			t.Fatalf("seed node %s: %v", n, err)
		}

		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			StoragePoolName: pool,
			NodeName:        n,
			ProviderKind:    apiv1.StoragePoolKindLVMThin,
		}); err != nil {
			t.Fatalf("seed pool %s: %v", n, err)
		}
	}

	// One diskful replica on worker-1 (mirrors the post-`r d worker-2`
	// state from the user's reproduction).
	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name:     rdName,
		NodeName: "worker-1",
		Props:    map[string]string{"StorPoolName": pool},
	}); err != nil {
		t.Fatalf("seed diskful replica worker-1: %v", err)
	}

	// Pre-existing TIE_BREAKER witness on worker-3 — the auto-quorum
	// reconciler kept it across the `r d worker-2`.
	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name:     rdName,
		NodeName: "worker-3",
		Flags:    []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker},
	}); err != nil {
		t.Fatalf("seed TIE_BREAKER replica worker-3: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// Wire shape of `linstor r c worker-2 test` — no --diskless,
	// no --storage-pool, no flags. Python CLI posts a ResourceCreate
	// with only `resource.node_name` populated.
	body, err := json.Marshal(apiv1.ResourceCreate{
		Resource: apiv1.Resource{NodeName: "worker-2"},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-definitions/"+rdName+"/resources", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		gotBody, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 2xx (Bug 327: bare `r c` must succeed). Body: %s",
			resp.StatusCode, gotBody)
	}

	// Inspect the persisted Resource — this is the load-bearing
	// assertion. The dispatcher reads `Spec.Flags` and `Spec.StoragePool`
	// to decide diskful-vs-diskless when rendering the DRBD `.res` file.
	got, err := st.Resources().Get(ctx, rdName, "worker-2")
	if err != nil {
		t.Fatalf("get worker-2 replica after create: %v", err)
	}

	// MUST NOT carry DISKLESS — bare `r c` is a diskful create.
	if slices.Contains(got.Flags, apiv1.ResourceFlagDiskless) {
		t.Errorf("Bug 327: Spec.Flags contains DISKLESS after bare `r c worker-2 test`: %v "+
			"(want flags free of DISKLESS so the satellite brings the replica up diskful)", got.Flags)
	}

	// MUST NOT carry TIE_BREAKER — the witness on worker-3 must NOT
	// leak its flag into the new spawn on worker-2.
	if slices.Contains(got.Flags, apiv1.ResourceFlagTieBreaker) {
		t.Errorf("Bug 327: Spec.Flags contains TIE_BREAKER after bare `r c worker-2 test`: %v "+
			"(witness flag from worker-3 must NOT leak into worker-2's spawn)", got.Flags)
	}

	// MUST have a backing StoragePool resolved — without it the
	// dispatcher emits an empty pool and the satellite reconciler
	// has nowhere to provision the backing LV/zvol; the DRBD slot
	// comes up `disk none;` and `r l` shows Diskless even though
	// the operator never asked for it.
	gotPool := got.Props["StorPoolName"]
	if gotPool == "" {
		t.Errorf("Bug 327: Resource on worker-2 has no StorPoolName after bare `r c` "+
			"(want a resolved pool — RG.SelectFilter.StoragePool=%q or sibling pool=%q — "+
			"so the satellite dispatches the replica diskful). Flags=%v Props=%v",
			pool, pool, got.Flags, got.Props)
	}
}

// TestBug327ResourceCreateNoFlagsNoPoolResolvesFromSiblingsWhenRGEmpty
// covers the second resolution source: the parent RG has no
// `SelectFilter.StoragePool` set, so the pool must be resolved from a
// sibling diskful replica. Mirrors the Bug 260 takeover fallback chain
// but at the create-fresh layer (no existing replica on the target
// node).
func TestBug327ResourceCreateNoFlagsNoPoolResolvesFromSiblingsWhenRGEmpty(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	const (
		rdName = "test"
		rgName = "rg-empty"
		pool   = "stand"
	)

	// RG without a default pool — the resolver must fall through to
	// the sibling diskful replica.
	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: rgName,
	}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:              rdName,
		ResourceGroupName: rgName,
	}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	for _, n := range []string{"worker-1", "worker-2"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{
			Name: n, Type: apiv1.NodeTypeSatellite,
		}); err != nil {
			t.Fatalf("seed node %s: %v", n, err)
		}

		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			StoragePoolName: pool,
			NodeName:        n,
			ProviderKind:    apiv1.StoragePoolKindLVMThin,
		}); err != nil {
			t.Fatalf("seed pool %s: %v", n, err)
		}
	}

	// Sibling diskful replica on worker-1 with a pinned pool — the
	// sibling-fallback source.
	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name:     rdName,
		NodeName: "worker-1",
		Props:    map[string]string{"StorPoolName": pool},
	}); err != nil {
		t.Fatalf("seed sibling worker-1: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.ResourceCreate{
		Resource: apiv1.Resource{NodeName: "worker-2"},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-definitions/"+rdName+"/resources", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		gotBody, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 2xx. Body: %s", resp.StatusCode, gotBody)
	}

	got, err := st.Resources().Get(ctx, rdName, "worker-2")
	if err != nil {
		t.Fatalf("get worker-2 after create: %v", err)
	}

	if slices.Contains(got.Flags, apiv1.ResourceFlagDiskless) {
		t.Errorf("Bug 327: Spec.Flags contains DISKLESS: %v (want diskful)", got.Flags)
	}

	if got.Props["StorPoolName"] == "" {
		t.Errorf("Bug 327: Resource on worker-2 has no StorPoolName after bare `r c` "+
			"(want pool resolved from sibling diskful replica). Props=%v", got.Props)
	}
}

// TestBug327BareCreateWithExplicitDisklessFlagStaysDiskless pins the
// inverse: when the operator DOES pass `--diskless`, the handler must
// NOT silently flip the flag off by "resolving" a pool. Honour
// explicit intent.
func TestBug327BareCreateWithExplicitDisklessFlagStaysDiskless(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	const (
		rdName = "test"
		rgName = "rg-default"
		pool   = "stand"
	)

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: rgName,
		SelectFilter: apiv1.AutoSelectFilter{
			StoragePool: pool,
		},
	}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:              rdName,
		ResourceGroupName: rgName,
	}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	for _, n := range []string{"worker-1", "worker-2"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{
			Name: n, Type: apiv1.NodeTypeSatellite,
		}); err != nil {
			t.Fatalf("seed node %s: %v", n, err)
		}

		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			StoragePoolName: pool,
			NodeName:        n,
			ProviderKind:    apiv1.StoragePoolKindLVMThin,
		}); err != nil {
			t.Fatalf("seed pool %s: %v", n, err)
		}
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name:     rdName,
		NodeName: "worker-1",
		Props:    map[string]string{"StorPoolName": pool},
	}); err != nil {
		t.Fatalf("seed diskful replica: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// `linstor r c worker-2 test --diskless` — explicit diskless intent.
	body, err := json.Marshal(apiv1.ResourceCreate{
		Resource: apiv1.Resource{
			NodeName: "worker-2",
			Flags:    []string{apiv1.ResourceFlagDiskless},
		},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-definitions/"+rdName+"/resources", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		gotBody, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 2xx. Body: %s", resp.StatusCode, gotBody)
	}

	got, err := st.Resources().Get(ctx, rdName, "worker-2")
	if err != nil {
		t.Fatalf("get worker-2: %v", err)
	}

	if !slices.Contains(got.Flags, apiv1.ResourceFlagDiskless) {
		t.Errorf("Bug 327: explicit --diskless flag must NOT be stripped: Flags=%v", got.Flags)
	}
}
