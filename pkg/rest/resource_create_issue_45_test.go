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
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestResourceCreateOnNodeIssue45RejectsFullPool pins the Issue #45
// fix on the real linstor-csi CreateVolume path:
// `POST /v1/resource-definitions/{rd}/resources/{node}` against a
// pool whose `FreeCapacity < requiredKib` MUST refuse the placement
// with a structured 409 envelope BEFORE the Resource is persisted.
// Pre-fix the request reached the store, the Resource CRD was
// stamped, the PVC reached Bound, and only the satellite's LV
// allocation surfaced the capacity problem.
//
// This is the endpoint linstor-csi's `manual` scheduler hits when
// the StorageClass sets `nodeList` + `placementCount=1` — the
// `/autoplace` PR #47 gate never sees this traffic.
func TestResourceCreateOnNodeIssue45RejectsFullPool(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	const (
		rdName = "pvc-issue45-on-node"
		node   = "n1"
		pool   = "lvm-thin"
	)

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: node}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rdName}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	// 1 GiB VolumeDefinition.
	if err := st.VolumeDefinitions().Create(ctx, rdName, &apiv1.VolumeDefinition{
		VolumeNumber: 0,
		SizeKib:      1048576,
	}); err != nil {
		t.Fatalf("seed VD: %v", err)
	}

	// FreeCapacity=0: pool is fully consumed. The gate compares
	// `pool.FreeCapacity < requiredKib` so any FreeCapacity below
	// 1 GiB MUST trip the gate.
	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		StoragePoolName: pool,
		NodeName:        node,
		ProviderKind:    apiv1.StoragePoolKindLVMThin,
		FreeCapacity:    0,
		TotalCapacity:   13631488, // 13 GiB
	}); err != nil {
		t.Fatalf("seed pool: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// Body matches the wire shape linstor-csi's `Resources.Create`
	// posts: empty Props (StorPoolName is resolved server-side from
	// the parent RG / fallback chain), no flags, just the node target.
	body, _ := json.Marshal(apiv1.ResourceCreate{
		Resource: apiv1.Resource{
			NodeName: node,
			Props:    map[string]string{"StorPoolName": pool},
		},
	})

	resp := httpPost(t, base+"/v1/resource-definitions/"+rdName+"/resources/"+node, body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status: got %d, want 409 (Issue #45 per-pool capacity gate)", resp.StatusCode)
	}

	var rc []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rc); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}

	if len(rc) == 0 {
		t.Fatalf("envelope: empty array")
	}

	// Wire shape: same RetCode bits as the /autoplace gate so
	// operators classify both paths with one rule.
	if rc[0].RetCode&apiCallRcError == 0 {
		t.Errorf("ret_code missing MASK_ERROR bit: %#x", rc[0].RetCode)
	}

	if rc[0].RetCode&apiCallRcFailNotEnoughNodes == 0 {
		t.Errorf("ret_code missing FAIL_NOT_ENOUGH_NODES sub-code: %#x", rc[0].RetCode)
	}

	// Critical: the placement must NOT have happened.
	got, err := st.Resources().ListByDefinition(ctx, rdName)
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	if len(got) != 0 {
		t.Errorf("Issue #45: placement should be refused, got %d Resource(s): %+v", len(got), got)
	}
}

// TestResourceCreateOnNodeIssue45AllowsFittingPool sanity-checks the
// happy path: a pool with sufficient FreeCapacity must let the
// placement through.
func TestResourceCreateOnNodeIssue45AllowsFittingPool(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	const (
		rdName = "pvc-issue45-ok"
		node   = "n1"
		pool   = "lvm-thin"
	)

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: node}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rdName}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.VolumeDefinitions().Create(ctx, rdName, &apiv1.VolumeDefinition{
		VolumeNumber: 0,
		SizeKib:      1024, // 1 MiB
	}); err != nil {
		t.Fatalf("seed VD: %v", err)
	}

	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		StoragePoolName: pool,
		NodeName:        node,
		ProviderKind:    apiv1.StoragePoolKindLVMThin,
		FreeCapacity:    13631488, // 13 GiB free
		TotalCapacity:   13631488,
	}); err != nil {
		t.Fatalf("seed pool: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.ResourceCreate{
		Resource: apiv1.Resource{
			NodeName: node,
			Props:    map[string]string{"StorPoolName": pool},
		},
	})

	resp := httpPost(t, base+"/v1/resource-definitions/"+rdName+"/resources/"+node, body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201 (sufficient capacity)", resp.StatusCode)
	}

	got, err := st.Resources().ListByDefinition(ctx, rdName)
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	if len(got) != 1 {
		t.Errorf("placed: got %d, want 1", len(got))
	}
}

// TestResourceCreateOnNodeIssue45SkipsGateForDiskless verifies that
// a DISKLESS / TIE_BREAKER replica passes through unchecked — no
// backing storage is allocated, so a full target pool is irrelevant.
// The witness-takeover / make-available paths land here; gating them
// would regress legitimate auto-witness placement on a near-full
// pool.
func TestResourceCreateOnNodeIssue45SkipsGateForDiskless(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	const (
		rdName = "pvc-issue45-diskless"
		node   = "n1"
		pool   = "lvm-thin"
	)

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: node}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rdName}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.VolumeDefinitions().Create(ctx, rdName, &apiv1.VolumeDefinition{
		VolumeNumber: 0,
		SizeKib:      1048576,
	}); err != nil {
		t.Fatalf("seed VD: %v", err)
	}

	// Pool full — gate WOULD fire on a diskful create.
	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		StoragePoolName: pool,
		NodeName:        node,
		ProviderKind:    apiv1.StoragePoolKindLVMThin,
		FreeCapacity:    0,
		TotalCapacity:   13631488,
	}); err != nil {
		t.Fatalf("seed pool: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// Diskless witness: gate must NOT fire.
	body, _ := json.Marshal(apiv1.ResourceCreate{
		Resource: apiv1.Resource{
			NodeName: node,
			Flags:    []string{apiv1.ResourceFlagDiskless},
		},
	})

	resp := httpPost(t, base+"/v1/resource-definitions/"+rdName+"/resources/"+node, body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("diskless create on full pool: got %d, want 201 (gate must skip diskless)", resp.StatusCode)
	}

	got, err := st.Resources().ListByDefinition(ctx, rdName)
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	if len(got) != 1 {
		t.Errorf("diskless placed: got %d, want 1", len(got))
	}
}

// TestResourceCreateBulkIssue45RejectsFullPool pins the gate on the
// bulk array endpoint `POST /v1/resource-definitions/{rd}/resources`
// — same shape upstream LINSTOR uses for `linstor r c n1 n2 n3 rd`
// and the alternate route some CSI clients pick. The per-pool gate
// must fire on the FIRST envelope that exceeds capacity; no
// envelopes should land in the store on the reject path.
func TestResourceCreateBulkIssue45RejectsFullPool(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	const (
		rdName = "pvc-issue45-bulk"
		node   = "n1"
		pool   = "lvm-thin"
	)

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: node}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rdName}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.VolumeDefinitions().Create(ctx, rdName, &apiv1.VolumeDefinition{
		VolumeNumber: 0,
		SizeKib:      1048576,
	}); err != nil {
		t.Fatalf("seed VD: %v", err)
	}

	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		StoragePoolName: pool,
		NodeName:        node,
		ProviderKind:    apiv1.StoragePoolKindLVMThin,
		FreeCapacity:    0,
		TotalCapacity:   13631488,
	}); err != nil {
		t.Fatalf("seed pool: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal([]apiv1.ResourceCreate{{
		Resource: apiv1.Resource{
			NodeName: node,
			Props:    map[string]string{"StorPoolName": pool},
		},
	}})

	resp := httpPost(t, base+"/v1/resource-definitions/"+rdName+"/resources", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status: got %d, want 409 (bulk endpoint capacity gate)", resp.StatusCode)
	}

	got, err := st.Resources().ListByDefinition(ctx, rdName)
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	if len(got) != 0 {
		t.Errorf("Issue #45 bulk: placement should be refused, got %d", len(got))
	}
}

// TestResourceCreateOnNodeIssue45ResolvesPoolFromRGStoragePoolList
// pins the actual CSI wire shape: linstor-csi's `Resources.Create`
// posts an empty body (no `Props.StorPoolName`); the pool name lives
// only on the parent ResourceGroup's
// `SelectFilter.StoragePoolList`. The gate MUST resolve the pool
// from tier 4 of `resolveGatePoolName`, look up its FreeCapacity,
// and fail-fast. Pre-fix the gate skipped because `Props` was empty
// and the RG single-pool tier didn't match StoragePoolList — the
// observed dev-stand failure mode that drove the empirical Phase 3
// re-run.
func TestResourceCreateOnNodeIssue45ResolvesPoolFromRGStoragePoolList(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	const (
		rgName = "rg-issue45-list"
		rdName = "pvc-issue45-list"
		node   = "n1"
		pool   = "lvm-thin"
	)

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: node}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	// RG carries the pool ONLY on StoragePoolList — this is the
	// shape linstor-csi posts when the SC sets
	// `linstor.csi.linbit.com/storagePool: <p>`.
	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: rgName,
		SelectFilter: apiv1.AutoSelectFilter{
			StoragePoolList: []string{pool},
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

	if err := st.VolumeDefinitions().Create(ctx, rdName, &apiv1.VolumeDefinition{
		VolumeNumber: 0,
		SizeKib:      1048576,
	}); err != nil {
		t.Fatalf("seed VD: %v", err)
	}

	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		StoragePoolName: pool,
		NodeName:        node,
		ProviderKind:    apiv1.StoragePoolKindLVMThin,
		FreeCapacity:    0,
		TotalCapacity:   13631488,
	}); err != nil {
		t.Fatalf("seed pool: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// CSI wire shape: empty body — the URL path carries the node,
	// Props is left empty for the server to resolve from the RG.
	body := []byte(`{}`)

	resp := httpPost(t, base+"/v1/resource-definitions/"+rdName+"/resources/"+node, body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status: got %d, want 409 (gate must resolve pool from RG StoragePoolList)", resp.StatusCode)
	}

	got, err := st.Resources().ListByDefinition(ctx, rdName)
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	if len(got) != 0 {
		t.Errorf("Issue #45 RG-list path: placement should be refused, got %d", len(got))
	}
}

// TestResourceCreateOnNodeIssue45SkipsWhenNoVDs verifies the
// definitions-only path: an RD with no VolumeDefinitions yet must
// pass the gate (nothing to size against). Mirrors the autoplace
// and spawn gate semantics for empty VDs.
func TestResourceCreateOnNodeIssue45SkipsWhenNoVDs(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	const (
		rdName = "pvc-issue45-novds"
		node   = "n1"
		pool   = "lvm-thin"
	)

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: node}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rdName}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	// Pool with zero free — would trip the gate IF there were VDs.
	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		StoragePoolName: pool,
		NodeName:        node,
		ProviderKind:    apiv1.StoragePoolKindLVMThin,
		FreeCapacity:    0,
		TotalCapacity:   13631488,
	}); err != nil {
		t.Fatalf("seed pool: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.ResourceCreate{
		Resource: apiv1.Resource{
			NodeName: node,
			Props:    map[string]string{"StorPoolName": pool},
		},
	})

	resp := httpPost(t, base+"/v1/resource-definitions/"+rdName+"/resources/"+node, body)
	_ = resp.Body.Close()

	// No VDs → gate is a no-op. Resource Create should succeed.
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201 (no-VDs gate skip)", resp.StatusCode)
	}
}
