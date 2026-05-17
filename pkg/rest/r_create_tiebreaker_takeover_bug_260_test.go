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

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Bug 260 (P1) — stand-caught on dev-kvaps. `linstor r c <node> <rd>`
// (no `--storage-pool`, no flags) against an existing TIE_BREAKER
// replica returned 409 "object already exists" instead of taking over
// the witness slot. Upstream LINSTOR's
// CtrlRscCrtApiHelper.resourceToggleDisk handles this exact shape:
// when the existing replica is a witness, the create is interpreted
// as an implicit toggle-disk-to-diskful and the storage pool is
// resolved from sibling diskful replicas (or the parent RG default)
// when the caller didn't pin one.
//
// Pre-fix, the gate in createOrPromoteResource required
// `Props["StorPoolName"]!=""` OR `Flags:[DISKLESS]` — a bare
// promote-via-takeover request (no pool, no flag) tripped the gate
// and writeStoreError(ErrAlreadyExists) returned 409.
//
// The fix probes the existing replica's flags; if it carries
// TIE_BREAKER, allow promote even without an explicit StorPoolName
// and resolve the pool from the first sibling diskful replica.
func TestBug260ResourceCreateTakesOverTieBreakerWithoutStorPool(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	const rdName = "pvc-takeover"

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rdName}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	for _, n := range []string{"worker-1", "worker-2", "worker-3"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{
			Name: n, Type: apiv1.NodeTypeSatellite,
		}); err != nil {
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

	// Two diskful replicas on worker-1 and worker-2 with explicit
	// StorPoolName so the promote helper can resolve the pool from
	// the siblings.
	for _, n := range []string{"worker-1", "worker-2"} {
		if err := st.Resources().Create(ctx, &apiv1.Resource{
			Name:     rdName,
			NodeName: n,
			Props:    map[string]string{"StorPoolName": "pool"},
		}); err != nil {
			t.Fatalf("seed diskful replica %s: %v", n, err)
		}
	}

	// Pre-existing TIE_BREAKER witness on worker-3.
	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name:     rdName,
		NodeName: "worker-3",
		Flags:    []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker},
	}); err != nil {
		t.Fatalf("seed TIE_BREAKER replica: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// `linstor r c worker-3 pvc-takeover` — bare request, no
	// `--storage-pool`, no flags. Wire shape: ResourceCreate body
	// with only `resource.node_name` populated.
	body, err := json.Marshal(apiv1.ResourceCreate{
		Resource: apiv1.Resource{NodeName: "worker-3"},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-definitions/"+rdName+"/resources", body)
	defer func() { _ = resp.Body.Close() }()

	// Pre-fix: 409 "already exists". Post-fix: 201 Created /
	// 200 OK (upstream's `[]ApiCallRc` envelope). Accept either
	// 2xx — the contract is "not a conflict".
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		t.Fatalf("status: got %d, want 2xx (TIE_BREAKER takeover; Bug 260)",
			resp.StatusCode)
	}

	got, err := st.Resources().Get(ctx, rdName, "worker-3")
	if err != nil {
		t.Fatalf("get worker-3 replica after takeover: %v", err)
	}

	// Post-takeover: TIE_BREAKER flag stripped, DISKLESS stripped
	// (we promoted to diskful), and StorPoolName resolved from
	// siblings.
	if slices.Contains(got.Flags, apiv1.ResourceFlagTieBreaker) {
		t.Errorf("Flags still contain TIE_BREAKER after takeover: %v", got.Flags)
	}

	if slices.Contains(got.Flags, apiv1.ResourceFlagDiskless) {
		t.Errorf("Flags still contain DISKLESS after takeover: %v", got.Flags)
	}

	if got.Props["StorPoolName"] != "pool" {
		t.Errorf("StorPoolName: got %q, want %q (resolved from sibling diskful replicas)",
			got.Props["StorPoolName"], "pool")
	}
}

// TestBug260TakeoverResolvesStorPoolFromRG covers the second
// fallback path: the operator's request carries no StorPoolName AND
// no sibling diskful replica advertises one. The parent RG's
// SelectFilter.StoragePool should fill the gap. Skipped if no
// candidate exists (caller must seed at least one source).
func TestBug260TakeoverResolvesStorPoolFromRG(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	const (
		rdName = "pvc-rg-pool"
		rgName = "rg-with-pool"
	)

	// RG with a default storage pool — the fallback source for the
	// takeover when sibling replicas don't pin one.
	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: rgName,
		SelectFilter: apiv1.AutoSelectFilter{
			StoragePool: "pool",
		},
	}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: rdName, ResourceGroupName: rgName,
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
			StoragePoolName: "pool",
			NodeName:        n,
			ProviderKind:    apiv1.StoragePoolKindLVMThin,
		}); err != nil {
			t.Fatalf("seed pool %s: %v", n, err)
		}
	}

	// Two diskful replicas WITHOUT explicit StorPoolName (relying
	// on the RG default). The takeover helper must fall through to
	// the RG.
	for _, n := range []string{"worker-1", "worker-2"} {
		if err := st.Resources().Create(ctx, &apiv1.Resource{
			Name:     rdName,
			NodeName: n,
		}); err != nil {
			t.Fatalf("seed diskful replica %s: %v", n, err)
		}
	}

	// Pre-existing TIE_BREAKER on worker-3.
	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name:     rdName,
		NodeName: "worker-3",
		Flags:    []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker},
	}); err != nil {
		t.Fatalf("seed TIE_BREAKER replica: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.ResourceCreate{
		Resource: apiv1.Resource{NodeName: "worker-3"},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-definitions/"+rdName+"/resources", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		t.Fatalf("status: got %d, want 2xx (Bug 260 RG-pool fallback)", resp.StatusCode)
	}

	got, err := st.Resources().Get(ctx, rdName, "worker-3")
	if err != nil {
		t.Fatalf("get worker-3 replica: %v", err)
	}

	if slices.Contains(got.Flags, apiv1.ResourceFlagTieBreaker) {
		t.Errorf("Flags still contain TIE_BREAKER after takeover: %v", got.Flags)
	}

	if got.Props["StorPoolName"] != "pool" {
		t.Errorf("StorPoolName: got %q, want %q (resolved from parent RG SelectFilter.StoragePool)",
			got.Props["StorPoolName"], "pool")
	}
}

// TestBug260BareCreateOnExistingDiskfulStillReturns409 pins the
// existing behaviour of TestResourceCreateDuplicateNodeRDPairConflict
// — the Bug 260 fix MUST NOT regress the real-conflict surface.
// Posting a bare create against an already-diskful replica (no
// TIE_BREAKER, no DISKLESS) still has to land 409 + "already exists".
func TestBug260BareCreateOnExistingDiskfulStillReturns409(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	const rdName = "pvc-already-diskful"

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rdName}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n1", Type: apiv1.NodeTypeSatellite}); err != nil {
		t.Fatalf("seed Node: %v", err)
	}

	// Plain diskful replica — no DISKLESS / TIE_BREAKER flag.
	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name: rdName, NodeName: "n1",
	}); err != nil {
		t.Fatalf("seed Resource: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.ResourceCreate{
		Resource: apiv1.Resource{NodeName: "n1"},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-definitions/"+rdName+"/resources", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status: got %d, want 409 (real diskful conflict must NOT promote)",
			resp.StatusCode)
	}
}
