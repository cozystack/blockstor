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

// Corner-case campaign D5 (UG9 linstor-administration.adoc ~1742-1755):
// the StorPoolName resolution chain for `r c <node> <rd>` WITHOUT
// `--storage-pool`.
//
// Upstream LINSTOR resolves the pool via VD → Resource → RD → Node →
// literal `DfltStorPool`. blockstor resolves via a DIFFERENT source
// chain (`resolveStorPoolForFreshCreate` / `resolveTakeoverStorPool`):
//
//   sibling diskful replica's StorPoolName  →  RG SelectFilter.StoragePool
//                                           →  RG SelectFilter.StoragePoolList[0]
//
// This is a documented BEHAVIOR delta (cli-parity-known-deltas.md #56):
//
//   - BS does NOT honor a per-VolumeDefinition `StorPoolName` prop, so
//     the upstream "two volumes of one resource in two different pools"
//     layout is not reproducible on BS.
//   - BS has no literal `DfltStorPool` terminal fallback; when nothing
//     in the chain resolves, the Resource lands with an empty
//     StorPoolName (a named-but-missing pool is the only 404 path, via
//     refuseResourceCreateOnUnknownPool).
//
// These tests pin BOTH the supported path (sibling-replica resolution)
// and the delta (per-VD pool ignored), so a future change that silently
// alters either is caught and the delta row stays honest.

// TestCornerD5SiblingReplicaPoolResolution pins the happy path: a fresh
// `r c <node> <rd>` (no pool, no diskless) inherits the StorPoolName of
// an existing diskful sibling replica on another node. This is the
// first tier of BS's resolution chain and the one CSI relies on for the
// witness-takeover / add-replica flows.
func TestCornerD5SiblingReplicaPoolResolution(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "rd"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	// Existing diskful sibling on n1 using "fast".
	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name: "rd", NodeName: "n1",
		Props: map[string]string{"StorPoolName": "fast"},
	}); err != nil {
		t.Fatalf("seed sibling: %v", err)
	}

	for _, n := range []string{"n1", "n2"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{
			Name: n, Type: apiv1.NodeTypeSatellite,
			NetInterfaces: []apiv1.NetInterface{
				{Name: DefaultNetInterfaceName, Address: "10.0.0.9"},
			},
		}); err != nil {
			t.Fatalf("seed node %s: %v", n, err)
		}

		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			NodeName: n, StoragePoolName: "fast",
			ProviderKind: apiv1.StoragePoolKindLVMThin,
		}); err != nil {
			t.Fatalf("seed pool %s: %v", n, err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// Bare create on n2 — no body, relying on sibling resolution.
	resp := httpPost(t, base+"/v1/resource-definitions/rd/resources/n2", []byte(""))
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		got, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 201. Body: %s", resp.StatusCode, got)
	}

	res, err := st.Resources().Get(ctx, "rd", "n2")
	if err != nil {
		t.Fatalf("re-fetch resource: %v", err)
	}

	if got, want := res.Props["StorPoolName"], "fast"; got != want {
		t.Errorf("StorPoolName: got %q, want %q (must inherit sibling diskful pool)", got, want)
	}
}

// TestCornerD5PerVDStorPoolNameIgnored pins the DELTA half: a per-VD
// `StorPoolName` prop on the resource definition's volume-definitions is
// NOT consulted by the resolution chain. With NO sibling replica and an
// RG that pins no pool, the resource lands with an EMPTY StorPoolName
// even though VD 0 carries StorPoolName=vdpool — proving BS does not
// implement the upstream per-VD pool source. If a future change wires
// per-VD resolution, this test fails loudly and delta row #56 must be
// revisited.
func TestCornerD5PerVDStorPoolNameIgnored(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: "rd",
		// No ResourceGroupName → no RG-tier fallback.
		VolumeDefinitions: []apiv1.VolumeDefinition{
			{VolumeNumber: 0, SizeKib: 32768, Props: map[string]string{"StorPoolName": "vdpool"}},
		},
	}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Nodes().Create(ctx, &apiv1.Node{
		Name: "n1", Type: apiv1.NodeTypeSatellite,
		NetInterfaces: []apiv1.NetInterface{
			{Name: DefaultNetInterfaceName, Address: "10.0.0.10"},
		},
	}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	// The VD-named pool DOES exist on the node, so this is purely a
	// "does the chain look at the VD prop" test — not a missing-pool 404.
	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		NodeName: "n1", StoragePoolName: "vdpool",
		ProviderKind: apiv1.StoragePoolKindLVMThin,
	}); err != nil {
		t.Fatalf("seed pool: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/resource-definitions/rd/resources/n1", []byte(""))
	defer func() { _ = resp.Body.Close() }()

	// Create still succeeds (empty pool is not a 404 — only a
	// named-but-missing pool is), but the per-VD prop is NOT picked up.
	if resp.StatusCode != http.StatusCreated {
		got, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 201. Body: %s", resp.StatusCode, got)
	}

	res, err := st.Resources().Get(ctx, "rd", "n1")
	if err != nil {
		t.Fatalf("re-fetch resource: %v", err)
	}

	if got := res.Props["StorPoolName"]; got != "" {
		t.Errorf("StorPoolName: got %q, want empty — BS must NOT resolve per-VD StorPoolName (delta #56)", got)
	}
}

// TestCornerD5ResolveTakeoverNoChainReturnsEmpty pins the terminal
// behavior: resolveTakeoverStorPool returns the empty string (NOT a
// literal `DfltStorPool`) when nothing in the chain resolves. This is
// the absence of the upstream `DfltStorPool` terminal fallback.
func TestCornerD5ResolveTakeoverNoChainReturnsEmpty(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "rd"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	s := &Server{Store: st}

	got, err := s.resolveTakeoverStorPool(ctx, "rd", "n1")
	if err != nil {
		t.Fatalf("resolveTakeoverStorPool: %v", err)
	}

	if got != "" {
		t.Errorf("resolveTakeoverStorPool: got %q, want empty (no DfltStorPool terminal fallback — delta #56)", got)
	}
}
