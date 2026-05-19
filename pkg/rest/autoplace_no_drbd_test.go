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
	"io"
	"net/http"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestAutoplaceRejectsMultiPlaceWithoutDRBD pins Bug 335: an autoplace
// request with `place_count > 1` against a RD whose effective
// LayerStack carries no replication layer (i.e. no DRBD) MUST be
// rejected with a structured 4xx error envelope.
//
// Stand reproduction:
//
//	$ linstor r c test3 --auto-place=2 -l STORAGE -s stand
//
// pre-fix: silently spawned 2 INDEPENDENT local volumes on 2 nodes.
// Without DRBD there is no inter-node replication — the two volumes
// diverge silently on the first write. The CLI surfaced "SUCCESS"
// and the operator only discovered the data-loss footgun much later.
//
// post-fix: the REST handler refuses the request with a 400 + an
// actionable error envelope listing the three ways forward (add
// DRBD to the layer list, drop to place_count=1, or wait for
// shared-LUN support).
func TestAutoplaceRejectsMultiPlaceWithoutDRBD(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	// 3-node stand (matches the user-reported reproduction). FILE_THIN
	// because that's the simplest non-replicating provider — the stand
	// uses it for the "stand" pool. The provider-kind itself is not
	// load-bearing for the gate; what matters is that the LayerStack
	// has no DRBD.
	for _, n := range []string{"n1", "n2", "n3"} {
		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			StoragePoolName: "stand",
			NodeName:        n,
			ProviderKind:    apiv1.StoragePoolKindFileThin,
		}); err != nil {
			t.Fatalf("seed pool %s: %v", n, err)
		}
	}

	// RD created with LayerStack=[STORAGE] — the exact `-l STORAGE`
	// shape the user-reported `linstor r c test3 -l STORAGE` lowers to.
	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:       "test3",
		LayerStack: []string{apiv1.LayerKindStorage},
	}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.AutoPlaceRequest{
		SelectFilter: apiv1.AutoSelectFilter{PlaceCount: 2, StoragePool: "stand"},
	})

	resp := httpPost(t, base+"/v1/resource-definitions/test3/autoplace", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status: got %d, want %d (Bug 335 — STORAGE-only multi-place must 400)",
			resp.StatusCode, http.StatusBadRequest)
	}

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	bodyLower := strings.ToLower(string(raw))

	// The error envelope MUST mention "replication layer" or
	// "divergence" / "diverge" so the operator can grep their
	// reproduction's stderr for the actionable cause.
	if !strings.Contains(bodyLower, "replication layer") &&
		!strings.Contains(bodyLower, "diverge") {
		t.Errorf("body should explain the no-replication-layer hazard; got %s", raw)
	}

	// No Resource CRDs must have been created — the gate fires BEFORE
	// the placer runs.
	got, err := st.Resources().ListByDefinition(ctx, "test3")
	if err != nil {
		t.Fatalf("list resources: %v", err)
	}

	if len(got) != 0 {
		t.Errorf("Bug 335 regression: gate fired but %d Resource(s) leaked: %+v", len(got), got)
	}
}

// TestAutoplaceAllowsSinglePlaceWithoutDRBD pins the positive half of
// Bug 335: `auto-place=1` with `-l STORAGE` is the legitimate
// single-local-volume shape and MUST succeed. The gate only fires for
// multi-place (N>1).
func TestAutoplaceAllowsSinglePlaceWithoutDRBD(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	for _, n := range []string{"n1", "n2", "n3"} {
		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			StoragePoolName: "stand",
			NodeName:        n,
			ProviderKind:    apiv1.StoragePoolKindFileThin,
		}); err != nil {
			t.Fatalf("seed pool %s: %v", n, err)
		}
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:       "test3",
		LayerStack: []string{apiv1.LayerKindStorage},
	}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.AutoPlaceRequest{
		SelectFilter: apiv1.AutoSelectFilter{PlaceCount: 1, StoragePool: "stand"},
	})

	resp := httpPost(t, base+"/v1/resource-definitions/test3/autoplace", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200 (place_count=1 with STORAGE-only is legitimate)",
			resp.StatusCode)
	}

	got, err := st.Resources().ListByDefinition(ctx, "test3")
	if err != nil {
		t.Fatalf("list resources: %v", err)
	}

	if len(got) != 1 {
		t.Errorf("placed: got %d, want 1 (single local volume)", len(got))
	}
}

// TestAutoplaceAllowsMultiPlaceWithDRBD is the inverse pin: a RD that
// has DRBD in its LayerStack must accept multi-place — the gate must
// only fire on non-replicated stacks. Regression catcher if a refactor
// accidentally over-broadens the gate to ALL multi-place calls.
func TestAutoplaceAllowsMultiPlaceWithDRBD(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	for _, n := range []string{"n1", "n2", "n3"} {
		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			StoragePoolName: "pool",
			NodeName:        n,
			ProviderKind:    apiv1.StoragePoolKindLVMThin,
		}); err != nil {
			t.Fatalf("seed pool %s: %v", n, err)
		}
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:       "rd-drbd",
		LayerStack: []string{apiv1.LayerKindDRBD, apiv1.LayerKindStorage},
	}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.AutoPlaceRequest{
		SelectFilter: apiv1.AutoSelectFilter{PlaceCount: 2, StoragePool: "pool"},
	})

	resp := httpPost(t, base+"/v1/resource-definitions/rd-drbd/autoplace", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200 (DRBD multi-place is the canonical path)",
			resp.StatusCode)
	}
}
