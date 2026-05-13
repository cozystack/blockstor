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

// These tests pin Bug 57: the auto-created default resource group
// must surface as the canonical CamelCase `DfltRscGrp` on the wire
// (matching upstream LINSTOR and what linstor-csi greps for) and
// with an empty Description (matching upstream's `linstor rg l`
// output). Before the fix, blockstor's k8s store lowercased the
// name to `dfltrscgrp` via Name() and the REST handler stamped a
// chatty downstream-only description on the auto-created RG.

// TestEnsureDefaultRGUsesCanonicalCamelCase walks the RD-create path
// against the InMemory store (which preserves the name exactly as
// given) so we can assert that `ensureDefaultRGAssignment` calls
// Store.Create with the canonical CamelCase literal. The k8s-store
// path (where the slugifier kicks in) is exercised separately by the
// integration test below.
func TestEnsureDefaultRGUsesCanonicalCamelCase(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.ResourceDefinitionCreate{
		ResourceDefinition: apiv1.ResourceDefinition{
			Name: "rd-default-rg",
			// ResourceGroupName intentionally empty — exercise the
			// default-assignment path.
		},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-definitions", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201", resp.StatusCode)
	}

	rg, err := st.ResourceGroups().Get(ctx, DefaultResourceGroupName)
	if err != nil {
		t.Fatalf("get default RG by canonical name: %v", err)
	}

	if rg.Name != "DfltRscGrp" {
		t.Errorf("RG name: got %q, want exactly %q (canonical CamelCase)",
			rg.Name, "DfltRscGrp")
	}

	rd, err := st.ResourceDefinitions().Get(ctx, "rd-default-rg")
	if err != nil {
		t.Fatalf("get rd: %v", err)
	}

	if rd.ResourceGroupName != "DfltRscGrp" {
		t.Errorf("RD.ResourceGroupName: got %q, want %q",
			rd.ResourceGroupName, "DfltRscGrp")
	}
}

// TestEnsureDefaultRGIdempotentOnSecondCall pins the no-duplicate /
// no-rename contract: a second RD that also omits resource_group_name
// must reuse the already-created `DfltRscGrp`, not create a sibling
// `dfltrscgrp` or rewrite the existing one. Concurrent RD-creates
// race here in production (CSI provisioner spawns N PVCs in parallel
// against the same fresh cluster) — this test exercises the
// `ErrAlreadyExists` swallow path.
func TestEnsureDefaultRGIdempotentOnSecondCall(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	base, stop := startServerWithStore(t, st)
	defer stop()

	for _, name := range []string{"rd-first", "rd-second"} {
		body, err := json.Marshal(apiv1.ResourceDefinitionCreate{
			ResourceDefinition: apiv1.ResourceDefinition{Name: name},
		})
		if err != nil {
			t.Fatalf("marshal %q: %v", name, err)
		}

		resp := httpPost(t, base+"/v1/resource-definitions", body)
		_ = resp.Body.Close()

		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("create %q: status %d, want 201", name, resp.StatusCode)
		}
	}

	rgs, err := st.ResourceGroups().List(ctx)
	if err != nil {
		t.Fatalf("list RGs: %v", err)
	}

	if len(rgs) != 1 {
		t.Fatalf("RG count: got %d (%v), want exactly 1 (no duplicates)",
			len(rgs), rgNames(rgs))
	}

	if rgs[0].Name != "DfltRscGrp" {
		t.Errorf("RG[0].Name: got %q, want %q", rgs[0].Name, "DfltRscGrp")
	}
}

// TestDefaultRGDescriptionMatchesUpstream pins the Description field
// to the empty-string upstream contract. `linstor rg l` against the
// piraeus controller renders an empty Description column for the
// auto-created `DfltRscGrp`; blockstor pre-Bug-57 stamped a verbose
// downstream-only string that diverged from upstream byte-for-byte.
// Tools that diff parity-raw dumps (`/tmp/cli-parity-raw/*.bs.out`
// vs `*.up.out` on the dev box) flag this kind of drift.
func TestDefaultRGDescriptionMatchesUpstream(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.ResourceDefinitionCreate{
		ResourceDefinition: apiv1.ResourceDefinition{Name: "rd-desc-check"},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-definitions", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201", resp.StatusCode)
	}

	rg, err := st.ResourceGroups().Get(ctx, DefaultResourceGroupName)
	if err != nil {
		t.Fatalf("get default RG: %v", err)
	}

	if rg.Description != "" {
		t.Errorf("Description: got %q, want empty (upstream parity)",
			rg.Description)
	}
}

// TestDefaultRGWireShapeFromList is the integration-style check: after
// the lazy auto-create fires, the GET /v1/resource-groups response
// body must contain a row whose `name` JSON field is exactly the
// canonical literal `DfltRscGrp`. This is the field linstor-csi reads
// when its `defaultResourceGroup` constant is in play; a lowercased
// name silently breaks string-equality lookups in downstream callers.
func TestDefaultRGWireShapeFromList(t *testing.T) {
	st := store.NewInMemory()

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.ResourceDefinitionCreate{
		ResourceDefinition: apiv1.ResourceDefinition{Name: "rd-wire"},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-definitions", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201", resp.StatusCode)
	}

	req, err := http.NewRequestWithContext(t.Context(),
		http.MethodGet, base+"/v1/resource-groups", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}

	getResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /v1/resource-groups: %v", err)
	}
	defer getResp.Body.Close()

	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("GET status: got %d, want 200", getResp.StatusCode)
	}

	var wire []apiv1.ResourceGroup
	if err := json.NewDecoder(getResp.Body).Decode(&wire); err != nil {
		t.Fatalf("decode list: %v", err)
	}

	if len(wire) != 1 {
		t.Fatalf("RG count: got %d (%v), want 1", len(wire), rgNames(wire))
	}

	if wire[0].Name != "DfltRscGrp" {
		t.Errorf("wire-shape name: got %q, want %q",
			wire[0].Name, "DfltRscGrp")
	}
}

func rgNames(rgs []apiv1.ResourceGroup) []string {
	out := make([]string, 0, len(rgs))
	for i := range rgs {
		out = append(out, rgs[i].Name)
	}

	return out
}
