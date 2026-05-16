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
	"errors"
	"net/http"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Bugs 133 + 134 reproducers — both wire-validate-on-create gaps the v3
// report flagged after Bug 101 (node-connection persistence) and Bug 118
// (storage-pool existence on r c) shipped.
//
// Bug 133: PUT /v1/node-connections/{a}/{b} with override_props persists
// a phantom pair entry even when neither node exists. After Bug 101 wired
// the persistence path the missing piece is the cross-check against the
// Node CRDs — same gate shape as Bug 94 on `r c`.
//
// Bug 134: POST /v1/resource-definitions with
// resource_definition.resource_group_name="bogus" creates the RD with a
// dangling RG reference. Subsequent rg-inherited operations fall back to
// DfltRscGrp silently. Same gate class as Bug 118 (sp existence on r c).

// TestBug133NodeConnectionSetPropertyRefusesBogusNodeA pins the primary
// repro: node-A doesn't exist → 404 + LINSTOR envelope, NO phantom pair
// row in ControllerConfig.Spec.NodeConnections / `node-connection list`.
func TestBug133NodeConnectionSetPropertyRefusesBogusNodeA(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	// Seed node-B only; node-A is intentionally missing.
	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "beta"}); err != nil {
		t.Fatalf("seed node-B: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(nodeConnectionPutBody{
		OverrideProps: map[string]string{"Sites/Site": "x"},
	})

	resp := httpPut(t, base+"/v1/node-connections/bogus-A/beta", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusNotFound {
		got, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 404 (Bug 133: bogus node-A must be refused). Body: %s",
			resp.StatusCode, got)
	}

	got, _ := readAllBody(resp)
	if !strings.Contains(string(got), "bogus-A") {
		t.Errorf("envelope missing offending node name: %s", got)
	}

	// LINSTOR-shaped envelope, not a bare error object.
	var rcs []apiv1.APICallRc

	if err := json.Unmarshal(got, &rcs); err != nil {
		t.Fatalf("body is not a []ApiCallRc envelope: %v\n%s", err, got)
	}

	if len(rcs) == 0 || rcs[0].RetCode&apiCallRcError == 0 {
		t.Errorf("envelope ret_code does not carry MASK_ERROR: %+v", rcs)
	}

	// No phantom pair entry must be visible via the list surface.
	listResp := httpGet(t, base+"/v1/node-connections")
	defer func() { _ = listResp.Body.Close() }()

	var pairs []nodeConnectionWire

	if err := json.NewDecoder(listResp.Body).Decode(&pairs); err != nil {
		t.Fatalf("decode list: %v", err)
	}

	if len(pairs) != 0 {
		t.Errorf("phantom NodeConnection entry persisted despite 404: %+v", pairs)
	}
}

// TestBug133NodeConnectionSetPropertyRefusesBogusNodeB is the symmetric
// case: node-A exists, node-B doesn't. Same 404 + envelope, no phantom.
func TestBug133NodeConnectionSetPropertyRefusesBogusNodeB(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "alpha"}); err != nil {
		t.Fatalf("seed node-A: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(nodeConnectionPutBody{
		OverrideProps: map[string]string{"Sites/Site": "x"},
	})

	resp := httpPut(t, base+"/v1/node-connections/alpha/bogus-B", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusNotFound {
		got, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 404 (Bug 133: bogus node-B must be refused). Body: %s",
			resp.StatusCode, got)
	}

	got, _ := readAllBody(resp)
	if !strings.Contains(string(got), "bogus-B") {
		t.Errorf("envelope missing offending node name: %s", got)
	}

	var rcs []apiv1.APICallRc

	if err := json.Unmarshal(got, &rcs); err != nil {
		t.Fatalf("body is not a []ApiCallRc envelope: %v\n%s", err, got)
	}

	if len(rcs) == 0 || rcs[0].RetCode&apiCallRcError == 0 {
		t.Errorf("envelope ret_code does not carry MASK_ERROR: %+v", rcs)
	}

	listResp := httpGet(t, base+"/v1/node-connections")
	defer func() { _ = listResp.Body.Close() }()

	var pairs []nodeConnectionWire

	if err := json.NewDecoder(listResp.Body).Decode(&pairs); err != nil {
		t.Fatalf("decode list: %v", err)
	}

	if len(pairs) != 0 {
		t.Errorf("phantom NodeConnection entry persisted despite 404: %+v", pairs)
	}
}

// TestBug133NodeConnectionSetPropertyBothNodesExistWorks is the happy
// path: both Node CRDs exist; the set-property still returns 200 + the
// LINSTOR envelope and the pair entry persists. Guards against the new
// existence gate regressing the Bug 101 contract.
func TestBug133NodeConnectionSetPropertyBothNodesExistWorks(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "alpha"}); err != nil {
		t.Fatalf("seed node-A: %v", err)
	}

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "beta"}); err != nil {
		t.Fatalf("seed node-B: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(nodeConnectionPutBody{
		OverrideProps: map[string]string{"Sites/Site": "happy"},
	})

	resp := httpPut(t, base+"/v1/node-connections/alpha/beta", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		got, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 200 (happy path). Body: %s",
			resp.StatusCode, got)
	}

	listResp := httpGet(t, base+"/v1/node-connections")
	defer func() { _ = listResp.Body.Close() }()

	var pairs []nodeConnectionWire

	if err := json.NewDecoder(listResp.Body).Decode(&pairs); err != nil {
		t.Fatalf("decode list: %v", err)
	}

	if len(pairs) != 1 {
		t.Fatalf("len pairs: got %d, want 1: %+v", len(pairs), pairs)
	}

	if pairs[0].Props["Sites/Site"] != "happy" {
		t.Errorf("Sites/Site: got %q, want happy", pairs[0].Props["Sites/Site"])
	}
}

// TestBug134RDCreateRefusesBogusResourceGroup is the Bug 134 primary
// repro: POST /v1/resource-definitions with
// resource_definition.resource_group_name="nonexistent" → 404 + LINSTOR
// envelope. No RD persisted.
func TestBug134RDCreateRefusesBogusResourceGroup(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.ResourceDefinitionCreate{
		ResourceDefinition: apiv1.ResourceDefinition{
			Name:              "poke134",
			ResourceGroupName: "nonexistent",
		},
	})

	resp := httpPost(t, base+"/v1/resource-definitions", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusNotFound {
		got, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 404 (Bug 134: bogus RG must be refused). Body: %s",
			resp.StatusCode, got)
	}

	got, _ := readAllBody(resp)
	if !strings.Contains(string(got), "nonexistent") {
		t.Errorf("envelope missing offending RG name: %s", got)
	}

	var rcs []apiv1.APICallRc

	if err := json.Unmarshal(got, &rcs); err != nil {
		t.Fatalf("body is not a []ApiCallRc envelope: %v\n%s", err, got)
	}

	if len(rcs) == 0 || rcs[0].RetCode&apiCallRcError == 0 {
		t.Errorf("envelope ret_code does not carry MASK_ERROR: %+v", rcs)
	}

	// RD must not be persisted.
	_, err := st.ResourceDefinitions().Get(t.Context(), "poke134")
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("RD poke134 persisted despite 404: err=%v", err)
	}
}

// TestBug134RDCreateAcceptsValidResourceGroup pins the happy-path: when
// the named RG already exists the RD persists with that RG reference.
func TestBug134RDCreateAcceptsValidResourceGroup(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{Name: "myrg"}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.ResourceDefinitionCreate{
		ResourceDefinition: apiv1.ResourceDefinition{
			Name:              "poke134-ok",
			ResourceGroupName: "myrg",
		},
	})

	resp := httpPost(t, base+"/v1/resource-definitions", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		got, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 201 (valid RG happy-path). Body: %s",
			resp.StatusCode, got)
	}

	rd, err := st.ResourceDefinitions().Get(ctx, "poke134-ok")
	if err != nil {
		t.Fatalf("RD poke134-ok not persisted: %v", err)
	}

	if rd.ResourceGroupName != "myrg" {
		t.Errorf("ResourceGroupName: got %q, want myrg", rd.ResourceGroupName)
	}
}

// TestBug134RDCreateWithoutRGFallsBackToDfltRscGrp guards the existing
// auto-default behavior: an RD-create body with no resource_group_name
// still succeeds and falls back to DfltRscGrp.
func TestBug134RDCreateWithoutRGFallsBackToDfltRscGrp(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.ResourceDefinitionCreate{
		ResourceDefinition: apiv1.ResourceDefinition{Name: "poke134-dflt"},
	})

	resp := httpPost(t, base+"/v1/resource-definitions", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		got, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 201 (no RG → DfltRscGrp). Body: %s",
			resp.StatusCode, got)
	}

	rd, err := st.ResourceDefinitions().Get(t.Context(), "poke134-dflt")
	if err != nil {
		t.Fatalf("RD poke134-dflt not persisted: %v", err)
	}

	if rd.ResourceGroupName != DefaultResourceGroupName {
		t.Errorf("ResourceGroupName: got %q, want %q (DfltRscGrp fallback)",
			rd.ResourceGroupName, DefaultResourceGroupName)
	}
}
