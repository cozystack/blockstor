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
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Bug 372 (P1, hunt-caught 2026-06-02): PUT /v1/resource-definitions/
// {rd} with a body `{"resource_group":"nonexistent-rg-xyz"}` returned
// HTTP 200 + "resource definition modified", silently stamping the
// bogus name onto `RD.Spec.ResourceGroupName`. The RD then lived on
// with a dangling reference: `linstor rd l` rendered the RD fine,
// but the placer's Controller→RG→RD prop-inheritance walk silently
// dropped the RG tier, and `rg query-size-info`, auto-place, auto-
// diskful, place_count observability, and the RGRebalanceReconciler
// all started seeing a phantom RD with no parent.
//
// Bug-hunt v7 (2026-06-02) reproduced on a live dev stand:
//
//   $ curl -sS -X PUT http://.../v1/resource-definitions/testrd \
//       -d '{"resource_group":"nonexistent-rg-xyz"}'
//   [{"ret_code":4294967296,"message":"resource definition modified: testrd"}]
//   HTTP=200
//   $ curl -sS http://.../v1/resource-definitions/testrd | jq .resource_group_name
//   "nonexistent-rg-xyz"
//
// Sibling Bug 134 closed the same hole on RD create; this is the
// symmetric gap on `rd modify --resource-group`.
//
// The fix routes through the existing `refuseRDCreateOnUnknownRG`
// helper: 404 + LINSTOR envelope naming the missing RG; no spec
// mutation persists when the gate rejects.

// TestBug372PUTRDRefusesNonexistentRG pins the canonical reproducer.
func TestBug372PUTRDRefusesNonexistentRG(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{Name: "okrg"}); err != nil {
		t.Fatalf("seed okrg: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:              "testrd",
		ResourceGroupName: "okrg",
	}); err != nil {
		t.Fatalf("seed testrd: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPut(t, base+"/v1/resource-definitions/testrd",
		[]byte(`{"resource_group":"nonexistent-rg-xyz"}`))
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusNotFound {
		got, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 404 (Bug 372). Body: %s",
			resp.StatusCode, got)
	}

	got, _ := readAllBody(resp)
	if !strings.Contains(string(got), "nonexistent-rg-xyz") {
		t.Errorf("envelope missing offending RG name: %s", got)
	}

	if !strings.Contains(string(got), "resource group") {
		t.Errorf("envelope missing 'resource group' label: %s", got)
	}

	// RD must NOT have its RG reference rewritten.
	rd, err := st.ResourceDefinitions().Get(ctx, "testrd")
	if err != nil {
		t.Fatalf("re-fetch rd: %v", err)
	}

	if got, want := rd.ResourceGroupName, "okrg"; got != want {
		t.Errorf("stored resource_group_name: got %q, want %q (must not persist after rejection)",
			got, want)
	}
}

// TestBug372PUTRDAcceptsExistingRG pins the positive path so the
// gate doesn't block legitimate `rd modify --resource-group` calls
// against a real RG.
func TestBug372PUTRDAcceptsExistingRG(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{Name: "rg-a"}); err != nil {
		t.Fatalf("seed rg-a: %v", err)
	}

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{Name: "rg-b"}); err != nil {
		t.Fatalf("seed rg-b: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:              "testrd",
		ResourceGroupName: "rg-a",
	}); err != nil {
		t.Fatalf("seed rd: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPut(t, base+"/v1/resource-definitions/testrd",
		[]byte(`{"resource_group":"rg-b"}`))
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		got, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 200 (valid RG move). Body: %s",
			resp.StatusCode, got)
	}

	rd, err := st.ResourceDefinitions().Get(ctx, "testrd")
	if err != nil {
		t.Fatalf("re-fetch rd: %v", err)
	}

	if got, want := rd.ResourceGroupName, "rg-b"; got != want {
		t.Errorf("stored resource_group_name: got %q, want %q (move did not stamp)",
			got, want)
	}
}

// TestBug372PUTRDPropsOnlyDoesNotTriggerRGGate pins the canonical
// `rd set-property` shape (no resource_group key in the body): the
// gate must NOT fire and the props merge runs normally.
func TestBug372PUTRDPropsOnlyDoesNotTriggerRGGate(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{Name: "rg-a"}); err != nil {
		t.Fatalf("seed rg-a: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:              "testrd",
		ResourceGroupName: "rg-a",
	}); err != nil {
		t.Fatalf("seed rd: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPut(t, base+"/v1/resource-definitions/testrd",
		[]byte(`{"override_props":{"Foo/Bar":"baz"}}`))
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		got, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 200 (props-only modify). Body: %s",
			resp.StatusCode, got)
	}

	rd, err := st.ResourceDefinitions().Get(ctx, "testrd")
	if err != nil {
		t.Fatalf("re-fetch rd: %v", err)
	}

	if got, want := rd.Props["Foo/Bar"], "baz"; got != want {
		t.Errorf("stored prop Foo/Bar: got %q, want %q", got, want)
	}

	if got, want := rd.ResourceGroupName, "rg-a"; got != want {
		t.Errorf("stored resource_group_name: got %q, want %q (props-only must not touch RG)",
			got, want)
	}
}

// TestBug372PUTRDDstRscGrpAliasTriggersGate pins the `dst_rsc_grp`
// alias (python-linstor 1.27.0's third spelling — see Bug 232). The
// validation has to fire on every spelling the merge reads.
func TestBug372PUTRDDstRscGrpAliasTriggersGate(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{Name: "rg-a"}); err != nil {
		t.Fatalf("seed rg-a: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:              "testrd",
		ResourceGroupName: "rg-a",
	}); err != nil {
		t.Fatalf("seed rd: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPut(t, base+"/v1/resource-definitions/testrd",
		[]byte(`{"dst_rsc_grp":"phantom-rg-via-dst-alias"}`))
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusNotFound {
		got, _ := readAllBody(resp)
		t.Fatalf("status: got %d, want 404 (dst_rsc_grp alias must also gate). Body: %s",
			resp.StatusCode, got)
	}

	rd, err := st.ResourceDefinitions().Get(ctx, "testrd")
	if err != nil {
		t.Fatalf("re-fetch rd: %v", err)
	}

	if got, want := rd.ResourceGroupName, "rg-a"; got != want {
		t.Errorf("stored resource_group_name: got %q, want %q (must not persist after rejection)",
			got, want)
	}
}
