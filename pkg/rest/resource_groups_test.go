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
	"maps"
	"net/http"
	"strings"
	"testing"

	lapi "github.com/LINBIT/golinstor/client"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestResourceGroupsListEmpty: golinstor sees an empty list, not nil.
func TestResourceGroupsListEmpty(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	c := newClient(t, base)

	got, err := c.ResourceGroups.GetAll(t.Context())
	if err != nil {
		t.Fatalf("GetAll: %v", err)
	}

	if got == nil || len(got) != 0 {
		t.Errorf("got %v, want empty", got)
	}
}

// TestResourceGroupsCreateRoundTrip: create via golinstor, fetch it back.
func TestResourceGroupsCreateRoundTrip(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	c := newClient(t, base)

	if err := c.ResourceGroups.Create(t.Context(), lapi.ResourceGroup{
		Name:        "rg-1",
		Description: "test",
		SelectFilter: lapi.AutoSelectFilter{
			PlaceCount:  3,
			StoragePool: "pool",
		},
	}); err != nil {
		t.Fatalf("Create: %v", err)
	}

	got, err := c.ResourceGroups.Get(t.Context(), "rg-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.Name != "rg-1" || got.Description != "test" {
		t.Errorf("got %+v", got)
	}

	if got.SelectFilter.PlaceCount != 3 || got.SelectFilter.StoragePool != "pool" {
		t.Errorf("SelectFilter: got %+v", got.SelectFilter)
	}
}

// TestResourceGroupsCreateConflict: 409 on duplicate name.
func TestResourceGroupsCreateConflict(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceGroups().Create(t.Context(), &apiv1.ResourceGroup{Name: "rg-1"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.ResourceGroup{Name: "rg-1"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-groups", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusConflict {
		t.Errorf("status: got %d, want 409", resp.StatusCode)
	}
}

// TestResourceGroupsGetMissing: 404 on missing rg.
func TestResourceGroupsGetMissing(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpGet(t, base+"/v1/resource-groups/ghost")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", resp.StatusCode)
	}
}

// TestResourceGroupsUpdate: PUT /v1/resource-groups/{rg} round-trips
// a SelectFilter change onto the existing RG. Pins the path the
// `linstor rg modify` command drives — and which an operator setting
// LayerStack on a parent RG depends on (RDs spawned afterwards
// inherit the new filter).
func TestResourceGroupsUpdate(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name:         "rg-1",
		SelectFilter: apiv1.AutoSelectFilter{PlaceCount: 2},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.ResourceGroup{
		SelectFilter: apiv1.AutoSelectFilter{
			PlaceCount: 3,
			LayerStack: []string{"DRBD", "LUKS", "STORAGE"},
		},
	})

	resp := httpPut(t, base+"/v1/resource-groups/rg-1", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.ResourceGroups().Get(ctx, "rg-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.SelectFilter.PlaceCount != 3 {
		t.Errorf("PlaceCount: got %d, want 3", got.SelectFilter.PlaceCount)
	}

	if len(got.SelectFilter.LayerStack) != 3 {
		t.Errorf("LayerStack: got %v, want [DRBD LUKS STORAGE]", got.SelectFilter.LayerStack)
	}
}

// TestResourceGroupsUpdateMissing: PUT against a non-existent RG → 404.
func TestResourceGroupsUpdateMissing(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	body, _ := json.Marshal(apiv1.ResourceGroup{})
	resp := httpPut(t, base+"/v1/resource-groups/ghost", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", resp.StatusCode)
	}
}

// TestResourceGroupsDelete: round-trip via golinstor.
func TestResourceGroupsDelete(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceGroups().Create(t.Context(), &apiv1.ResourceGroup{Name: "rg-1"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	c := newClient(t, base)
	if err := c.ResourceGroups.Delete(t.Context(), "rg-1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	all, err := c.ResourceGroups.GetAll(t.Context())
	if err != nil {
		t.Fatalf("GetAll: %v", err)
	}

	if len(all) != 0 {
		t.Errorf("after Delete, len=%d, want 0", len(all))
	}
}

// TestResourceGroupsWithoutStore: 503 when no store wired in.
func TestResourceGroupsWithoutStore(t *testing.T) {
	base, stop := startServerCustom(t, &Server{Addr: pickFreeAddr(t), Store: nil})
	defer stop()

	for _, path := range []string{
		"/v1/resource-groups",
		"/v1/resource-groups/rg-1",
	} {
		resp := httpGet(t, base+path)
		_ = resp.Body.Close()

		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Errorf("%s: got %d, want 503", path, resp.StatusCode)
		}
	}
}

// TestResourceGroupsCreateBadJSON: malformed body → 400.
func TestResourceGroupsCreateBadJSON(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpPost(t, base+"/v1/resource-groups", []byte("{not-json"))
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400", resp.StatusCode)
	}
}

// TestResourceGroupsCreateMissingName: empty name → 400.
func TestResourceGroupsCreateMissingName(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	body, err := json.Marshal(apiv1.ResourceGroup{}) // Name omitted
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-groups", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400", resp.StatusCode)
	}
}

// TestResourceGroupsUpdateBadJSON: malformed body → 400 on PUT.
func TestResourceGroupsUpdateBadJSON(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceGroups().Create(t.Context(), &apiv1.ResourceGroup{Name: "rg-1"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPut(t, base+"/v1/resource-groups/rg-1", []byte("{not-json"))
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400", resp.StatusCode)
	}
}

// TestRGModifyStampsRebalanceAnnotationOnPlaceCountIncrease: Bug 60.
// Raising PlaceCount on `linstor rg modify` must stamp the
// `blockstor.io/rebalance-pending` annotation onto the persisted RG
// so the controller-side reconciler picks up the deferred autoplace
// pass. Mirrors upstream LINSTOR's `CtrlRscGrpApiCallHandler.modify`
// RescheduleAutoPlace hook — see docs/cli-parity-audit-2026-05-14.md
// row #41.
func TestRGModifyStampsRebalanceAnnotationOnPlaceCountIncrease(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name:         "rg-1",
		SelectFilter: apiv1.AutoSelectFilter{PlaceCount: 2},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.ResourceGroup{
		SelectFilter: apiv1.AutoSelectFilter{PlaceCount: 3},
	})

	resp := httpPut(t, base+"/v1/resource-groups/rg-1", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.ResourceGroups().Get(ctx, "rg-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	stamp, ok := got.Annotations[apiv1.AnnotationRGRebalancePending]
	if !ok {
		t.Fatalf("annotation %q not stamped; got annotations=%v",
			apiv1.AnnotationRGRebalancePending, got.Annotations)
	}

	if stamp == "" {
		t.Errorf("rebalance-pending stamp must be a non-empty RFC3339 timestamp; got %q", stamp)
	}
}

// TestRGModifyNoAnnotationOnPlaceCountDecrease: scale-DOWN is not a
// rebalance trigger. Upstream LINSTOR's rebalance is strictly
// additive — shedding replicas requires explicit `linstor r d`. So a
// PUT that lowers PlaceCount must NOT stamp the rebalance annotation
// (no churn of the reconciler, no surprise replica deletions).
func TestRGModifyNoAnnotationOnPlaceCountDecrease(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name:         "rg-1",
		SelectFilter: apiv1.AutoSelectFilter{PlaceCount: 3},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.ResourceGroup{
		SelectFilter: apiv1.AutoSelectFilter{PlaceCount: 2},
	})

	resp := httpPut(t, base+"/v1/resource-groups/rg-1", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.ResourceGroups().Get(ctx, "rg-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if _, ok := got.Annotations[apiv1.AnnotationRGRebalancePending]; ok {
		t.Errorf("scale-down must not stamp rebalance annotation; got %v", got.Annotations)
	}
}

// TestRGModifyNoAnnotationOnNoOp: a PUT that doesn't touch the
// placement-affecting filter (here: prop-only override) must not
// stamp the rebalance annotation. The existing reconciler runs on
// every CRD change anyway, but we keep the explicit trigger narrow
// so operators can grep for "actual rebalance scheduled" rather than
// chasing noise from every prop write.
func TestRGModifyNoAnnotationOnNoOp(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name:         "rg-1",
		SelectFilter: apiv1.AutoSelectFilter{PlaceCount: 3},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// Prop-only patch: zero-value SelectFilter, just an OverrideProps
	// entry. The handler must merge the prop, leave SelectFilter
	// alone, and NOT stamp the rebalance annotation.
	body, _ := json.Marshal(apiv1.ResourceGroup{
		OverrideProps: map[string]string{"DrbdOptions/auto-quorum": "io-error"},
	})

	resp := httpPut(t, base+"/v1/resource-groups/rg-1", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.ResourceGroups().Get(ctx, "rg-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if _, ok := got.Annotations[apiv1.AnnotationRGRebalancePending]; ok {
		t.Errorf("no-op modify must not stamp rebalance annotation; got %v", got.Annotations)
	}

	if got.SelectFilter.PlaceCount != 3 {
		t.Errorf("prop-only patch wiped SelectFilter; got %+v", got.SelectFilter)
	}
}

// TestResourceGroupsDeleteMissingRG: DELETE on missing RG folds into
// 200 + warn-mask ApiCallRc envelope (Bug 66). Previously asserted 404;
// the python linstor CLI parses non-2xx responses via xml.etree and
// crashes on the JSON body, so the bare 404 made `linstor rg d` exit
// non-zero with a ParseError instead of the no-op the operator wanted.
// TestRGModifyResponseIncludesRebalanceCount: Bug 60. When the
// modify call schedules a rebalance, the REST handler appends a
// second APICallRc envelope with the count of child RDs that the
// reconciler will process. golinstor walks the slice and the
// Python CLI prints both entries, so the operator sees the
// deferred work surface at the original call site rather than
// silently in the controller log.
func TestRGModifyResponseIncludesRebalanceCount(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name:         "rg-1",
		SelectFilter: apiv1.AutoSelectFilter{PlaceCount: 2},
	}); err != nil {
		t.Fatalf("seed rg: %v", err)
	}

	// Two child RDs that should be counted.
	for _, n := range []string{"pvc-a", "pvc-b"} {
		if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
			Name:              n,
			ResourceGroupName: "rg-1",
		}); err != nil {
			t.Fatalf("seed rd %q: %v", n, err)
		}
	}

	// Unrelated RD attached to a different RG — must not be counted.
	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:              "pvc-elsewhere",
		ResourceGroupName: "other-rg",
	}); err != nil {
		t.Fatalf("seed unrelated rd: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.ResourceGroup{
		SelectFilter: apiv1.AutoSelectFilter{PlaceCount: 3},
	})

	resp := httpPut(t, base+"/v1/resource-groups/rg-1", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var reply []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&reply); err != nil {
		t.Fatalf("decode reply: %v", err)
	}

	if len(reply) != 2 {
		t.Fatalf("reply envelope count: got %d, want 2; reply=%+v", len(reply), reply)
	}

	if reply[0].Message != "resource group modified: rg-1" {
		t.Errorf("first envelope: got %q", reply[0].Message)
	}

	wantSecond := "rebalance scheduled for 2 RDs"
	if reply[1].Message != wantSecond {
		t.Errorf("second envelope: got %q, want %q", reply[1].Message, wantSecond)
	}

	if reply[1].RetCode != reply[0].RetCode {
		t.Errorf("second envelope ret_code: got %#x, want %#x (maskInfo)", reply[1].RetCode, reply[0].RetCode)
	}
}

// TestRGModifyResponseSingleEnvelopeWhenNoRebalance: prop-only
// modify keeps the response a single APICallRc — no rebalance
// advisory line. Pins the inverse of TestRGModifyResponseIncludesRebalanceCount
// so a future change that always appends the advisory regresses
// loudly.
func TestRGModifyResponseSingleEnvelopeWhenNoRebalance(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name:         "rg-1",
		SelectFilter: apiv1.AutoSelectFilter{PlaceCount: 3},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.ResourceGroup{
		OverrideProps: map[string]string{"DrbdOptions/auto-quorum": "io-error"},
	})

	resp := httpPut(t, base+"/v1/resource-groups/rg-1", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var reply []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&reply); err != nil {
		t.Fatalf("decode reply: %v", err)
	}

	if len(reply) != 1 {
		t.Errorf("no-op modify must return a single envelope; got %+v", reply)
	}
}

// TestResourceGroupsDeleteMissingRG: DELETE on missing RG → 404.
func TestResourceGroupsDeleteMissingRG(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpDelete(t, base+"/v1/resource-groups/ghost")
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status: got %d, want 200", resp.StatusCode)
	}
}

// TestRGDeleteUnknownReturns200Warning pins the Bug 66 idempotence
// contract for `DELETE /v1/resource-groups/{rg}`: the response is 200
// + ApiCallRc envelope with the WARN bit set and an "already absent"
// message that names the RG. Catches a regression that would either
// fall back to a 404 (crashes python CLI on its XML decoder) or drop
// the WARN bit (audit-log can no longer tell a real drop from a
// no-op replay).
func TestRGDeleteUnknownReturns200Warning(t *testing.T) {
	t.Parallel()

	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpDelete(t, base+"/v1/resource-groups/ghost-rg")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var rc []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rc); err != nil {
		t.Fatalf("decode ApiCallRc envelope: %v", err)
	}

	if len(rc) == 0 {
		t.Fatalf("ApiCallRc envelope: got empty, want one entry")
	}

	if rc[0].RetCode&maskWarn == 0 {
		t.Errorf("ret_code: got %#x, want WARN bit (%#x) set", rc[0].RetCode, maskWarn)
	}

	if !strings.Contains(rc[0].Message, "already absent") {
		t.Errorf("message: got %q, want 'already absent' marker", rc[0].Message)
	}

	if !strings.Contains(rc[0].Message, "ghost-rg") {
		t.Errorf("message: got %q, want it to name ghost-rg", rc[0].Message)
	}
}

// TestResourceGroupListPropertiesRoundTripAllNamespaces pins scenario
// 1.W01 (P0, unit) for the ResourceGroup scope: `linstor
// resource-group list-properties` reads the `props` field of
// `GET /v1/resource-groups/{rg}`. Every LINSTOR-known namespace
// (`DrbdOptions/`, `Aux/`, `FileSystem/`, `StorDriver/`) must
// round-trip verbatim so RG-level templating (the inheritance
// source for every RD spawned off the group) keeps its operator-
// authored keys intact.
func TestResourceGroupListPropertiesRoundTripAllNamespaces(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	seed := map[string]string{
		"DrbdOptions/auto-quorum":  "io-error",
		"DrbdOptions/Net/protocol": "C",
		"Aux/cozystack.io/owner":   "team-storage",
		"FileSystem/Type":          "xfs",
		"StorDriver/StorPoolName":  "blockstor-zfs",
	}

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name:  "rg-props",
		Props: maps.Clone(seed),
	}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	c := newClient(t, base)
	got, err := c.ResourceGroups.Get(ctx, "rg-props")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.Props == nil {
		t.Fatalf("Props: got nil, want a {Key,Value} map")
	}

	for k, want := range seed {
		if got.Props[k] != want {
			t.Errorf("Props[%q]: got %q, want %q (namespace round-trip drift)", k, got.Props[k], want)
		}
	}
}

// TestResourceGroupListPropertiesUnknownRGReturns404 pins the
// unknown-scope half of scenario 1.W01 for resource groups.
func TestResourceGroupListPropertiesUnknownRGReturns404(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpGet(t, base+"/v1/resource-groups/ghost-rg")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", resp.StatusCode)
	}
}

// TestRGDeleteRefusedWhenRDsExist pins scenario 9.W02 (P1, unit;
// cross-listed with wave1 4.5 + Bug 11). `DELETE /v1/resource-groups/
// {rg}` MUST refuse the drop with 409 + FAIL_EXISTS_RSC_DFN when at
// least one ResourceDefinition still references the named RG.
// Mirrors upstream LINSTOR's CtrlRscGrpApiCallHandler.deleteResourceGroup
// `rscGrpData.hasResourceDefinitions(apiCtx)` guard — operator must
// clear or reassign the child RDs first; there is no `--force`. The
// refusal message includes the count substring `N resource-definitions
// exist` so operators can grep the audit log for the failure cause
// without re-listing the RG's children by hand.
func TestRGDeleteRefusedWhenRDsExist(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{Name: "rg-busy"}); err != nil {
		t.Fatalf("seed rg: %v", err)
	}

	// Two child RDs pointing at rg-busy plus one unrelated RD
	// attached to a different RG (must NOT inflate the count or
	// block the refusal). Mirrors the TestRGModifyResponseIncludesRebalanceCount
	// seeding pattern so a future refactor that swaps countChildRDs
	// in either place stays consistent.
	for _, n := range []string{"pvc-a", "pvc-b"} {
		if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
			Name:              n,
			ResourceGroupName: "rg-busy",
		}); err != nil {
			t.Fatalf("seed rd %q: %v", n, err)
		}
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:              "pvc-elsewhere",
		ResourceGroupName: "other-rg",
	}); err != nil {
		t.Fatalf("seed unrelated rd: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t, base+"/v1/resource-groups/rg-busy")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status: got %d, want 409", resp.StatusCode)
	}

	var rc []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rc); err != nil {
		t.Fatalf("decode ApiCallRc envelope: %v", err)
	}

	if len(rc) != 1 {
		t.Fatalf("envelope count: got %d, want 1; rc=%+v", len(rc), rc)
	}

	// Sub-code must carry FAIL_EXISTS_RSC_DFN OR'd with the
	// MASK_ERROR wrapper — same shape the snapshot-dfn refusal on
	// `DELETE /v1/resource-definitions/{rd}` emits.
	wantCode := apiCallRcError | apiCallRcFailExistsRscDfn
	if rc[0].RetCode != wantCode {
		t.Errorf("ret_code: got %#x, want %#x (apiCallRcError|FAIL_EXISTS_RSC_DFN)", rc[0].RetCode, wantCode)
	}

	// Both substrings must be present:
	//   - upstream-wire text "because it has existing resource definitions."
	//   - blockstor-extension count "2 resource-definitions exist"
	for _, want := range []string{
		"resource group 'rg-busy'",
		"because it has existing resource definitions.",
		"cannot delete: 2 resource-definitions exist",
	} {
		if !strings.Contains(rc[0].Message, want) {
			t.Errorf("message: got %q, want substring %q", rc[0].Message, want)
		}
	}

	if rc[0].ObjRefs[objRefRscGrp] != "rg-busy" {
		t.Errorf("ObjRefs[%q]: got %q, want %q", objRefRscGrp, rc[0].ObjRefs[objRefRscGrp], "rg-busy")
	}

	// The RG must STILL be in the store — the refusal cannot leave
	// a half-torn-down state (RG dropped but RDs still pointing at
	// it). Asserts ordering: refuse BEFORE Delete().
	if _, err := st.ResourceGroups().Get(ctx, "rg-busy"); err != nil {
		t.Errorf("rg-busy must survive the refused delete; Get err=%v", err)
	}
}

// TestRGDeleteAllowedAfterChildRDsCleared pins the post-condition of
// 9.W02: once every child RD is gone, the same DELETE call succeeds
// with the normal 200 + maskInfo envelope. Without this, a regression
// that latched the refusal (e.g. cached the count) would silently
// brick `linstor rg d` for the lifetime of the controller.
func TestRGDeleteAllowedAfterChildRDsCleared(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{Name: "rg-drain"}); err != nil {
		t.Fatalf("seed rg: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:              "pvc-going",
		ResourceGroupName: "rg-drain",
	}); err != nil {
		t.Fatalf("seed rd: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// First pass: refused.
	resp := httpDelete(t, base+"/v1/resource-groups/rg-drain")
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("pre-drain delete: got %d, want 409", resp.StatusCode)
	}

	// Drain the child RD; second pass must succeed.
	if err := st.ResourceDefinitions().Delete(ctx, "pvc-going"); err != nil {
		t.Fatalf("drain rd: %v", err)
	}

	resp = httpDelete(t, base+"/v1/resource-groups/rg-drain")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("post-drain delete: got %d, want 200", resp.StatusCode)
	}

	var rc []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rc); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}

	if len(rc) != 1 || rc[0].RetCode != maskInfo {
		t.Errorf("post-drain envelope: got %+v, want single maskInfo entry", rc)
	}

	// And the RG is genuinely gone.
	if _, err := st.ResourceGroups().Get(ctx, "rg-drain"); err == nil {
		t.Errorf("rg-drain must be gone after successful delete")
	}
}

// TestRGDeleteIdempotentOnAbsentPostRefusal cross-checks the Bug 66
// idempotence guarantee against scenario 9.W02: a DELETE on a
// never-created RG still returns 200 + warnRGNotFound, regardless of
// whether unrelated RDs (pointing at OTHER groups) exist in the store.
// Pins that the child-RD check is scoped by RG name — not a global
// "any RD exists, refuse all RG deletes" filter.
func TestRGDeleteIdempotentOnAbsentPostRefusal(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	// Unrelated RD attached to a different RG. Must NOT block the
	// idempotent no-op on `ghost-rg`.
	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:              "pvc-elsewhere",
		ResourceGroupName: "other-rg",
	}); err != nil {
		t.Fatalf("seed unrelated rd: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t, base+"/v1/resource-groups/ghost-rg")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var rc []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rc); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}

	if len(rc) == 0 {
		t.Fatalf("envelope: got empty, want one entry")
	}

	if rc[0].RetCode&maskWarn == 0 {
		t.Errorf("ret_code: got %#x, want WARN bit (%#x) set", rc[0].RetCode, maskWarn)
	}

	if !strings.Contains(rc[0].Message, "already absent") {
		t.Errorf("message: got %q, want 'already absent' marker", rc[0].Message)
	}
}

// TestResourceGroupListPropertiesEmptyDecodes pins the "empty scope
// returns empty map (not nil)" clause: an RG with no Props decodes
// without panic. golinstor ranges over the (possibly nil) map; the
// pin guards the no-panic contract.
func TestResourceGroupListPropertiesEmptyDecodes(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{Name: "rg-empty"}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/resource-groups/rg-empty")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got apiv1.ResourceGroup
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	for k, v := range got.Props {
		t.Errorf("Props: unexpected entry %q=%q on an empty seed", k, v)
	}
}

// TestRGModifyUnsetReplicasOnSameViaEmptyList pins scenario 9.W12
// (P1, unit) for the SelectFilter clear-via-empty-list path:
// `linstor rg modify <rg> --replicas-on-same ""` translates into a
// PATCH body whose `select_filter.replicas_on_same` is a non-nil
// empty list (JSON `[]`). The handler must clear ReplicasOnSame on
// the persisted RG and — critically — must NOT wipe sibling
// SelectFilter fields (PlaceCount, LayerStack, …) that the patch
// did not mention. Upstream's `AutoSelectorConfig.applyChanges`
// treats null = leave alone, non-null = overwrite (incl. empty);
// our Go decoder distinguishes those via nil-vs-empty slice.
//
// Subsequent `rg spawn` reads the updated RG, so once ReplicasOnSame
// is empty the autoplacer's per-call filter merge no longer applies
// that constraint to new RDs spawned off the group.
func TestRGModifyUnsetReplicasOnSameViaEmptyList(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "rg-1",
		SelectFilter: apiv1.AutoSelectFilter{
			PlaceCount:           3,
			ReplicasOnSame:       []string{"Aux/zone"},
			ReplicasOnDifferent:  []string{"Aux/rack"},
			NotPlaceWithRsc:      []string{"other-rd"},
			NotPlaceWithRscRegex: "^lock-.*$",
			LayerStack:           []string{"DRBD", "STORAGE"},
			ProviderList:         []string{"LVM", "LVM_THIN"},
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// Build the wire body by hand so we can emit an explicit empty
	// list — Go struct + json.Marshal would elide `replicas_on_same`
	// because of the `omitempty` tag and produce an absent field,
	// not the empty list the CLI sends.
	body := []byte(`{"select_filter":{"replicas_on_same":[]}}`)

	resp := httpPut(t, base+"/v1/resource-groups/rg-1", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.ResourceGroups().Get(ctx, "rg-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	// Targeted clear.
	if len(got.SelectFilter.ReplicasOnSame) != 0 {
		t.Errorf("ReplicasOnSame: got %v, want empty/nil", got.SelectFilter.ReplicasOnSame)
	}

	// Sibling SelectFilter fields must survive the field-level clear.
	if got.SelectFilter.PlaceCount != 3 {
		t.Errorf("PlaceCount wiped: got %d, want 3", got.SelectFilter.PlaceCount)
	}

	if len(got.SelectFilter.ReplicasOnDifferent) != 1 || got.SelectFilter.ReplicasOnDifferent[0] != "Aux/rack" {
		t.Errorf("ReplicasOnDifferent wiped: got %v, want [Aux/rack]", got.SelectFilter.ReplicasOnDifferent)
	}

	if len(got.SelectFilter.NotPlaceWithRsc) != 1 || got.SelectFilter.NotPlaceWithRsc[0] != "other-rd" {
		t.Errorf("NotPlaceWithRsc wiped: got %v, want [other-rd]", got.SelectFilter.NotPlaceWithRsc)
	}

	if got.SelectFilter.NotPlaceWithRscRegex != "^lock-.*$" {
		t.Errorf("NotPlaceWithRscRegex wiped: got %q, want ^lock-.*$", got.SelectFilter.NotPlaceWithRscRegex)
	}

	if len(got.SelectFilter.LayerStack) != 2 {
		t.Errorf("LayerStack wiped: got %v, want [DRBD STORAGE]", got.SelectFilter.LayerStack)
	}

	if len(got.SelectFilter.ProviderList) != 2 {
		t.Errorf("ProviderList wiped: got %v, want [LVM LVM_THIN]", got.SelectFilter.ProviderList)
	}
}

// TestRGModifyUnsetMultipleListFieldsViaEmptyList pins scenario
// 9.W12 for the multi-field-at-once shape: `linstor rg modify` can
// chain `--replicas-on-same "" --layer-list "" --providers ""` and
// each individual list field has to clear independently. Anchors
// the "list-typed SelectFilter clears compose" invariant.
func TestRGModifyUnsetMultipleListFieldsViaEmptyList(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "rg-1",
		SelectFilter: apiv1.AutoSelectFilter{
			PlaceCount:          2,
			ReplicasOnSame:      []string{"Aux/zone"},
			ReplicasOnDifferent: []string{"Aux/rack"},
			NotPlaceWithRsc:     []string{"other-rd"},
			LayerStack:          []string{"DRBD", "STORAGE"},
			ProviderList:        []string{"LVM"},
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body := []byte(`{"select_filter":{` +
		`"replicas_on_same":[],` +
		`"replicas_on_different":[],` +
		`"not_place_with_rsc":[],` +
		`"layer_stack":[],` +
		`"provider_list":[]` +
		`}}`)

	resp := httpPut(t, base+"/v1/resource-groups/rg-1", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.ResourceGroups().Get(ctx, "rg-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if len(got.SelectFilter.ReplicasOnSame) != 0 {
		t.Errorf("ReplicasOnSame: got %v, want cleared", got.SelectFilter.ReplicasOnSame)
	}

	if len(got.SelectFilter.ReplicasOnDifferent) != 0 {
		t.Errorf("ReplicasOnDifferent: got %v, want cleared", got.SelectFilter.ReplicasOnDifferent)
	}

	if len(got.SelectFilter.NotPlaceWithRsc) != 0 {
		t.Errorf("NotPlaceWithRsc: got %v, want cleared", got.SelectFilter.NotPlaceWithRsc)
	}

	if len(got.SelectFilter.LayerStack) != 0 {
		t.Errorf("LayerStack: got %v, want cleared", got.SelectFilter.LayerStack)
	}

	if len(got.SelectFilter.ProviderList) != 0 {
		t.Errorf("ProviderList: got %v, want cleared", got.SelectFilter.ProviderList)
	}

	if got.SelectFilter.PlaceCount != 2 {
		t.Errorf("PlaceCount: got %d, want 2 (unmentioned field must survive)", got.SelectFilter.PlaceCount)
	}
}

// TestRGModifyUnsetNotPlaceWithRscRegexViaEmptyString pins the
// `--do-not-place-with-regex ""` clear path: the regex is a scalar
// string (not a list), so the wire shape is
// `not_place_with_rsc_regex: ""`. Distinguishing absent-vs-empty for
// scalars in Go requires a sentinel value — here we accept the
// pragmatic semantic that an explicit empty string in the patch
// clears the regex when other SelectFilter slice fields tell us the
// patch DOES carry a select_filter envelope.
func TestRGModifyUnsetNotPlaceWithRscRegexViaEmptyString(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "rg-1",
		SelectFilter: apiv1.AutoSelectFilter{
			PlaceCount:           2,
			NotPlaceWithRscRegex: "^lock-.*$",
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// The wire body emits an empty-string regex alongside any other
	// field (here an empty list) so the handler can tell the patch
	// carries a select_filter envelope. Pure `{"select_filter":{}}`
	// is ambiguous and intentionally must not clear anything.
	body := []byte(`{"select_filter":{` +
		`"not_place_with_rsc_regex":"",` +
		`"replicas_on_same":[]` +
		`}}`)

	resp := httpPut(t, base+"/v1/resource-groups/rg-1", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.ResourceGroups().Get(ctx, "rg-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.SelectFilter.NotPlaceWithRscRegex != "" {
		t.Errorf("NotPlaceWithRscRegex: got %q, want cleared", got.SelectFilter.NotPlaceWithRscRegex)
	}

	if got.SelectFilter.PlaceCount != 2 {
		t.Errorf("PlaceCount: got %d, want 2 (sibling field must survive)", got.SelectFilter.PlaceCount)
	}
}

// TestRGModifyUnsetSpawnNoLongerAppliesConstraint pins the
// downstream half of scenario 9.W12: once the placement constraint
// is cleared on the RG, the next `rg spawn` reads the updated
// SelectFilter, so the merged spawn-time filter no longer carries
// the constraint. We verify by reading the persisted RG back after
// the PATCH and re-running effectiveSpawnFilter (the same helper the
// spawn handler uses) — its output is what the autoplacer sees.
func TestRGModifyUnsetSpawnNoLongerAppliesConstraint(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "rg-1",
		SelectFilter: apiv1.AutoSelectFilter{
			PlaceCount:     2,
			ReplicasOnSame: []string{"Aux/zone"},
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body := []byte(`{"select_filter":{"replicas_on_same":[]}}`)

	resp := httpPut(t, base+"/v1/resource-groups/rg-1", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.ResourceGroups().Get(ctx, "rg-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	// effectiveSpawnFilter is the helper /v1/resource-groups/{rg}/spawn
	// calls before handing the filter to the autoplacer. A spawn
	// with no request-side overrides must inherit the RG's now-empty
	// ReplicasOnSame.
	filter := effectiveSpawnFilter(&got, &apiv1.ResourceGroupSpawn{
		ResourceDefinitionName: "pvc-x",
	})

	if len(filter.ReplicasOnSame) != 0 {
		t.Errorf("spawn-time filter still carries ReplicasOnSame: got %v, want empty",
			filter.ReplicasOnSame)
	}

	if filter.PlaceCount != 2 {
		t.Errorf("spawn-time PlaceCount: got %d, want 2 (unmentioned RG field must survive)",
			filter.PlaceCount)
	}
}

// TestRGModifyUnsetViaDeletePropsArray pins the alternate wire
// shape for SelectFilter clears: the scenario lists `delete_props`
// as a supported entry point so the Python CLI's namespace-based
// path (e.g. `delete_props: ["SelectFilter/ReplicasOnSame"]`)
// reaches the same end state as the empty-list shape. Treated as a
// surface-parity convenience — the handler maps the well-known
// SelectFilter/* keys to the same per-field clear path.
func TestRGModifyUnsetViaDeletePropsArray(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "rg-1",
		SelectFilter: apiv1.AutoSelectFilter{
			PlaceCount:           3,
			ReplicasOnSame:       []string{"Aux/zone"},
			NotPlaceWithRscRegex: "^lock-.*$",
			LayerStack:           []string{"DRBD", "STORAGE"},
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.ResourceGroup{
		DeleteProps: []string{
			"SelectFilter/ReplicasOnSame",
			"SelectFilter/NotPlaceWithRscRegex",
			"SelectFilter/LayerStack",
		},
	})

	resp := httpPut(t, base+"/v1/resource-groups/rg-1", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.ResourceGroups().Get(ctx, "rg-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if len(got.SelectFilter.ReplicasOnSame) != 0 {
		t.Errorf("ReplicasOnSame: got %v, want cleared via delete_props",
			got.SelectFilter.ReplicasOnSame)
	}

	if got.SelectFilter.NotPlaceWithRscRegex != "" {
		t.Errorf("NotPlaceWithRscRegex: got %q, want cleared via delete_props",
			got.SelectFilter.NotPlaceWithRscRegex)
	}

	if len(got.SelectFilter.LayerStack) != 0 {
		t.Errorf("LayerStack: got %v, want cleared via delete_props",
			got.SelectFilter.LayerStack)
	}

	if got.SelectFilter.PlaceCount != 3 {
		t.Errorf("PlaceCount: got %d, want 3 (unmentioned field must survive)",
			got.SelectFilter.PlaceCount)
	}
}

// TestRGCreateWithLayerListDRBDLUKSStorage_W10 pins scenario 9.W10
// (cross-listed with wave1 6.9 + 6.11 + Bug 54): the create-RG
// endpoint accepts `--layer-list drbd,luks,storage` as a wire-shape
// `select_filter.layer_stack` array, persists the exact slice on the
// stored RG so subsequent `rg spawn` calls inherit it onto the child
// RD's LayerStack (= the dispatcher's read side for satellite
// reconcile). The test drives a hand-built JSON body so any future
// rename of the struct tag (e.g. `layer_stack` → `layerList`) breaks
// compilation here rather than producing a silent wire-format
// regression.
//
// CACHE / WRITECACHE / NVME stacks are rejected by validateLayerStack
// (see TestValidateLayerStack_RejectsUnsupportedLayers); this test
// covers the positive half — the documented allowed ordering — so
// the create handler's allowlist + persistence path stay pinned at
// the wire boundary.
func TestRGCreateWithLayerListDRBDLUKSStorage_W10(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	base, stop := startServerWithStore(t, st)
	defer stop()

	// Hand-rolled wire body — the `linstor rg c rg-1 --place-count 3
	// --layer-list drbd,luks,storage` CLI call lands on the apiserver
	// as this JSON envelope (`select_filter.layer_stack` array, all
	// upper-case, terminal STORAGE). Pinning the literal here keeps
	// the test independent of golinstor's Go struct shape.
	body := []byte(`{
		"name": "rg-1",
		"select_filter": {
			"place_count": 3,
			"layer_stack": ["DRBD", "LUKS", "STORAGE"]
		}
	}`)

	resp := httpPost(t, base+"/v1/resource-groups", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create status: got %d, want 201", resp.StatusCode)
	}

	got, err := st.ResourceGroups().Get(ctx, "rg-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	wantStack := []string{"DRBD", "LUKS", "STORAGE"}

	if len(got.SelectFilter.LayerStack) != len(wantStack) {
		t.Fatalf("LayerStack: got %v, want %v",
			got.SelectFilter.LayerStack, wantStack)
	}

	for i, want := range wantStack {
		if got.SelectFilter.LayerStack[i] != want {
			t.Errorf("LayerStack[%d]: got %q, want %q",
				i, got.SelectFilter.LayerStack[i], want)
		}
	}

	if got.SelectFilter.PlaceCount != 3 {
		t.Errorf("PlaceCount: got %d, want 3", got.SelectFilter.PlaceCount)
	}
}

// TestSpawnInheritsLayerListDRBDLUKSStorage_W10 pins the Bug 54
// half of scenario 9.W10: an `rg spawn` off an RG whose
// SelectFilter.LayerStack is ["DRBD","LUKS","STORAGE"] must produce a
// child RD whose persisted LayerStack carries the same slice — that's
// the dispatcher's read side (rd.Spec.LayerStack flows into
// DesiredResource.LayerStack, which gates satellite-side
// pkg/luks.applyLUKS via needsLUKS). Without the spawn-time copy the
// legacy DefaultLayerStack() = [DRBD,STORAGE] would silently win and
// the LUKS layer would never materialise.
func TestSpawnInheritsLayerListDRBDLUKSStorage_W10(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	// Seed the RG with the encrypted-on-DRBD stack.
	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "rg-luks",
		SelectFilter: apiv1.AutoSelectFilter{
			LayerStack: []string{"DRBD", "LUKS", "STORAGE"},
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.ResourceGroupSpawn{
		ResourceDefinitionName: "pvc-luks",
		VolumeSizes:            []int64{1 << 20},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-groups/rg-luks/spawn", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("spawn status: got %d, want 201", resp.StatusCode)
	}

	gotRD, err := st.ResourceDefinitions().Get(ctx, "pvc-luks")
	if err != nil {
		t.Fatalf("Get RD: %v", err)
	}

	wantStack := []string{"DRBD", "LUKS", "STORAGE"}

	if len(gotRD.LayerStack) != len(wantStack) {
		t.Fatalf("RD LayerStack: got %v, want %v (Bug 54 inheritance)",
			gotRD.LayerStack, wantStack)
	}

	for i, want := range wantStack {
		if gotRD.LayerStack[i] != want {
			t.Errorf("RD LayerStack[%d]: got %q, want %q",
				i, gotRD.LayerStack[i], want)
		}
	}

	// The LUKS slot must sit BETWEEN DRBD and STORAGE — DRBD-above-
	// LUKS means DRBD replicates ciphertext (the whole point of the
	// allowed ordering), and STORAGE-terminal anchors the disk
	// backend at the bottom of the chain.
	drbdIdx := -1
	luksIdx := -1
	storageIdx := -1

	for i, layer := range gotRD.LayerStack {
		switch layer {
		case "DRBD":
			drbdIdx = i
		case "LUKS":
			luksIdx = i
		case "STORAGE":
			storageIdx = i
		}
	}

	if drbdIdx < 0 || luksIdx <= drbdIdx || storageIdx <= luksIdx {
		t.Errorf("layer ordering: got %v, want DRBD < LUKS < STORAGE",
			gotRD.LayerStack)
	}
}
