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
