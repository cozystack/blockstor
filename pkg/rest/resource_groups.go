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
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"reflect"
	"slices"
	"time"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// registerResourceGroups wires the /v1/resource-groups CRUD endpoints.
// Spawn (POST /resource-groups/{rg}/spawn) lands once ResourceDefinition is
// implemented — see docs/csi-api-surface.md.
func (s *Server) registerResourceGroups(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/resource-groups", s.requireStore(s.handleRGList))
	mux.HandleFunc("GET /v1/resource-groups/{rg}", s.requireStore(s.handleRGGet))
	mux.HandleFunc("POST /v1/resource-groups", s.requireStore(s.handleRGCreate))
	mux.HandleFunc("PUT /v1/resource-groups/{rg}", s.requireStore(s.handleRGUpdate))
	mux.HandleFunc("DELETE /v1/resource-groups/{rg}", s.requireStore(s.handleRGDelete))
}

func (s *Server) handleRGList(w http.ResponseWriter, r *http.Request) {
	rgs, err := s.Store.ResourceGroups().List(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	// Optional filter the upstream LINSTOR CLI sends on
	// `linstor rg l --resource-groups <name>...` — mirrors the
	// `resource_definitions` filter on `rd l` (Bug 61) for the RG
	// list endpoint. Same semantics: case-insensitive name match,
	// unknown names => empty list, missing param => no filter.
	nameFilter := multiValueQuery(r, "resource_groups")
	if len(nameFilter) > 0 {
		filtered := rgs[:0]

		for i := range rgs {
			if matchAnyFold(nameFilter, rgs[i].Name) {
				filtered = append(filtered, rgs[i])
			}
		}

		rgs = filtered
	}

	// Defensive non-nil: linstor-csi rejects `null` in place of the
	// empty-list envelope. Pin the invariant at the wire edge so a
	// future store backend that elides `make()` on the no-rows path
	// doesn't silently regress to a `null` body.
	if rgs == nil {
		rgs = []apiv1.ResourceGroup{}
	}

	writeJSON(w, http.StatusOK, rgs)
}

func (s *Server) handleRGGet(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("rg")

	// CreateVolume hot path: linstor-csi follows `POST /resource-groups`
	// with `GET /resource-groups/{rg}` that may land on a sibling
	// apiserver replica whose informer cache still trails the write.
	// Retry on NotFound to absorb the lag — see pkg/rest/cache_retry.go.
	rg, err := getRGWithCacheRetry(r.Context(), s.Store, name)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusOK, rg)
}

func (s *Server) handleRGCreate(w http.ResponseWriter, r *http.Request) {
	var rg apiv1.ResourceGroup

	err := json.NewDecoder(r.Body).Decode(&rg)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	if rg.Name == "" {
		writeError(w, http.StatusBadRequest, "resource group name is required")

		return
	}

	err = validateLayerStack(rg.SelectFilter.LayerStack)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	err = s.Store.ResourceGroups().Create(r.Context(), &rg)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	// Upstream LINSTOR convention: write APIs respond with an
	// `ApiCallRc` list — not the object that was created. golinstor
	// discards the body, but the Python CLI walks `replies[0].ret_code`
	// unconditionally and crashes ("TypeError: string indices must be
	// integers") when handed the bare object.
	writeJSON(w, http.StatusCreated, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "resource group created: " + rg.Name,
	}})
}

func (s *Server) handleRGUpdate(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("rg")

	var patch apiv1.ResourceGroup

	err := json.NewDecoder(r.Body).Decode(&patch)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	// golinstor's RG Modify sends a `ResourceGroupModify` body —
	// override_props / delete_props on top of the existing
	// SelectFilter. Load existing and merge instead of clobbering
	// (the old replace-whole-object semantic nuked select_filter
	// + props on every prop-only PUT).
	existing, err := s.Store.ResourceGroups().Get(r.Context(), name)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	mergeRGProps(&existing, &patch)

	// Remember the pre-patch placement filter so we can detect whether
	// this modify call needs to schedule a rebalance pass on the
	// child RDs (Bug 60).
	prevFilter := existing.SelectFilter

	// SelectFilter only overwrites when the client sent a non-zero
	// envelope. Zero-value `select_filter:{}` from a prop-only patch
	// must NOT wipe the existing placement filter. reflect-equal
	// against the zero value catches all leaf fields (PlaceCount,
	// NodeNameList, …) without listing them by hand.
	if !reflect.DeepEqual(patch.SelectFilter, apiv1.AutoSelectFilter{}) {
		existing.SelectFilter = patch.SelectFilter
	}

	if patch.Description != "" {
		existing.Description = patch.Description
	}

	// Bug 60 (cli-parity-audit row #41): upstream LINSTOR re-runs
	// autoplace on every child RD when `rg modify` raises PlaceCount
	// or changes a placement-affecting filter. Phase 11.x split
	// pushed reconcilers into a separate process, so the REST
	// handler can't walk RDs inline — instead it stamps the
	// `blockstor.io/rebalance-pending` annotation and the
	// RGRebalanceReconciler picks it up.
	//
	// Scale-DOWN intentionally does NOT trigger anything: upstream's
	// rebalance is strictly additive (operator must `linstor r d`
	// manually to shed replicas), so a PlaceCount reduction passes
	// through without an annotation stamp.
	rebalanceScheduled := rgNeedsRebalance(prevFilter, existing.SelectFilter)
	if rebalanceScheduled {
		if existing.Annotations == nil {
			existing.Annotations = map[string]string{}
		}

		existing.Annotations[apiv1.AnnotationRGRebalancePending] = time.Now().UTC().Format(time.RFC3339Nano)
	}

	err = s.Store.ResourceGroups().Update(r.Context(), &existing)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	reply := []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "resource group modified: " + name,
	}}

	if rebalanceScheduled {
		// Append a second envelope so the Python CLI surfaces the
		// deferred work. golinstor walks the slice and prints every
		// entry whose message is non-empty, so operators see both
		// "modified" and "rebalance scheduled for N RDs" on a single
		// `linstor rg modify` call. The count is best-effort (errors
		// while listing child RDs degrade to an unsized advisory
		// rather than failing the modify).
		reply = append(reply, apiv1.APICallRc{
			RetCode: maskInfo,
			Message: rebalanceMessage(countChildRDs(r.Context(), s.Store, name)),
		})
	}

	writeJSON(w, http.StatusOK, reply)
}

// countChildRDs returns the number of ResourceDefinitions whose
// ResourceGroupName equals the named RG. The lookup is best-effort:
// any error from the store is folded into a -1 sentinel so the
// caller can fall back to an unsized "rebalance scheduled" advisory
// rather than failing the parent `rg modify` over a list-side hiccup.
func countChildRDs(ctx context.Context, st store.Store, rgName string) int {
	rds, err := st.ResourceDefinitions().List(ctx)
	if err != nil {
		return -1
	}

	n := 0

	for i := range rds {
		if rds[i].ResourceGroupName == rgName {
			n++
		}
	}

	return n
}

// rebalanceMessage formats the deferred-work APICallRc message. A
// negative count means the child-RD lookup failed; we still surface
// the advisory line so the operator knows a rebalance is queued.
func rebalanceMessage(count int) string {
	if count < 0 {
		return "rebalance scheduled (child RD count unavailable)"
	}

	return fmt.Sprintf("rebalance scheduled for %d RDs", count)
}

// rgNeedsRebalance reports whether the placement-affecting subset of
// the SelectFilter changed in a way that should re-run autoplace on
// the RG's child RDs. The decision is additive-only:
//
//   - PlaceCount: trigger only when the NEW value strictly exceeds
//     the old one. Scale-down is a no-op (upstream LINSTOR forces the
//     operator to remove replicas explicitly via `linstor r d`).
//   - LayerStack / ReplicasOnSame / NotPlaceWithRsc /
//     NotPlaceWithRscRegex: trigger on ANY change. The placer's next
//     pass honours the new constraint; existing replicas that violate
//     it stay (no auto-shuffle), but missing ones get spawned on
//     matching nodes.
//
// Fields that don't affect placement (Description, Props, peer slots,
// etc.) are deliberately excluded so prop-only `rg modify` calls
// don't churn the reconciler.
func rgNeedsRebalance(prev, next apiv1.AutoSelectFilter) bool {
	if next.PlaceCount > prev.PlaceCount {
		return true
	}

	if !slices.Equal(prev.LayerStack, next.LayerStack) {
		return true
	}

	if !slices.Equal(prev.ReplicasOnSame, next.ReplicasOnSame) {
		return true
	}

	if !slices.Equal(prev.NotPlaceWithRsc, next.NotPlaceWithRsc) {
		return true
	}

	if prev.NotPlaceWithRscRegex != next.NotPlaceWithRscRegex {
		return true
	}

	// Scenario 9.W08: XReplicasOnDifferentMap is a placement-affecting
	// SelectFilter field. A modify that changes the per-key cap
	// (e.g. relaxing "site 1" to "site 2") must re-run the placer on
	// every child RD so existing under-placed RDs get caught up.
	if !maps.Equal(prev.XReplicasOnDifferentMap, next.XReplicasOnDifferentMap) {
		return true
	}

	return false
}

// mergeRGProps applies the OverrideProps / DeleteProps merge
// semantic LINSTOR uses for any property-bag-bearing object:
// override entries land first, then delete entries strip their keys.
func mergeRGProps(existing, patch *apiv1.ResourceGroup) {
	if existing.Props == nil && (len(patch.OverrideProps) > 0 || len(patch.DeleteProps) > 0) {
		existing.Props = map[string]string{}
	}

	maps.Copy(existing.Props, patch.OverrideProps)

	for _, k := range patch.DeleteProps {
		delete(existing.Props, k)
	}
}

// handleRGDelete drops a ResourceGroup.
//
// Idempotent on NotFound (Bug 66): the Python linstor CLI feeds the
// response body to `xml.etree.ElementTree.fromstring` whenever the
// HTTP layer reports non-2xx, so a bare 404 on a delete-of-missing
// crashes the CLI with a ParseError before it can surface the
// "already absent" condition. Folding NotFound into a 200 + warn-mask
// envelope keeps `linstor rg d` exit-0 on the second call (matches the
// CSI idempotence guarantee Bug 56 established for resources / RDs).
//
// Scenario 9.W02 (cross-listed with wave1 4.5 + Bug 11): the delete is
// REFUSED with 409 + FAIL_EXISTS_RSC_DFN when any ResourceDefinition
// still references this RG. Mirrors upstream LINSTOR's
// CtrlRscGrpApiCallHandler.deleteResourceGroup
// `rscGrpData.hasResourceDefinitions(apiCtx)` guard — operator must
// clear or reassign the RDs first; there's no `--force`. The refuse
// runs BEFORE the store Delete so a half-torn-down state (RG dropped
// but RDs still pointing at it) can never arise.
func (s *Server) handleRGDelete(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("rg")

	// Refuse the delete if any child RD still references this RG.
	// The count is best-effort: a -1 sentinel from countChildRDs
	// signals the list-side hiccup. We surface the refusal anyway
	// (the RG is presumed unsafe to drop) but degrade the message
	// to omit the unknown count rather than print "-1 resource-
	// definitions".
	childCount := countChildRDs(r.Context(), s.Store, name)
	if childCount != 0 {
		writeJSON(w, http.StatusConflict, []apiv1.APICallRc{{
			RetCode: apiCallRcError | apiCallRcFailExistsRscDfn,
			Message: rgDeleteRefusedMessage(name, childCount),
			ObjRefs: map[string]string{
				objRefRscGrp: name,
			},
		}})

		return
	}

	err := s.Store.ResourceGroups().Delete(r.Context(), name)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		writeStoreError(w, err)

		return
	}

	if err != nil {
		writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
			RetCode: warnRGNotFound,
			Message: "resource group already absent: " + name,
		}})

		return
	}

	writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "resource group deleted: " + name,
	}})
}

// rgDeleteRefusedMessage formats the FAIL_EXISTS_RSC_DFN refusal text.
// Two flavours:
//   - happy path (count > 0): includes the upstream-wire text PLUS the
//     blockstor-extension "cannot delete: N resource-definitions exist"
//     so operators see the failing count without having to `rd l` the
//     parent RG (matches the scenario 9.W02 / wave1 4.5 substring).
//   - degraded (count < 0): the list-side lookup failed; omit the
//     count but keep the upstream-wire refusal text so the operator
//     still gets a clear "won't delete" signal.
func rgDeleteRefusedMessage(name string, count int) string {
	upstream := "Cannot delete resource group '" + name +
		"' because it has existing resource definitions."

	if count < 0 {
		return upstream
	}

	return fmt.Sprintf("%s cannot delete: %d resource-definitions exist", upstream, count)
}
