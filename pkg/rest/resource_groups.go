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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"slices"
	"time"

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
	// Bug 154: `linstor rg dp <rg> <key>` returned 404 because the
	// per-key DELETE route was never registered. Mirrors Bug 142's
	// `n dp` shape exactly — Go 1.22's `{key...}` wildcard captures
	// slash-bearing keys like `DrbdOptions/auto-quorum` whole, and a
	// delete-of-missing folds into 200 + warn-mask so reconciler
	// retries don't hot-spin.
	mux.HandleFunc("DELETE /v1/resource-groups/{rg}/properties/{key...}",
		s.requireStore(s.handleRGPropDelete))
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

	// Bug 181: scrub sensitive keys (passphrase, password, shared-
	// secret, ...) from every RG's Props map before the JSON encode.
	// Bug 115 wired this same deny list into RD / Controller /
	// Resource read paths but never extended it to RG. `linstor rg lp
	// <rg>` rendered DrbdOptions/EncryptPassphrase verbatim from the
	// list path the python CLI hits when it filters by name.
	for i := range rgs {
		redactSensitiveProps(rgs[i].Props)
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

	// Bug 181: scrub the per-RG Props bag at the REST boundary.
	// getRGWithCacheRetry returns a value copy, so the in-place
	// mutation is local to this response and does not leak back
	// into the store cache.
	redactSensitiveProps(rg.Props)

	writeJSON(w, http.StatusOK, rg)
}

func (s *Server) handleRGCreate(w http.ResponseWriter, r *http.Request) {
	var rg apiv1.ResourceGroup

	if !decodeJSON(w, r, &rg) {
		return
	}

	// Bug 97: see pkg/rest/input_validation.go — RFC-1123 subdomain
	// validation at the REST boundary, before pkg/store/k8s.Name()
	// mangles the input.
	nameErr := validateLinstorName("resource group", rg.Name)
	if nameErr != nil {
		writeError(w, http.StatusBadRequest, nameErr.Error())

		return
	}

	err := validateLayerStack(rg.SelectFilter.LayerStack)
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

	raw, patch, ok := readRGUpdatePatch(w, r)
	if !ok {
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

	prevFilter := existing.SelectFilter
	applyRGUpdatePatch(&existing, &patch, raw)

	// Bug 60 (cli-parity-audit row #41): upstream LINSTOR re-runs
	// autoplace on every child RD when `rg modify` raises PlaceCount
	// or changes a placement-affecting filter. Phase 11.x split
	// pushed reconcilers into a separate process, so the REST
	// handler can't walk RDs inline — instead it stamps the
	// `blockstor.io/rebalance-pending` annotation and the
	// RGRebalanceReconciler picks it up. Scale-DOWN intentionally
	// does NOT trigger anything.
	rebalanceScheduled := rgNeedsRebalance(&prevFilter, &existing.SelectFilter)
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
		// entry whose message is non-empty.
		reply = append(reply, apiv1.APICallRc{
			RetCode: maskInfo,
			Message: rebalanceMessage(countChildRDs(r.Context(), s.Store, name)),
		})
	}

	writeJSON(w, http.StatusOK, reply)
}

// readRGUpdatePatch decodes the PATCH body twice: once as the typed
// struct (for the merge helpers) and once as a raw envelope so the
// SelectFilter merge can tell which sub-keys the client actually
// mentioned. Scenario 9.W12: `linstor rg modify --replicas-on-same ""`
// sends `select_filter: {"replicas_on_same": []}` — distinguishable
// from "field absent" only at the raw-JSON level for sub-fields whose
// Go type elides empty values via `omitempty`. Returns (raw, patch,
// false) on any I/O or decode error after writing the 400 response.
func readRGUpdatePatch(w http.ResponseWriter, r *http.Request) ([]byte, apiv1.ResourceGroup, bool) {
	var patch apiv1.ResourceGroup

	raw, err := io.ReadAll(r.Body)
	if err != nil {
		writeDecodeError(w, err)

		return nil, patch, false
	}

	// Bug 158/161: typed-envelope decode + DisallowUnknownFields. We
	// keep `raw` for the downstream Bug 156-style "field explicitly
	// mentioned?" probe; the decode just disciplines the typed view.
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()

	err = dec.Decode(&patch)
	if err != nil {
		writeDecodeError(w, err)

		return nil, patch, false
	}

	return raw, patch, true
}

// applyRGUpdatePatch merges the decoded patch onto `existing` in
// place. Three independent layers run in order: property bag
// (override + delete), SelectFilter (field-level merge), and the
// scalar description string. Extracted from handleRGUpdate to keep
// the handler under the funlen budget; the split tracks the natural
// "decode → merge → persist" boundary in the request lifecycle.
func applyRGUpdatePatch(existing, patch *apiv1.ResourceGroup, raw []byte) {
	mergeRGProps(existing, patch)

	// Merge the SelectFilter envelope field-by-field. Mirrors upstream
	// LINSTOR's `AutoSelectorConfig.applyChanges`: per-field null =
	// leave alone, non-null (including the empty list) = overwrite
	// (Scenario 9.W12).
	mergeRGSelectFilter(&existing.SelectFilter, &patch.SelectFilter, raw)

	// Scenario 9.W12 / surface-parity convenience: accept the
	// `delete_props: ["SelectFilter/<Field>"]` shape as an alternate
	// entry point to the same per-field clear. Upstream LINSTOR
	// reserves `delete_props` for the property bag, but the scenario
	// calls out both wire shapes as supported.
	applyRGSelectFilterDeleteProps(&existing.SelectFilter, patch.DeleteProps)

	if patch.Description != "" {
		existing.Description = patch.Description
	}
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
func rgNeedsRebalance(prev, next *apiv1.AutoSelectFilter) bool {
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

// Property-namespace keys for the `delete_props` alternate clear path
// (Scenario 9.W12). Shared between the delete-props applier and the
// per-field clear table so a typo on either side breaks compilation.
const (
	sfDPPlaceCount              = "SelectFilter/PlaceCount"
	sfDPAdditionalPlaceCount    = "SelectFilter/AdditionalPlaceCount"
	sfDPNodeNameList            = "SelectFilter/NodeNameList"
	sfDPStoragePool             = "SelectFilter/StoragePool"
	sfDPStoragePoolList         = "SelectFilter/StoragePoolList"
	sfDPStoragePoolDisklessList = "SelectFilter/StoragePoolDisklessList"
	sfDPNotPlaceWithRsc         = "SelectFilter/NotPlaceWithRsc"
	sfDPNotPlaceWithRscRegex    = "SelectFilter/NotPlaceWithRscRegex"
	sfDPReplicasOnSame          = "SelectFilter/ReplicasOnSame"
	sfDPReplicasOnDifferent     = "SelectFilter/ReplicasOnDifferent"
	sfDPXReplicasOnDifferentMap = "SelectFilter/XReplicasOnDifferentMap"
	sfDPLayerStack              = "SelectFilter/LayerStack"
	sfDPProviderList            = "SelectFilter/ProviderList"
	sfDPDisklessOnRemaining     = "SelectFilter/DisklessOnRemaining"
	sfDPOverrideVlmID           = "SelectFilter/OverrideVlmID"
)

// mergeRGSelectFilter applies a `linstor rg modify`-style patch onto
// the existing SelectFilter using the field-by-field semantic upstream
// LINSTOR uses (`AutoSelectorConfig.applyChanges`): null = leave alone,
// non-null (including the empty list / empty string) = overwrite.
//
// The Go decoder can't distinguish "field absent in JSON" from
// "field present with zero value" once the body is unmarshalled into
// a struct with `omitempty` tags, so for list-typed fields we use
// the nil-vs-empty distinction the json package DOES preserve. For
// scalar fields (string regex, int counts) we walk the raw JSON
// envelope to detect whether the client mentioned the key.
//
// Scenario 9.W12: `rg modify <rg> --replicas-on-same ""` reaches here
// with `patch.ReplicasOnSame == []string{}` (non-nil, length 0); the
// merge clears the existing list without touching sibling fields.
func mergeRGSelectFilter(existing, patch *apiv1.AutoSelectFilter, raw []byte) {
	mergeRGSelectFilterLists(existing, patch)
	mergeRGSelectFilterScalars(existing, patch, rgSelectFilterKeys(raw))
}

// mergeRGSelectFilterLists overwrites every list-typed sub-field
// whose patch value is non-nil — nil-vs-empty is the wire-shape
// signal for "clear" vs "untouched" (Scenario 9.W12).
func mergeRGSelectFilterLists(existing, patch *apiv1.AutoSelectFilter) {
	if patch.NodeNameList != nil {
		existing.NodeNameList = patch.NodeNameList
	}

	if patch.StoragePoolList != nil {
		existing.StoragePoolList = patch.StoragePoolList
	}

	if patch.StoragePoolDisklessList != nil {
		existing.StoragePoolDisklessList = patch.StoragePoolDisklessList
	}

	if patch.NotPlaceWithRsc != nil {
		existing.NotPlaceWithRsc = patch.NotPlaceWithRsc
	}

	if patch.ReplicasOnSame != nil {
		existing.ReplicasOnSame = patch.ReplicasOnSame
	}

	if patch.ReplicasOnDifferent != nil {
		existing.ReplicasOnDifferent = patch.ReplicasOnDifferent
	}

	if patch.XReplicasOnDifferentMap != nil {
		existing.XReplicasOnDifferentMap = patch.XReplicasOnDifferentMap
	}

	if patch.LayerStack != nil {
		existing.LayerStack = patch.LayerStack
	}

	if patch.ProviderList != nil {
		existing.ProviderList = patch.ProviderList
	}
}

// mergeRGSelectFilterScalars overwrites every scalar SelectFilter
// sub-field the client explicitly mentioned in the raw envelope.
// Required for scalars where an absent field and an explicit zero
// value share the same Go representation.
func mergeRGSelectFilterScalars(existing, patch *apiv1.AutoSelectFilter, mentioned map[string]struct{}) {
	if _, ok := mentioned["place_count"]; ok {
		existing.PlaceCount = patch.PlaceCount
	}

	if _, ok := mentioned["additional_place_count"]; ok {
		existing.AdditionalPlaceCount = patch.AdditionalPlaceCount
	}

	if _, ok := mentioned["storage_pool"]; ok {
		existing.StoragePool = patch.StoragePool
	}

	if _, ok := mentioned["not_place_with_rsc_regex"]; ok {
		existing.NotPlaceWithRscRegex = patch.NotPlaceWithRscRegex
	}

	if _, ok := mentioned["diskless_on_remaining"]; ok {
		existing.DisklessOnRemaining = patch.DisklessOnRemaining
	}

	if _, ok := mentioned["override_vlm_id"]; ok {
		existing.OverrideVlmID = patch.OverrideVlmID
	}
}

// rgSelectFilterKeys returns the set of select_filter sub-keys the
// client explicitly mentioned in the raw PATCH body. Empty map when
// the envelope is absent / malformed — callers degrade to the
// "no mutation" path, matching the leave-alone branch of the merge.
func rgSelectFilterKeys(raw []byte) map[string]struct{} {
	out := map[string]struct{}{}

	if len(bytes.TrimSpace(raw)) == 0 {
		return out
	}

	var envelope struct {
		SelectFilter map[string]json.RawMessage `json:"select_filter"`
	}

	err := json.Unmarshal(raw, &envelope)
	if err != nil {
		return out
	}

	for k := range envelope.SelectFilter {
		out[k] = struct{}{}
	}

	return out
}

// applyRGSelectFilterDeleteProps walks the `delete_props` array and
// clears any well-known `SelectFilter/<Field>` entry. Mirrors the
// scenario 9.W12 surface-parity hedge: the property-namespace shape
// the Python CLI sometimes emits for `linstor rg modify --delete
// <key>` lands the same end state as the empty-list path. Unknown
// keys pass through (they were already filtered against the Props
// bag by `mergeRGProps`).
func applyRGSelectFilterDeleteProps(existing *apiv1.AutoSelectFilter, keys []string) {
	clearers := rgSelectFilterClearTable()

	for _, k := range keys {
		clearFn, ok := clearers[k]
		if !ok {
			continue
		}

		clearFn(existing)
	}
}

// rgSelectFilterClearTable maps each well-known `SelectFilter/<Field>`
// delete-props key to the per-field clear action. Pulled out of the
// per-iteration body so the cyclomatic complexity of the public
// applier stays under the project budget — and to keep the
// key-namespace and the Go field it touches paired on one line.
func rgSelectFilterClearTable() map[string]func(*apiv1.AutoSelectFilter) {
	return map[string]func(*apiv1.AutoSelectFilter){
		sfDPPlaceCount:              func(f *apiv1.AutoSelectFilter) { f.PlaceCount = 0 },
		sfDPAdditionalPlaceCount:    func(f *apiv1.AutoSelectFilter) { f.AdditionalPlaceCount = 0 },
		sfDPNodeNameList:            func(f *apiv1.AutoSelectFilter) { f.NodeNameList = nil },
		sfDPStoragePool:             func(f *apiv1.AutoSelectFilter) { f.StoragePool = "" },
		sfDPStoragePoolList:         func(f *apiv1.AutoSelectFilter) { f.StoragePoolList = nil },
		sfDPStoragePoolDisklessList: func(f *apiv1.AutoSelectFilter) { f.StoragePoolDisklessList = nil },
		sfDPNotPlaceWithRsc:         func(f *apiv1.AutoSelectFilter) { f.NotPlaceWithRsc = nil },
		sfDPNotPlaceWithRscRegex:    func(f *apiv1.AutoSelectFilter) { f.NotPlaceWithRscRegex = "" },
		sfDPReplicasOnSame:          func(f *apiv1.AutoSelectFilter) { f.ReplicasOnSame = nil },
		sfDPReplicasOnDifferent:     func(f *apiv1.AutoSelectFilter) { f.ReplicasOnDifferent = nil },
		sfDPXReplicasOnDifferentMap: func(f *apiv1.AutoSelectFilter) { f.XReplicasOnDifferentMap = nil },
		sfDPLayerStack:              func(f *apiv1.AutoSelectFilter) { f.LayerStack = nil },
		sfDPProviderList:            func(f *apiv1.AutoSelectFilter) { f.ProviderList = nil },
		sfDPDisklessOnRemaining:     func(f *apiv1.AutoSelectFilter) { f.DisklessOnRemaining = false },
		sfDPOverrideVlmID:           func(f *apiv1.AutoSelectFilter) { f.OverrideVlmID = "" },
	}
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

	(&deleteWithRollback[apiv1.ResourceGroup]{
		refuseIfReferenced: func() bool {
			return s.refuseRGDeleteIfReferenced(w, r, name)
		},
		capture: func() (apiv1.ResourceGroup, bool) {
			return s.captureResourceGroup(r.Context(), name)
		},
		remove: func() error {
			return s.Store.ResourceGroups().Delete(r.Context(), name)
		},
		rolledBackIfRaced: func(captured apiv1.ResourceGroup, capturedOK bool) bool {
			if !capturedOK {
				return false
			}

			return s.rollbackRGDeleteIfRaced(w, r, name, &captured)
		},
		writeWarn: func() {
			writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
				RetCode: warnRGNotFound,
				Message: "resource group already absent: " + name,
			}})
		},
		writeSuccess: func() {
			writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
				RetCode: maskInfo,
				Message: "resource group deleted: " + name,
			}})
		},
	}).run(w)
}

// refuseRGDeleteIfReferenced runs the pre-Delete Scenario 9.W02
// walk. Returns true when the HTTP error has already been
// written (the caller must stop processing) and false when the
// delete may proceed. Pulled out so the shared Bug 174 close
// (deleteWithRollback) can call it from both pre-walk and
// post-walk slots.
func (s *Server) refuseRGDeleteIfReferenced(w http.ResponseWriter, r *http.Request, name string) bool {
	// The count is best-effort: a -1 sentinel from countChildRDs
	// signals the list-side hiccup. We surface the refusal anyway
	// (the RG is presumed unsafe to drop) but degrade the message
	// to omit the unknown count rather than print "-1 resource-
	// definitions".
	childCount := countChildRDs(r.Context(), s.Store, name)
	if childCount == 0 {
		return false
	}

	writeJSON(w, http.StatusConflict, []apiv1.APICallRc{{
		RetCode: apiCallRcError | apiCallRcFailExistsRscDfn,
		Message: rgDeleteRefusedMessage(name, childCount),
		ObjRefs: map[string]string{
			objRefRscGrp: name,
		},
	}})

	return true
}

// captureResourceGroup grabs a snapshot of the RG CRD so the
// Bug 174 post-delete re-scan has something to restore when a
// racing `rd c --resource-group <rg>` slipped past the pre-walk.
// The second return is false when the RG no longer exists at
// capture time (benign idempotent-delete replay) — the rollback
// path is skipped in that case.
func (s *Server) captureResourceGroup(ctx context.Context, name string) (apiv1.ResourceGroup, bool) {
	rg, err := s.Store.ResourceGroups().Get(ctx, name)
	if err != nil {
		return apiv1.ResourceGroup{}, false
	}

	return rg, true
}

// rollbackRGDeleteIfRaced runs the Bug 174 post-Delete re-scan.
// If a child RD reference appeared between the pre-walk and the
// Delete, restore the captured RG and write the 409 envelope the
// pre-walk would have written. Returns true when the rollback
// fired (HTTP error already written, caller must stop) and false
// when the delete is safe to commit. Mirrors Bug 145's
// `rollbackSPDeleteIfRaced` shape.
func (s *Server) rollbackRGDeleteIfRaced(w http.ResponseWriter, r *http.Request, name string, captured *apiv1.ResourceGroup) bool {
	childCount := countChildRDs(r.Context(), s.Store, name)
	if childCount == 0 {
		return false
	}

	// Bug 178: a Create error here used to be silently swallowed,
	// so the cluster ended up with the RG deleted, the racing
	// child RD pointing at a dropped row (the spawn / rd-list
	// path then silently falls back to DfltRscGrp), and the
	// operator handed a 409 "still has resource definitions"
	// envelope that referenced an RG which no longer exists.
	// Surface a 500 envelope that names the rollback failure so
	// the operator knows the deleted primary may need manual
	// restoration.
	createErr := s.Store.ResourceGroups().Create(r.Context(), captured)
	if createErr != nil {
		writeRollbackRestoreFailure(r.Context(), w, createErr,
			objRefRscGrp, name, "linstor rg l")

		return true
	}

	writeJSON(w, http.StatusConflict, []apiv1.APICallRc{{
		RetCode: apiCallRcError | apiCallRcFailExistsRscDfn,
		Message: rgDeleteRefusedMessage(name, childCount),
		ObjRefs: map[string]string{
			objRefRscGrp: name,
		},
	}})

	return true
}

// handleRGPropDelete implements Bug 154: per-key DELETE for resource-
// group properties. Mirrors Bug 142's `handleNodePropDelete` byte-for-
// byte aside from the store accessor — slash-bearing keys like
// `DrbdOptions/auto-quorum` round-trip via Go 1.22's `{key...}`
// wildcard, and a delete-of-missing folds into a 200 + warn-mask
// envelope so reconciler retries don't hot-spin on the second pass.
func (s *Server) handleRGPropDelete(w http.ResponseWriter, r *http.Request) {
	rgName := r.PathValue("rg")

	key := r.PathValue("key")
	if key == "" {
		writeError(w, http.StatusBadRequest, "missing property key")

		return
	}

	existing, err := s.Store.ResourceGroups().Get(r.Context(), rgName)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	if _, present := existing.Props[key]; !present {
		writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
			RetCode: maskWarn,
			Message: "resource group " + rgName + " property already absent: " + key,
			ObjRefs: map[string]string{objRefRscGrp: rgName},
		}})

		return
	}

	delete(existing.Props, key)

	err = s.Store.ResourceGroups().Update(r.Context(), &existing)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "resource group " + rgName + " property deleted: " + key,
		ObjRefs: map[string]string{objRefRscGrp: rgName},
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
