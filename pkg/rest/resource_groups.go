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
	"reflect"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
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

	writeJSON(w, http.StatusOK, rgs)
}

func (s *Server) handleRGGet(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("rg")

	rg, err := s.Store.ResourceGroups().Get(r.Context(), name)
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

	err = s.Store.ResourceGroups().Update(r.Context(), &existing)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "resource group modified: " + name,
	}})
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

func (s *Server) handleRGDelete(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("rg")

	err := s.Store.ResourceGroups().Delete(r.Context(), name)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "resource group deleted: " + name,
	}})
}
