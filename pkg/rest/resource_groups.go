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

	err = s.Store.ResourceGroups().Create(r.Context(), &rg)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusCreated, rg)
}

func (s *Server) handleRGUpdate(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("rg")

	var rg apiv1.ResourceGroup

	err := json.NewDecoder(r.Body).Decode(&rg)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	rg.Name = name

	err = s.Store.ResourceGroups().Update(r.Context(), &rg)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusOK, rg)
}

func (s *Server) handleRGDelete(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("rg")

	err := s.Store.ResourceGroups().Delete(r.Context(), name)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	w.WriteHeader(http.StatusNoContent)
}
