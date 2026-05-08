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
)

// rdCloneRequest is the body for `resource-definition clone`. Only the
// new name is required; advanced options (override props, RG override)
// land when there's demand.
type rdCloneRequest struct {
	Name string `json:"name"`
}

// registerRDClone wires the /v1/resource-definitions/{rd}/clone endpoint.
func (s *Server) registerRDClone(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/resource-definitions/{rd}/clone",
		s.requireStore(s.handleRDClone))
}

// handleRDClone duplicates the source RD's metadata (props, RG ref)
// under a new name. Volume cloning is the satellite's job once the new
// RD enters the reconcile pass.
func (s *Server) handleRDClone(w http.ResponseWriter, r *http.Request) {
	srcName := r.PathValue("rd")

	var req rdCloneRequest

	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "name is required")

		return
	}

	src, err := s.Store.ResourceDefinitions().Get(r.Context(), srcName)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	// Shallow-copy mutable fields. We deliberately don't copy the
	// generated UUID — Create assigns a fresh one.
	clone := src
	clone.Name = req.Name
	clone.UUID = ""

	if src.Props != nil {
		clone.Props = make(map[string]string, len(src.Props))
		maps.Copy(clone.Props, src.Props)
	}

	err = s.Store.ResourceDefinitions().Create(r.Context(), &clone)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusCreated, clone)
}
