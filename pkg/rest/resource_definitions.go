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

// registerResourceDefinitions wires /v1/resource-definitions CRUD. Spawn,
// Clone, snapshot-restore, and per-volume endpoints land in later slices.
func (s *Server) registerResourceDefinitions(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/resource-definitions", s.requireStore(s.handleRDList))
	mux.HandleFunc("GET /v1/resource-definitions/{rd}", s.requireStore(s.handleRDGet))
	mux.HandleFunc("POST /v1/resource-definitions", s.requireStore(s.handleRDCreate))
	mux.HandleFunc("PUT /v1/resource-definitions/{rd}", s.requireStore(s.handleRDUpdate))
	mux.HandleFunc("DELETE /v1/resource-definitions/{rd}", s.requireStore(s.handleRDDelete))
}

func (s *Server) handleRDList(w http.ResponseWriter, r *http.Request) {
	rds, err := s.Store.ResourceDefinitions().List(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	writeJSON(w, http.StatusOK, rds)
}

func (s *Server) handleRDGet(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("rd")

	rd, err := s.Store.ResourceDefinitions().Get(r.Context(), name)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusOK, rd)
}

func (s *Server) handleRDCreate(w http.ResponseWriter, r *http.Request) {
	var body apiv1.ResourceDefinitionCreate

	dec := json.NewDecoder(r.Body)
	// upstream LINSTOR has tolerated extra fields here historically; mirror
	// that to keep golinstor (and any home-grown clients) happy.
	err := dec.Decode(&body)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	rd := body.ResourceDefinition
	if body.ExternalName != "" && rd.ExternalName == "" {
		rd.ExternalName = body.ExternalName
	}

	if rd.Name == "" {
		writeError(w, http.StatusBadRequest, "resource definition name is required")

		return
	}

	err = s.Store.ResourceDefinitions().Create(r.Context(), &rd)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusCreated, rd)
}

func (s *Server) handleRDUpdate(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("rd")

	var rd apiv1.ResourceDefinition

	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()

	err := dec.Decode(&rd)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	rd.Name = name

	err = s.Store.ResourceDefinitions().Update(r.Context(), &rd)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusOK, rd)
}

func (s *Server) handleRDDelete(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("rd")

	err := s.Store.ResourceDefinitions().Delete(r.Context(), name)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	w.WriteHeader(http.StatusNoContent)
}
