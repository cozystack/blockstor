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
	"strconv"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// registerVolumeDefinitions wires
// /v1/resource-definitions/{rd}/volume-definitions[/{vn}] CRUD.
func (s *Server) registerVolumeDefinitions(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/view/volume-definitions",
		s.requireStore(s.handleVDView))
	mux.HandleFunc("GET /v1/resource-definitions/{rd}/volume-definitions",
		s.requireStore(s.handleVDList))
	mux.HandleFunc("POST /v1/resource-definitions/{rd}/volume-definitions",
		s.requireStore(s.handleVDCreate))
	mux.HandleFunc("GET /v1/resource-definitions/{rd}/volume-definitions/{vn}",
		s.requireStore(s.handleVDGet))
	mux.HandleFunc("PUT /v1/resource-definitions/{rd}/volume-definitions/{vn}",
		s.requireStore(s.handleVDUpdate))
	mux.HandleFunc("DELETE /v1/resource-definitions/{rd}/volume-definitions/{vn}",
		s.requireStore(s.handleVDDelete))
}

// handleVDView is the cluster-wide aggregate for
// `linstor vd l` / golinstor's VolumeDefinitions.GetAll(). Iterates
// over every RD and flattens each (rd, volume_definition) pair onto
// the response, embedding the parent RD name so consumers can group.
// Matches upstream LINSTOR's `/v1/view/volume-definitions` shape:
// each entry wraps the VolumeDefinition plus `resource_name` so the
// Python CLI's table renderer has the column it expects.
func (s *Server) handleVDView(w http.ResponseWriter, r *http.Request) {
	rds, err := s.Store.ResourceDefinitions().List(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	type entry struct {
		apiv1.VolumeDefinition

		ResourceName string `json:"resource_name"`
	}

	out := make([]entry, 0)

	for i := range rds {
		vds, listErr := s.Store.VolumeDefinitions().List(r.Context(), rds[i].Name)
		if listErr != nil {
			writeError(w, http.StatusInternalServerError, listErr.Error())

			return
		}

		for j := range vds {
			out = append(out, entry{
				VolumeDefinition: vds[j],
				ResourceName:     rds[i].Name,
			})
		}
	}

	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleVDList(w http.ResponseWriter, r *http.Request) {
	rd := r.PathValue("rd")

	// Verify the parent RD exists so a missing RD is 404, not 200 with [].
	// k8s store does this internally; in-memory does not, so we do it here.
	_, err := s.Store.ResourceDefinitions().Get(r.Context(), rd)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	vds, err := s.Store.VolumeDefinitions().List(r.Context(), rd)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusOK, vds)
}

func (s *Server) handleVDGet(w http.ResponseWriter, r *http.Request) {
	rd := r.PathValue("rd")

	vn, err := parseVolNum(r.PathValue("vn"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	vd, err := s.Store.VolumeDefinitions().Get(r.Context(), rd, vn)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusOK, vd)
}

// handleVDCreate accepts either the upstream `VolumeDefinitionCreate`
// envelope (`{"volume_definition": {...}}`) or a bare VolumeDefinition body —
// both shapes appear in the wild.
func (s *Server) handleVDCreate(w http.ResponseWriter, r *http.Request) {
	rd := r.PathValue("rd")

	var envelope apiv1.VolumeDefinitionCreate

	dec := json.NewDecoder(r.Body)

	err := dec.Decode(&envelope)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	vd := envelope.VolumeDefinition

	err = s.Store.VolumeDefinitions().Create(r.Context(), rd, &vd)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusCreated, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "volume definition created",
	}})
}

func (s *Server) handleVDUpdate(w http.ResponseWriter, r *http.Request) {
	rd := r.PathValue("rd")

	vn, err := parseVolNum(r.PathValue("vn"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	var vd apiv1.VolumeDefinition

	err = json.NewDecoder(r.Body).Decode(&vd)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	vd.VolumeNumber = vn

	err = s.Store.VolumeDefinitions().Update(r.Context(), rd, &vd)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "volume definition modified",
	}})
}

func (s *Server) handleVDDelete(w http.ResponseWriter, r *http.Request) {
	rd := r.PathValue("rd")

	vn, err := parseVolNum(r.PathValue("vn"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	err = s.Store.VolumeDefinitions().Delete(r.Context(), rd, vn)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "volume definition deleted",
	}})
}

func parseVolNum(raw string) (int32, error) {
	v, err := strconv.ParseInt(raw, 10, 32)
	if err != nil {
		return 0, err //nolint:wrapcheck // returned to handler that wraps it
	}

	return int32(v), nil
}
