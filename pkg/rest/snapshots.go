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

// registerSnapshots wires snapshot endpoints. Three different paths land
// here: per-RD CRUD, the cross-RD aggregate (/v1/view/snapshots), and the
// multi-snapshot atomic action upstream uses for snapshot-of-many.
func (s *Server) registerSnapshots(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/view/snapshots", s.requireStore(s.handleSnapshotsView))
	mux.HandleFunc("GET /v1/resource-definitions/{rd}/snapshots",
		s.requireStore(s.handleSnapshotList))
	mux.HandleFunc("POST /v1/resource-definitions/{rd}/snapshots",
		s.requireStore(s.handleSnapshotCreate))
	mux.HandleFunc("GET /v1/resource-definitions/{rd}/snapshots/{snap}",
		s.requireStore(s.handleSnapshotGet))
	mux.HandleFunc("DELETE /v1/resource-definitions/{rd}/snapshots/{snap}",
		s.requireStore(s.handleSnapshotDelete))
}

func (s *Server) handleSnapshotsView(w http.ResponseWriter, r *http.Request) {
	snaps, err := s.Store.Snapshots().List(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	writeJSON(w, http.StatusOK, snaps)
}

func (s *Server) handleSnapshotList(w http.ResponseWriter, r *http.Request) {
	rd := r.PathValue("rd")

	// Verify the parent RD exists so missing RD is 404, not [].
	_, err := s.Store.ResourceDefinitions().Get(r.Context(), rd)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	snaps, err := s.Store.Snapshots().ListByDefinition(r.Context(), rd)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusOK, snaps)
}

func (s *Server) handleSnapshotGet(w http.ResponseWriter, r *http.Request) {
	rd := r.PathValue("rd")
	snapName := r.PathValue("snap")

	snap, err := s.Store.Snapshots().Get(r.Context(), rd, snapName)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusOK, snap)
}

func (s *Server) handleSnapshotCreate(w http.ResponseWriter, r *http.Request) {
	rd := r.PathValue("rd")

	var snap apiv1.Snapshot

	err := json.NewDecoder(r.Body).Decode(&snap)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	if snap.Name == "" {
		writeError(w, http.StatusBadRequest, "snapshot name is required")

		return
	}

	snap.ResourceName = rd

	err = s.Store.Snapshots().Create(r.Context(), &snap)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusCreated, snap)
}

func (s *Server) handleSnapshotDelete(w http.ResponseWriter, r *http.Request) {
	rd := r.PathValue("rd")
	snapName := r.PathValue("snap")

	err := s.Store.Snapshots().Delete(r.Context(), rd, snapName)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	w.WriteHeader(http.StatusNoContent)
}
