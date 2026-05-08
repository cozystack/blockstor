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

// snapshotRestoreRequest is the JSON body upstream linstor expects on
// the restore endpoint.
type snapshotRestoreRequest struct {
	ToResource   string   `json:"to_resource"`
	FromSnapshot string   `json:"from_snapshot"`
	NodeNames    []string `json:"node_names,omitempty"`
}

// registerSnapshotRestore wires the controller-side restore endpoint.
// linstor CLI's `snapshot resource restore` lands here.
func (s *Server) registerSnapshotRestore(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/resource-definitions/{rd}/snapshot-restore-resource",
		s.requireStore(s.handleSnapshotRestore))
}

// handleSnapshotRestore creates a new ResourceDefinition from a
// snapshot. The data clone (zfs send|recv / lvcreate -s of a snapshot
// LV) is the satellite's job once it picks up the new RD via reconcile;
// the controller's job here is to seed the desired-state objects.
func (s *Server) handleSnapshotRestore(w http.ResponseWriter, r *http.Request) {
	srcRD := r.PathValue("rd")

	var req snapshotRestoreRequest

	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	if req.ToResource == "" {
		writeError(w, http.StatusBadRequest, "to_resource is required")

		return
	}

	snap, err := s.Store.Snapshots().Get(r.Context(), srcRD, req.FromSnapshot)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	newRD := apiv1.ResourceDefinition{
		Name:  req.ToResource,
		Props: snap.Props,
	}

	err = s.Store.ResourceDefinitions().Create(r.Context(), &newRD)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusCreated, newRD)
}
