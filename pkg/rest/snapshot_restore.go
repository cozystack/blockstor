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
// the restore endpoint. The snapshot name has two wire dialects:
//
//   - upstream LINSTOR CLI / golinstor: snapshot in URL path
//     (`/snapshot-restore-resource/{snap}`); body carries `nodes`,
//     `stor_pool_rename`, `to_resource` only.
//   - blockstor CSI clone shim + older callers: snapshot in body
//     under `snapshot_name`; URL is the bare path.
//   - legacy in-tree callers: snapshot in body under `from_snapshot`.
//
// Accept all three so the existing tests / linstor-csi / linstor CLI
// can all hit this endpoint without translation glue. The handler
// resolves the snapshot name in that precedence order: path > body
// `from_snapshot` > body `snapshot_name`.
type snapshotRestoreRequest struct {
	ToResource   string   `json:"to_resource"`
	FromSnapshot string   `json:"from_snapshot,omitempty"`
	SnapshotName string   `json:"snapshot_name,omitempty"`
	NodeNames    []string `json:"node_names,omitempty"`
	Nodes        []string `json:"nodes,omitempty"`
}

// registerSnapshotRestore wires the controller-side restore endpoint.
// linstor CLI's `snapshot resource restore` lands here.
func (s *Server) registerSnapshotRestore(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/resource-definitions/{rd}/snapshot-restore-resource",
		s.requireStore(s.handleSnapshotRestore))
	mux.HandleFunc("POST /v1/resource-definitions/{rd}/snapshot-restore-resource/{snap}",
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

	// Snapshot name precedence: URL path (upstream LINSTOR shape) >
	// body `from_snapshot` > body `snapshot_name`. Empty after all
	// three lookups → 400 with a meaningful message instead of the
	// confusing 404 from a NotFound on Get(ctx, rd, "").
	snapName := r.PathValue("snap")
	if snapName == "" {
		snapName = req.FromSnapshot
	}

	if snapName == "" {
		snapName = req.SnapshotName
	}

	if snapName == "" {
		writeError(w, http.StatusBadRequest, "snapshot name required (URL path, from_snapshot, or snapshot_name)")

		return
	}

	snap, err := s.Store.Snapshots().Get(r.Context(), srcRD, snapName)
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

	writeJSON(w, http.StatusCreated, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "snapshot restored: " + snapName + " → " + newRD.Name,
	}})
}
