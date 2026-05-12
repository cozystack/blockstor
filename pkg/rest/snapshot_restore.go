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
	"maps"
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

	snapName := resolveSnapshotName(r, &req)
	if snapName == "" {
		writeError(w, http.StatusBadRequest, "snapshot name required (URL path, from_snapshot, or snapshot_name)")

		return
	}

	snap, err := s.Store.Snapshots().Get(r.Context(), srcRD, snapName)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	newRDName, err := s.materializeRestoredRD(r.Context(), srcRD, &req, &snap)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusCreated, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "snapshot restored: " + snapName + " → " + newRDName,
	}})
}

// resolveSnapshotName picks the snapshot name from the three accepted
// wire dialects (URL path, body from_snapshot, body snapshot_name)
// in precedence order. Empty result = caller should reject with 400.
func resolveSnapshotName(r *http.Request, req *snapshotRestoreRequest) string {
	if v := r.PathValue("snap"); v != "" {
		return v
	}

	if req.FromSnapshot != "" {
		return req.FromSnapshot
	}

	return req.SnapshotName
}

// materializeRestoredRD creates the target RD inheriting the source
// RD's LayerStack + Props (snapshot Props win when set) and hydrates
// its VolumeDefinitions from the snapshot's recorded volume layout.
// Returns the new RD's name on success.
func (s *Server) materializeRestoredRD(ctx context.Context, srcRD string, req *snapshotRestoreRequest, snap *apiv1.Snapshot) (string, error) {
	srcRDObj, err := s.Store.ResourceDefinitions().Get(ctx, srcRD)
	if err != nil {
		return "", err //nolint:wrapcheck // surfaced via writeStoreError
	}

	newRD := apiv1.ResourceDefinition{
		Name:       req.ToResource,
		Props:      maps.Clone(snap.Props),
		LayerStack: srcRDObj.LayerStack,
	}

	if newRD.Props == nil {
		newRD.Props = maps.Clone(srcRDObj.Props)
	}

	// Stamp the clone-source so the dispatcher's buildVolumes (called
	// at every satellite-reconcile of placed Resources) emits
	// DesiredVolume.SourceSnapshot, which routes the storage provider
	// to RestoreVolumeFromSnapshot instead of CreateVolume.
	// `<srcRD>:<snapName>` is the agreed encoding — satellite splits
	// on the colon. We persist on the RD (not per-Resource) because
	// every replica of the new RD clones from the same source.
	if newRD.Props == nil {
		newRD.Props = map[string]string{}
	}

	newRD.Props["BlockstorRestoreFromSnapshot"] = srcRD + ":" + snap.Name

	err = s.Store.ResourceDefinitions().Create(ctx, &newRD)
	if err != nil {
		return "", err //nolint:wrapcheck // surfaced via writeStoreError
	}

	err = hydrateVolumesFromSnapshot(ctx, s, newRD.Name, snap)
	if err != nil {
		return "", err
	}

	return newRD.Name, nil
}

// hydrateVolumesFromSnapshot copies the snapshot's recorded
// VolumeDefinitions onto the freshly-created restore-target RD.
// Without this, the new RD has zero volumes and any subsequent
// autoplace creates empty Resources that never reach UpToDate.
// linstor-csi's CreateVolume-from-source path relies on this
// hydration to surface the cloned PVC's block device.
func hydrateVolumesFromSnapshot(ctx context.Context, s *Server, rdName string, snap *apiv1.Snapshot) error {
	for i := range snap.VolumeDefinitions {
		svd := &snap.VolumeDefinitions[i]
		vd := apiv1.VolumeDefinition{
			VolumeNumber: svd.VolumeNumber,
			SizeKib:      svd.SizeKib,
		}

		err := s.Store.VolumeDefinitions().Create(ctx, rdName, &vd)
		if err != nil {
			return err //nolint:wrapcheck // surfaced via writeStoreError
		}
	}

	return nil
}
