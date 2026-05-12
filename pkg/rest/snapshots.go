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
	"net/http"
	"slices"

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

	// Optional filters golinstor sends: ?resources=rd1,rd2 &
	// snapshots=name1,name2 — case-insensitive set membership against
	// Java LINSTOR's behaviour. Without filtering linstor-csi's "do
	// any snapshots exist for this volume?" poll has to scan the
	// whole cluster every cycle.
	rdFilter := multiValueQuery(r, "resources")
	nameFilter := multiValueQuery(r, "snapshots")

	out := make([]apiv1.Snapshot, 0, len(snaps))

	for i := range snaps {
		if !matchAnyFold(rdFilter, snaps[i].ResourceName) {
			continue
		}

		if !matchAnyFold(nameFilter, snaps[i].Name) {
			continue
		}

		out = append(out, snaps[i])
	}

	writeJSON(w, http.StatusOK, out)
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

	err = s.hydrateSnapshotFromRD(r.Context(), &snap, rd)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	err = s.Store.Snapshots().Create(r.Context(), &snap)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusCreated, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "snapshot created: " + snap.Name,
	}})
}

// hydrateSnapshotFromRD fills in the per-snapshot fields the
// snapshot-restore-resource handler + the autoplace constraint need
// downstream. Three derivations:
//
//   - VolumeDefinitions: copied from the source RD when absent; without
//     these a restore-target RD comes up with zero volumes.
//   - Props: inherited from the source RD when absent.
//   - Nodes: upstream-LINSTOR semantic — empty means "every diskful
//     replica". The satellite reconciler gates per-snapshot work on
//     slices.Contains(snap.Spec.Nodes, self), so an empty list would
//     silently produce a zero-replica snapshot.
func (s *Server) hydrateSnapshotFromRD(ctx context.Context, snap *apiv1.Snapshot, rd string) error {
	srcRD, err := s.Store.ResourceDefinitions().Get(ctx, rd)
	if err != nil {
		return err //nolint:wrapcheck // surfaced via writeStoreError
	}

	if len(snap.VolumeDefinitions) == 0 {
		vds, vdErr := s.Store.VolumeDefinitions().List(ctx, rd)
		if vdErr != nil {
			return vdErr //nolint:wrapcheck // surfaced via writeStoreError
		}

		snap.VolumeDefinitions = make([]apiv1.SnapshotVolumeDef, 0, len(vds))
		for _, vd := range vds {
			snap.VolumeDefinitions = append(snap.VolumeDefinitions, apiv1.SnapshotVolumeDef{
				VolumeNumber: vd.VolumeNumber,
				SizeKib:      vd.SizeKib,
			})
		}
	}

	if snap.Props == nil {
		snap.Props = srcRD.Props
	}

	if len(snap.Nodes) == 0 {
		snap.Nodes, err = listDiskfulNodes(ctx, s, rd)
		if err != nil {
			return err
		}
	}

	return nil
}

// listDiskfulNodes returns the node names that host a diskful
// (non-DISKLESS) replica of rd. Used to default snap.Nodes when the
// caller didn't pin a per-node list — matches upstream's
// "snapshot all diskful replicas" semantic.
func listDiskfulNodes(ctx context.Context, s *Server, rd string) ([]string, error) {
	resList, err := s.Store.Resources().ListByDefinition(ctx, rd)
	if err != nil {
		return nil, err //nolint:wrapcheck // surfaced via writeStoreError
	}

	out := make([]string, 0, len(resList))

	for i := range resList {
		if slices.Contains(resList[i].Flags, apiv1.ResourceFlagDiskless) {
			continue
		}

		out = append(out, resList[i].NodeName)
	}

	return out, nil
}

func (s *Server) handleSnapshotDelete(w http.ResponseWriter, r *http.Request) {
	rd := r.PathValue("rd")
	snapName := r.PathValue("snap")

	err := s.Store.Snapshots().Delete(r.Context(), rd, snapName)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "snapshot deleted: " + snapName,
	}})
}
