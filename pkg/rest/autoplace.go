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

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/placer"
	"github.com/cozystack/blockstor/pkg/store"
)

// registerAutoplace wires `POST /v1/resource-definitions/{rd}/autoplace` and
// the per-resource list/POST/DELETE used by linstor-csi for explicit placement.
func (s *Server) registerAutoplace(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/resource-definitions/{rd}/autoplace",
		s.requireStore(s.handleAutoplace))
	mux.HandleFunc("GET /v1/resource-definitions/{rd}/resources",
		s.requireStore(s.handleResourceList))
	mux.HandleFunc("GET /v1/resource-definitions/{rd}/resources/{node}",
		s.requireStore(s.handleResourceGet))
	mux.HandleFunc("POST /v1/resource-definitions/{rd}/resources",
		s.requireStore(s.handleResourceCreate))
	mux.HandleFunc("DELETE /v1/resource-definitions/{rd}/resources/{node}",
		s.requireStore(s.handleResourceDelete))
}

// handleResourceList answers `GET /v1/resource-definitions/{rd}/resources`,
// the per-RD aggregate linstor-csi polls during ControllerPublishVolume to
// answer "is the resource on this node?". Wraps each Resource in
// ResourceWithVolumes so the wire shape matches /v1/view/resources.
func (s *Server) handleResourceList(w http.ResponseWriter, r *http.Request) {
	rdName := r.PathValue("rd")

	_, err := s.Store.ResourceDefinitions().Get(r.Context(), rdName)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	resList, err := s.Store.Resources().ListByDefinition(r.Context(), rdName)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	out := make([]apiv1.ResourceWithVolumes, 0, len(resList))
	for i := range resList {
		out = append(out, apiv1.ResourceWithVolumes{Resource: resList[i]})
	}

	writeJSON(w, http.StatusOK, out)
}

// handleResourceGet answers `GET /v1/resource-definitions/{rd}/resources/{node}`,
// returning the single Resource on that node or 404.
func (s *Server) handleResourceGet(w http.ResponseWriter, r *http.Request) {
	rdName := r.PathValue("rd")
	node := r.PathValue("node")

	res, err := s.Store.Resources().Get(r.Context(), rdName, node)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusOK, apiv1.ResourceWithVolumes{Resource: res})
}

// handleAutoplace selects up to `place_count` nodes that have a storage
// pool of the requested kind/name and creates Resource objects on them.
//
// Phase 2.5 keeps the placement logic deliberately simple — we trust the
// CRD store as state and never reach out to a satellite. Phase 3's
// autoplacer will weigh free capacity, traits, anti-affinity, etc.
func (s *Server) handleAutoplace(w http.ResponseWriter, r *http.Request) {
	rdName := r.PathValue("rd")

	var req apiv1.AutoPlaceRequest

	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	rd, err := s.Store.ResourceDefinitions().Get(r.Context(), rdName)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	filter := mergeAutoplaceFilter(r.Context(), s.Store, &rd, &req.SelectFilter)

	placed, want, err := placer.New(s.Store).Place(r.Context(), rdName, &filter)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	if placed < want {
		writeError(w, http.StatusConflict,
			"not enough candidate storage pools for the requested placement")

		return
	}

	// Java LINSTOR replies with a `[]ApiCallRc` envelope on success.
	// golinstor's RD.Autoplace ignores an empty body, but tools that
	// surface API messages (e.g. the linstor CLI) want a real result
	// to log. Return MASK_INFO + RC_PLACEMENT_DONE-style entry so the
	// shape matches the oracle's.
	writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
		RetCode: apiCallRcInfo | apiCallRcRDAutoplaceDone,
		Message: "Resource definition '" + rdName + "' auto-placed",
	}})
}

// apiCallRcInfo is upstream LINSTOR's MASK_INFO bit (0x0040_…).
// Combined with a per-action code it lets clients distinguish
// success-with-info from a fatal error.
const (
	apiCallRcInfo            int64 = 0x0040_0000_0000_0000
	apiCallRcRDAutoplaceDone int64 = 0x4231 // ApiConsts.RC_RSC_DFN_PLACED
)

// mergeAutoplaceFilter merges the request's filter on top of the parent
// ResourceGroup's stored select filter. Request fields win.
func mergeAutoplaceFilter(ctx context.Context, st store.Store, rd *apiv1.ResourceDefinition, req *apiv1.AutoSelectFilter) apiv1.AutoSelectFilter {
	out := apiv1.AutoSelectFilter{}

	if rd.ResourceGroupName != "" {
		rg, err := st.ResourceGroups().Get(ctx, rd.ResourceGroupName)
		if err == nil {
			out = rg.SelectFilter
		}
	}

	if req.PlaceCount > 0 {
		out.PlaceCount = req.PlaceCount
	}

	if req.StoragePool != "" {
		out.StoragePool = req.StoragePool
	}

	if len(req.StoragePoolList) > 0 {
		out.StoragePoolList = req.StoragePoolList
	}

	if len(req.StoragePoolDisklessList) > 0 {
		out.StoragePoolDisklessList = req.StoragePoolDisklessList
	}

	if len(req.NodeNameList) > 0 {
		out.NodeNameList = req.NodeNameList
	}

	if len(req.ReplicasOnSame) > 0 {
		out.ReplicasOnSame = req.ReplicasOnSame
	}

	if len(req.ReplicasOnDifferent) > 0 {
		out.ReplicasOnDifferent = req.ReplicasOnDifferent
	}

	if out.PlaceCount == 0 {
		out.PlaceCount = 1
	}

	return out
}

// handleResourceCreate creates a single Resource on a named node from the
// upstream `ResourceCreate` envelope.
func (s *Server) handleResourceCreate(w http.ResponseWriter, r *http.Request) {
	rdName := r.PathValue("rd")

	var body apiv1.ResourceCreate

	err := json.NewDecoder(r.Body).Decode(&body)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	res := body.Resource
	res.Name = rdName

	if res.NodeName == "" {
		writeError(w, http.StatusBadRequest, "node_name is required")

		return
	}

	err = s.Store.Resources().Create(r.Context(), &res)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusCreated, res)
}

// handleResourceDelete drops a single Resource (replica) on a node.
func (s *Server) handleResourceDelete(w http.ResponseWriter, r *http.Request) {
	rdName := r.PathValue("rd")
	node := r.PathValue("node")

	err := s.Store.Resources().Delete(r.Context(), rdName, node)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	w.WriteHeader(http.StatusNoContent)
}
