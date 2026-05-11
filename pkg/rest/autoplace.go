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
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"

	"github.com/cockroachdb/errors"

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

	// linstor-csi (and piraeus-operator's
	// LinstorSatelliteConfiguration.spec.storageClasses[*].layerList) sets
	// `layer_list` on the autoplace call rather than on RD create. Persist
	// it onto rd.LayerStack here so the dispatcher → satellite chain sees
	// the right composition. RD-level LayerStack wins if already set
	// (operator-supplied via REST POST or CRD create).
	if len(req.LayerList) > 0 && len(rd.LayerStack) == 0 {
		rd.LayerStack = append([]string(nil), req.LayerList...)

		err = s.Store.ResourceDefinitions().Update(r.Context(), &rd)
		if err != nil {
			writeStoreError(w, err)

			return
		}
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
	apiCallRcRscDeleted      int64 = 0x4200 // ApiConsts.RC_RSC_DELETED
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

	if req.DisklessOnRemaining {
		out.DisklessOnRemaining = true
	}

	if out.PlaceCount == 0 {
		out.PlaceCount = 1
	}

	return out
}

// handleResourceCreate creates one or more Resources from the upstream
// `[]ResourceCreate` envelope. The upstream OpenAPI shape is an array
// (the CLI's `linstor resource create n1 n2 n3 rd` posts one item per
// node); we also accept a bare object for backwards-compat with
// pre-existing blockstor callers.
func (s *Server) handleResourceCreate(w http.ResponseWriter, r *http.Request) {
	rdName := r.PathValue("rd")

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	envelopes, err := decodeResourceCreateBody(body)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	if len(envelopes) == 0 {
		writeError(w, http.StatusBadRequest, "empty resource create body")

		return
	}

	created, ok := s.createResources(w, r, rdName, envelopes)
	if !ok {
		return
	}

	writeJSON(w, http.StatusCreated, created)
}

// createResources walks the envelopes from a POST to
// /v1/resource-definitions/{rd}/resources and either creates each
// Resource fresh or promotes an existing diskless/tiebreaker replica
// to diskful (upstream LINSTOR semantics). Returns (created, true) on
// success; writes the HTTP error and returns (nil, false) on the
// first failure.
func (s *Server) createResources(w http.ResponseWriter, r *http.Request, rdName string, envelopes []apiv1.ResourceCreate) ([]apiv1.Resource, bool) {
	created := make([]apiv1.Resource, 0, len(envelopes))

	for i := range envelopes {
		env := &envelopes[i]
		res := env.Resource
		res.Name = rdName

		if res.NodeName == "" {
			writeError(w, http.StatusBadRequest, "node_name is required on every resource create entry")

			return nil, false
		}

		// Same CSI pass-through as handleAutoplace: linstor-csi may set
		// layer_list on the explicit-placement call rather than on RD create.
		// Persist onto rd.LayerStack if not already set so the satellite
		// reconciler sees the right composition.
		if len(env.LayerList) > 0 {
			rd, getErr := s.Store.ResourceDefinitions().Get(r.Context(), rdName)
			if getErr == nil && len(rd.LayerStack) == 0 {
				rd.LayerStack = append([]string(nil), env.LayerList...)
				_ = s.Store.ResourceDefinitions().Update(r.Context(), &rd)
			}
		}

		out, ok := s.createOrPromoteResource(w, r, &res)
		if !ok {
			return nil, false
		}

		created = append(created, *out)
	}

	return created, true
}

// createOrPromoteResource creates res or promotes an existing
// diskless replica in place. Writes the HTTP error and returns
// (nil, false) on failure.
func (s *Server) createOrPromoteResource(w http.ResponseWriter, r *http.Request, res *apiv1.Resource) (*apiv1.Resource, bool) {
	err := s.Store.Resources().Create(r.Context(), res)
	if err == nil {
		return res, true
	}

	// Upstream LINSTOR semantics: `resource create <node> <rd>
	// --storage-pool <pool>` on top of an existing DISKLESS or
	// TIE_BREAKER replica converts it to diskful (effectively an
	// implicit toggle-disk-to-diskful). Mirror that here when
	// the only thing in the way is the diskless/tiebreaker flag.
	if errors.Is(err, store.ErrAlreadyExists) && res.Props["StorPoolName"] != "" {
		promoted, promErr := s.promoteDisklessReplica(r.Context(), res)
		if promErr != nil {
			writeStoreError(w, promErr)

			return nil, false
		}

		return promoted, true
	}

	writeStoreError(w, err)

	return nil, false
}

// promoteDisklessReplica takes a Resource the caller just tried to
// create-as-diskful, looks up the existing one on the same (node, RD),
// and if it's a DISKLESS / TIE_BREAKER replica converts it to diskful
// by dropping those flags and stamping the new StorPoolName onto
// Spec.Props. The satellite reconciler picks the Resource change up
// via its watch and runs the normal storage-attach chain.
//
// Returns the updated Resource on success, or wraps ErrAlreadyExists
// when the existing replica is NOT a diskless witness (i.e. a real
// conflict the caller should surface as 409).
func (s *Server) promoteDisklessReplica(ctx context.Context, target *apiv1.Resource) (*apiv1.Resource, error) {
	existing, err := s.Store.Resources().Get(ctx, target.Name, target.NodeName)
	if err != nil {
		return nil, errors.Wrapf(err, "lookup existing replica %s.%s", target.Name, target.NodeName)
	}

	wasDiskless := false

	keep := existing.Flags[:0]

	for _, flag := range existing.Flags {
		if flag == apiv1.ResourceFlagDiskless || flag == apiv1.ResourceFlagTieBreaker {
			wasDiskless = true

			continue
		}

		keep = append(keep, flag)
	}

	if !wasDiskless {
		// Existing replica is a real diskful one — true conflict.
		return nil, errors.Wrapf(store.ErrAlreadyExists,
			"resource %q on node %q already diskful", target.Name, target.NodeName)
	}

	existing.Flags = keep

	if existing.Props == nil {
		existing.Props = map[string]string{}
	}

	existing.Props["StorPoolName"] = target.Props["StorPoolName"]

	err = s.Store.Resources().Update(ctx, &existing)
	if err != nil {
		return nil, errors.Wrapf(err, "promote diskless %s.%s", target.Name, target.NodeName)
	}

	return &existing, nil
}

// decodeResourceCreateBody accepts either the upstream-LINSTOR
// `[]ResourceCreate` envelope (the shape the CLI posts) or a bare
// `ResourceCreate` object (legacy blockstor callers). Returns a
// normalised slice the handler iterates over.
func decodeResourceCreateBody(body []byte) ([]apiv1.ResourceCreate, error) {
	trimmed := bytes.TrimLeft(body, " \t\r\n")

	if len(trimmed) > 0 && trimmed[0] == '[' {
		var envelopes []apiv1.ResourceCreate

		err := json.Unmarshal(body, &envelopes)
		if err != nil {
			return nil, errors.Wrap(err, "decode resource create array")
		}

		return envelopes, nil
	}

	var single apiv1.ResourceCreate

	err := json.Unmarshal(body, &single)
	if err != nil {
		return nil, errors.Wrap(err, "decode resource create object")
	}

	return []apiv1.ResourceCreate{single}, nil
}

// handleResourceDelete drops a single Resource (replica) on a node.
// Upstream LINSTOR replies with an `[]ApiCallRc` JSON envelope; the
// `linstor` CLI insists on parsing one even when the HTTP status is
// 200/204. Returning a bare `204 No Content` makes the CLI emit
// "Unable to parse REST json data: Expecting value", so we mirror
// the upstream shape: HTTP 200 + `MASK_INFO | RC_RSC_DELETED` entry.
func (s *Server) handleResourceDelete(w http.ResponseWriter, r *http.Request) {
	rdName := r.PathValue("rd")
	node := r.PathValue("node")

	err := s.Store.Resources().Delete(r.Context(), rdName, node)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
		RetCode: apiCallRcInfo | apiCallRcRscDeleted,
		Message: "Resource '" + rdName + "' on node '" + node + "' deleted",
	}})
}
