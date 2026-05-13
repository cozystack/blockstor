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

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// registerStoragePools wires endpoints serving golinstor's StoragePool calls.
//
// linstor-csi calls /v1/view/storage-pools in its node-registration loop and
// /v1/nodes/{node}/storage-pools[/{pool}] for per-node operations. The POST
// path on /v1/nodes/{node}/storage-pools is hit by satellite Hello/heartbeat
// loops (and piraeus's operator) to register a pool — without it the
// satellite retry loop spins forever against a 405 (Bug 31).
func (s *Server) registerStoragePools(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/view/storage-pools", s.requireStore(s.handleStoragePoolsView))
	mux.HandleFunc("GET /v1/nodes/{node}/storage-pools", s.requireStore(s.handleNodeStoragePoolsList))
	mux.HandleFunc("POST /v1/nodes/{node}/storage-pools", s.requireStore(s.handleNodeStoragePoolCreate))
	mux.HandleFunc("GET /v1/nodes/{node}/storage-pools/{pool}", s.requireStore(s.handleNodeStoragePoolGet))
	mux.HandleFunc("DELETE /v1/nodes/{node}/storage-pools/{pool}", s.requireStore(s.handleNodeStoragePoolDelete))
}

// handleNodeStoragePoolDelete serves DELETE /v1/nodes/{node}/storage-pools/{pool}.
//
// Idempotent: removing a pool that's already absent folds into a 200
// success envelope. Python linstor-client's `_handle_response_error`
// path tries JSON then XML to decode error responses; a bare 405 from
// Go's default mux (the previous shape with no handler registered)
// trips its `xml.etree` parser and crashes the CLI. Returning a
// proper ApiCallRc envelope on every code path keeps both
// golinstor and the Python CLI happy.
func (s *Server) handleNodeStoragePoolDelete(w http.ResponseWriter, r *http.Request) {
	node := r.PathValue("node")
	pool := r.PathValue("pool")

	err := s.Store.StoragePools().Delete(r.Context(), node, pool)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		writeStoreError(w, err)

		return
	}

	msg := "storage pool deleted: " + pool + " on " + node
	if err != nil {
		msg = "storage pool already absent: " + pool + " on " + node
	}

	writeJSON(w, http.StatusOK, []apiv1.APICallRc{{RetCode: maskInfo, Message: msg}})
}

func (s *Server) handleStoragePoolsView(w http.ResponseWriter, r *http.Request) {
	// Optional filters golinstor sends: ?nodes=a,b&storage_pools=p1,p2.
	// Java LINSTOR honours both as case-insensitive set-membership;
	// we match that so /v1/view/storage-pools?nodes=X — the call
	// linstor-csi makes on every NodeRegister — does not return the
	// whole cluster's pools when only one node is asked about.
	out, err := s.listFilteredStoragePools(r.Context(),
		multiValueQuery(r, "nodes"),
		multiValueQuery(r, "storage_pools"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	writeJSON(w, http.StatusOK, out)
}

// handleNodeStoragePoolsList serves GET /v1/nodes/{node}/storage-pools.
//
// Implementation note: we deliberately go through the same List()+filter
// pipeline that /v1/view/storage-pools uses (rather than the store's
// ListByNode shortcut) for two reasons:
//
//  1. The k8s backend's ListByNode relies on a label selector that is only
//     populated when the CRD was created through our Create() path. Pools
//     that land in the cluster via operator `kubectl apply -f` or a
//     migration won't carry the label and would silently disappear from
//     the per-node listing — but they show up correctly in the view.
//     linstor-csi's autoplace probes /v1/nodes/{node}/storage-pools per
//     node, so an empty per-node response means "no candidate nodes",
//     leading to ResourceExhausted and stuck-Pending PVCs even though
//     the pools are visible in the aggregate view.
//  2. Java LINSTOR matches node names case-insensitively in both paths.
//     Routing the per-node handler through the same matchAnyFold filter
//     keeps the two endpoints in lockstep — a parity invariant the
//     storage_pools_test.go MatchesViewFiltering test pins.
func (s *Server) handleNodeStoragePoolsList(w http.ResponseWriter, r *http.Request) {
	node := r.PathValue("node")

	out, err := s.listFilteredStoragePools(r.Context(), []string{node}, nil)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	writeJSON(w, http.StatusOK, out)
}

// listFilteredStoragePools returns the wire-shape pool slice after applying
// the case-insensitive node / pool filters Java LINSTOR uses. Empty filters
// mean "no constraint on this dimension". The slice is always non-nil so
// JSON encoding yields [] instead of null — linstor-csi rejects null bodies.
func (s *Server) listFilteredStoragePools(ctx context.Context, nodeFilter, poolFilter []string) ([]apiv1.StoragePool, error) {
	pools, err := s.Store.StoragePools().List(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "list storage pools")
	}

	out := make([]apiv1.StoragePool, 0, len(pools))

	for i := range pools {
		if !matchAnyFold(nodeFilter, pools[i].NodeName) {
			continue
		}

		if !matchAnyFold(poolFilter, pools[i].StoragePoolName) {
			continue
		}

		out = append(out, pools[i])
	}

	return out, nil
}

func (s *Server) handleNodeStoragePoolGet(w http.ResponseWriter, r *http.Request) {
	node := r.PathValue("node")
	pool := r.PathValue("pool")

	sp, err := s.Store.StoragePools().Get(r.Context(), node, pool)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusOK, sp)
}

// isKnownStoragePoolKind mirrors the set of apiv1.StoragePoolKind*
// constants we support on the satellite side, minus the two upstream
// kinds (OPENFLEX_TARGET, REMOTE_SPDK) we deliberately stub as
// out-of-scope. Validating here gives the satellite an immediate 400
// instead of letting a garbage CRD land in the store and surface as
// a downstream NPE.
func isKnownStoragePoolKind(kind string) bool {
	switch kind {
	case apiv1.StoragePoolKindLVM,
		apiv1.StoragePoolKindLVMThin,
		apiv1.StoragePoolKindZFS,
		apiv1.StoragePoolKindZFSThin,
		apiv1.StoragePoolKindFile,
		apiv1.StoragePoolKindFileThin,
		apiv1.StoragePoolKindDiskless:
		return true
	default:
		return false
	}
}

// handleNodeStoragePoolCreate serves POST /v1/nodes/{node}/storage-pools.
//
// Body shape mirrors upstream's `JsonGenTypes.StoragePool` (the same struct
// GET emits). The path's {node} wins over any node_name in the body so we
// can't end up creating a pool on a different node than the URL claims —
// piraeus assumes that invariant when it iterates over per-node SPs in
// its reconcile loop.
//
// Idempotency: re-POSTing the same (node, pool) is a no-op success rather
// than 409 Conflict. Piraeus's satellite Hello/heartbeat reposts the same
// pool on every registration tick; treating it as upsert keeps the retry
// loop quiet and matches the operator's expectation that "re-announce"
// is safe. Upstream LINSTOR returns an error on duplicate-create, but
// piraeus retries through that error indefinitely, so the practical
// outcome is the same — we just skip the retry storm.
func (s *Server) handleNodeStoragePoolCreate(w http.ResponseWriter, r *http.Request) {
	node := r.PathValue("node")

	body, ok := decodeStoragePoolCreate(w, r, node)
	if !ok {
		return
	}

	// Reject pools on ghost nodes with a clean 404. Without this the
	// store-level create would succeed (the StoragePoolStore key is
	// (node, pool) and doesn't FK to NodeStore) and the satellite
	// would learn about a pool on a node the controller doesn't
	// know — a permanent orphan from the controller's POV.
	_, nodeErr := s.Store.Nodes().Get(r.Context(), node)
	if nodeErr != nil {
		if errors.Is(nodeErr, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "node not found: "+node)

			return
		}

		writeError(w, http.StatusInternalServerError, nodeErr.Error())

		return
	}

	createErr := s.Store.StoragePools().Create(r.Context(), &body)
	if createErr == nil {
		writeJSON(w, http.StatusCreated, []apiv1.APICallRc{{
			RetCode: maskInfo,
			Message: "storage pool created: " + body.StoragePoolName + " on " + node,
		}})

		return
	}

	if errors.Is(createErr, store.ErrAlreadyExists) {
		s.upsertStoragePool(w, r, &body, node)

		return
	}

	writeStoreError(w, createErr)
}

// decodeStoragePoolCreate parses the POST body, normalises NodeName
// from the URL path, and validates the mandatory fields. Writes the
// 400 response itself and returns ok=false if anything is off.
func decodeStoragePoolCreate(w http.ResponseWriter, r *http.Request, node string) (apiv1.StoragePool, bool) {
	var body apiv1.StoragePool

	err := json.NewDecoder(r.Body).Decode(&body)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return apiv1.StoragePool{}, false
	}

	body.NodeName = node

	if body.StoragePoolName == "" {
		writeError(w, http.StatusBadRequest, "storage_pool_name is required")

		return apiv1.StoragePool{}, false
	}

	if body.ProviderKind == "" {
		writeError(w, http.StatusBadRequest, "provider_kind is required")

		return apiv1.StoragePool{}, false
	}

	if !isKnownStoragePoolKind(body.ProviderKind) {
		writeError(w, http.StatusBadRequest, "unknown provider_kind: "+body.ProviderKind)

		return apiv1.StoragePool{}, false
	}

	return body, true
}

// upsertStoragePool merges the POST body's Spec fields onto the
// existing row when a pool with the same (node, pool) already exists.
// Capacity / status fields stay where SetCapacity put them. Returns
// 201 + envelope on success.
func (s *Server) upsertStoragePool(w http.ResponseWriter, r *http.Request, body *apiv1.StoragePool, node string) {
	existing, getErr := s.Store.StoragePools().Get(r.Context(), node, body.StoragePoolName)
	if getErr != nil {
		writeStoreError(w, getErr)

		return
	}

	existing.ProviderKind = body.ProviderKind
	if body.Props != nil {
		existing.Props = body.Props
	}

	if body.FreeSpaceMgrName != "" {
		existing.FreeSpaceMgrName = body.FreeSpaceMgrName
	}

	if body.SharedSpaceID != "" {
		existing.SharedSpaceID = body.SharedSpaceID
	}

	existing.ExternalLocking = body.ExternalLocking

	updateErr := s.Store.StoragePools().Update(r.Context(), &existing)
	if updateErr != nil {
		writeStoreError(w, updateErr)

		return
	}

	writeJSON(w, http.StatusCreated, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "storage pool already present, updated in place: " + body.StoragePoolName + " on " + node,
	}})
}
