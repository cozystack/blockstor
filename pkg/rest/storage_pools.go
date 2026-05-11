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
	"net/http"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// registerStoragePools wires endpoints serving golinstor's StoragePool calls.
//
// linstor-csi calls /v1/view/storage-pools in its node-registration loop and
// /v1/nodes/{node}/storage-pools[/{pool}] for per-node operations. We start
// with the read-only paths; create/delete land alongside Phase 2 reconcile.
func (s *Server) registerStoragePools(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/view/storage-pools", s.requireStore(s.handleStoragePoolsView))
	mux.HandleFunc("GET /v1/nodes/{node}/storage-pools", s.requireStore(s.handleNodeStoragePoolsList))
	mux.HandleFunc("GET /v1/nodes/{node}/storage-pools/{pool}", s.requireStore(s.handleNodeStoragePoolGet))
}

func (s *Server) handleStoragePoolsView(w http.ResponseWriter, r *http.Request) {
	pools, err := s.Store.StoragePools().List(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	// Optional filters golinstor sends: ?nodes=a,b&storage_pools=p1,p2.
	// Java LINSTOR honours both as case-insensitive set-membership;
	// we match that so /v1/view/storage-pools?nodes=X — the call
	// linstor-csi makes on every NodeRegister — does not return the
	// whole cluster's pools when only one node is asked about.
	nodeFilter := multiValueQuery(r, "nodes")
	poolFilter := multiValueQuery(r, "storage_pools")

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

	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleNodeStoragePoolsList(w http.ResponseWriter, r *http.Request) {
	node := r.PathValue("node")

	pools, err := s.Store.StoragePools().ListByNode(r.Context(), node)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	writeJSON(w, http.StatusOK, pools)
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
