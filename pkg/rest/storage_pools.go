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
	"net/http"

	"github.com/cockroachdb/errors"

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
