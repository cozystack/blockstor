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
)

// registerStats wires the cluster-wide counter endpoint. linstor CLI
// uses /v1/stats for `linstor controller list` summaries; monitoring
// stacks scrape it for high-level cluster gauges.
func (s *Server) registerStats(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/stats", s.requireStore(s.handleStats))
}

// handleStats counts top-level objects from the store. Errors at any
// step degrade gracefully — we surface zeros for whatever failed
// rather than 500-ing the whole endpoint, since stats is supposed to
// be cheap and resilient.
func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	out := map[string]int{
		statKeyNodes:               0,
		statKeyResourceDefinitions: 0,
		statKeyResources:           0,
		statKeyStoragePools:        0,
		statKeySnapshots:           0,
	}

	nodes, err := s.Store.Nodes().List(ctx)
	if err == nil {
		out[statKeyNodes] = len(nodes)
	}

	rds, err := s.Store.ResourceDefinitions().List(ctx)
	if err == nil {
		out[statKeyResourceDefinitions] = len(rds)
	}

	res, err := s.Store.Resources().List(ctx)
	if err == nil {
		out[statKeyResources] = len(res)
	}

	sps, err := s.Store.StoragePools().List(ctx)
	if err == nil {
		out[statKeyStoragePools] = len(sps)
	}

	snaps, err := s.Store.Snapshots().List(ctx)
	if err == nil {
		out[statKeySnapshots] = len(snaps)
	}

	writeJSON(w, http.StatusOK, out)
}

const (
	statKeyNodes               = "nodes"
	statKeyResourceDefinitions = "resource_definitions"
	statKeyResources           = "resources"
	statKeyStoragePools        = "storage_pools"
	statKeySnapshots           = "snapshots"
)
