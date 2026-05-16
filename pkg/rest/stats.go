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

// registerStats wires the cluster-wide counter endpoint family.
//
// Two surface shapes coexist:
//
//   - The legacy aggregate `GET /v1/stats` returns a multi-key map
//     (nodes / resource_definitions / resources / storage_pools /
//     snapshots) that pre-Phase-11 scrapers + the cluster-state
//     smoke test consume directly. It stays in place; flipping the
//     shape would silently break those callers.
//   - Bug 195: `linstor controller list-stats` walks the upstream
//     `/v1/stats/{kind}` sub-paths and reads `.count` off each
//     reply. The pre-fix apiserver wired only the aggregate, so the
//     CLI's per-kind GETs all 404'd. We register the six sub-paths
//     `linstor controller list-stats` touches — three upstream-
//     OpenAPI-declared (resource-definitions, resources,
//     storage-pools) plus three blockstor-extension (volume-
//     definitions, volumes, snapshots) for parity with the
//     aggregate's keys.
//
// Wire shape per sub-path: `{"count": int64}` — matches upstream's
// `ResourceDefinitionStats` / `ResourceStats` / `StoragePoolStats`
// schemas which all pin `required: [count]; format: int64`.
func (s *Server) registerStats(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/stats", s.requireStore(s.handleStats))
	mux.HandleFunc("GET /v1/stats/resource-definitions",
		s.requireStore(s.handleStatsResourceDefinitions))
	mux.HandleFunc("GET /v1/stats/resources",
		s.requireStore(s.handleStatsResources))
	mux.HandleFunc("GET /v1/stats/storage-pools",
		s.requireStore(s.handleStatsStoragePools))
	mux.HandleFunc("GET /v1/stats/volume-definitions",
		s.requireStore(s.handleStatsVolumeDefinitions))
	mux.HandleFunc("GET /v1/stats/volumes",
		s.requireStore(s.handleStatsVolumes))
	mux.HandleFunc("GET /v1/stats/snapshots",
		s.requireStore(s.handleStatsSnapshots))
}

// countEnvelope is the upstream `*Stats` schema shape: a single
// `count` field with int64 width. Centralised so every sub-path
// handler emits the byte-identical wire shape — flipping any one
// of them to a divergent envelope would silently break
// `linstor controller list-stats`.
type countEnvelope struct {
	Count int64 `json:"count"`
}

// handleStatsResourceDefinitions emits the count of resource
// definitions. Bug 195. List+len is the cheapest implementation —
// upstream's count endpoints don't promise a richer aggregation
// (no per-rd breakdown, no filtering), they're literally `wc -l`
// on the matching CRD list.
func (s *Server) handleStatsResourceDefinitions(w http.ResponseWriter, r *http.Request) {
	rds, err := s.Store.ResourceDefinitions().List(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	writeJSON(w, http.StatusOK, countEnvelope{Count: int64(len(rds))})
}

// handleStatsResources emits the count of replica placements (one
// per (RD, node) pair, mirroring `linstor r l`'s row count).
func (s *Server) handleStatsResources(w http.ResponseWriter, r *http.Request) {
	res, err := s.Store.Resources().List(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	writeJSON(w, http.StatusOK, countEnvelope{Count: int64(len(res))})
}

// handleStatsStoragePools emits the count of storage pools across
// every node — one row per (node, pool-name) pair, same row count
// `linstor sp l` returns.
func (s *Server) handleStatsStoragePools(w http.ResponseWriter, r *http.Request) {
	sps, err := s.Store.StoragePools().List(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	writeJSON(w, http.StatusOK, countEnvelope{Count: int64(len(sps))})
}

// handleStatsVolumeDefinitions emits the count of volume
// definitions across every RD. The store keys VDs by (rdName,
// volumeNumber) and the interface exposes List per-RD, so the
// handler iterates each RD once and accumulates.
//
// The two-step walk is the source-of-truth shape — there's no
// flat ListAll on the VolumeDefinitionStore interface (a Phase 10
// refactor explicitly kept it nested under RD to match the K8s
// CRD layout, where VDs are inline on the RD object). Iterating
// is O(num_rds) Get-equivalents, which on a populated cluster is
// cheap even at thousands of RDs.
func (s *Server) handleStatsVolumeDefinitions(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	rds, err := s.Store.ResourceDefinitions().List(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	var total int64

	for i := range rds {
		vds, vdErr := s.Store.VolumeDefinitions().List(ctx, rds[i].Name)
		if vdErr != nil {
			writeError(w, http.StatusInternalServerError, vdErr.Error())

			return
		}

		total += int64(len(vds))
	}

	writeJSON(w, http.StatusOK, countEnvelope{Count: total})
}

// handleStatsVolumes emits the count of Volume objects (the
// runtime per-replica view of each VD) across every Resource.
// Volumes are inline on Resource.Volumes; one Resource with N
// volumes contributes N to the total.
//
// This is distinct from /v1/stats/volume-definitions: a 3-replica
// RD with 2 VDs yields 6 volumes (3 × 2). Mirrors the row count
// of `linstor v l` (one row per (RD, node, volume_number) triple).
func (s *Server) handleStatsVolumes(w http.ResponseWriter, r *http.Request) {
	res, err := s.Store.Resources().List(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	var total int64
	for i := range res {
		total += int64(len(res[i].Volumes))
	}

	writeJSON(w, http.StatusOK, countEnvelope{Count: total})
}

// handleStatsSnapshots emits the count of snapshots, mirroring
// `linstor s l`'s row count.
func (s *Server) handleStatsSnapshots(w http.ResponseWriter, r *http.Request) {
	snaps, err := s.Store.Snapshots().List(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	writeJSON(w, http.StatusOK, countEnvelope{Count: int64(len(snaps))})
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
