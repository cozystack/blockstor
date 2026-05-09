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

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// registerQuerySizeInfo wires `linstor query-size-info` and the
// `linstor sp s i` (spaceinfo) shortcut. Both surface the cluster's
// storage capacity so a CLI / operator can answer "will an N-byte
// PVC fit?" before issuing the resource-create. golinstor uses the
// per-RG variant when validating capacity for a spawn.
func (s *Server) registerQuerySizeInfo(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/resource-groups/{rg}/query-size-info",
		s.requireStore(s.handleQuerySizeInfo))
	mux.HandleFunc("POST /v1/query-all-size-info",
		s.requireStore(s.handleQueryAllSizeInfo))
}

// handleQuerySizeInfo answers the per-RG capacity check.
func (s *Server) handleQuerySizeInfo(w http.ResponseWriter, r *http.Request) {
	rgName := r.PathValue("rg")

	rg, err := s.Store.ResourceGroups().Get(r.Context(), rgName)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	resp, err := s.computeSizeInfo(r.Context(), &rg.SelectFilter)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	writeJSON(w, http.StatusOK, resp)
}

// handleQueryAllSizeInfo answers the cluster-wide capacity query —
// per-RG capacity for every RG in one shot. golinstor uses this on
// the linstor CLI's autocomplete + storage planning paths.
func (s *Server) handleQueryAllSizeInfo(w http.ResponseWriter, r *http.Request) {
	rgs, err := s.Store.ResourceGroups().List(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	result := map[string]querySizeInfoResponse{}

	for i := range rgs {
		info, err := s.computeSizeInfo(r.Context(), &rgs[i].SelectFilter)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())

			return
		}

		result[rgs[i].Name] = info
	}

	writeJSON(w, http.StatusOK, queryAllSizeInfoResponse{Result: result})
}

// computeSizeInfo walks every storage pool that satisfies the filter
// and returns the worst-case (smallest free capacity) of the top-N
// candidates, where N = filter.PlaceCount. That's the size the next
// resource-create will be able to spawn at PlaceCount replicas
// without running anyone out of room — the cap that golinstor's
// capacity guard uses.
func (s *Server) computeSizeInfo(ctx context.Context, filter *apiv1.AutoSelectFilter) (querySizeInfoResponse, error) {
	pools, err := s.Store.StoragePools().List(ctx)
	if err != nil {
		return querySizeInfoResponse{}, err //nolint:wrapcheck // bubbled to handler
	}

	disabled, err := s.disabledNodes(ctx)
	if err != nil {
		return querySizeInfoResponse{}, err
	}

	var (
		totalCapacity int64
		availableSum  int64
	)

	candidates := make([]apiv1.StoragePool, 0, len(pools))

	for i := range pools {
		pool := pools[i]
		if pool.ProviderKind == apiv1.StoragePoolKindDiskless {
			continue
		}

		if _, off := disabled[pool.NodeName]; off {
			continue
		}

		if filter != nil && filter.StoragePool != "" && pool.StoragePoolName != filter.StoragePool {
			continue
		}

		candidates = append(candidates, pool)
		totalCapacity += pool.TotalCapacity
		availableSum += pool.FreeCapacity
	}

	maxVolKib := worstFreeOfTopN(candidates, replicaCount(filter))

	return querySizeInfoResponse{
		SpaceInfo: querySizeInfoSpaceInfo{
			MaxVlmSizeInKib:    maxVolKib,
			CapacityInKib:      totalCapacity,
			AvailableSizeInKib: availableSum,
		},
	}, nil
}

// worstFreeOfTopN sorts pools by FreeCapacity desc and returns the
// FreeCapacity of the n-th — i.e. the cap that all `n` replicas can
// actually fit. Fewer than n candidates → 0 (the request can't be
// satisfied at this placement count).
func worstFreeOfTopN(pools []apiv1.StoragePool, n int) int64 {
	if n <= 0 || len(pools) < n {
		return 0
	}

	sortedFree := make([]int64, 0, len(pools))
	for i := range pools {
		sortedFree = append(sortedFree, pools[i].FreeCapacity)
	}

	// insertion-sort descending — pools count is in single digits
	// in practice, no need for the heavier sort package import.
	for i := 1; i < len(sortedFree); i++ {
		for j := i; j > 0 && sortedFree[j] > sortedFree[j-1]; j-- {
			sortedFree[j], sortedFree[j-1] = sortedFree[j-1], sortedFree[j]
		}
	}

	return sortedFree[n-1]
}

// replicaCount extracts the desired replica count from the filter,
// defaulting to 1 when unset (matches autoplace's default).
func replicaCount(filter *apiv1.AutoSelectFilter) int {
	if filter == nil || filter.PlaceCount <= 0 {
		return 1
	}

	return int(filter.PlaceCount)
}

// disabledNodes mirrors placer's same-named helper for the
// query-size-info path (we don't want to count EVICTED/LOST node
// pools toward "available" capacity).
func (s *Server) disabledNodes(ctx context.Context) (map[string]struct{}, error) {
	nodes, err := s.Store.Nodes().List(ctx)
	if err != nil {
		return nil, err //nolint:wrapcheck // bubbled to handler
	}

	out := map[string]struct{}{}

	for i := range nodes {
		for _, f := range nodes[i].Flags {
			if f == apiv1.NodeFlagEvicted || f == apiv1.NodeFlagLost {
				out[nodes[i].Name] = struct{}{}

				break
			}
		}
	}

	return out, nil
}

// querySizeInfoResponse mirrors the upstream JSON shape (subset).
// We deliberately don't include the oversubscription ratios — DRBD
// volumes don't oversubscribe, the values would always be 1.0, and
// golinstor's parser treats absence as "default".
type querySizeInfoResponse struct {
	SpaceInfo querySizeInfoSpaceInfo `json:"space_info"`
}

type querySizeInfoSpaceInfo struct {
	MaxVlmSizeInKib    int64 `json:"max_vlm_size_in_kib"`
	CapacityInKib      int64 `json:"capacity_in_kib,omitempty"`
	AvailableSizeInKib int64 `json:"available_size_in_kib,omitempty"`
}

type queryAllSizeInfoResponse struct {
	Result map[string]querySizeInfoResponse `json:"result"`
}
