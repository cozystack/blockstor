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
	"fmt"
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
	// Bug 224: upstream LINSTOR mounts this under the canonical
	// /v1/queries/resource-groups/query-all-size-info URL; keep the
	// flat alias above for backwards compat with any pre-Bug-224
	// client and register the canonical form so current python-linstor
	// 1.27.1 + golinstor v0.60.0 don't 404.
	mux.HandleFunc("POST /v1/queries/resource-groups/query-all-size-info",
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
//
// MaxVlmSizeInKib honours the controller-level / per-pool
// over-subscription gates (MaxFreeCapacity..., MaxTotalCapacity...,
// MaxOversubscriptionRatio) — see `poolMaxVolumeKib` for the exact
// formula. Thin pools therefore advertise more capacity than they
// physically have; thick pools collapse to FreeCapacity.
//
// NextSpawnResult mirrors upstream's `next_spawn_result`: the N
// pool-on-node tuples the placer would pick for the next spawn,
// sorted by per-pool MaxVolumeSize descending. When the filter
// can't be satisfied (fewer than N candidates) the slot is empty
// and Reports carries an info-band ApiCallRc explaining why so
// `linstor rg query-size-info` shows the operator the actual gate
// that rejected placement instead of a silent zero.
func (s *Server) computeSizeInfo(ctx context.Context, filter *apiv1.AutoSelectFilter) (querySizeInfoResponse, error) {
	pools, err := s.Store.StoragePools().List(ctx)
	if err != nil {
		return querySizeInfoResponse{}, err //nolint:wrapcheck // bubbled to handler
	}

	disabled, err := s.disabledNodes(ctx)
	if err != nil {
		return querySizeInfoResponse{}, err
	}

	candidates, totalCapacity, availableSum := collectCandidates(pools, disabled, filter)

	replicas := replicaCount(filter)
	deduped := dedupShared(candidates)
	ctrlProps := s.readCtrlPropsOrEmpty(ctx)
	maxVolKib := worstMaxVolOfTopN(deduped, replicas, ctrlProps)
	spawn := nextSpawnResult(deduped, replicas, ctrlProps)

	resp := querySizeInfoResponse{
		SpaceInfo: querySizeInfoSpaceInfo{
			MaxVlmSizeInKib:    maxVolKib,
			CapacityInKib:      totalCapacity,
			AvailableSizeInKib: availableSum,
			NextSpawnResult:    spawn,
		},
	}

	if reason := unsatisfiableReason(filter, deduped, replicas); reason != "" {
		resp.Reports = []apiv1.APICallRc{{
			RetCode: maskInfo,
			Message: reason,
		}}
	}

	return resp, nil
}

// collectCandidates filters the cluster's storage pools down to the
// set that's eligible for the next spawn under `filter`, returning
// the candidate slice plus the (TotalCapacity, FreeCapacity) rollup
// with shared-LUN dedup applied so a SAN/EXOS slice attached to N
// satellites contributes its capacity once, not N times.
//
// Extracted from computeSizeInfo to keep that handler within the
// linter funlen budget; the candidate set is what feeds both the
// max-volume cap and the next_spawn_result preview.
func collectCandidates(pools []apiv1.StoragePool, disabled map[string]struct{}, filter *apiv1.AutoSelectFilter) ([]apiv1.StoragePool, int64, int64) {
	candidates := make([]apiv1.StoragePool, 0, len(pools))
	sharedSeen := map[string]struct{}{}

	var (
		totalCapacity int64
		availableSum  int64
	)

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

		if pool.SharedSpaceID != "" {
			if _, dup := sharedSeen[pool.SharedSpaceID]; dup {
				continue
			}

			sharedSeen[pool.SharedSpaceID] = struct{}{}
		}

		totalCapacity += pool.TotalCapacity
		availableSum += pool.FreeCapacity
	}

	return candidates, totalCapacity, availableSum
}

// nextSpawnResult returns the top-N pool/node tuples the placer
// would pick for the next spawn, sorted by per-pool MaxVolumeSize
// descending. Returns nil when fewer than N candidates exist —
// the operator-facing UI then renders an empty preview, which
// matches upstream's behaviour for an unsatisfiable RG.
//
// The ratios are populated only for thin pools — thick pools
// collapse to 1.0 (see effectiveOversubRatios) and omitting them
// keeps golinstor's JSON parser from surfacing meaningless 1.0
// ratios on every preview row.
func nextSpawnResult(pools []apiv1.StoragePool, n int, ctrlProps map[string]string) []querySizeInfoSpawnResult {
	if n <= 0 || len(pools) < n {
		return nil
	}

	type capped struct {
		pool   apiv1.StoragePool
		maxVol int64
	}

	ranked := make([]capped, 0, len(pools))
	for i := range pools {
		ranked = append(ranked, capped{pool: pools[i], maxVol: poolMaxVolumeKib(&pools[i], ctrlProps)})
	}

	for i := 1; i < len(ranked); i++ {
		for j := i; j > 0 && ranked[j].maxVol > ranked[j-1].maxVol; j-- {
			ranked[j], ranked[j-1] = ranked[j-1], ranked[j]
		}
	}

	out := make([]querySizeInfoSpawnResult, 0, n)

	for i := range n {
		pool := ranked[i].pool

		row := querySizeInfoSpawnResult{
			NodeName:     pool.NodeName,
			StorPoolName: pool.StoragePoolName,
		}

		if isThinProvider(pool.ProviderKind) {
			freeRatio, totalRatio := effectiveOversubRatios(&pool, ctrlProps)

			overall := freeRatio
			if totalRatio < overall {
				overall = totalRatio
			}

			row.StorPoolOversubscriptionRatio = overall
			row.StorPoolFreeCapacityOversubscriptionRatio = freeRatio
			row.StorPoolTotalCapacityOversubscriptionRatio = totalRatio
		}

		out = append(out, row)
	}

	return out
}

// unsatisfiableReason returns a one-line explanation when the
// requested RG can't be honoured, or "" when placement preview
// succeeded. golinstor surfaces this as the "Next Spawn Result"
// reason line so the operator sees *why* a spawn would fail
// (filter too restrictive, no eligible pools, etc) before they
// actually attempt the spawn.
func unsatisfiableReason(filter *apiv1.AutoSelectFilter, pools []apiv1.StoragePool, replicas int) string {
	if replicas <= 0 {
		return "place-count is 0; nothing to spawn"
	}

	if len(pools) == 0 {
		if filter != nil && filter.StoragePool != "" {
			return fmt.Sprintf("no eligible storage pools match filter %q", filter.StoragePool)
		}

		return "no eligible storage pools in cluster (all DISKLESS / EVICTED / LOST)"
	}

	if len(pools) < replicas {
		return fmt.Sprintf("only %d eligible pool(s); place-count=%d cannot be satisfied",
			len(pools), replicas)
	}

	return ""
}

// dedupShared collapses pools that share a backing LUN to a single
// representative. The placer's anti-affinity already prevents 2 of N
// replicas from landing on the same SharedSpaceID; the n-th-largest
// computation must reflect that, otherwise it would overcount the
// shared LUN as N independent pools.
func dedupShared(pools []apiv1.StoragePool) []apiv1.StoragePool {
	out := make([]apiv1.StoragePool, 0, len(pools))
	seen := map[string]struct{}{}

	for i := range pools {
		if pools[i].SharedSpaceID != "" {
			if _, dup := seen[pools[i].SharedSpaceID]; dup {
				continue
			}

			seen[pools[i].SharedSpaceID] = struct{}{}
		}

		out = append(out, pools[i])
	}

	return out
}

// worstMaxVolOfTopN sorts pools by their per-pool MaxVolumeSize
// (free × MaxFreeCapacityOversubscriptionRatio capped by
// total × MaxTotalCapacityOversubscriptionRatio) descending and
// returns the cap of the n-th. That's the largest volume size all
// `n` replicas can fit honouring the over-subscription gates.
//
// Fewer than n candidates → 0 (request can't be satisfied at this
// placement count). For thick (non-thin) pools the ratios collapse
// to 1.0 and the result reduces to the legacy FreeCapacity-based cap.
func worstMaxVolOfTopN(pools []apiv1.StoragePool, n int, ctrlProps map[string]string) int64 {
	if n <= 0 || len(pools) < n {
		return 0
	}

	sortedMax := make([]int64, 0, len(pools))
	for i := range pools {
		sortedMax = append(sortedMax, poolMaxVolumeKib(&pools[i], ctrlProps))
	}

	// insertion-sort descending — pools count is in single digits
	// in practice, no need for the heavier sort package import.
	for i := 1; i < len(sortedMax); i++ {
		for j := i; j > 0 && sortedMax[j] > sortedMax[j-1]; j-- {
			sortedMax[j], sortedMax[j-1] = sortedMax[j-1], sortedMax[j]
		}
	}

	return sortedMax[n-1]
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
// We deliberately don't include the cluster-level oversubscription
// ratio summary fields — thick pools never oversubscribe, and the
// thin-pool numbers are surfaced per-pool in NextSpawnResult so
// the operator can see them where they actually apply.
//
// Reports carries the ApiCallRc envelope upstream uses for warning /
// info lines on `linstor rg query-size-info`. Populated only when
// the RG is constraint-impossible (e.g. place-count > eligible
// pools); empty otherwise so the wire shape stays compact.
type querySizeInfoResponse struct {
	SpaceInfo querySizeInfoSpaceInfo `json:"space_info"`
	Reports   []apiv1.APICallRc      `json:"reports,omitempty"`
}

type querySizeInfoSpaceInfo struct {
	MaxVlmSizeInKib    int64                      `json:"max_vlm_size_in_kib"`
	CapacityInKib      int64                      `json:"capacity_in_kib,omitempty"`
	AvailableSizeInKib int64                      `json:"available_size_in_kib,omitempty"`
	NextSpawnResult    []querySizeInfoSpawnResult `json:"next_spawn_result,omitempty"`
}

// querySizeInfoSpawnResult is one preview row — `{node, pool}`
// the next spawn would land on. Matches upstream's
// `QuerySizeInfoSpawnResult` model: node_name and stor_pool_name
// are required (the placer always has a target); the ratio fields
// are emitted only for thin pools where they're meaningful.
type querySizeInfoSpawnResult struct {
	NodeName                                   string  `json:"node_name"`
	StorPoolName                               string  `json:"stor_pool_name"`
	StorPoolOversubscriptionRatio              float64 `json:"stor_pool_oversubscription_ratio,omitempty"`
	StorPoolFreeCapacityOversubscriptionRatio  float64 `json:"stor_pool_free_capacity_oversubscription_ratio,omitempty"`
	StorPoolTotalCapacityOversubscriptionRatio float64 `json:"stor_pool_total_capacity_oversubscription_ratio,omitempty"`
}

type queryAllSizeInfoResponse struct {
	Result map[string]querySizeInfoResponse `json:"result"`
}
