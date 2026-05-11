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

// registerAdvise wires `linstor advise resource` and the cluster-wide
// `linstor advise` shortcut. Both surface placement recommendations
// without persisting anything: the operator runs the advice through
// their own change-management before issuing the actual create.
//
// We re-use the same candidate-pool selection the placer would use,
// minus the actual Resource creation. Output is the list of (node,
// pool) tuples a hypothetical autoplace at the given filter would
// pick.
func (s *Server) registerAdvise(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/view/advise/resources", s.requireStore(s.handleAdviseResources))
	mux.HandleFunc("GET /v1/resource-definitions/{rd}/advise",
		s.requireStore(s.handleAdviseRD))
}

// handleAdviseResources surfaces a per-RD recommendation. Output is
// `[]adviceEntry`. RDs whose autoplace would fail (insufficient
// candidates, oversubscription) include a non-empty `Conflict`.
func (s *Server) handleAdviseResources(w http.ResponseWriter, r *http.Request) {
	rds, err := s.Store.ResourceDefinitions().List(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	out := make([]adviceEntry, 0, len(rds))

	for i := range rds {
		entry, err := s.adviseOne(r.Context(), &rds[i])
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())

			return
		}

		out = append(out, entry)
	}

	writeJSON(w, http.StatusOK, out)
}

// handleAdviseRD answers the per-RD variant. linstor CLI uses this
// when the operator runs `linstor advise resource <name>`.
func (s *Server) handleAdviseRD(w http.ResponseWriter, r *http.Request) {
	rdName := r.PathValue("rd")

	rd, err := s.Store.ResourceDefinitions().Get(r.Context(), rdName)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	entry, err := s.adviseOne(r.Context(), &rd)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	writeJSON(w, http.StatusOK, entry)
}

// adviseOne computes the recommendation for a single RD. We don't
// consult the placer's full state machine here — the topology guards
// only matter once we're actually creating Resources. For the
// "advice" surface the simple top-N free-capacity pick is what
// upstream LINSTOR does, and matches the operator's intuition.
func (s *Server) adviseOne(ctx context.Context, rd *apiv1.ResourceDefinition) (adviceEntry, error) {
	filter := apiv1.AutoSelectFilter{}

	if rd.ResourceGroupName != "" {
		rg, err := s.Store.ResourceGroups().Get(ctx, rd.ResourceGroupName)
		if err == nil {
			filter = rg.SelectFilter
		}
	}

	if filter.PlaceCount == 0 {
		filter.PlaceCount = 1
	}

	pools, err := s.Store.StoragePools().List(ctx)
	if err != nil {
		return adviceEntry{}, err //nolint:wrapcheck // bubbled to handler
	}

	disabled, err := s.disabledNodes(ctx)
	if err != nil {
		return adviceEntry{}, err
	}

	picks := pickAdvicePools(pools, &filter, disabled, int(filter.PlaceCount))

	entry := adviceEntry{
		Name:        rd.Name,
		PlaceCount:  int32(filter.PlaceCount),
		Suggestions: picks,
	}

	if len(picks) < int(filter.PlaceCount) {
		entry.Conflict = "not enough candidate storage pools"
	}

	return entry, nil
}

// pickAdvicePools is a stripped-down candidate selection for the
// advice surface — same pool filters as the placer (skip diskless,
// skip disabled, match the SP filter) but no topology constraints.
// Sort by FreeCapacity desc to mirror the placer's bias.
func pickAdvicePools(pools []apiv1.StoragePool, filter *apiv1.AutoSelectFilter, disabled map[string]struct{}, want int) []adviceSuggestion {
	candidates := make([]apiv1.StoragePool, 0, len(pools))

	for i := range pools {
		pool := pools[i]
		if pool.ProviderKind == apiv1.StoragePoolKindDiskless {
			continue
		}

		if _, off := disabled[pool.NodeName]; off {
			continue
		}

		if filter.StoragePool != "" && pool.StoragePoolName != filter.StoragePool {
			continue
		}

		candidates = append(candidates, pool)
	}

	for i := 1; i < len(candidates); i++ {
		for j := i; j > 0 && candidates[j].FreeCapacity > candidates[j-1].FreeCapacity; j-- {
			candidates[j], candidates[j-1] = candidates[j-1], candidates[j]
		}
	}

	// Shared-LUN anti-affinity: the placer would refuse to put two
	// replicas on pools sharing a backing LUN, so the advice surface
	// must mirror that — otherwise we'd recommend a layout the
	// resource-create would then reject.
	candidates = dedupShared(candidates)

	if want > len(candidates) {
		want = len(candidates)
	}

	out := make([]adviceSuggestion, 0, want)
	for i := range want {
		out = append(out, adviceSuggestion{
			NodeName:     candidates[i].NodeName,
			StoragePool:  candidates[i].StoragePoolName,
			FreeCapacity: candidates[i].FreeCapacity,
		})
	}

	return out
}

// adviceEntry is the per-RD advice payload. golinstor's CLI prints
// the suggestion list and surfaces Conflict when non-empty.
type adviceEntry struct {
	Name        string             `json:"name"`
	PlaceCount  int32              `json:"place_count"`
	Suggestions []adviceSuggestion `json:"suggestions,omitempty"`
	Conflict    string             `json:"conflict,omitempty"`
}

type adviceSuggestion struct {
	NodeName     string `json:"node_name"`
	StoragePool  string `json:"storage_pool"`
	FreeCapacity int64  `json:"free_capacity_kib,omitempty"`
}
