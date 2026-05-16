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
	"maps"
	"net/http"
	"strconv"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// registerResourceGroupExtras wires the secondary ResourceGroup
// surface the upstream `linstor` CLI exercises beyond CRUD:
//
//   - GET    /v1/resource-groups/{rg}/volume-groups
//   - POST   /v1/resource-groups/{rg}/volume-groups          (vg create)
//   - GET    /v1/resource-groups/{rg}/volume-groups/{vlmNr}
//   - PUT    /v1/resource-groups/{rg}/volume-groups/{vlmNr}  (vg modify)
//   - DELETE /v1/resource-groups/{rg}/volume-groups/{vlmNr}
//   - GET    /v1/resource-groups/{rg}/query-max-volume-size
//   - POST   /v1/resource-groups/{rg}/adjust                 (rg adjust)
//
// The Python CLI calls volume-groups list on EVERY `rg list` and
// crashes parsing the default ServeMux 404 as XML — implementing
// the read paths is the cheapest way to keep the CLI working.
func (s *Server) registerResourceGroupExtras(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/resource-groups/{rg}/volume-groups",
		s.requireStore(s.handleVGList))
	mux.HandleFunc("POST /v1/resource-groups/{rg}/volume-groups",
		s.requireStore(s.handleVGCreate))
	mux.HandleFunc("GET /v1/resource-groups/{rg}/volume-groups/{vlmNr}",
		s.requireStore(s.handleVGGet))
	mux.HandleFunc("PUT /v1/resource-groups/{rg}/volume-groups/{vlmNr}",
		s.requireStore(s.handleVGUpdate))
	mux.HandleFunc("DELETE /v1/resource-groups/{rg}/volume-groups/{vlmNr}",
		s.requireStore(s.handleVGDelete))

	mux.HandleFunc("GET /v1/resource-groups/{rg}/query-max-volume-size",
		s.requireStore(s.handleQueryMaxVolumeSize))
	mux.HandleFunc("POST /v1/resource-groups/{rg}/adjust",
		s.requireStore(s.handleRGAdjust))

	// The Python CLI walks /v1/storage-pool-definitions on every
	// `rg query-max-volume-size`. Upstream LINSTOR keeps a separate
	// "definition" registry decoupled from per-node StoragePools;
	// blockstor folds both into the StoragePool CRD, so we just
	// dedup the per-node pools by name and return that surface.
	mux.HandleFunc("GET /v1/storage-pool-definitions",
		s.requireStore(s.handleStoragePoolDefinitionList))
}

func (s *Server) handleStoragePoolDefinitionList(w http.ResponseWriter, r *http.Request) {
	pools, err := s.Store.StoragePools().List(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	// Upstream's StoragePoolDefinition uses `storage_pool_name` on
	// the wire but the Python parser exposes it as `.name`. golinstor
	// uses the same lookup as `Name`, so the wire field is the
	// canonical one. We also surface an empty props map — the Python
	// CLI dereferences `.properties` on every entry.
	type storagePoolDefinition struct {
		StoragePoolName string            `json:"storage_pool_name"`
		Props           map[string]string `json:"props"`
	}

	seen := map[string]struct{}{}
	out := make([]storagePoolDefinition, 0, len(pools))

	for i := range pools {
		if _, dup := seen[pools[i].StoragePoolName]; dup {
			continue
		}

		seen[pools[i].StoragePoolName] = struct{}{}
		out = append(out, storagePoolDefinition{
			StoragePoolName: pools[i].StoragePoolName,
			Props:           map[string]string{},
		})
	}

	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleVGList(w http.ResponseWriter, r *http.Request) {
	rgName := r.PathValue("rg")

	rg, err := s.Store.ResourceGroups().Get(r.Context(), rgName)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	out := rg.VolumeGroups
	if out == nil {
		out = []apiv1.VolumeGroup{}
	}

	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleVGGet(w http.ResponseWriter, r *http.Request) {
	rgName := r.PathValue("rg")

	vlmNr, ok := parseVolumeNumber(w, r.PathValue("vlmNr"))
	if !ok {
		return
	}

	rg, err := s.Store.ResourceGroups().Get(r.Context(), rgName)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	for i := range rg.VolumeGroups {
		if rg.VolumeGroups[i].VolumeNumber == vlmNr {
			writeJSON(w, http.StatusOK, rg.VolumeGroups[i])

			return
		}
	}

	writeError(w, http.StatusNotFound, "volume group not found")
}

func (s *Server) handleVGCreate(w http.ResponseWriter, r *http.Request) {
	rgName := r.PathValue("rg")

	var in apiv1.VolumeGroup

	if !decodeJSON(w, r, &in) {
		return
	}

	rg, err := s.Store.ResourceGroups().Get(r.Context(), rgName)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	for i := range rg.VolumeGroups {
		if rg.VolumeGroups[i].VolumeNumber == in.VolumeNumber {
			writeError(w, http.StatusConflict, "volume group already exists")

			return
		}
	}

	rg.VolumeGroups = append(rg.VolumeGroups, in)

	err = s.Store.ResourceGroups().Update(r.Context(), &rg)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	// Python CLI parses the response body as JSON unconditionally and
	// crashes on a bare 201 with empty body. Return an ApiCallRc list
	// envelope (the upstream success shape) so the parser is happy.
	writeJSON(w, http.StatusCreated, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "volume group created",
	}})
}

func (s *Server) handleVGUpdate(w http.ResponseWriter, r *http.Request) {
	rgName := r.PathValue("rg")

	vlmNr, ok := parseVolumeNumber(w, r.PathValue("vlmNr"))
	if !ok {
		return
	}

	var in struct {
		OverrideProps map[string]string `json:"override_props,omitempty"`
		DeleteProps   []string          `json:"delete_props,omitempty"`
	}

	if !decodeJSON(w, r, &in) {
		return
	}

	rg, err := s.Store.ResourceGroups().Get(r.Context(), rgName)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	idx := -1

	for i := range rg.VolumeGroups {
		if rg.VolumeGroups[i].VolumeNumber == vlmNr {
			idx = i

			break
		}
	}

	if idx < 0 {
		writeError(w, http.StatusNotFound, "volume group not found")

		return
	}

	mergeVGProps(&rg.VolumeGroups[idx], in.OverrideProps, in.DeleteProps)

	err = s.Store.ResourceGroups().Update(r.Context(), &rg)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	w.WriteHeader(http.StatusOK)
}

// handleVGDelete drops a VolumeGroup entry from its parent RG.
//
// Idempotent on NotFound (Bug 66): both NotFound shapes — parent RG
// missing OR vlmNr absent inside an extant RG — fold into 200 + warn-
// mask. A bare 404 crashed the Python CLI's XML decoder fallback;
// switching to the ApiCallRc envelope keeps `linstor rg vg d` exit-0
// on the no-op replay and matches the pattern Bug 56 set for RDs.
func (s *Server) handleVGDelete(w http.ResponseWriter, r *http.Request) {
	rgName := r.PathValue("rg")

	vlmNr, ok := parseVolumeNumber(w, r.PathValue("vlmNr"))
	if !ok {
		return
	}

	rg, err := s.Store.ResourceGroups().Get(r.Context(), rgName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
				RetCode: warnVGNotFound,
				Message: fmt.Sprintf("volume group already absent: %s/%d", rgName, vlmNr),
			}})

			return
		}

		writeStoreError(w, err)

		return
	}

	dst := rg.VolumeGroups[:0]
	found := false

	for i := range rg.VolumeGroups {
		if rg.VolumeGroups[i].VolumeNumber == vlmNr {
			found = true

			continue
		}

		dst = append(dst, rg.VolumeGroups[i])
	}

	if !found {
		writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
			RetCode: warnVGNotFound,
			Message: fmt.Sprintf("volume group already absent: %s/%d", rgName, vlmNr),
		}})

		return
	}

	rg.VolumeGroups = dst

	err = s.Store.ResourceGroups().Update(r.Context(), &rg)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	// Mirror upstream LINSTOR: DELETE returns 200 + an
	// ApiCallRc[] envelope, not 204 / empty body. golinstor's
	// `Delete` decodes the body to detect downstream warnings
	// even on success.
	writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: fmt.Sprintf("volume group deleted: %s/%d", rgName, vlmNr),
	}})
}

// handleQueryMaxVolumeSize answers the deprecated-but-still-used
// `linstor resource-group query-max-volume-size` CLI path. The
// modern surface is /query-size-info (POST) which we already
// implement; this just reuses computeSizeInfo and reshapes into the
// older `candidates` envelope. golinstor's `Candidate.all_thin`
// and `default_max_oversubscription_ratio` are accessed
// unconditionally — we set both to safe defaults.
func (s *Server) handleQueryMaxVolumeSize(w http.ResponseWriter, r *http.Request) {
	rgName := r.PathValue("rg")

	rg, err := s.Store.ResourceGroups().Get(r.Context(), rgName)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	info, err := s.computeSizeInfo(r.Context(), &rg.SelectFilter)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	// linstor-client matches `candidate.storage_pool` against the
	// upstream `storage-pool-definitions` registry and crashes with
	// IndexError if no entry matches. Emit one candidate per pool
	// the filter could land on — single SP (StoragePool) → one entry;
	// SP list (StoragePoolList) → one per entry; empty filter → the
	// union of pool names seen across the cluster.
	pools := filterPoolNames(&rg.SelectFilter)

	if len(pools) == 0 {
		pools = clusterPoolNames(r.Context(), s)
	}

	candidates := make([]maxVolumeSizeCandidate, 0, len(pools))

	for _, p := range pools {
		candidates = append(candidates, maxVolumeSizeCandidate{
			MaxVolumeSizeKib: info.SpaceInfo.MaxVlmSizeInKib,
			StoragePool:      p,
			NodeNames:        poolNodeNamesFor(r.Context(), s, p),
			AllThin:          false,
		})
	}

	resp := maxVolumeSizeResponse{
		DefaultMaxOversubscriptionRatio: 1.0,
		Candidates:                      candidates,
	}

	writeJSON(w, http.StatusOK, resp)
}

// handleRGAdjust answers `linstor rg adjust`. Upstream LINSTOR
// synchronously re-runs autoplace on every Resource spawned from the
// group; blockstor's RD reconciler already does that on every Spec
// change, so this is a no-op confirming the RG exists. Body is an
// ApiCallRc list — the CLI's response parser requires a JSON array
// at the top level.
func (s *Server) handleRGAdjust(w http.ResponseWriter, r *http.Request) {
	rgName := r.PathValue("rg")

	_, err := s.Store.ResourceGroups().Get(r.Context(), rgName)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusOK, []apiv1.APICallRc{{
		RetCode: maskInfo,
		Message: "resource group adjusted",
	}})
}

// mergeVGProps applies the override / delete-props patch to a
// VolumeGroup's Props in place. Pulled out of handleVGUpdate to
// keep that handler under the funlen budget.
func mergeVGProps(vg *apiv1.VolumeGroup, override map[string]string, deletes []string) {
	if vg.Props == nil {
		vg.Props = map[string]string{}
	}

	maps.Copy(vg.Props, override)

	for _, k := range deletes {
		delete(vg.Props, k)
	}
}

func parseVolumeNumber(w http.ResponseWriter, raw string) (int32, bool) {
	n, err := strconv.ParseInt(raw, 10, 32)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid volume number: "+raw)

		return 0, false
	}

	return int32(n), true
}

// filterPoolNames returns the pool-name set the filter constrains to.
// Empty result = the filter is unconstrained (any pool allowed).
func filterPoolNames(filter *apiv1.AutoSelectFilter) []string {
	if filter.StoragePool != "" {
		return []string{filter.StoragePool}
	}

	if len(filter.StoragePoolList) > 0 {
		out := make([]string, len(filter.StoragePoolList))
		copy(out, filter.StoragePoolList)

		return out
	}

	return nil
}

// clusterPoolNames returns the deduped set of all pool names the
// cluster currently advertises. Used as the QMVS candidate fallback
// when the RG's filter is unconstrained.
func clusterPoolNames(ctx context.Context, s *Server) []string {
	pools, err := s.Store.StoragePools().List(ctx)
	if err != nil {
		return nil
	}

	seen := map[string]struct{}{}
	out := make([]string, 0, len(pools))

	for i := range pools {
		name := pools[i].StoragePoolName
		if _, dup := seen[name]; dup {
			continue
		}

		seen[name] = struct{}{}

		out = append(out, name)
	}

	return out
}

// poolNodeNamesFor returns the deduped set of nodes that host a pool
// named `pool`. Used by the QMVS handler to populate each candidate's
// node_names field — the Python CLI renders the column even though
// the placer ultimately picks per-replica.
func poolNodeNamesFor(ctx context.Context, s *Server, pool string) []string {
	pools, err := s.Store.StoragePools().List(ctx)
	if err != nil {
		return nil
	}

	seen := map[string]struct{}{}
	out := make([]string, 0, len(pools))

	for i := range pools {
		if pools[i].StoragePoolName != pool {
			continue
		}

		node := pools[i].NodeName
		if _, dup := seen[node]; dup {
			continue
		}

		seen[node] = struct{}{}

		out = append(out, node)
	}

	return out
}

type maxVolumeSizeResponse struct {
	Candidates                      []maxVolumeSizeCandidate `json:"candidates"`
	DefaultMaxOversubscriptionRatio float64                  `json:"default_max_oversubscription_ratio"`
}

type maxVolumeSizeCandidate struct {
	MaxVolumeSizeKib int64    `json:"max_volume_size_kib"`
	StoragePool      string   `json:"storage_pool"`
	NodeNames        []string `json:"node_names,omitempty"`
	AllThin          bool     `json:"all_thin"`
}
