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
	"slices"
	"sort"
	"strings"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
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

	filter := mergeAutoplaceFilter(r.Context(), s.Store, &rd, &req.SelectFilter)

	placed, want, err := s.placeResources(r.Context(), rdName, &filter)
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
)

// placerCtx bundles the lookup tables placeResources needs, so the
// per-candidate hot loop stays readable. Built once per autoplace
// call from the store's current snapshot.
type placerCtx struct {
	nodes     map[string]map[string]string
	taken     map[string]struct{}
	sameTuple map[string]string
	diffSeen  map[string]struct{}
	filter    *apiv1.AutoSelectFilter
}

// placeResources picks free pools from the candidates and creates Resource
// objects up to filter.PlaceCount. Returns (placed, want, err).
func (s *Server) placeResources(ctx context.Context, rdName string, filter *apiv1.AutoSelectFilter) (int, int, error) {
	candidates, err := s.candidatePools(ctx, filter)
	if err != nil {
		return 0, 0, err
	}

	existing, err := s.Store.Resources().ListByDefinition(ctx, rdName)
	if err != nil {
		return 0, 0, err //nolint:wrapcheck // bubbled to handler
	}

	nodes, err := s.nodesByName(ctx)
	if err != nil {
		return 0, 0, err
	}

	placer := &placerCtx{
		nodes:     nodes,
		taken:     make(map[string]struct{}, len(existing)),
		sameTuple: topologyTuple(existing, nodes, filter.ReplicasOnSame),
		diffSeen:  topologySeen(existing, nodes, filter.ReplicasOnDifferent),
		filter:    filter,
	}

	for i := range existing {
		placer.taken[existing[i].NodeName] = struct{}{}
	}

	// replicas_on_same with no existing replica yet: greedy
	// placement picks the wrong tuple half the time. Look ahead —
	// group candidates by their tuple, lock onto a group that can
	// fit the whole place_count, drop the rest. After this the
	// greedy loop respects the constraint trivially.
	if placer.sameTuple == nil && len(filter.ReplicasOnSame) > 0 {
		candidates, placer.sameTuple = pickSameGroup(candidates, nodes, filter.ReplicasOnSame, int(filter.PlaceCount))
	}

	placed := 0
	want := int(filter.PlaceCount)

	for i := range candidates {
		if placed >= want {
			break
		}

		ok, err := placer.tryPlace(ctx, s.Store, rdName, &candidates[i])
		if err != nil {
			return placed, want, err
		}

		if ok {
			placed++
		}
	}

	return placed, want, nil
}

// tryPlace checks the topology guards and (on a pass) creates the
// Resource + commits the candidate's Aux values into the diff/same
// bookkeeping. Returns (placed, err).
func (p *placerCtx) tryPlace(ctx context.Context, st store.Store, rdName string, pool *apiv1.StoragePool) (bool, error) {
	if _, busy := p.taken[pool.NodeName]; busy {
		return false, nil
	}

	nodeProps := p.nodes[pool.NodeName]

	if !matchesTuple(nodeProps, p.filter.ReplicasOnSame, p.sameTuple) {
		return false, nil
	}

	if collidesWithDiff(nodeProps, p.filter.ReplicasOnDifferent, p.diffSeen) {
		return false, nil
	}

	res := apiv1.Resource{
		Name:     rdName,
		NodeName: pool.NodeName,
		Props:    map[string]string{"StorPoolName": pool.StoragePoolName},
	}

	err := st.Resources().Create(ctx, &res)
	if err != nil && !errors.Is(err, store.ErrAlreadyExists) {
		return false, err //nolint:wrapcheck // bubbled to handler
	}

	p.taken[pool.NodeName] = struct{}{}

	if p.sameTuple == nil && len(p.filter.ReplicasOnSame) > 0 {
		p.sameTuple = lookupKeys(nodeProps, p.filter.ReplicasOnSame)
	}

	for _, k := range p.filter.ReplicasOnDifferent {
		p.diffSeen[k+"="+nodeProps[auxKey(k)]] = struct{}{}
	}

	return true, nil
}

// pickSameGroup partitions candidates by their `replicas_on_same`
// tuple, picks a group big enough to hold place_count, and returns
// only those candidates plus the locked-in tuple. When no group is
// large enough we return the candidates unchanged — the placer will
// then fail the conflict check honestly with 409.
//
// Tiebreak between equally-sized feasible groups: the one with the
// greatest total FreeCapacity wins, alphabetical group key on a tie.
// Deterministic so two callers see the same answer.
func pickSameGroup(candidates []apiv1.StoragePool, nodes map[string]map[string]string, keys []string, want int) ([]apiv1.StoragePool, map[string]string) {
	type group struct {
		tuple map[string]string
		key   string
		pools []apiv1.StoragePool
		free  int64
	}

	byKey := map[string]*group{}

	for i := range candidates {
		pool := candidates[i]
		tuple := lookupKeys(nodes[pool.NodeName], keys)
		key := tupleKey(tuple)

		grp, ok := byKey[key]
		if !ok {
			grp = &group{tuple: tuple, key: key}
			byKey[key] = grp
		}

		grp.pools = append(grp.pools, pool)
		grp.free += pool.FreeCapacity
	}

	groups := make([]*group, 0, len(byKey))
	for _, grp := range byKey {
		if len(grp.pools) >= want {
			groups = append(groups, grp)
		}
	}

	if len(groups) == 0 {
		return candidates, nil
	}

	sort.SliceStable(groups, func(i, j int) bool {
		if groups[i].free != groups[j].free {
			return groups[i].free > groups[j].free
		}

		return groups[i].key < groups[j].key
	})

	winner := groups[0]

	return winner.pools, winner.tuple
}

// tupleKey turns the (Aux/k, value) map into a deterministic string
// key for grouping. Pairs are joined with the field-separator byte
// (\x1f) so a value containing `=` can't forge a different tuple's key.
func tupleKey(tuple map[string]string) string {
	const fieldSep = "\x1f"

	propKeys := make([]string, 0, len(tuple))
	for propKey := range tuple {
		propKeys = append(propKeys, propKey)
	}

	sort.Strings(propKeys)

	var buf strings.Builder

	for i, propKey := range propKeys {
		if i > 0 {
			buf.WriteString(fieldSep)
		}

		buf.WriteString(propKey)
		buf.WriteByte('=')
		buf.WriteString(tuple[propKey])
	}

	return buf.String()
}

// nodesByName returns a snapshot of the cluster's nodes keyed on
// metadata.name. The autoplacer needs Node prop bags to evaluate
// topology constraints (replicas-on-same / different).
func (s *Server) nodesByName(ctx context.Context) (map[string]map[string]string, error) {
	list, err := s.Store.Nodes().List(ctx)
	if err != nil {
		return nil, err //nolint:wrapcheck // bubbled to handler
	}

	out := make(map[string]map[string]string, len(list))
	for i := range list {
		out[list[i].Name] = list[i].Props
	}

	return out, nil
}

// topologyTuple computes the canonical (key, value) tuple from the
// first replica of an RD. All future replicas must match this tuple
// for `replicas_on_same` to hold. Returns nil when no replicas exist
// yet — in that case the first placement establishes the tuple.
func topologyTuple(existing []apiv1.Resource, nodes map[string]map[string]string, keys []string) map[string]string {
	if len(keys) == 0 || len(existing) == 0 {
		return nil
	}

	return lookupKeys(nodes[existing[0].NodeName], keys)
}

// topologySeen builds the "values already used" set for
// `replicas_on_different`. A new candidate whose Aux/<key>= value
// is already in this set is rejected. Format: "<key>=<value>" so
// two keys with overlapping value namespaces don't false-collide.
func topologySeen(existing []apiv1.Resource, nodes map[string]map[string]string, keys []string) map[string]struct{} {
	out := map[string]struct{}{}

	for _, k := range keys {
		for i := range existing {
			value := nodes[existing[i].NodeName][auxKey(k)]
			out[k+"="+value] = struct{}{}
		}
	}

	return out
}

func matchesTuple(nodeProps map[string]string, keys []string, want map[string]string) bool {
	if want == nil {
		return true
	}

	for _, k := range keys {
		if nodeProps[auxKey(k)] != want[auxKey(k)] {
			return false
		}
	}

	return true
}

func collidesWithDiff(nodeProps map[string]string, keys []string, seen map[string]struct{}) bool {
	for _, k := range keys {
		if _, dup := seen[k+"="+nodeProps[auxKey(k)]]; dup {
			return true
		}
	}

	return false
}

// lookupKeys reads Aux/<k> for every k in keys off the Node prop bag.
func lookupKeys(nodeProps map[string]string, keys []string) map[string]string {
	out := make(map[string]string, len(keys))
	for _, k := range keys {
		out[auxKey(k)] = nodeProps[auxKey(k)]
	}

	return out
}

// auxKey wraps a topology key into the upstream LINSTOR `Aux/<k>`
// namespace. Operators set topology props via
// `linstor n set-property NODE Aux/zone us-east-1a`.
func auxKey(key string) string {
	const prefix = "Aux/"
	if strings.HasPrefix(key, prefix) {
		return key
	}

	return prefix + key
}

// candidatePools returns storage pools that satisfy the placement filter.
// Empty `StoragePool` and empty `StoragePoolList` mean "any". `NodeNameList`
// further restricts the candidates. Nodes flagged EVICTED or LOST are
// filtered out — autoplace must never pick a satellite the operator
// has marked unavailable, otherwise eviction/evacuation can't drain a
// node before maintenance.
func (s *Server) candidatePools(ctx context.Context, filter *apiv1.AutoSelectFilter) ([]apiv1.StoragePool, error) {
	all, err := s.Store.StoragePools().List(ctx)
	if err != nil {
		return nil, err //nolint:wrapcheck // bubbled to handler
	}

	disabled, err := s.disabledNodes(ctx)
	if err != nil {
		return nil, err
	}

	out := make([]apiv1.StoragePool, 0, len(all))

	for i := range all {
		pool := all[i]

		if pool.ProviderKind == apiv1.StoragePoolKindDiskless {
			continue
		}

		if _, off := disabled[pool.NodeName]; off {
			continue
		}

		if filter.StoragePool != "" && pool.StoragePoolName != filter.StoragePool {
			continue
		}

		if len(filter.StoragePoolList) > 0 && !slices.Contains(filter.StoragePoolList, pool.StoragePoolName) {
			continue
		}

		if len(filter.NodeNameList) > 0 && !slices.Contains(filter.NodeNameList, pool.NodeName) {
			continue
		}

		out = append(out, pool)
	}

	// Greatest-free-first; ties break on NodeName for determinism.
	// Without this the placer skews toward the first-listed pool and
	// starves a single node faster than the others.
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].FreeCapacity != out[j].FreeCapacity {
			return out[i].FreeCapacity > out[j].FreeCapacity
		}

		return out[i].NodeName < out[j].NodeName
	})

	return out, nil
}

// disabledNodes returns a set of node names that autoplace must never
// pick — currently the union of EVICTED and LOST flags. The set is
// rebuilt on every autoplace call so flag changes (evacuate, restore)
// take effect on the next placement.
func (s *Server) disabledNodes(ctx context.Context) (map[string]struct{}, error) {
	nodes, err := s.Store.Nodes().List(ctx)
	if err != nil {
		return nil, err //nolint:wrapcheck // bubbled to handler
	}

	out := make(map[string]struct{}, len(nodes))

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

	if out.PlaceCount == 0 {
		out.PlaceCount = 1
	}

	return out
}

// handleResourceCreate creates a single Resource on a named node from the
// upstream `ResourceCreate` envelope.
func (s *Server) handleResourceCreate(w http.ResponseWriter, r *http.Request) {
	rdName := r.PathValue("rd")

	var body apiv1.ResourceCreate

	err := json.NewDecoder(r.Body).Decode(&body)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return
	}

	res := body.Resource
	res.Name = rdName

	if res.NodeName == "" {
		writeError(w, http.StatusBadRequest, "node_name is required")

		return
	}

	err = s.Store.Resources().Create(r.Context(), &res)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	writeJSON(w, http.StatusCreated, res)
}

// handleResourceDelete drops a single Resource (replica) on a node.
func (s *Server) handleResourceDelete(w http.ResponseWriter, r *http.Request) {
	rdName := r.PathValue("rd")
	node := r.PathValue("node")

	err := s.Store.Resources().Delete(r.Context(), rdName, node)
	if err != nil {
		writeStoreError(w, err)

		return
	}

	w.WriteHeader(http.StatusNoContent)
}
