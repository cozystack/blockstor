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

// Package placer is the autoplacer: given a target replica count and
// constraints, pick free pools and create Resource objects there. Used
// by both the REST autoplace handler and the eviction-driven migration
// reconciler — both have the same job (add N replicas where N is the
// gap between current and desired), differing only in the trigger.
package placer

import (
	"context"
	"slices"
	"sort"
	"strings"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Placer adds replicas to satisfy a placement filter. Construct via
// New; one instance per autoplace call (it does no internal caching).
type Placer struct {
	store store.Store
}

// New constructs a Placer over the given store. The store reference
// is stashed; callers that share state across goroutines should pass
// the same store.Store the rest of the controller uses.
func New(st store.Store) *Placer {
	return &Placer{store: st}
}

// Place creates new Resources for `rdName` until either filter.PlaceCount
// is reached or the candidate set is exhausted. Returns the count
// actually placed plus the requested count so the caller can decide
// between 200 / 409 / partial.
//
// The filter governs both pool selection (StoragePool, NodeNameList,
// disabled-node skip) and topology constraints (replicas_on_same /
// replicas_on_different against Aux/* props on the Node CRD).
//
// Existing replicas on non-disabled nodes count toward PlaceCount.
// Replicas hosted on EVICTED / LOST nodes are NOT counted — that's
// the migration semantic: even if 2 replicas exist, if one is on an
// evicted node the placer will create a third on a healthy peer.
func (p *Placer) Place(ctx context.Context, rdName string, filter *apiv1.AutoSelectFilter) (int, int, error) {
	candidates, err := p.candidatePools(ctx, filter)
	if err != nil {
		return 0, 0, err
	}

	existing, err := p.store.Resources().ListByDefinition(ctx, rdName)
	if err != nil {
		return 0, 0, errors.Wrap(err, "list resources by definition")
	}

	disabled, err := p.disabledNodes(ctx)
	if err != nil {
		return 0, 0, err
	}

	nodes, err := p.nodesByName(ctx)
	if err != nil {
		return 0, 0, err
	}

	state := newState(filter, existing, nodes)

	if state.sameTuple == nil && len(filter.ReplicasOnSame) > 0 {
		candidates, state.sameTuple = pickSameGroup(candidates, nodes,
			filter.ReplicasOnSame, int(filter.PlaceCount))
	}

	placed := 0

	for i := range existing {
		if _, off := disabled[existing[i].NodeName]; !off {
			placed++
		}
	}

	want := int(filter.PlaceCount)

	for i := range candidates {
		if placed >= want {
			break
		}

		ok, err := state.tryPlace(ctx, p.store, rdName, &candidates[i])
		if err != nil {
			return placed, want, err
		}

		if ok {
			placed++
		}
	}

	return placed, want, nil
}

// state holds the per-call lookup tables. Pulled out of Placer so a
// concurrent Placer call doesn't share the mutable bookkeeping.
type state struct {
	nodes     map[string]map[string]string
	taken     map[string]struct{}
	sameTuple map[string]string
	diffSeen  map[string]struct{}
	filter    *apiv1.AutoSelectFilter
}

func newState(filter *apiv1.AutoSelectFilter, existing []apiv1.Resource, nodes map[string]map[string]string) *state {
	s := &state{
		nodes:     nodes,
		taken:     make(map[string]struct{}, len(existing)),
		sameTuple: topologyTuple(existing, nodes, filter.ReplicasOnSame),
		diffSeen:  topologySeen(existing, nodes, filter.ReplicasOnDifferent),
		filter:    filter,
	}

	for i := range existing {
		s.taken[existing[i].NodeName] = struct{}{}
	}

	return s
}

func (s *state) tryPlace(ctx context.Context, st store.Store, rdName string, pool *apiv1.StoragePool) (bool, error) {
	if _, busy := s.taken[pool.NodeName]; busy {
		return false, nil
	}

	nodeProps := s.nodes[pool.NodeName]

	if !matchesTuple(nodeProps, s.filter.ReplicasOnSame, s.sameTuple) {
		return false, nil
	}

	if collidesWithDiff(nodeProps, s.filter.ReplicasOnDifferent, s.diffSeen) {
		return false, nil
	}

	res := apiv1.Resource{
		Name:     rdName,
		NodeName: pool.NodeName,
		Props:    map[string]string{"StorPoolName": pool.StoragePoolName},
	}

	err := st.Resources().Create(ctx, &res)
	if err != nil && !errors.Is(err, store.ErrAlreadyExists) {
		return false, errors.Wrap(err, "create resource")
	}

	s.taken[pool.NodeName] = struct{}{}

	if s.sameTuple == nil && len(s.filter.ReplicasOnSame) > 0 {
		s.sameTuple = lookupKeys(nodeProps, s.filter.ReplicasOnSame)
	}

	for _, k := range s.filter.ReplicasOnDifferent {
		s.diffSeen[k+"="+nodeProps[auxKey(k)]] = struct{}{}
	}

	return true, nil
}

func (p *Placer) candidatePools(ctx context.Context, filter *apiv1.AutoSelectFilter) ([]apiv1.StoragePool, error) {
	all, err := p.store.StoragePools().List(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "list storage pools")
	}

	disabled, err := p.disabledNodes(ctx)
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

	sort.SliceStable(out, func(i, j int) bool {
		if out[i].FreeCapacity != out[j].FreeCapacity {
			return out[i].FreeCapacity > out[j].FreeCapacity
		}

		return out[i].NodeName < out[j].NodeName
	})

	return out, nil
}

// disabledNodes is the union of EVICTED + LOST flagged nodes. Autoplace
// must never pick them — eviction is the operator's signal to drain.
func (p *Placer) disabledNodes(ctx context.Context) (map[string]struct{}, error) {
	nodes, err := p.store.Nodes().List(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "list nodes")
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

func (p *Placer) nodesByName(ctx context.Context) (map[string]map[string]string, error) {
	list, err := p.store.Nodes().List(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "list nodes")
	}

	out := make(map[string]map[string]string, len(list))
	for i := range list {
		out[list[i].Name] = list[i].Props
	}

	return out, nil
}

// --- topology helpers ---------------------------------------------------

func topologyTuple(existing []apiv1.Resource, nodes map[string]map[string]string, keys []string) map[string]string {
	if len(keys) == 0 || len(existing) == 0 {
		return nil
	}

	return lookupKeys(nodes[existing[0].NodeName], keys)
}

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

func lookupKeys(nodeProps map[string]string, keys []string) map[string]string {
	out := make(map[string]string, len(keys))
	for _, k := range keys {
		out[auxKey(k)] = nodeProps[auxKey(k)]
	}

	return out
}

func auxKey(key string) string {
	const prefix = "Aux/"
	if strings.HasPrefix(key, prefix) {
		return key
	}

	return prefix + key
}

// pickSameGroup partitions candidates by their `replicas_on_same`
// tuple, picks a group big enough to hold place_count, and returns
// only those candidates plus the locked-in tuple. When no group is
// large enough we return the candidates unchanged — the placer will
// then fail the conflict check honestly.
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
