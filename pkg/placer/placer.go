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
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// CapacityShortfallError reports that no candidate pool has enough
// FreeCapacity to host the RD's largest volume. Surfaced by Place
// when the post-capacity-filter candidate set is empty AND the RD
// carries a non-zero required size. REST handlers translate this
// into a 409 with operator-actionable text.
//
// Origin (Bug 35 / 7.15 e2e): without this gate, autoplace returned
// 200 even when every candidate pool reported FreeCapacity=0 — the
// satellite then failed opaquely on volume create. The placer must
// fail-fast at the controller before any Resource is stamped.
type CapacityShortfallError struct {
	RequiredKib int64
	MaxFreeKib  int64
}

// Error formats the actionable shortfall message. Format is stable:
// "not enough free capacity on candidate pools: required N KiB, max free M KiB".
// Operators grep for the prefix; tools surface the numbers verbatim.
func (e *CapacityShortfallError) Error() string {
	return fmt.Sprintf(
		"not enough free capacity on candidate pools: required %d KiB, max free %d KiB",
		e.RequiredKib, e.MaxFreeKib,
	)
}

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
	plan, err := p.buildPlan(ctx, rdName, filter)
	if err != nil {
		return 0, 0, err
	}

	if plan.capacityShortfall {
		// Fail-fast: no pool can host the largest volume. Surface the
		// actionable error so REST returns 409 instead of 200 + a
		// downstream satellite failure (Bug 35 / 7.15 e2e).
		return 0, int(filter.PlaceCount), &CapacityShortfallError{
			RequiredKib: plan.requiredKib,
			MaxFreeKib:  plan.maxFreeKib,
		}
	}

	placed := countDiskfulReplicas(plan.existing, plan.disabled)
	want := int(filter.PlaceCount)

	placed, err = p.placeDiskful(ctx, rdName, plan.state, plan.preferred, placed, want)
	if err != nil {
		return placed, want, err
	}

	placed, err = p.placeDiskful(ctx, rdName, plan.state, plan.lastResort, placed, want)
	if err != nil {
		return placed, want, err
	}

	if filter.DisklessOnRemaining && placed >= want {
		err = p.placeDisklessOnRemaining(ctx, rdName, plan.state)
		if err != nil {
			return placed, want, err
		}
	}

	// Partial-shortfall path (Bug 35): the diskful loop ran out of
	// candidates because the capacity gate dropped pools that would
	// otherwise have qualified. Surface the actionable error so the
	// REST 409 names the missing capacity. We only fire when capacity
	// was actually the rejecter (maxFreeKib > 0); a shortfall caused
	// purely by topology/anti-affinity still falls through to the
	// generic "not enough candidate storage pools" path.
	if placed < want && plan.requiredKib > 0 && plan.maxFreeKib > 0 {
		return placed, want, &CapacityShortfallError{
			RequiredKib: plan.requiredKib,
			MaxFreeKib:  plan.maxFreeKib,
		}
	}

	return placed, want, nil
}

// placePlan bundles everything Place needs after the read-side
// store lookups complete. Pulled into a struct so the main Place
// function stays under the gocyclo budget once the Bug 35 capacity
// gate added two more branches.
//
// capacityShortfall=true means buildPlan has already determined that
// no pool can host the largest volume and the rest of placement
// must be skipped; requiredKib + maxFreeKib then carry the numbers
// for the actionable CapacityShortfallError envelope.
type placePlan struct {
	state             *state
	preferred         []apiv1.StoragePool
	lastResort        []apiv1.StoragePool
	existing          []apiv1.Resource
	disabled          map[string]struct{}
	requiredKib       int64
	maxFreeKib        int64
	capacityShortfall bool
}

// buildPlan does the read-side legwork: load VDs, candidate pools,
// existing resources, disabled / node-prop maps, and the topology-
// partitioned candidate buckets. When the capacity gate has already
// rejected every pool it returns a plan with capacityShortfall=true
// and only the shortfall numbers populated — the caller fail-fasts
// with CapacityShortfallError without touching the unset fields.
func (p *Placer) buildPlan(ctx context.Context, rdName string, filter *apiv1.AutoSelectFilter) (*placePlan, error) {
	// Capacity-gate sizing (Bug 35): every volume of an RD provisions
	// against the same pool, so any candidate pool must accommodate
	// the biggest of them. requiredKib==0 (no VDs yet, e.g. a
	// definitions-only spawn) leaves the capacity filter a no-op.
	requiredKib, err := p.requiredKib(ctx, rdName)
	if err != nil {
		return nil, err
	}

	candidates, maxFreeKib, err := p.candidatePools(ctx, filter, requiredKib)
	if err != nil {
		return nil, err
	}

	if requiredKib > 0 && len(candidates) == 0 {
		return &placePlan{
			requiredKib:       requiredKib,
			maxFreeKib:        maxFreeKib,
			capacityShortfall: true,
		}, nil
	}

	return p.assemblePlan(ctx, rdName, filter, candidates, requiredKib, maxFreeKib)
}

// assemblePlan finishes off buildPlan once the capacity gate has
// passed. Split out so buildPlan stays under the gocyclo budget.
func (p *Placer) assemblePlan(ctx context.Context, rdName string, filter *apiv1.AutoSelectFilter, candidates []apiv1.StoragePool, requiredKib, maxFreeKib int64) (*placePlan, error) {
	existing, err := p.store.Resources().ListByDefinition(ctx, rdName)
	if err != nil {
		return nil, errors.Wrap(err, "list resources by definition")
	}

	disabled, err := p.disabledNodes(ctx)
	if err != nil {
		return nil, err
	}

	nodes, err := p.nodesByName(ctx)
	if err != nil {
		return nil, err
	}

	allPools, err := p.store.StoragePools().List(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "list storage pools")
	}

	state := newState(filter, existing, nodes, allPools)

	if state.sameTuple == nil && len(filter.ReplicasOnSame) > 0 {
		candidates, state.sameTuple = pickSameGroup(candidates, nodes,
			filter.ReplicasOnSame, int(filter.PlaceCount))
	}

	// replicas_on_different in "key=value" form is a soft-exclusion:
	// nodes carrying that exact pair are considered LAST. Split the
	// candidate set into preferred (no excluded pair) and last-resort
	// (excluded pair present) so we exhaust preferred before touching
	// the excluded bucket — see UG9 §replicasOnDifferent.
	preferred, lastResort := partitionByExclusion(candidates, nodes, filter.ReplicasOnDifferent)

	return &placePlan{
		state:       state,
		preferred:   preferred,
		lastResort:  lastResort,
		existing:    existing,
		disabled:    disabled,
		requiredKib: requiredKib,
		maxFreeKib:  maxFreeKib,
	}, nil
}

// countDiskfulReplicas returns the number of existing replicas that
// satisfy place_count: diskful (no DISKLESS flag) and not on an
// EVICTED/LOST node.
//
// DISKLESS replicas — including auto-tiebreaker witnesses, which carry
// both DISKLESS and TIE_BREAKER — do NOT count. place_count is the
// diskful-replica target. A 3-replica RG sitting at 2 diskful + 1
// diskless witness must be treated as 1-short so the placer fills the
// gap rather than declaring satisfaction.
func countDiskfulReplicas(existing []apiv1.Resource, disabled map[string]struct{}) int {
	count := 0

	for i := range existing {
		if _, off := disabled[existing[i].NodeName]; off {
			continue
		}

		if slices.Contains(existing[i].Flags, apiv1.ResourceFlagDiskless) {
			continue
		}

		count++
	}

	return count
}

// placeDiskful is the inner loop of Place — tries each candidate pool
// in order until placed reaches want. Pulled out of Place to keep the
// main path under the cyclomatic-complexity budget.
func (p *Placer) placeDiskful(ctx context.Context, rdName string, state *state, candidates []apiv1.StoragePool, placed, want int) (int, error) {
	for i := range candidates {
		if placed >= want {
			break
		}

		ok, err := state.tryPlace(ctx, p.store, rdName, &candidates[i])
		if err != nil {
			return placed, err
		}

		if ok {
			placed++
		}
	}

	return placed, nil
}

// placeDisklessOnRemaining ensures every healthy node not already
// hosting a replica gets a DISKLESS one. Upstream LINSTOR uses this
// for "cluster-wide attachable" volumes — any consumer Pod can mount
// the PVC because every node has at least the DRBD-network presence.
//
// Only runs after the diskful place_count is satisfied; we never
// substitute diskless for diskful when the diskful target hasn't been
// met.
func (p *Placer) placeDisklessOnRemaining(ctx context.Context, rdName string, state *state) error {
	for nodeName, hostProps := range state.nodes {
		if _, busy := state.taken[nodeName]; busy {
			continue
		}

		_ = hostProps // topology constraints don't apply to diskless witnesses

		res := apiv1.Resource{
			Name:     rdName,
			NodeName: nodeName,
			Flags:    []string{"DISKLESS"},
		}

		err := p.store.Resources().Create(ctx, &res)
		if err != nil && !errors.Is(err, store.ErrAlreadyExists) {
			return errors.Wrap(err, "create diskless witness")
		}

		state.taken[nodeName] = struct{}{}
	}

	return nil
}

// state holds the per-call lookup tables. Pulled out of Placer so a
// concurrent Placer call doesn't share the mutable bookkeeping.
type state struct {
	nodes      map[string]map[string]string
	taken      map[string]struct{}
	sameTuple  map[string]string
	diffSeen   map[string]struct{}
	sharedSeen map[string]struct{} // sharedSpaceIDs already used by an existing replica
	pools      map[string]apiv1.StoragePool
	filter     *apiv1.AutoSelectFilter
}

func newState(filter *apiv1.AutoSelectFilter, existing []apiv1.Resource, nodes map[string]map[string]string, pools []apiv1.StoragePool) *state {
	s := &state{
		nodes:      nodes,
		taken:      make(map[string]struct{}, len(existing)),
		sameTuple:  topologyTuple(existing, nodes, filter.ReplicasOnSame),
		diffSeen:   topologySeen(existing, nodes, filter.ReplicasOnDifferent),
		sharedSeen: make(map[string]struct{}),
		pools:      poolsByKey(pools),
		filter:     filter,
	}

	for i := range existing {
		s.taken[existing[i].NodeName] = struct{}{}

		// Pre-seed shared-space anti-affinity: if an existing replica
		// already lives on a shared-LUN pool, no new replica may land
		// on a pool sharing that LUN identifier.
		stor := existing[i].Props["StorPoolName"]
		if stor == "" {
			continue
		}

		pool, ok := s.pools[poolKey(existing[i].NodeName, stor)]
		if !ok {
			continue
		}

		if pool.SharedSpaceID != "" {
			s.sharedSeen[pool.SharedSpaceID] = struct{}{}
		}
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

	// Shared-LUN anti-affinity: pools sharing a backing LUN identifier
	// cannot host two replicas of the same RD — at the physical layer
	// they are the same disk, so a 2-replica placement onto the same
	// SharedSpaceID would offer zero redundancy.
	if pool.SharedSpaceID != "" {
		if _, dup := s.sharedSeen[pool.SharedSpaceID]; dup {
			return false, nil
		}
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

	for _, spec := range s.filter.ReplicasOnDifferent {
		if _, _, hasValue := parseDiffSpec(spec); hasValue {
			continue
		}

		k := spec
		s.diffSeen[k+"="+nodeProps[auxKey(k)]] = struct{}{}
	}

	if pool.SharedSpaceID != "" {
		s.sharedSeen[pool.SharedSpaceID] = struct{}{}
	}

	return true, nil
}

// poolKey is the composite (node, pool) lookup key used to find a
// StoragePool from a Resource's StorPoolName + NodeName pair.
func poolKey(node, pool string) string {
	return node + "\x1f" + pool
}

func poolsByKey(pools []apiv1.StoragePool) map[string]apiv1.StoragePool {
	out := make(map[string]apiv1.StoragePool, len(pools))
	for i := range pools {
		out[poolKey(pools[i].NodeName, pools[i].StoragePoolName)] = pools[i]
	}

	return out
}

// candidatePools returns the pool set eligible for a placement, after
// applying all filter constraints (kind, node list, pool list, provider
// list) and — Bug 35 — dropping pools whose FreeCapacity is below the
// volume the placer is about to land. Returns the second value as the
// largest FreeCapacity among pools that PASSED every non-capacity gate
// but FAILED the capacity one; callers use it to build the actionable
// "max free M KiB" 409 message.
//
// minFreeKib==0 disables the capacity filter (definitions-only or test
// paths with no VDs).
func (p *Placer) candidatePools(ctx context.Context, filter *apiv1.AutoSelectFilter, minFreeKib int64) ([]apiv1.StoragePool, int64, error) {
	all, err := p.store.StoragePools().List(ctx)
	if err != nil {
		return nil, 0, errors.Wrap(err, "list storage pools")
	}

	disabled, err := p.disabledNodes(ctx)
	if err != nil {
		return nil, 0, err
	}

	out := make([]apiv1.StoragePool, 0, len(all))

	var maxFreeBelow int64

	for i := range all {
		pool := all[i]

		if !matchesPoolFilter(&pool, filter, disabled) {
			continue
		}

		// Capacity gate (Bug 35): drop pools that physically can't
		// host the largest volume of this RD. Track the largest
		// FreeCapacity among rejected pools so the caller can render
		// the actionable 409 ("required N KiB, max free M KiB").
		// The gate is FreeCapacity-only — over-subscription is the
		// spawn-time gate's job (see pkg/rest/spawn.go's
		// rejectIfExceedsOversubGate); this layer is the hard floor
		// that protects against the 0-KiB-free pool race seen in
		// 7.15 e2e.
		if minFreeKib > 0 && pool.FreeCapacity < minFreeKib {
			if pool.FreeCapacity > maxFreeBelow {
				maxFreeBelow = pool.FreeCapacity
			}

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

	return out, maxFreeBelow, nil
}

// matchesPoolFilter is the AutoSelectFilter eligibility check for a
// single pool: drops the DISKLESS provider kind, drops pools on
// EVICTED / LOST nodes, and enforces every name-list / provider-list
// constraint on the filter. Returns true when the pool clears every
// non-capacity gate; the capacity gate stays in candidatePools so it
// can also track the largest rejected FreeCapacity for the error
// envelope. Split out to keep candidatePools under the gocyclo budget.
func matchesPoolFilter(pool *apiv1.StoragePool, filter *apiv1.AutoSelectFilter, disabled map[string]struct{}) bool {
	if pool.ProviderKind == apiv1.StoragePoolKindDiskless {
		return false
	}

	if _, off := disabled[pool.NodeName]; off {
		return false
	}

	if filter.StoragePool != "" && pool.StoragePoolName != filter.StoragePool {
		return false
	}

	if len(filter.StoragePoolList) > 0 && !slices.Contains(filter.StoragePoolList, pool.StoragePoolName) {
		return false
	}

	if len(filter.NodeNameList) > 0 && !slices.Contains(filter.NodeNameList, pool.NodeName) {
		return false
	}

	// Provider-kind filter (Bug 15 / snapshot-restore-resource):
	// `zfs send` and `dd`/`lvm` payloads are not interchangeable,
	// so cloning from a snapshot on a ZFS_THIN pool onto an
	// LVM_THIN target fails opaquely at satellite send/recv time.
	// Fail-fast at the placer layer by dropping pools whose
	// ProviderKind isn't in the caller's allow-list.
	if len(filter.ProviderList) > 0 && !slices.Contains(filter.ProviderList, pool.ProviderKind) {
		return false
	}

	return true
}

// requiredKib returns the SizeKib of the largest VolumeDefinition on
// the named RD. Every volume of an RD provisions against the same pool
// (upstream LINSTOR contract), so the per-pool capacity gate must clear
// the biggest of them. Returns 0 — no filter — when the RD has no VDs
// yet (e.g. definitions-only spawn path where the autoplacer races
// ahead of spawnVolumeDefinitions, or a bare-RD autoplace before any
// resize). A storage backend reporting an empty VD list is the
// idempotent "nothing to gate" case, not an error.
func (p *Placer) requiredKib(ctx context.Context, rdName string) (int64, error) {
	vds, err := p.store.VolumeDefinitions().List(ctx, rdName)
	if err != nil {
		return 0, errors.Wrap(err, "list volume definitions")
	}

	var maxKib int64

	for i := range vds {
		if vds[i].SizeKib > maxKib {
			maxKib = vds[i].SizeKib
		}
	}

	return maxKib, nil
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

	for _, spec := range keys {
		// "key=value" form is a soft-exclusion, not anti-affinity —
		// don't pre-seed it into the diff seen-set. Such entries are
		// handled by partitionByExclusion + the two-pass place loop.
		if _, _, hasValue := parseDiffSpec(spec); hasValue {
			continue
		}

		k := spec

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
	for _, spec := range keys {
		// Value-form entries don't participate in anti-affinity; they
		// are soft-exclusions handled at the candidate-partitioning
		// layer. Skip them so a key="zone=us-east" filter doesn't
		// accidentally collide with itself.
		if _, _, hasValue := parseDiffSpec(spec); hasValue {
			continue
		}

		k := spec
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

// parseDiffSpec splits a replicas_on_different entry into its key and
// optional value half. Per UG9, a bare "key" means "spread replicas
// across distinct values of key" (anti-affinity); a "key=value" form
// means "nodes carrying that exact pair are LAST resort" (soft
// exclusion). The third return tells the caller which mode this spec
// is in.
func parseDiffSpec(spec string) (string, string, bool) {
	k, v, ok := strings.Cut(spec, "=")
	if !ok {
		return spec, "", false
	}

	return k, v, true
}

// partitionByExclusion separates the candidate pool list into a
// "preferred" bucket (nodes that don't carry any of the soft-exclusion
// `key=value` pairs from replicas_on_different) and a "last-resort"
// bucket (nodes that carry at least one). The placer drains preferred
// first; only when preferred is exhausted does it touch last-resort,
// matching the UG9 contract: value-form excludes those nodes from
// normal selection but does not hard-forbid them.
//
// When the filter carries no value-form entries, every candidate ends
// up in preferred — the function is a no-op for the common case.
func partitionByExclusion(candidates []apiv1.StoragePool, nodes map[string]map[string]string, keys []string) ([]apiv1.StoragePool, []apiv1.StoragePool) {
	// Fast path: no value-form entries → everything is preferred.
	hasAnyValueForm := false

	for _, spec := range keys {
		if _, _, hv := parseDiffSpec(spec); hv {
			hasAnyValueForm = true

			break
		}
	}

	if !hasAnyValueForm {
		return candidates, nil
	}

	var (
		preferred  []apiv1.StoragePool
		lastResort []apiv1.StoragePool
	)

	for i := range candidates {
		pool := candidates[i]
		nodeProps := nodes[pool.NodeName]

		if matchesAnyExcludedPair(nodeProps, keys) {
			lastResort = append(lastResort, pool)

			continue
		}

		preferred = append(preferred, pool)
	}

	return preferred, lastResort
}

// matchesAnyExcludedPair returns true when at least one value-form
// entry in keys matches the node's Aux/<key> property. This is the
// per-node test that drives partitionByExclusion's bucketing.
func matchesAnyExcludedPair(nodeProps map[string]string, keys []string) bool {
	for _, spec := range keys {
		k, v, hasValue := parseDiffSpec(spec)
		if !hasValue {
			continue
		}

		if nodeProps[auxKey(k)] == v {
			return true
		}
	}

	return false
}

// pickSameGroup partitions candidates by their `replicas_on_same`
// tuple, picks a group big enough to hold place_count, and returns
// only those candidates plus the locked-in tuple. When no group is
// large enough we return the candidates unchanged — the placer will
// then fail the conflict check honestly.
//
// Group sizing counts UNIQUE nodes, not pools (Bug 44). A node with
// two pools still hosts at most one replica per RD, so a group
// holding `pools=[n1.fast, n1.slow]` can satisfy `want=2` only on
// paper. Counting unique nodes is what matches the placer's per-node
// taken-set semantic and keeps cross-zone leaks from sneaking in via
// multi-pool nodes.
func pickSameGroup(candidates []apiv1.StoragePool, nodes map[string]map[string]string, keys []string, want int) ([]apiv1.StoragePool, map[string]string) {
	type group struct {
		tuple map[string]string
		key   string
		pools []apiv1.StoragePool
		seen  map[string]struct{}
		free  int64
	}

	byKey := map[string]*group{}

	for i := range candidates {
		pool := candidates[i]
		tuple := lookupKeys(nodes[pool.NodeName], keys)
		key := tupleKey(tuple)

		grp, ok := byKey[key]
		if !ok {
			grp = &group{tuple: tuple, key: key, seen: map[string]struct{}{}}
			byKey[key] = grp
		}

		grp.pools = append(grp.pools, pool)
		grp.seen[pool.NodeName] = struct{}{}
		grp.free += pool.FreeCapacity
	}

	groups := make([]*group, 0, len(byKey))
	for _, grp := range byKey {
		if len(grp.seen) >= want {
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
