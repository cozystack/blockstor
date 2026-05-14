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
	"regexp"
	"slices"
	"sort"
	"strconv"
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

// OversubRatioKind names which over-subscription cap a pool tripped on.
// Used by OversubscriptionShortfallError so operators can see the exact
// property to tune.
//   - OversubRatioFree:   MaxFreeCapacityOversubscriptionRatio  (7.W09 / 7.19)
//   - OversubRatioTotal:  MaxTotalCapacityOversubscriptionRatio (7.W10 / 7.20)
//   - OversubRatioMaster: MaxOversubscriptionRatio              (7.W11 / 7.21)
//     The master backstop is only reported when the specific Free/Total
//     property is unset and the umbrella fell through to it; otherwise
//     the specific kind is named even if its effective value came from
//     the umbrella fallback chain.
type OversubRatioKind string

const (
	OversubRatioFree   OversubRatioKind = "MaxFreeCapacityOversubscriptionRatio"
	OversubRatioTotal  OversubRatioKind = "MaxTotalCapacityOversubscriptionRatio"
	OversubRatioMaster OversubRatioKind = "MaxOversubscriptionRatio"
)

// OversubscriptionShortfallError reports that every candidate pool's
// effective over-subscription cap (the lesser of free×freeRatio and
// total×totalRatio, with the umbrella MaxOversubscriptionRatio as the
// final backstop) is below the volume the placer must land. Surfaced
// by Place when the FreeCapacity floor cleared but no pool can host
// the request once the ratio caps are applied — the placer's mirror
// of the spawn-layer 409 gate (pkg/rest/spawn.go's
// rejectIfExceedsOversubGate), needed because autoplace and spawn are
// independent code paths.
//
// Origin (scenarios 7.W10 / 7.W11): without this gate the placer
// would accept a pool whose FreeCapacity covers the request but whose
// `MaxTotalCapacityOversubscriptionRatio` (or the master backstop)
// already caps the per-pool logical budget below the ask — leading
// to a successful Resource create followed by an opaque satellite
// failure when the next ZVOL extend trips the same ratio cap.
//
// The error names the pool that was CLOSEST to satisfying the ask
// (largest EffectiveCapKib among rejected pools) plus the specific
// ratio kind the operator must tune. REST handlers translate this
// into a 409 with operator-actionable text.
type OversubscriptionShortfallError struct {
	RequiredKib     int64
	PoolName        string
	NodeName        string
	Ratio           OversubRatioKind
	RatioValue      float64
	EffectiveCapKib int64
}

// Error formats the actionable shortfall message. Format is stable:
// "over-subscription cap on pool <pool>@<node>: required N KiB exceeds
// MaxVolumeSize M KiB (raise <Ratio> currently V on the storage pool
// or controller, or shrink the request)".
// Operators grep for the prefix; tools surface the numbers verbatim.
func (e *OversubscriptionShortfallError) Error() string {
	return fmt.Sprintf(
		"over-subscription cap on pool %s@%s: required %d KiB exceeds MaxVolumeSize %d KiB "+
			"(raise %s currently %g on the storage pool or controller, or shrink the request)",
		e.PoolName, e.NodeName,
		e.RequiredKib, e.EffectiveCapKib,
		e.Ratio, e.RatioValue,
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
		//
		// Precedence: the physical FreeCapacity floor (Bug 35) is the
		// more fundamental gate, so a non-zero maxFreeKib wins over an
		// oversub witness — operators see the hard floor first. Only
		// when the floor was clean (every rejected pool tripped a ratio
		// cap, not the floor) do we surface the OversubscriptionShortfall
		// envelope so the relevant ratio (Free / Total / Master) is
		// named in the 409 (scenarios 7.W10 / 7.W11).
		if plan.maxFreeKib == 0 && plan.oversubShortfall != nil {
			return 0, int(filter.PlaceCount), plan.oversubShortfall
		}

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

	// Partial-shortfall path (Bug 35 + scenarios 7.W10 / 7.W11): the
	// diskful loop ran out of candidates because the capacity or
	// over-subscription gate dropped pools that would otherwise have
	// qualified. Surface the actionable error so the REST 409 names
	// the missing capacity OR the over-subscription cap. We only fire
	// when one of those gates was actually the rejecter; a shortfall
	// caused purely by topology/anti-affinity still falls through to
	// the generic "not enough candidate storage pools" path.
	//
	// Precedence: physical floor (maxFreeKib > 0) wins over oversub —
	// see the capacityShortfall branch above for the rationale.
	if placed < want && plan.requiredKib > 0 {
		if plan.maxFreeKib > 0 {
			return placed, want, &CapacityShortfallError{
				RequiredKib: plan.requiredKib,
				MaxFreeKib:  plan.maxFreeKib,
			}
		}

		if plan.oversubShortfall != nil {
			return placed, want, plan.oversubShortfall
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
	// oversubShortfall carries the closest-miss over-subscription
	// rejection from candidatePools. Surfaced by Place when no pool
	// survived the ratio gates (scenarios 7.W09 / 7.W10 / 7.W11) and
	// the FreeCapacity floor did NOT also trip — the two errors are
	// mutually exclusive so REST can render a single 409 naming the
	// exact ratio the operator needs to tune.
	oversubShortfall *OversubscriptionShortfallError
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

	candidates, gates, err := p.candidatePools(ctx, filter, requiredKib)
	if err != nil {
		return nil, err
	}

	if requiredKib > 0 && len(candidates) == 0 {
		return &placePlan{
			requiredKib:       requiredKib,
			maxFreeKib:        gates.maxFreeBelow,
			capacityShortfall: true,
			oversubShortfall:  gates.oversubBest,
		}, nil
	}

	return p.assemblePlan(ctx, rdName, filter, candidates, requiredKib, gates)
}

// assemblePlan finishes off buildPlan once the capacity gate has
// passed. Split out so buildPlan stays under the gocyclo budget.
func (p *Placer) assemblePlan(ctx context.Context, rdName string, filter *apiv1.AutoSelectFilter, candidates []apiv1.StoragePool, requiredKib int64, gates candidateGates) (*placePlan, error) {
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

	// do-not-place-with anti-affinity (scenarios 2.10 / 2.11): exclude
	// every candidate pool whose node already hosts a replica of an RD
	// named in NotPlaceWithRsc (verbatim) or matching
	// NotPlaceWithRscRegex (compiled pattern). Resources of the RD
	// being placed are ignored — otherwise the very first replica
	// locks out the rest of the cluster. Invalid regex is a silent
	// no-op so operator typos don't strand placement.
	candidates, err = p.applyNotPlaceWith(ctx, rdName, filter, candidates)
	if err != nil {
		return nil, err
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
		state:            state,
		preferred:        preferred,
		lastResort:       lastResort,
		existing:         existing,
		disabled:         disabled,
		requiredKib:      requiredKib,
		maxFreeKib:       gates.maxFreeBelow,
		oversubShortfall: gates.oversubBest,
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
	// firstKind is the ProviderKind of the first diskful replica
	// either already on the RD (seeded from `existing`) or placed
	// during this Place call. Subsequent diskful candidates are
	// rejected unless IsProviderKindMixingAllowed(firstKind, pool)
	// returns true (Bug 76: mirror upstream LINSTOR's same-kind
	// constraint). Empty string means "no diskful replica yet, no
	// constraint" — the first replica is free to pick any kind.
	firstKind string
	// xBuckets enforces XReplicasOnDifferentMap (scenario 9.W08 /
	// wave1 2.8). Per <key,N> entry the placer caps the number of
	// replicas that share the same Aux/<key> value at N. Keys here
	// are "<key>=<value>"; counts include both pre-existing replicas
	// (seeded in newState) and replicas placed in the current call.
	// A node whose property is unset still occupies the empty-value
	// bucket "<key>=", matching upstream LINSTOR's behaviour where
	// "missing property" is just another value of that key.
	xBuckets map[string]int
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
		xBuckets:   make(map[string]int, len(filter.XReplicasOnDifferentMap)),
	}

	for i := range existing {
		s.taken[existing[i].NodeName] = struct{}{}

		// Seed XReplicasOnDifferentMap buckets from any existing
		// diskful replica. DISKLESS witnesses don't occupy a
		// topology bucket — they're network-only attachments and
		// upstream LINSTOR doesn't count them toward x-replicas
		// either (mirrors place_count's diskful-only semantic).
		if !slices.Contains(existing[i].Flags, apiv1.ResourceFlagDiskless) {
			nodeProps := nodes[existing[i].NodeName]
			for key := range filter.XReplicasOnDifferentMap {
				s.xBuckets[xBucketKey(key, nodeProps)]++
			}
		}

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

		// Pre-seed Bug 76 same-kind constraint from the first
		// diskful replica already on the RD. DISKLESS replicas
		// don't claim a backing pool so they're skipped — see
		// IsProviderKindMixingAllowed for the rationale.
		if s.firstKind == "" && !slices.Contains(existing[i].Flags, apiv1.ResourceFlagDiskless) {
			s.firstKind = pool.ProviderKind
		}
	}

	return s
}

// candidateNodeProps returns the node props for pool.NodeName when
// every per-candidate constraint passes, plus an ok flag. Pulled out
// of tryPlace to keep that function under the gocyclo budget: each
// constraint is a short-circuit branch that adds to the count and
// the list grew long enough (taken / replicas_on_same / _different /
// x_replicas / shared-LUN / provider-kind-mix) to need its own frame.
// Returns (nil, false) on rejection so the caller stays a single
// if-not-ok-return.
func (s *state) candidateNodeProps(pool *apiv1.StoragePool) (map[string]string, bool) {
	if _, busy := s.taken[pool.NodeName]; busy {
		return nil, false
	}

	nodeProps := s.nodes[pool.NodeName]

	if !matchesTuple(nodeProps, s.filter.ReplicasOnSame, s.sameTuple) {
		return nil, false
	}

	if collidesWithDiff(nodeProps, s.filter.ReplicasOnDifferent, s.diffSeen) {
		return nil, false
	}

	// X-replicas-on-different bucket cap (scenario 9.W08): for each
	// <key, N> entry, reject the candidate when its bucket is full.
	// `--x-replicas-on-different site 1` becomes a hard "all-different"
	// rule; N=2 lets two replicas share a value (e.g. two per rack)
	// while still spreading the remaining replicas across other values.
	if s.exceedsXBucket(nodeProps) {
		return nil, false
	}

	// Shared-LUN anti-affinity: pools sharing a backing LUN identifier
	// cannot host two replicas of the same RD — at the physical layer
	// they are the same disk, so a 2-replica placement onto the same
	// SharedSpaceID would offer zero redundancy.
	if pool.SharedSpaceID != "" {
		if _, dup := s.sharedSeen[pool.SharedSpaceID]; dup {
			return nil, false
		}
	}

	// ProviderKind mixing gate (Bug 76): see allowsKindMix doc.
	if !s.allowsKindMix(pool.ProviderKind) {
		return nil, false
	}

	return nodeProps, true
}

// exceedsXBucket reports whether placing a replica on the candidate
// node would push any XReplicasOnDifferentMap bucket past its cap.
// Scenario 9.W08: a bare `--replicas-on-different <key>` says "all
// replicas on different values"; `--x-replicas-on-different <key> N`
// loosens that to "at most N replicas per value of <key>", supporting
// stretched-DR layouts like 2 replicas per site × 2 sites.
//
// Nodes without the Aux property hash into the empty-value bucket,
// matching upstream LINSTOR — missing-property nodes are not special-
// cased the way bare `--replicas-on-different` treats them (see
// scenario 9.W07's "different from any value" gotcha).
func (s *state) exceedsXBucket(nodeProps map[string]string) bool {
	for key, maxPerBucket := range s.filter.XReplicasOnDifferentMap {
		if maxPerBucket <= 0 {
			// Defensive: a non-positive cap would silently lock the
			// cluster out of every node. Treat it as "no constraint"
			// so an operator's typo (`--x-replicas-on-different a 0`)
			// degrades gracefully instead of breaking placement.
			continue
		}

		if s.xBuckets[xBucketKey(key, nodeProps)] >= int(maxPerBucket) {
			return true
		}
	}

	return false
}

// xBucketKey returns the bucket-counter key for a node's value of
// the given X-replicas property. Format mirrors topologySeen's
// "<key>=<value>" so a future debug dump can render both maps with
// the same legend.
func xBucketKey(key string, nodeProps map[string]string) string {
	return key + "=" + nodeProps[auxKey(key)]
}

// allowsKindMix is the Bug 76 same-kind gate. Once the RD has one
// diskful replica, every subsequent diskful replica must land on a
// pool whose ProviderKind is compatible with the first one. This
// mirrors upstream LINSTOR's DeviceProviderKind.isMixingAllowed
// and prevents `r c test --auto-place 2` from spreading replicas
// across e.g. FILE_THIN + LVM_THIN — the symptom Bug 76 reports.
//
// Returns true when no diskful replica has been pinned yet
// (firstKind is the empty string) — the first replica is free to
// pick any kind.
func (s *state) allowsKindMix(kind string) bool {
	if s.firstKind == "" {
		return true
	}

	return IsProviderKindMixingAllowed(s.firstKind, kind)
}

func (s *state) tryPlace(ctx context.Context, st store.Store, rdName string, pool *apiv1.StoragePool) (bool, error) {
	nodeProps, ok := s.candidateNodeProps(pool)
	if !ok {
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

	for _, spec := range s.filter.ReplicasOnDifferent {
		if _, _, hasValue := parseDiffSpec(spec); hasValue {
			continue
		}

		k := spec
		s.diffSeen[k+"="+nodeProps[auxKey(k)]] = struct{}{}
	}

	// Increment the X-replicas buckets after a successful placement
	// so subsequent candidates see the updated counts.
	for key := range s.filter.XReplicasOnDifferentMap {
		s.xBuckets[xBucketKey(key, nodeProps)]++
	}

	if pool.SharedSpaceID != "" {
		s.sharedSeen[pool.SharedSpaceID] = struct{}{}
	}

	// Lock in the RD's ProviderKind from the first diskful replica
	// (Bug 76). Subsequent tryPlace calls compare against this
	// captured value via IsProviderKindMixingAllowed.
	if s.firstKind == "" {
		s.firstKind = pool.ProviderKind
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

// applyNotPlaceWith drops every candidate pool whose node already hosts
// a replica of an RD named verbatim in filter.NotPlaceWithRsc OR matching
// filter.NotPlaceWithRscRegex (scenarios 2.10 / 2.11). Replicas of the RD
// being placed are skipped so the first replica doesn't lock the placer
// out of the cluster. An invalid regex is a silent no-op — operator typos
// must not strand placement.
//
// Returns the filtered candidates slice. When neither field is set the
// input is returned unchanged (zero allocations on the common path).
func (p *Placer) applyNotPlaceWith(ctx context.Context, rdName string, filter *apiv1.AutoSelectFilter, candidates []apiv1.StoragePool) ([]apiv1.StoragePool, error) {
	if len(filter.NotPlaceWithRsc) == 0 && filter.NotPlaceWithRscRegex == "" {
		return candidates, nil
	}

	var pattern *regexp.Regexp

	if filter.NotPlaceWithRscRegex != "" {
		// Compile-error is a silent no-op per spec: an operator typo
		// must never strand placement. The verbatim list still applies.
		re, compileErr := regexp.Compile(filter.NotPlaceWithRscRegex)
		if compileErr == nil {
			pattern = re
		}
	}

	all, err := p.store.Resources().List(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "list resources for not-place-with filter")
	}

	excluded := make(map[string]struct{})

	for i := range all {
		// Skip resources of the RD being placed — otherwise the first
		// replica we land locks every subsequent placement out.
		if all[i].Name == rdName {
			continue
		}

		if slices.Contains(filter.NotPlaceWithRsc, all[i].Name) {
			excluded[all[i].NodeName] = struct{}{}

			continue
		}

		if pattern != nil && pattern.MatchString(all[i].Name) {
			excluded[all[i].NodeName] = struct{}{}
		}
	}

	if len(excluded) == 0 {
		return candidates, nil
	}

	out := candidates[:0:0]

	for i := range candidates {
		if _, blocked := excluded[candidates[i].NodeName]; blocked {
			continue
		}

		out = append(out, candidates[i])
	}

	return out, nil
}

// candidateGates bundles the rejection metadata produced by
// candidatePools alongside the surviving pool set. Pulled into a
// struct so candidatePools can report BOTH the FreeCapacity-floor
// witness (Bug 35) and the over-subscription cap witness (scenarios
// 7.W10 / 7.W11) without growing a four-return signature.
//
// maxFreeBelow: largest FreeCapacity among pools rejected by the
// physical floor. Zero when no pool tripped that gate.
//
// oversubBest: the over-subscription rejection with the LARGEST
// effective cap (i.e. the pool that came CLOSEST to satisfying the
// request after applying the ratio gates). Nil when no pool tripped
// the oversub gate. The caller surfaces this in
// OversubscriptionShortfallError so operators see the most relevant
// ratio to tune.
type candidateGates struct {
	maxFreeBelow int64
	oversubBest  *OversubscriptionShortfallError
}

// candidatePools returns the pool set eligible for a placement, after
// applying all filter constraints (kind, node list, pool list, provider
// list), the FreeCapacity floor (Bug 35) and the three over-subscription
// ratio caps (scenarios 7.W09 / 7.W10 / 7.W11). The returned
// candidateGates carries the witnesses needed to render an actionable
// 409 when placement ultimately falls short.
//
// minFreeKib==0 disables both gates (definitions-only or test paths
// with no VDs).
func (p *Placer) candidatePools(ctx context.Context, filter *apiv1.AutoSelectFilter, minFreeKib int64) ([]apiv1.StoragePool, candidateGates, error) {
	all, err := p.store.StoragePools().List(ctx)
	if err != nil {
		return nil, candidateGates{}, errors.Wrap(err, "list storage pools")
	}

	disabled, err := p.disabledNodes(ctx)
	if err != nil {
		return nil, candidateGates{}, err
	}

	ctrlProps, err := p.store.ControllerProps().Get(ctx)
	if err != nil {
		return nil, candidateGates{}, errors.Wrap(err, "get controller props")
	}

	out, gates := applyCapacityAndOversubGates(all, filter, disabled, ctrlProps, minFreeKib)

	weights, err := p.loadWeights(ctx)
	if err != nil {
		return nil, candidateGates{}, err
	}

	rscPerNode, err := p.resourcesPerNode(ctx)
	if err != nil {
		return nil, candidateGates{}, err
	}

	rankCandidates(out, weights, rscPerNode)

	return out, gates, nil
}

// applyCapacityAndOversubGates is the gate-loop core split out of
// candidatePools so the latter stays under the funlen budget. Returns
// the surviving pool slice plus the closest-miss witnesses for the
// FreeCapacity floor (Bug 35) and the three over-subscription ratio
// caps (scenarios 7.W09 / 7.W10 / 7.W11).
func applyCapacityAndOversubGates(all []apiv1.StoragePool, filter *apiv1.AutoSelectFilter, disabled map[string]struct{}, ctrlProps map[string]string, minFreeKib int64) ([]apiv1.StoragePool, candidateGates) {
	out := make([]apiv1.StoragePool, 0, len(all))
	gates := candidateGates{}

	for i := range all {
		pool := all[i]

		if !matchesPoolFilter(&pool, filter, disabled) {
			continue
		}

		// Capacity gate (Bug 35): drop pools that physically can't
		// host the largest volume of this RD. Track the largest
		// FreeCapacity among rejected pools so the caller can render
		// the actionable 409 ("required N KiB, max free M KiB").
		// This is the hard floor that protects against the 0-KiB-free
		// pool race seen in 7.15 e2e.
		if minFreeKib > 0 && pool.FreeCapacity < minFreeKib {
			if pool.FreeCapacity > gates.maxFreeBelow {
				gates.maxFreeBelow = pool.FreeCapacity
			}

			continue
		}

		// Over-subscription gate (scenarios 7.W09 / 7.W10 / 7.W11):
		// even if the physical floor passes, the per-pool logical
		// budget set by MaxFreeCapacityOversubscriptionRatio /
		// MaxTotalCapacityOversubscriptionRatio / MaxOversubscriptionRatio
		// can be below the request — accepting such a pool would
		// produce a successful Resource create followed by an opaque
		// satellite failure when the volume is sized.
		if minFreeKib > 0 && oversubRejects(&pool, ctrlProps, minFreeKib, &gates) {
			continue
		}

		out = append(out, pool)
	}

	return out, gates
}

// oversubRejects evaluates the over-subscription gate for a single
// pool and, on rejection, updates the closest-miss witness on gates.
// Returns true to signal the caller to drop the pool.
//
// "Closest miss" = the pool with the LARGEST effective cap among
// rejections — that's the one operators are closest to unlocking by
// raising the named ratio.
func oversubRejects(pool *apiv1.StoragePool, ctrlProps map[string]string, minFreeKib int64, gates *candidateGates) bool {
	capKib, kind, ratioValue := effectiveOversubCaps(pool, ctrlProps)
	if capKib >= minFreeKib {
		return false
	}

	if gates.oversubBest == nil || capKib > gates.oversubBest.EffectiveCapKib {
		gates.oversubBest = &OversubscriptionShortfallError{
			RequiredKib:     minFreeKib,
			PoolName:        pool.StoragePoolName,
			NodeName:        pool.NodeName,
			Ratio:           kind,
			RatioValue:      ratioValue,
			EffectiveCapKib: capKib,
		}
	}

	return true
}

// weights bundles the four `Autoplacer/Weights/*` controller-scope
// multipliers consumed by the scoring path. Default (unset) is 1.0 for
// every weight so a cluster that never touches the knobs gets "all four
// strategies equally weighted", which is the UG9 default.
type weights struct {
	maxFreeSpace     float64
	minReservedSpace float64
	minRscCount      float64
	maxThroughput    float64
}

// loadWeights reads the controller-scope props bag and decodes the four
// scoring weights. Missing or unparseable keys default to 1.0 so the
// composite score remains well-defined for fresh clusters. A negative
// weight is clamped to 0 — operators that fat-finger a "-1" shouldn't
// invert the strategy.
func (p *Placer) loadWeights(ctx context.Context) (weights, error) {
	props, err := p.store.ControllerProps().Get(ctx)
	if err != nil {
		return weights{}, errors.Wrap(err, "get controller props")
	}

	return weights{
		maxFreeSpace:     parseWeight(props[apiv1.PropAutoplacerWeightMaxFreeSpace]),
		minReservedSpace: parseWeight(props[apiv1.PropAutoplacerWeightMinReservedSpace]),
		minRscCount:      parseWeight(props[apiv1.PropAutoplacerWeightMinRscCount]),
		maxThroughput:    parseWeight(props[apiv1.PropAutoplacerWeightMaxThroughput]),
	}, nil
}

// parseWeight returns the float value of raw, defaulting to 1.0 for the
// empty string and clamping negative values to 0. Unparseable inputs
// fall back to the default to keep a typo from breaking placement.
func parseWeight(raw string) float64 {
	if raw == "" {
		return 1.0
	}

	weight, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 1.0
	}

	if weight < 0 {
		return 0
	}

	return weight
}

// resourcesPerNode counts every Resource the cluster is hosting,
// grouped by NodeName. Feeds the MinRscCount strategy's per-pool score
// of 1/(1+numResourcesOnNode).
func (p *Placer) resourcesPerNode(ctx context.Context) (map[string]int, error) {
	all, err := p.store.Resources().List(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "list resources")
	}

	out := make(map[string]int, len(all))
	for i := range all {
		out[all[i].NodeName]++
	}

	return out, nil
}

// rankCandidates sorts pools in place by descending composite score,
// breaking ties on NodeName (stable, matches the legacy sort's
// secondary key so downstream tests pinning specific node orders stay
// green). The composite score is sum(weight_i * score_i) with each
// strategy scored as:
//   - MaxFreeSpace:     FreeCapacity / TotalCapacity, or 0 when total=0
//   - MinReservedSpace: 1 - reservedKib/totalKib (reserved from
//     pool.Props[Aux/blockstor.io/reserved-kib], 0 if absent)
//   - MinRscCount:      1 / (1 + numResourcesOnNode(pool.NodeName))
//   - MaxThroughput:    throughput / maxThroughputInSet (0 when nobody
//     advertises a hint, or when this pool's hint is 0)
func rankCandidates(pools []apiv1.StoragePool, w weights, rscPerNode map[string]int) {
	if len(pools) < 2 {
		return
	}

	maxThroughput := 0.0

	for i := range pools {
		t := throughputHint(&pools[i])
		if t > maxThroughput {
			maxThroughput = t
		}
	}

	scores := make([]float64, len(pools))
	for i := range pools {
		scores[i] = composite(&pools[i], w, rscPerNode, maxThroughput)
	}

	sort.SliceStable(pools, func(i, j int) bool {
		if scores[i] != scores[j] {
			return scores[i] > scores[j]
		}

		return pools[i].NodeName < pools[j].NodeName
	})
}

// composite is the single-pool weighted score. Each per-strategy score
// is in [0..1] (so the four weights are commensurable); the function
// short-circuits when a strategy has weight 0 to avoid e.g. a divide-
// by-zero in the throughput path when no pool advertises a hint.
func composite(pool *apiv1.StoragePool, w weights, rscPerNode map[string]int, maxThroughput float64) float64 {
	score := 0.0

	if w.maxFreeSpace > 0 && pool.TotalCapacity > 0 {
		score += w.maxFreeSpace * (float64(pool.FreeCapacity) / float64(pool.TotalCapacity))
	}

	if w.minReservedSpace > 0 && pool.TotalCapacity > 0 {
		reserved := min(reservedKib(pool), pool.TotalCapacity)
		score += w.minReservedSpace * (1.0 - float64(reserved)/float64(pool.TotalCapacity))
	}

	if w.minRscCount > 0 {
		score += w.minRscCount * (1.0 / float64(1+rscPerNode[pool.NodeName]))
	}

	if w.maxThroughput > 0 && maxThroughput > 0 {
		score += w.maxThroughput * (throughputHint(pool) / maxThroughput)
	}

	return score
}

// reservedKib reads the optional `Aux/blockstor.io/reserved-kib` pool
// prop. Missing/unparseable values score 0 — the MinReservedSpace
// strategy degrades to "no reservation reported", same as a literal 0.
func reservedKib(pool *apiv1.StoragePool) int64 {
	raw := pool.Props[apiv1.PropAuxPoolReservedKib]
	if raw == "" {
		return 0
	}

	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || v < 0 {
		return 0
	}

	return v
}

// throughputHint reads the optional `Autoplacer/MaxThroughput` pool
// prop (bytes/sec — scenario 6.W11, mirrors upstream LINSTOR's
// `Autoplacer/MaxThroughput` long). Missing/unparseable returns 0 —
// the MaxThroughput strategy treats it as "unknown" and the pool
// contributes 0 to that strategy.
//
// Unit is bytes/sec (integer), but we decode through float64 so the
// composite scorer can normalise without a second int→float cast.
// Negative values clamp to 0 — an operator typo must not invert the
// strategy.
func throughputHint(pool *apiv1.StoragePool) float64 {
	raw := pool.Props[apiv1.PropAutoplacerMaxThroughput]
	if raw == "" {
		return 0
	}

	v, err := strconv.ParseFloat(raw, 64)
	if err != nil || v < 0 {
		return 0
	}

	return v
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

	// Drop pools whose satellite has marked them as missing
	// (backing zpool / VG / FILE_THIN dir was destroyed out-of-band).
	// Without this gate the placer would happily land a replica on a
	// pool whose ZVOL create is guaranteed to fail, leaving DRBD slot
	// up on the healthy peer and resync stuck at 1%.
	if pool.PoolMissing {
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
