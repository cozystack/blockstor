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

package v1

// GenericPropsModify is the upstream payload for any "modify properties"
// request. Set/delete pairs are mutually independent — they all run in one
// transaction.
type GenericPropsModify struct {
	OverrideProps   map[string]string `json:"override_props,omitempty"`
	DeleteProps     []string          `json:"delete_props,omitempty"`
	DeleteNamespace []string          `json:"delete_namespaces,omitempty"`
}

// KV is the upstream `KeyValueStore` view of a single instance — name plus
// its current property map.
type KV struct {
	Name  string            `json:"name"`
	Props map[string]string `json:"props,omitempty"`
}

// Autoplacer scoring-weight controller-scope property keys. Each one
// scales a [0..1] per-pool score the placer computes in
// candidatePools; the composite is the weighted sum. UG9 §"Storage
// pool placement" (lines 933-993) ships these as the operator-visible
// tuning knobs. Default (unset) value is 1.0 for every weight, so the
// composite degenerates to "all four strategies equally weighted" — a
// stable behaviour for clusters that never touch the knobs.
const (
	// PropAutoplacerWeightMaxFreeSpace scales the "FreeCapacity /
	// TotalCapacity" score (bigger ratio is better).
	PropAutoplacerWeightMaxFreeSpace = "Autoplacer/Weights/MaxFreeSpace"
	// PropAutoplacerWeightMinReservedSpace scales the
	// "1 - reservedKib/totalKib" score (less reserved is better).
	// Reserved is read from the pool's
	// `Aux/blockstor.io/reserved-kib` prop; missing = 0.
	PropAutoplacerWeightMinReservedSpace = "Autoplacer/Weights/MinReservedSpace"
	// PropAutoplacerWeightMinRscCount scales the
	// "1 / (1 + numResourcesOnNode)" score — pools on busier nodes
	// rank lower.
	PropAutoplacerWeightMinRscCount = "Autoplacer/Weights/MinRscCount"
	// PropAutoplacerWeightMaxThroughput scales the per-pool
	// `Autoplacer/MaxThroughput` advertised hint, normalised across
	// the candidate set. Pools without the hint contribute 0 to this
	// strategy.
	PropAutoplacerWeightMaxThroughput = "Autoplacer/Weights/MaxThroughput"

	// PropAuxPoolReservedKib is the optional pool-scope hint for
	// reserved (non-Free) capacity in KiB. Used by the
	// MinReservedSpace strategy when present.
	PropAuxPoolReservedKib = "Aux/blockstor.io/reserved-kib"
	// PropAutoplacerMaxThroughput is the per-StoragePool advertised
	// maximum throughput in bytes/sec (scenario 6.W11, mirrors
	// upstream LINSTOR's `Autoplacer/MaxThroughput`). Consumed by
	// the placer's MaxThroughput scoring strategy: the per-pool
	// score is hint / max(hint_in_candidate_set), so a pool that
	// advertises 2x the throughput of its peers scores 1.0 against
	// peers' 0.5.
	//
	// 6.W11 explicitly notes that the *enforcement* half — subtract
	// per-volume IO budget from the pool's running balance — depends
	// on QoS (wave1 7.22) which is out-of-scope. This key is the
	// SCORING half only.
	PropAutoplacerMaxThroughput = "Autoplacer/MaxThroughput"
)

// ControllerPropsName is the singleton row key for the controller-
// scope properties bag. Mirrors upstream LINSTOR's "Controller"
// pseudo-object that owns the `Autoplacer/Weights/*` knobs and any
// future cluster-wide tunables.
const ControllerPropsName = "default"

// Effective-prop scope identifiers. Match the upstream LINSTOR
// `(R)` marker hierarchy that python-linstor-client's
// `linstor rd lp --effective` walks — Controller → ResourceGroup →
// ResourceDefinition → Resource. The Python CLI compares the
// `scope` of each entry with the object the user asked about and
// prints `(R)` when the value was inherited from a parent.
const (
	EffectivePropScopeController         = "CTRL"
	EffectivePropScopeResourceGroup      = "RG"
	EffectivePropScopeResourceDefinition = "RD"
	EffectivePropScopeResource           = "RSC"
)

// EffectivePropEntry is one row of the merged-property bag exposed
// alongside the raw `props` map on RG / RD / Resource GET handlers.
// Value is the wire-effective value at the queried scope; Scope is
// the highest-precedence origin contributing that value (the parent
// the key was inherited from, or the queried scope itself when set
// locally). Python-linstor-client's `--effective` mode reads `scope`
// to decide whether to print the `(R)` inheritance marker next to
// the value.
type EffectivePropEntry struct {
	Value string `json:"value"`
	Scope string `json:"scope"`
}

// EffectiveProperties maps property key → resolved entry. Sibling
// to `props` on RG / RD / Resource responses — the raw map carries
// LOCAL settings, this map carries the merged-from-parents view.
type EffectiveProperties map[string]EffectivePropEntry

// PropBalanceResourcesEnabled is the controller / RG / RD-scope
// kill-switch for the additive rebalance reconciler. Mirrors upstream
// LINSTOR's `BalanceResourcesEnabled` knob — UG9 §"Automatically
// maintaining resource group placement count" (lines 885-907).
//
// Scenario 2.W02: when this prop resolves to "false" at controller
// scope, the RGRebalanceReconciler MUST short-circuit even with the
// `blockstor.io/rebalance-pending` annotation present — operators
// disable the periodic rescheduling for clusters that prefer manual
// placement decisions, and a stamped annotation from a stale REST
// modify shouldn't override that choice.
//
// Resolution hierarchy (most-specific wins): RD > RG > controller.
// The reconciler reads controller scope only for the first cut;
// RD/RG-scope kill-switches land alongside the placer integration in
// a follow-up.
const PropBalanceResourcesEnabled = "BalanceResourcesEnabled"

// PropBalanceResourcesInterval is the controller / RG / RD-scope
// periodic-tick interval (in minutes) for the additive rebalance
// reconciler. Scenario 2.15 — UG9 §"Automatically maintaining
// resource group placement count" describes the rebalance pass as a
// periodic background task rather than a purely event-driven one,
// so a freshly-vacated replica slot doesn't sit unfilled until the
// next unrelated RG / RD mutation wakes the reconciler.
//
// The reconciler returns `Result.RequeueAfter = Interval` on every
// pass — annotation-driven, no-op, and disabled alike — so the
// scheduled tick is the steady-state cadence. A positive value at any
// scope arms the tick; an unset / non-positive value at every scope
// falls back to `DefaultBalanceResourcesIntervalMinutes`.
//
// Resolution hierarchy (most-specific wins): RD > RG > controller >
// default. Mirrors PropBalanceResourcesEnabled / PropAutoplacerWeight*
// so operators tune one knob the same way across the BalanceResources
// family.
const PropBalanceResourcesInterval = "BalanceResourcesInterval"

// PropBalanceResourcesGracePeriod is the controller / RG / RD-scope
// "wait before churning replicas" window (in minutes) applied after a
// Satellite first observes Node `ConnectionStatus=OFFLINE`. Scenario
// 2.15 — gives a flapping / rebooting node a chance to return before
// the rebalance pass starts spawning replacement replicas on healthy
// peers.
//
// While at least one Node has been offline for < GracePeriod minutes,
// the reconciler skips the additive placer pass on the scheduled
// tick. The annotation-driven explicit path (operator just bumped
// PlaceCount via REST) is NOT gated — the grace window only suppresses
// the auto-rebalance, an operator-driven placement decision still
// flows through.
//
// Resolution hierarchy (most-specific wins): RD > RG > controller >
// default. The reconciler tracks the offline-since timestamp in-memory
// per node — a controller restart clears the window, which is the
// conservative behaviour (the freshly-started reconciler waits an
// additional GracePeriod before churning anything).
const PropBalanceResourcesGracePeriod = "BalanceResourcesGracePeriod"

// PropAllowMixingStoragePoolDriver is the cluster-scope override that
// opens the LVM_THIN ↔ ZFS_THIN cell in the same-provider-kind
// enforcement (Bug 76). Mirrors upstream LINSTOR's
// `linstor-common/consts.json` KEY_RSC_ALLOW_MIXING_DEVICE_KIND.
// Other cross-family pairs stay forbidden regardless of this flag.
// Preconditions: DRBD ≥ 9.2.7, LINSTOR ≥ 1.27.0. Wave2 scenario 6.W07.
const PropAllowMixingStoragePoolDriver = "AllowMixingStoragePoolDriver"

// DefaultBalanceResourcesIntervalMinutes is the fallback when no
// `BalanceResourcesInterval` prop is set at any scope. 5 minutes
// matches upstream LINSTOR's `Controller/Scheduler/Rebalance/Interval`
// default — short enough that a vacated slot gets refilled within a
// reasonable operator-attention window, long enough that the placer's
// list-scan cost stays negligible at the controller's typical RD count.
const DefaultBalanceResourcesIntervalMinutes = 5

// DefaultBalanceResourcesGracePeriodMinutes is the fallback when no
// `BalanceResourcesGracePeriod` prop is set at any scope. 60 minutes
// is the upstream-LINSTOR default — covers a typical node-reboot or
// short network blip without triggering replica churn. Operators on
// flakier networks bump this to several hours; operators on
// fast-failover clusters drop it to single-digit minutes.
const DefaultBalanceResourcesGracePeriodMinutes = 60
