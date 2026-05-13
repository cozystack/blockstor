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
	// `Aux/throughput-mbps` advertised hint, normalised across the
	// candidate set. Pools without the hint contribute 0 to this
	// strategy.
	PropAutoplacerWeightMaxThroughput = "Autoplacer/Weights/MaxThroughput"

	// PropAuxPoolReservedKib is the optional pool-scope hint for
	// reserved (non-Free) capacity in KiB. Used by the
	// MinReservedSpace strategy when present.
	PropAuxPoolReservedKib = "Aux/blockstor.io/reserved-kib"
	// PropAuxPoolThroughputMBps is the optional pool-scope advertised
	// throughput hint in MB/s. Used by the MaxThroughput strategy
	// when present.
	PropAuxPoolThroughputMBps = "Aux/throughput-mbps"
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
