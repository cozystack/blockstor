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

package placer

import (
	"math"
	"strconv"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// Property keys for the over-subscription gates, mirroring upstream
// LINSTOR. The placer-side copy is intentional: pkg/rest already
// owns a spawn-time gate (rest.poolMaxVolumeKib) and importing it
// would create a controller→rest cycle. Both layers MUST stay in
// lockstep — scenarios 7.W09 / 7.W10 / 7.W11 cross-check that
// raising any of these props lets a previously-rejected placement
// through.
//
// Precedence (PriorityProps order): StoragePool.Props > ControllerProps.
// Storage-pool-definition props would sit between them, but blockstor
// has no SPD object so a pool-level override always wins over controller.
const (
	propOversubMaster = "MaxOversubscriptionRatio"
	propOversubFree   = "MaxFreeCapacityOversubscriptionRatio"
	propOversubTotal  = "MaxTotalCapacityOversubscriptionRatio"

	// defaultOversubRatio matches upstream LINSTOR's hard-coded
	// default for thin pools (linstor-common properties.json reads
	// "default 20"). Thick providers can't oversubscribe so the
	// ratio collapses to 1.0 — see effectiveOversubCaps.
	defaultOversubRatio = 20.0
)

// effectiveOversubCaps returns the per-pool MaxVolumeSize budget (in
// KiB) implied by the three over-subscription ratio properties, plus
// the ratio kind and numeric value that PRODUCED the cap. The kind +
// value let the caller render an actionable error naming the exact
// property to tune (scenarios 7.W10 / 7.W11).
//
// Resolution rules — same as pkg/rest/oversubscription.go but copied
// here to keep the placer free of a controller→rest import:
//
//  1. Look up the specific key (MaxFree.../MaxTotal...). If present,
//     use it. Pool-level override wins; controller-level is the
//     fallback for the SAME prop.
//  2. Otherwise fall back to MaxOversubscriptionRatio (umbrella).
//  3. If MaxOversubscriptionRatio is unset, fall back to the hard-coded
//     default (20.0 for thin providers, 1.0 for thick).
//
// Returned cap = min(free × freeRatio, total × totalRatio). The kind
// names whichever of the two produced the tighter cap so operators
// see the exact knob to relax. When the two ratios tie, Free wins
// (it's the more commonly tuned property).
//
// For thick providers the cap is just FreeCapacity and the returned
// kind is OversubRatioFree with ratio=1.0 — there is no oversub in
// play, the report is informational so the error format stays uniform.
func effectiveOversubCaps(pool *apiv1.StoragePool, ctrlProps map[string]string) (int64, OversubRatioKind, float64) {
	if !isThinForOversub(pool.ProviderKind) {
		return pool.FreeCapacity, OversubRatioFree, 1.0
	}

	overall, hasOverall := lookupOversubRatio(pool.Props, ctrlProps, propOversubMaster)
	if !hasOverall {
		overall = defaultOversubRatio
	}

	freeRatio, hasFree := lookupOversubRatio(pool.Props, ctrlProps, propOversubFree)
	if !hasFree {
		freeRatio = overall
	}

	totalRatio, hasTotal := lookupOversubRatio(pool.Props, ctrlProps, propOversubTotal)
	if !hasTotal {
		totalRatio = overall
	}

	freeCap := scaleClampOversub(pool.FreeCapacity, freeRatio)
	totalCap := scaleClampOversub(pool.TotalCapacity, totalRatio)

	// If TotalCapacity is unreported (synthetic test fixtures, freshly-
	// seeded satellite) don't let it crush the cap to zero — fall back
	// to free-only and report the Free kind.
	if pool.TotalCapacity == 0 {
		return freeCap, attributeRatio(OversubRatioFree, hasFree, hasOverall), pickRatioValue(freeRatio, hasFree, hasOverall, overall)
	}

	// Pick the tighter cap; ties go to Free (more commonly tuned).
	if totalCap < freeCap {
		return totalCap, attributeRatio(OversubRatioTotal, hasTotal, hasOverall), pickRatioValue(totalRatio, hasTotal, hasOverall, overall)
	}

	return freeCap, attributeRatio(OversubRatioFree, hasFree, hasOverall), pickRatioValue(freeRatio, hasFree, hasOverall, overall)
}

// attributeRatio decides which Ratio name to surface in the error
// envelope. When the specific Free/Total property is unset AND the
// effective value came from the umbrella MaxOversubscriptionRatio,
// report OversubRatioMaster so operators know to tune the master
// backstop instead of the specific knob. Mirrors the wave2 7.W11
// "master backstop applies regardless of Free/Total split" contract.
func attributeRatio(specific OversubRatioKind, hasSpecific, hasOverall bool) OversubRatioKind {
	if !hasSpecific && hasOverall {
		return OversubRatioMaster
	}

	return specific
}

// pickRatioValue returns the effective ratio numeric for the error
// envelope. When the specific Free/Total prop is set we surface that
// value; when it falls through to the umbrella we surface the umbrella
// value (or the hard-coded default if neither is set).
func pickRatioValue(specificValue float64, hasSpecific, hasOverall bool, overallValue float64) float64 {
	if hasSpecific {
		return specificValue
	}

	if hasOverall {
		return overallValue
	}

	return defaultOversubRatio
}

// isThinForOversub reports whether a pool's provider kind supports
// over-provisioning. Only thin providers honour the ratio caps;
// thick providers (LVM/ZFS/FILE/DISKLESS) always cap MaxVolumeSize
// at FreeCapacity. The list mirrors pkg/rest/oversubscription.go's
// isThinProvider; both must change together if a new thin backend
// is added.
func isThinForOversub(kind string) bool {
	switch kind {
	case apiv1.StoragePoolKindLVMThin,
		apiv1.StoragePoolKindZFSThin,
		apiv1.StoragePoolKindFileThin:
		return true
	}

	return false
}

// parseOversubRatio reads a property value as a positive float64.
// Empty or non-numeric values return (0, false) so callers fall back
// to the next layer in the precedence chain. Negative / zero / NaN /
// Inf values are rejected too — a 0 ratio would set MaxVolumeSize
// to zero and silently block every placement (very confusing
// operator UX).
func parseOversubRatio(raw string) (float64, bool) {
	if raw == "" {
		return 0, false
	}

	ratio, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, false
	}

	if ratio <= 0 || math.IsNaN(ratio) || math.IsInf(ratio, 0) {
		return 0, false
	}

	return ratio, true
}

// lookupOversubRatio walks the precedence chain (pool props →
// controller props) for the given key. Returns (value, true) on the
// first hit, (0, false) if no layer carries it.
func lookupOversubRatio(poolProps, ctrlProps map[string]string, key string) (float64, bool) {
	if v, ok := parseOversubRatio(poolProps[key]); ok {
		return v, true
	}

	if v, ok := parseOversubRatio(ctrlProps[key]); ok {
		return v, true
	}

	return 0, false
}

// scaleClampOversub multiplies size×ratio safely. If the result
// would overflow int64 we clamp to math.MaxInt64 so the caller can
// keep the value as "effectively unlimited" without crashing the
// rollup. A zero or negative input collapses to 0 — there is no
// budget left to oversubscribe.
func scaleClampOversub(size int64, ratio float64) int64 {
	if size <= 0 || ratio <= 0 {
		return 0
	}

	scaled := float64(size) * ratio
	if scaled >= float64(math.MaxInt64) {
		return math.MaxInt64
	}

	return int64(scaled)
}
