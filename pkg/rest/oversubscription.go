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
	"math"
	"strconv"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// LINSTOR's canonical property keys for the over-subscription gates.
// `MaxOversubscriptionRatio` is the umbrella default that both
// `MaxFreeCapacityOversubscriptionRatio` and
// `MaxTotalCapacityOversubscriptionRatio` fall back to when unset
// (upstream `FreeCapacityAutoPoolSelectorUtils.getRatioPrivileged`).
//
// Property precedence mirrors upstream's PriorityProps order:
// StoragePool.Props > ControllerProps. Storage-pool-definition props
// would sit in the middle, but blockstor doesn't carry a separate SPD
// object — pool-instance props is the highest layer that exists here,
// so a pool-level override always wins.
const (
	propMaxOversubscriptionRatio              = "MaxOversubscriptionRatio"
	propMaxFreeCapacityOversubscriptionRatio  = "MaxFreeCapacityOversubscriptionRatio"
	propMaxTotalCapacityOversubscriptionRatio = "MaxTotalCapacityOversubscriptionRatio"

	// defaultOversubscriptionRatio matches upstream LINSTOR's hard-coded
	// default for thin pools (see `linstor-common/properties.json` — the
	// `MaxOversubscriptionRatio` info text reads "default 20"). Applied
	// only to thin providers; raw LVM / ZFS get a ratio of 1.0 because
	// the backend allocates physically and can't oversubscribe.
	defaultOversubscriptionRatio = 20.0
)

// isThinProvider reports whether the pool's provider supports
// over-provisioning (volume sizes can sum above the backing
// device's usable bytes). Only thin providers honour the ratio
// caps; thick providers always cap MaxVolumeSize at FreeCapacity.
func isThinProvider(kind string) bool {
	switch kind {
	case apiv1.StoragePoolKindLVMThin,
		apiv1.StoragePoolKindZFSThin,
		apiv1.StoragePoolKindFileThin:
		return true
	}

	return false
}

// parseRatio reads a property value as a positive float64. Empty or
// non-numeric values return (0, false) so callers fall back to the
// next layer in the precedence chain. Negative / zero / NaN values
// are rejected too — a 0 ratio would set MaxVolumeSize to zero and
// silently block every spawn (very confusing operator UX).
func parseRatio(raw string) (float64, bool) {
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

// lookupRatio walks the precedence chain (pool props → controller
// props) for a given key. Returns (value, true) on the first hit,
// (0, false) if no layer carries it.
func lookupRatio(poolProps, ctrlProps map[string]string, key string) (float64, bool) {
	if v, ok := parseRatio(poolProps[key]); ok {
		return v, true
	}

	if v, ok := parseRatio(ctrlProps[key]); ok {
		return v, true
	}

	return 0, false
}

// effectiveOversubRatios resolves the (free-capacity, total-capacity)
// ratio pair for a pool, applying the upstream LINSTOR fallback rules:
//
//  1. Look up the specific key (MaxFree.../MaxTotal...). If present,
//     use it.
//  2. Otherwise fall back to MaxOversubscriptionRatio.
//  3. If MaxOversubscriptionRatio is unset, fall back to the hard-coded
//     default (20.0 for thin providers, 1.0 for thick).
//
// Returns 1.0 ratios for thick providers — they can't oversubscribe,
// and `free × 1.0` collapses the gate to "free space" semantics.
func effectiveOversubRatios(pool *apiv1.StoragePool, ctrlProps map[string]string) (float64, float64) {
	if !isThinProvider(pool.ProviderKind) {
		return 1.0, 1.0
	}

	overall, hasOverall := lookupRatio(pool.Props, ctrlProps, propMaxOversubscriptionRatio)
	if !hasOverall {
		overall = defaultOversubscriptionRatio
	}

	freeRatio, hasFree := lookupRatio(pool.Props, ctrlProps, propMaxFreeCapacityOversubscriptionRatio)
	if !hasFree {
		freeRatio = overall
	}

	totalRatio, hasTotal := lookupRatio(pool.Props, ctrlProps, propMaxTotalCapacityOversubscriptionRatio)
	if !hasTotal {
		totalRatio = overall
	}

	return freeRatio, totalRatio
}

// poolMaxVolumeKib computes the per-pool MaxVolumeSize cap in KiB,
// applying the over-subscription gates from upstream LINSTOR's
// `FreeCapacityAutoPoolSelectorUtils.getFreeCapacityCurrentEstimationPrivileged`:
//
//	candidate1 = FreeCapacity  * MaxFreeCapacityOversubscriptionRatio
//	candidate2 = TotalCapacity * MaxTotalCapacityOversubscriptionRatio
//	cap        = min(candidate1, candidate2)
//
// For thick providers (LVM, ZFS, FILE, DISKLESS) the ratios collapse
// to 1.0 and the cap reduces to FreeCapacity, matching the legacy
// pre-Phase-7 behaviour.
//
// The multiplication is in float64 to dodge int64 overflow at the
// boundary; we clamp to math.MaxInt64 before casting back.
func poolMaxVolumeKib(pool *apiv1.StoragePool, ctrlProps map[string]string) int64 {
	freeRatio, totalRatio := effectiveOversubRatios(pool, ctrlProps)

	freeCap := scaleClamp(pool.FreeCapacity, freeRatio)
	totalCap := scaleClamp(pool.TotalCapacity, totalRatio)

	// If TotalCapacity is zero (pool not yet reporting / synthetic
	// test fixture), don't let it crush the cap to 0 — fall back to
	// the free-space gate only.
	if pool.TotalCapacity == 0 {
		return freeCap
	}

	if freeCap < totalCap {
		return freeCap
	}

	return totalCap
}

// scaleClamp multiplies size×ratio safely. If the result would
// overflow int64 we clamp to math.MaxInt64 so the caller can keep
// the value as "effectively unlimited" without crashing the rollup.
func scaleClamp(size int64, ratio float64) int64 {
	if size <= 0 || ratio <= 0 {
		return 0
	}

	scaled := float64(size) * ratio
	if scaled >= float64(math.MaxInt64) {
		return math.MaxInt64
	}

	return int64(scaled)
}

// readCtrlPropsOrEmpty fetches the ControllerConfig.ExtraProps bag
// and folds every failure (no client wired, no CRD yet, transport
// error) into an empty map. The over-subscription gates run on
// every CreateVolume / spawn — a transient apiserver hiccup should
// degrade to "use upstream defaults", not blow up the path.
func (s *Server) readCtrlPropsOrEmpty(ctx context.Context) map[string]string {
	if s == nil || s.Client == nil {
		return map[string]string{}
	}

	props, err := readControllerProps(ctx, s.Client)
	if err != nil || props == nil {
		return map[string]string{}
	}

	return props
}
