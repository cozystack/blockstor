// SPDX-License-Identifier: Apache-2.0

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
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// Corner-case J1: over-subscription ratio resolution + MaxVolumeSize
// cap. Pins the worked examples from UG9 §"Over provisioning storage in
// LINSTOR" (~3435-3606) against poolMaxVolumeKib:
//
//   - Each ratio defaults to 20.
//   - MaxOversubscriptionRatio (master) substitutes ONLY for the two
//     unset specific ratios (Free / Total).
//   - A storage-pool-level prop beats a controller-level prop.
//   - Placement cap = min(total × totalRatio, free × freeRatio).
//   - rg query-size-info's MaxVolumeSize uses the same poolMaxVolumeKib.
//
// FILE_THIN is the thin provider used here (matches the stand's `stand`
// pool) so the ratios are actually honoured (thick providers collapse
// to free-only).

const (
	tenGiBKib = int64(10 * 1024 * 1024) // 10 GiB in KiB
)

func thinPool(props map[string]string) *apiv1.StoragePool {
	return &apiv1.StoragePool{
		NodeName:        "n1",
		StoragePoolName: "thin",
		ProviderKind:    apiv1.StoragePoolKindFileThin,
		FreeCapacity:    tenGiBKib,
		TotalCapacity:   tenGiBKib,
		Props:           props,
	}
}

// TestJ1DefaultRatioIs20: with no ratio props set, a 10 GiB FILE_THIN
// pool caps MaxVolumeSize at 200 GiB (10 × default 20).
func TestJ1DefaultRatioIs20(t *testing.T) {
	t.Parallel()

	got := poolMaxVolumeKib(thinPool(nil), nil)
	want := tenGiBKib * 20

	if got != want {
		t.Errorf("default-ratio cap: got %d KiB, want %d KiB (10 GiB × 20)", got, want)
	}
}

// TestJ1MasterSubstitutesForUnsetSpecifics: UG9 worked example —
// master=4, total=3, free unset. The placement cap uses
// min(total×3, free×4) = min(30, 40) GiB = 30 GiB. The master backstop
// (4) fills in for the unset Free ratio.
func TestJ1MasterSubstitutesForUnsetSpecifics(t *testing.T) {
	t.Parallel()

	got := poolMaxVolumeKib(thinPool(map[string]string{
		"MaxOversubscriptionRatio":              "4",
		"MaxTotalCapacityOversubscriptionRatio": "3",
	}), nil)
	want := tenGiBKib * 3 // total×3 = 30 GiB is the tighter cap

	if got != want {
		t.Errorf("master-backstop cap: got %d KiB, want %d KiB (min(total×3, free×4))", got, want)
	}
}

// TestJ1PoolBeatsController: a pool-level MaxOversubscriptionRatio (5)
// overrides a controller-level one (20) for that pool.
func TestJ1PoolBeatsController(t *testing.T) {
	t.Parallel()

	ctrlProps := map[string]string{"MaxOversubscriptionRatio": "20"}
	got := poolMaxVolumeKib(thinPool(map[string]string{
		"MaxOversubscriptionRatio": "5",
	}), ctrlProps)
	want := tenGiBKib * 5

	if got != want {
		t.Errorf("pool-over-controller cap: got %d KiB, want %d KiB (pool ratio 5 wins)", got, want)
	}
}

// TestJ1ControllerUsedWhenPoolUnset: with no pool-level prop, the
// controller-level MaxOversubscriptionRatio (5) applies.
func TestJ1ControllerUsedWhenPoolUnset(t *testing.T) {
	t.Parallel()

	ctrlProps := map[string]string{"MaxOversubscriptionRatio": "5"}
	got := poolMaxVolumeKib(thinPool(nil), ctrlProps)
	want := tenGiBKib * 5

	if got != want {
		t.Errorf("controller-fallback cap: got %d KiB, want %d KiB (controller ratio 5)", got, want)
	}
}

// TestJ1MinOfTwoRatios: both specifics set — placement takes the
// SMALLER of (total × totalRatio, free × freeRatio). free=6 GiB,
// total=10 GiB; totalRatio=2 → 20 GiB, freeRatio=10 → 60 GiB; min is
// the total cap, 20 GiB.
func TestJ1MinOfTwoRatios(t *testing.T) {
	t.Parallel()

	pool := thinPool(map[string]string{
		"MaxFreeCapacityOversubscriptionRatio":  "10",
		"MaxTotalCapacityOversubscriptionRatio": "2",
	})
	pool.FreeCapacity = 6 * 1024 * 1024 // 6 GiB free

	got := poolMaxVolumeKib(pool, nil)
	want := tenGiBKib * 2 // total(10 GiB)×2 = 20 GiB is tighter than free(6 GiB)×10 = 60 GiB

	if got != want {
		t.Errorf("min-of-two cap: got %d KiB, want %d KiB (min(total×2, free×10))", got, want)
	}
}
