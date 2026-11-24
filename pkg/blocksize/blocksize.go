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

// Package blocksize decides the DRBD block size a volume definition
// carries, the way LINSTOR does.
//
// LINSTOR keeps a volume's logical sector size on its volume definition,
// as DrbdOptions/Disk/block-size, and writes it itself: when a replica is
// created and the definition has none, it takes the minimum I/O size of
// the storage pool the replica lands in (the pool's physical block size,
// recorded on the pool as StorDriver/internal/minIoSize, clamped to
// 512..4096). A replica whose stack carries a layer other than DRBD and
// STORAGE counts as 4096 whatever its pool. A later replica on a pool with
// a larger size raises the value while no replica is Primary and the
// value is not frozen; the first time a replica of the definition becomes
// Primary, LINSTOR stamps Linstor/Drbd/freeze-block-size True and the
// value holds from then on. When the last replica goes, both properties
// are removed. All of that is switched off per volume by
// Linstor/Drbd/auto-block-size false.
//
// Without the stamp DRBD falls back to the backing device's logical
// sector size, 512 on a zvol, so a volume blockstor created came up with
// 512-byte sectors where LINSTOR gave the same volume 4096.
package blocksize

import (
	"slices"
	"strconv"
	"strings"
)

const (
	// PropBlockSize is the DRBD disk option LINSTOR keeps on the volume
	// definition.
	PropBlockSize = "DrbdOptions/Disk/block-size"

	// PropFreeze marks a block size that may no longer change.
	PropFreeze = "Linstor/Drbd/freeze-block-size"

	// PropAuto switches the automatic block size off when false.
	PropAuto = "Linstor/Drbd/auto-block-size"

	// PropPoolMinIoSize is where a storage pool records its minimum I/O
	// size. Pools adopted from LINSTOR carry it.
	PropPoolMinIoSize = "StorDriver/internal/minIoSize"

	// MinIoSize is the smallest block size LINSTOR writes.
	MinIoSize int64 = 512

	// MaxIoSize is the largest block size LINSTOR writes.
	MaxIoSize int64 = 4096

	// DefaultIoSize is what a pool without a recorded size counts as,
	// and what DRBD uses on a volume with no block-size.
	DefaultIoSize = MinIoSize

	// SpecialLayerIoSize is what a replica whose stack carries a layer
	// other than DRBD and STORAGE counts as.
	SpecialLayerIoSize = MaxIoSize

	// frozenValue is the value LINSTOR stamps on PropFreeze.
	frozenValue = "True"
)

// Clamp bounds a size to what LINSTOR writes.
func Clamp(size int64) int64 {
	return min(max(size, MinIoSize), MaxIoSize)
}

// Parse reads a size property; ok is false when it is absent or not a
// number, which LINSTOR treats the same way.
func Parse(value string) (int64, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, false
	}

	size, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, false
	}

	return Clamp(size), true
}

// Format renders a size the way it is stored.
func Format(size int64) string {
	return strconv.FormatInt(size, 10)
}

// PoolIoSize is the minimum I/O size a storage pool counts as, from its
// properties. A pool with none recorded (a diskless one, or one created
// before anything probed it) counts as DefaultIoSize.
func PoolIoSize(poolProps map[string]string) int64 {
	if size, ok := Parse(poolProps[PropPoolMinIoSize]); ok {
		return size
	}

	return DefaultIoSize
}

// HasSpecialLayers reports whether a layer stack carries a layer other
// than DRBD and STORAGE.
func HasSpecialLayers(stack []string) bool {
	for _, layer := range stack {
		if !strings.EqualFold(layer, "DRBD") && !strings.EqualFold(layer, "STORAGE") {
			return true
		}
	}

	return false
}

// parseBoolean is Java's Boolean.parseBoolean, which LINSTOR reads both
// switches with: true only for "true" in any case, everything else
// false.
func parseBoolean(value string) bool {
	return strings.EqualFold(value, "true")
}

// AutoEnabled resolves Linstor/Drbd/auto-block-size over a volume's
// property levels, closest first: volume definition, resource definition,
// volume group, resource group, controller. Unset is on; a value set is
// on only when it reads "true", so "yes" or "1" switch it off, as in
// LINSTOR.
func AutoEnabled(levels ...map[string]string) bool {
	for _, level := range levels {
		if value, ok := level[PropAuto]; ok {
			return parseBoolean(value)
		}
	}

	return true
}

// Frozen reports whether a volume definition's block size is frozen.
func Frozen(vdProps map[string]string) bool {
	return parseBoolean(vdProps[PropFreeze])
}

// Freeze stamps the freeze marker. It reports whether props changed.
func Freeze(vdProps map[string]string) bool {
	if Frozen(vdProps) {
		return false
	}

	vdProps[PropFreeze] = frozenValue

	return true
}

// Release removes both properties LINSTOR owns, which it does once the
// last replica of the definition is gone. It reports whether props
// changed.
func Release(vdProps map[string]string) bool {
	_, hasSize := vdProps[PropBlockSize]
	_, hasFreeze := vdProps[PropFreeze]

	delete(vdProps, PropBlockSize)
	delete(vdProps, PropFreeze)

	return hasSize || hasFreeze
}

// Verdict is what to do with one volume definition when a replica is
// created.
type Verdict int

const (
	// Keep leaves the volume definition as it is.
	Keep Verdict = iota

	// Set writes Decision.Size as the block size.
	Set

	// Incompatible is LINSTOR's refusal: the replica's pool needs a
	// larger block size than the definition has, and the size may not
	// change. LINSTOR refuses to create the replica; blockstor has the
	// replica already and keeps the definition's size.
	Incompatible
)

// Decision is the outcome for one volume definition.
type Decision struct {
	Verdict Verdict
	Size    int64
}

// Input is what the decision for one volume definition reads.
type Input struct {
	// VDProps are the volume definition's properties.
	VDProps map[string]string

	// Auto is the resolved auto-block-size switch for this volume.
	Auto bool

	// ReplicaSize is the size the new replica counts as: its pool's,
	// or SpecialLayerIoSize when its stack has a special layer.
	ReplicaSize int64

	// CanChange is whether the size may still change: no replica of
	// the definition is Primary, no volume definition is frozen, and
	// every volume definition has the automatic size on. With no other
	// replica it is whether every volume definition has it on.
	CanChange bool
}

// Decide applies LINSTOR's rule for a replica being created to one
// volume definition.
func Decide(in Input) Decision {
	if !in.Auto {
		return Decision{Verdict: Keep}
	}

	current, has := Parse(in.VDProps[PropBlockSize])

	if has && in.ReplicaSize <= current {
		return Decision{Verdict: Keep}
	}

	if !in.CanChange {
		return Decision{Verdict: Incompatible, Size: current}
	}

	return Decision{Verdict: Set, Size: in.ReplicaSize}
}

// CanChange is LINSTOR's CtrlRscStateHelper.canChangeMinIoSize, which the
// autoplacer asks: a definition without replicas may take any size; one
// with replicas only while every volume definition has the automatic size
// on, no replica is Primary, and no volume definition is frozen.
func CanChange(placed, auto, primary bool, vdProps []map[string]string) bool {
	if !placed {
		return true
	}

	if !auto || primary {
		return false
	}

	return !slices.ContainsFunc(vdProps, Frozen)
}

// Floor is LINSTOR's ResourceDefinition.getFloorVolumesMinIoSize: the
// smallest block size a placed definition's volumes run at, which a pool
// must not exceed once the size may no longer change. A definition with
// no replica has none; one with a special layer runs at
// SpecialLayerIoSize. With the automatic size on, a volume without a size
// counts as DefaultIoSize; with it off only sizes set count, and none set
// means no floor.
func Floor(placed, special, auto bool, vdProps []map[string]string) (int64, bool) {
	if !placed {
		return 0, false
	}

	if special {
		return SpecialLayerIoSize, true
	}

	floor, has := MaxIoSize, auto

	for _, props := range vdProps {
		size, ok := Parse(props[PropBlockSize])
		if !ok {
			if !auto {
				continue
			}

			size = DefaultIoSize
		}

		if !has || size < floor {
			floor, has = size, true
		}
	}

	if !has {
		return 0, false
	}

	return floor, true
}

// PoolFits is LINSTOR's autoplacer filter on block size
// (StorPoolFilter.filter, SelectionManager.isValid): while the size may
// no longer change, a pool whose minimum I/O size exceeds the
// definition's floor is not a candidate.
func PoolFits(poolSize int64, canChange bool, floor int64, hasFloor bool) bool {
	return canChange || !hasFloor || poolSize <= floor
}
