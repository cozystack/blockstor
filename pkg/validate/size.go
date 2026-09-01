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

package validate

import (
	"errors"
	"fmt"
)

var (
	// ErrVolumeSizeBelowMinimum refuses a volume smaller than DRBD can carry.
	ErrVolumeSizeBelowMinimum = errors.New("volume size below minimum")

	// ErrVolumeSizeAboveMaximum refuses a volume beyond DRBD's per-device
	// ceiling.
	ErrVolumeSizeAboveMaximum = errors.New("volume size above maximum")
)

const (
	// MinVolumeSizeKib is DRBD's practical floor. It reserves roughly 32 KiB
	// of metadata per peer and the backing layers add alignment on top, so a
	// volume below this never becomes functional: the satellite loops on
	// create-md instead of failing outright.
	MinVolumeSizeKib int64 = 4 * 1024

	// MaxVolumeSizeKib is DRBD 9's documented per-device ceiling, 1 PiB.
	//
	// It was 16 TiB, which is below what both DRBD 9 and upstream LINSTOR
	// handle. That mattered beyond the odd large volume: linstormigrate
	// copies sizes verbatim, so a LINSTOR cluster holding anything larger
	// failed at apply time part-way through its own migration.
	MaxVolumeSizeKib int64 = 1024 * 1024 * 1024 * 1024
)

// VolumeSizeKib holds a requested volume size inside the range DRBD can
// actually serve.
//
// This lives here, and not as a CRD bound, on purpose. A bound in the schema
// looks stronger — it would hold for `kubectl apply` too — but
// `spec.volumeDefinitions` carries no list-map key, so it is an atomic list
// and Kubernetes correlates it as a whole for ratcheting. Any update touching
// the list re-validates every element, including ones written before the
// bound existed. A cluster carrying a single grandfathered sub-floor volume
// would then reject every subsequent write to that resource definition —
// including the controller's own, since it rewrites the list to stamp
// drbdMinor. The bound belongs where new sizes enter instead.
func VolumeSizeKib(sizeKib int64) error {
	if sizeKib < MinVolumeSizeKib {
		return fmt.Errorf(
			"%w: size_kib=%d below minimum %d KiB (DRBD reserves ~32 KiB of "+
				"metadata per peer; backing layers add alignment on top)",
			ErrVolumeSizeBelowMinimum, sizeKib, MinVolumeSizeKib)
	}

	if sizeKib > MaxVolumeSizeKib {
		return fmt.Errorf(
			"%w: size_kib=%d above maximum %d KiB (DRBD's per-device ceiling)",
			ErrVolumeSizeAboveMaximum, sizeKib, MaxVolumeSizeKib)
	}

	return nil
}
