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
// The same bound is on the CRD field, and both are wanted. The schema is what
// holds for `kubectl apply`, a GitOps controller, and blockstor's own
// controller rewriting the list; this one names the offending size in the
// LINSTOR-shaped error the client expects, and runs before the first write
// rather than part-way through a spawn.
//
// The schema bound only works because `spec.volumeDefinitions` is keyed on
// volumeNumber. On an atomic list Kubernetes correlates the slice as a whole
// for ratcheting, so one grandfathered sub-floor volume would reject every
// later write to that resource definition — including the controller's own,
// since it rewrites the list to stamp drbdMinor. Keyed, an untouched element
// is not re-validated.
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
