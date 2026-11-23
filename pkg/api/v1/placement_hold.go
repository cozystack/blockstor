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

package v1

import (
	"strings"
	"time"
)

// PlacementInProgressUntil returns the latest deadline in the
// PlacementInProgressUntilAnnotation of annotations that is still
// ahead of now, and whether there is one: a placement of the definition
// is still writing its replicas until then.
func PlacementInProgressUntil(annotations map[string]string, now time.Time) (time.Time, bool) {
	var (
		latest time.Time
		found  bool
	)

	for stamp := range strings.FieldsSeq(annotations[PlacementInProgressUntilAnnotation]) {
		deadline, err := time.Parse(time.RFC3339, stamp)
		if err != nil || !deadline.After(now) {
			continue
		}

		if !found || deadline.After(latest) {
			latest, found = deadline, true
		}
	}

	return latest, found
}

// AddPlacementHold returns the PlacementInProgressUntilAnnotation value
// with stamp added to the holds in value that are still due at now.
// The holds of other placements stay; the expired ones, of placements
// that never got to remove theirs, go.
func AddPlacementHold(value, stamp string, now time.Time) string {
	return strings.Join(append(duePlacementHolds(value, "", now), stamp), " ")
}

// RemovePlacementHold returns the PlacementInProgressUntilAnnotation
// value without stamp and without the holds already expired at now,
// empty when no hold is left.
func RemovePlacementHold(value, stamp string, now time.Time) string {
	return strings.Join(duePlacementHolds(value, stamp, now), " ")
}

func duePlacementHolds(value, drop string, now time.Time) []string {
	var due []string

	for stamp := range strings.FieldsSeq(value) {
		if stamp == drop {
			continue
		}

		deadline, err := time.Parse(time.RFC3339, stamp)
		if err != nil || !deadline.After(now) {
			continue
		}

		due = append(due, stamp)
	}

	return due
}
