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

package dispatcher

import (
	"testing"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// TestReplicaPeerSlots: the satellite gets the replica's own count; a
// replica the controller has not stamped yet gets its definition's, which
// is what the controller stamps; one whose definition has none either is
// left without, which the satellite reads as the 15 it was created with.
// A count changed on the definition does not reach a replica that has
// its own.
func TestReplicaPeerSlots(t *testing.T) {
	t.Parallel()

	ptr := func(v int32) *int32 { return &v }

	for name, tc := range map[string]struct {
		replica, definition *int32
		want                *int32
	}{
		"replica's own":                      {replica: ptr(7), definition: ptr(12), want: ptr(7)},
		"definition's before stamp":          {definition: ptr(12), want: ptr(12)},
		"neither":                            {},
		"replica's own, no definition count": {replica: ptr(15), want: ptr(15)},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			target := &blockstoriov1alpha1.Resource{}
			target.Spec.PeerSlots = tc.replica

			rd := &blockstoriov1alpha1.ResourceDefinition{}
			rd.Spec.PeerSlots = tc.definition

			got := assembleDesired(target, nil, rd, nil, map[string]string{}, nil).GetPeerSlots()

			switch {
			case tc.want == nil && got != nil:
				t.Errorf("got %d, want none", *got)
			case tc.want != nil && (got == nil || *got != *tc.want):
				t.Errorf("got %v, want %d", got, *tc.want)
			}

			if got != nil && got == tc.replica {
				t.Errorf("the payload aliases the replica's spec")
			}
		})
	}
}
