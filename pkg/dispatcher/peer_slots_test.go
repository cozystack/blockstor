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
// new replica the controller has not stamped yet gets its definition's,
// which is what the controller stamps; one whose definition has none
// either, or one that already had a node id before the count was
// recorded, gets the count its superblock holds once its satellite read
// it, and is left without until then, which the satellite reads as 15.
// A count changed on the definition does not reach a replica that has
// its own.
func TestReplicaPeerSlots(t *testing.T) {
	t.Parallel()

	ptr := func(v int32) *int32 { return &v }

	for name, tc := range map[string]struct {
		replica, definition *int32
		nodeID, metadata    *int32
		want                *int32
	}{
		"predates the record":                {definition: ptr(15), nodeID: ptr(0)},
		"predates the record, read":          {definition: ptr(15), nodeID: ptr(0), metadata: ptr(7), want: ptr(7)},
		"replica's own":                      {replica: ptr(7), definition: ptr(12), want: ptr(7)},
		"definition's before stamp":          {definition: ptr(12), want: ptr(12)},
		"neither":                            {},
		"replica's own, no definition count": {replica: ptr(15), want: ptr(15)},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			target := &blockstoriov1alpha1.Resource{}
			target.Spec.PeerSlots = tc.replica
			target.Spec.DRBDNodeID = tc.nodeID
			target.Status.MetadataPeerSlots = tc.metadata

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

// TestReadMetadataPeerSlots: the satellite is asked to read the count of
// the superblock of a diskful replica that predates the record and whose
// count nobody read yet, and of no other.
func TestReadMetadataPeerSlots(t *testing.T) {
	t.Parallel()

	ptr := func(v int32) *int32 { return &v }

	for name, tc := range map[string]struct {
		replica, nodeID, metadata *int32
		flags                     []string
		want                      bool
	}{
		"predates the record":       {nodeID: ptr(0), want: true},
		"read already":              {nodeID: ptr(0), metadata: ptr(7)},
		"recorded":                  {nodeID: ptr(0), replica: ptr(7)},
		"new":                       {},
		"diskless":                  {nodeID: ptr(0), flags: []string{"DISKLESS"}},
		"tiebreaker":                {nodeID: ptr(0), flags: []string{"DISKLESS", "TIE_BREAKER"}},
		"predates, has a disk flag": {nodeID: ptr(0), flags: []string{"INACTIVE"}, want: true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			target := &blockstoriov1alpha1.Resource{}
			target.Spec.PeerSlots = tc.replica
			target.Spec.DRBDNodeID = tc.nodeID
			target.Spec.Flags = tc.flags
			target.Status.MetadataPeerSlots = tc.metadata

			got := assembleDesired(target, nil, &blockstoriov1alpha1.ResourceDefinition{}, nil, map[string]string{}, nil).
				GetReadMetadataPeerSlots()
			if got != tc.want {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}
