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

package store_test

import (
	"testing"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/store"
)

// LINSTOR parses PeerSlotsNewResource as a number and fails the creation
// on anything else, rather than create the definition with a count
// nobody asked for.
func TestResolvePeerSlotsRefusesAPropThatIsNotACount(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	rd := &apiv1.ResourceDefinition{Name: "pvc-a", Props: map[string]string{drbd.PeerSlotsNewResourceProp: "many"}}

	if err := store.ResolvePeerSlots(t.Context(), st, rd, 0); !errors.Is(err, drbd.ErrInvalidPeerSlots) {
		t.Errorf("got %v, want ErrInvalidPeerSlots", err)
	}
}

// A count the definition already carries, as a clone or a migration sets
// it, is kept; the request still wins over it.
func TestResolvePeerSlotsKeepsACountAlreadySet(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	rd := &apiv1.ResourceDefinition{Name: "pvc-a", PeerSlots: 12}

	if err := store.ResolvePeerSlots(t.Context(), st, rd, 0); err != nil || rd.PeerSlots != 12 {
		t.Errorf("got %d, %v; want 12 kept", rd.PeerSlots, err)
	}

	if err := store.ResolvePeerSlots(t.Context(), st, rd, 3); err != nil || rd.PeerSlots != 3 {
		t.Errorf("got %d, %v; want the request's 3", rd.PeerSlots, err)
	}
}

func TestReplicaPeerSlots(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct{ replica, definition, want int32 }{
		{replica: 7, definition: 12, want: 7},
		{definition: 12, want: 12},
		{want: 15},
	} {
		if got := store.ReplicaPeerSlots(tc.replica, tc.definition); got != int(tc.want) {
			t.Errorf("ReplicaPeerSlots(%d, %d) = %d, want %d", tc.replica, tc.definition, got, tc.want)
		}
	}
}
