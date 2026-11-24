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

package drbd_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"

	"github.com/cozystack/blockstor/pkg/drbd"
)

// A definition or replica blockstor created before it recorded a count
// was created with `create-md --max-peers=15`; reading a missing count as
// LINSTOR's 7 would size and create its metadata for slots it does not
// have.
func TestEffectivePeerSlotsReadsAMissingCountAsFifteen(t *testing.T) {
	t.Parallel()

	if got := drbd.EffectivePeerSlots(nil); got != 15 {
		t.Errorf("missing count: got %d, want 15", got)
	}

	seven := int32(7)
	if got := drbd.EffectivePeerSlots(&seven); got != 7 {
		t.Errorf("recorded 7: got %d, want 7", got)
	}
}

func TestValidatePeerSlotsTakesWhatASuperblockHolds(t *testing.T) {
	t.Parallel()

	for n := int64(1); n <= 31; n++ {
		if err := drbd.ValidatePeerSlots(n); err != nil {
			t.Errorf("%d: %v", n, err)
		}
	}

	for _, n := range []int64{0, -1, 32, 100} {
		if err := drbd.ValidatePeerSlots(n); !errors.Is(err, drbd.ErrInvalidPeerSlots) {
			t.Errorf("%d: got %v, want ErrInvalidPeerSlots", n, err)
		}
	}

	err := drbd.ValidatePeerSlots(32)
	if err == nil || !strings.Contains(err.Error(), "given peer_slots is larger then allowed: 32. Allowed: 31") {
		t.Errorf("32: got %v, want LINSTOR's words", err)
	}
}

func TestParsePeerSlotsProp(t *testing.T) {
	t.Parallel()

	if got, err := drbd.ParsePeerSlotsProp(" 12 "); err != nil || got != 12 {
		t.Errorf("12: got %d, %v", got, err)
	}

	for _, value := range []string{"", "seven", "0", "32", "7.5"} {
		if _, err := drbd.ParsePeerSlotsProp(value); !errors.Is(err, drbd.ErrInvalidPeerSlots) {
			t.Errorf("%q: got %v, want ErrInvalidPeerSlots", value, err)
		}
	}
}

func replicas(slots int, diskful int, diskless int) []drbd.PeerSlotReplica {
	var out []drbd.PeerSlotReplica

	for i := range diskful {
		out = append(out, drbd.PeerSlotReplica{Node: fmt.Sprintf("d%02d", i), Diskful: true, PeerSlots: slots})
	}

	for i := range diskless {
		out = append(out, drbd.PeerSlotReplica{Node: fmt.Sprintf("c%02d", i), PeerSlots: slots})
	}

	return out
}

// Seven peer slots hold eight replicas with a disk: the eighth joins seven
// peers, the ninth is refused, as LINSTOR refuses it.
func TestCheckPeerSlotsForNewPeerAllowsOneMoreThanTheSlots(t *testing.T) {
	t.Parallel()

	if err := drbd.CheckPeerSlotsForNewPeer(replicas(7, 7, 0), "new", 7); err != nil {
		t.Errorf("eighth replica: %v", err)
	}

	err := drbd.CheckPeerSlotsForNewPeer(replicas(7, 8, 0), "new", 7)
	if !errors.Is(err, drbd.ErrInsufficientPeerSlots) {
		t.Fatalf("ninth replica: got %v, want ErrInsufficientPeerSlots", err)
	}

	if !strings.Contains(err.Error(), "insufficient peer slots to add another peer") ||
		!strings.Contains(err.Error(), "d00 (7)") || !strings.Contains(err.Error(), "8 needed") {
		t.Errorf("ninth replica: %v does not name the replicas short of slots", err)
	}
}

// Diskless replicas need no bitmap, so they do not count against the
// slots; their own slots are checked like any replica's.
func TestCheckPeerSlotsForNewPeerCountsOnlyReplicasWithADisk(t *testing.T) {
	t.Parallel()

	if err := drbd.CheckPeerSlotsForNewPeer(replicas(2, 2, 10), "new", 2); err != nil {
		t.Errorf("third diskful beside ten diskless: %v", err)
	}

	short := append(replicas(7, 3, 0), drbd.PeerSlotReplica{Node: "client", PeerSlots: 2})
	if err := drbd.CheckPeerSlotsForNewPeer(short, "new", 7); !errors.Is(err, drbd.ErrInsufficientPeerSlots) {
		t.Errorf("diskless replica with 2 slots and 3 diskful peers: got %v", err)
	}
}

// The new replica is held to the same bound: a definition whose count was
// lowered must not create a replica DRBD cannot attach.
func TestCheckPeerSlotsForNewPeerChecksTheNewReplica(t *testing.T) {
	t.Parallel()

	err := drbd.CheckPeerSlotsForNewPeer(replicas(7, 3, 0), "new", 2)
	if !errors.Is(err, drbd.ErrInsufficientPeerSlots) || !strings.Contains(err.Error(), "new (2)") {
		t.Errorf("new replica with 2 slots and 3 peers: got %v", err)
	}
}

// A replica turning diskful on its own node is not one of its own peers.
func TestCheckPeerSlotsForNewPeerSkipsTheReplicaBeingAdded(t *testing.T) {
	t.Parallel()

	existing := append(replicas(1, 1, 0), drbd.PeerSlotReplica{Node: "new", Diskful: true, PeerSlots: 1})
	if err := drbd.CheckPeerSlotsForNewPeer(existing, "new", 1); err != nil {
		t.Errorf("second replica of a one-slot definition: %v", err)
	}
}

// `rd modify --peer-slots` keeps one slot for a replica still to come and
// counts the replicas already there, as LINSTOR's checkPeerSlotCount does.
func TestCheckPeerSlotCount(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		slots, diskful int
		ok             bool
	}{
		{slots: 7, diskful: 3, ok: true},
		{slots: 1, diskful: 3, ok: true},
		{slots: 1, diskful: 4, ok: false},
		{slots: 2, diskful: 5, ok: false},
		{slots: 3, diskful: 5, ok: true},
	} {
		err := drbd.CheckPeerSlotCount(tc.slots, tc.diskful)
		if (err == nil) != tc.ok {
			t.Errorf("%d slots, %d diskful: got %v, want ok=%v", tc.slots, tc.diskful, err, tc.ok)
		}
	}
}
