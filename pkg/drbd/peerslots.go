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

package drbd

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
)

// A replica keeps one bitmap per peer slot in its metadata, and a peer
// with a disk takes one of them, so the peer slots bound how many
// replicas with a disk a resource can have: one more than the slots.
// LINSTOR keeps the count on the definition's DRBD layer, resolves it
// once when that layer is created, and copies it to every replica it
// creates (RscDrbdLayerHelper).
const (
	// DefaultPeerSlots is the count LINSTOR gives a definition when
	// neither the request nor PeerSlotsNewResource names one
	// (InternalApiConsts.DEFAULT_PEER_SLOTS).
	DefaultPeerSlots = 7

	// LegacyPeerSlots is the count blockstor created metadata with
	// before definitions carried one: every replica was created with
	// `create-md --max-peers=15`. A definition or replica without a
	// count was created that way, so it is read as this.
	LegacyPeerSlots = 15

	// MaxPeerSlots is the most a v09 superblock holds, and the most
	// LINSTOR accepts (MetaData.DRBD_MAX_PEERS).
	MaxPeerSlots = MetadataMaxPeerSlots

	// PeerSlotsNewResourceProp is the property LINSTOR reads the count
	// of a new definition from, on the definition, its group and the
	// controller in that order (ApiConsts.KEY_PEER_SLOTS_NEW_RESOURCE).
	PeerSlotsNewResourceProp = "PeerSlotsNewResource"
)

// ErrInsufficientPeerSlots reports a replica with a disk that its
// resource has no peer slot left for. LINSTOR refuses it with
// FAIL_INSUFFICIENT_PEER_SLOTS rather than let DRBD fail on it.
var ErrInsufficientPeerSlots = errors.New("insufficient peer slots")

// ErrInvalidPeerSlots reports a peer-slot count outside 1..MaxPeerSlots.
var ErrInvalidPeerSlots = errors.New("invalid peer slots")

// EffectivePeerSlots is the count a stored value stands for: the value
// itself, or LegacyPeerSlots for one never recorded.
func EffectivePeerSlots(stored *int32) int32 {
	if stored == nil || *stored <= 0 {
		return LegacyPeerSlots
	}

	return *stored
}

// ValidatePeerSlots refuses a count no superblock can carry. LINSTOR
// refuses more than MaxPeerSlots with the same words
// (ResourceGroups.parsePeerSlots); a count below one would have
// create-md fail on every replica.
func ValidatePeerSlots(n int64) error {
	if n > MaxPeerSlots {
		return errors.Wrapf(ErrInvalidPeerSlots,
			"given peer_slots is larger then allowed: %d. Allowed: %d", n, MaxPeerSlots)
	}

	if n < 1 {
		return errors.Wrapf(ErrInvalidPeerSlots, "peer_slots must be at least 1. Received %d", n)
	}

	return nil
}

// PeerSlotCount returns n as a peer-slot count, false for a count no
// superblock holds.
func PeerSlotCount(n int) (int32, bool) {
	if n < 1 || n > MaxPeerSlots {
		return 0, false
	}

	return int32(n), true
}

// ParsePeerSlotsProp reads a PeerSlotsNewResource value. LINSTOR parses
// it as a number and fails the creation on anything else.
func ParsePeerSlotsProp(value string) (int32, error) {
	n, err := strconv.ParseInt(strings.TrimSpace(value), 10, 32)
	if err != nil {
		return 0, errors.Wrapf(ErrInvalidPeerSlots, "%s %q is not a number", PeerSlotsNewResourceProp, value)
	}

	err = ValidatePeerSlots(n)
	if err != nil {
		return 0, errors.Wrap(err, PeerSlotsNewResourceProp)
	}

	return int32(n), nil
}

// PeerSlotReplica is what the peer-slot checks need to know of one
// replica of a resource.
type PeerSlotReplica struct {
	Node      string
	Diskful   bool
	PeerSlots int
}

// CheckPeerSlotsForNewPeer refuses one more replica with a disk when a
// replica of the resource, the new one included, would have fewer peer
// slots than the peers with a disk it would then have.
//
// LINSTOR checks the replicas already there the same way before it
// creates a replica with a disk (CtrlRscCrtApiHelper.
// checkPeerSlotsForNewPeer): with n of them having a disk, each needs n
// slots once the new one joins. It checks the new replica itself only
// loosely (RscDrbdLayerHelper.checkPeerSlotCount), which lets a
// definition whose count was lowered create a replica DRBD then cannot
// attach; here the new replica is held to the same bound.
func CheckPeerSlotsForNewPeer(existing []PeerSlotReplica, newNode string, newPeerSlots int) error {
	diskful := 0

	for _, replica := range existing {
		if replica.Diskful && replica.Node != newNode {
			diskful++
		}
	}

	var short []string

	for _, replica := range existing {
		if replica.Node != newNode && replica.PeerSlots < diskful {
			short = append(short, fmt.Sprintf("%s (%d)", replica.Node, replica.PeerSlots))
		}
	}

	if newPeerSlots < diskful {
		short = append(short, fmt.Sprintf("%s (%d)", newNode, newPeerSlots))
	}

	if len(short) == 0 {
		return nil
	}

	sort.Strings(short)

	return errors.Wrapf(ErrInsufficientPeerSlots,
		"resource on node %s has insufficient peer slots to add another peer: %d needed",
		strings.Join(short, ", "), diskful)
}

// CheckPeerSlotCount refuses a new count for a definition that already
// has more replicas with a disk than the count leaves room for, keeping
// one slot for a replica still to come, as LINSTOR does on `rd modify
// --peer-slots` (RscDrbdLayerHelper.checkPeerSlotCount with the default
// reserve of one).
func CheckPeerSlotCount(peerSlots, diskful int) error {
	const reserved = 1

	if peerSlots+reserved >= diskful-1 {
		return nil
	}

	return errors.Wrapf(ErrInsufficientPeerSlots,
		"Peerslot count '%d' is too low since we already have '%d' diskful resources and we still need '%d' for new resources",
		peerSlots, diskful, reserved)
}
