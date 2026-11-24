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

package store

import (
	"context"
	"slices"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/naming"
)

// ResolvePeerSlots records on rd, a definition about to be created, the
// DRBD peer-slot count its replicas get, resolved the way LINSTOR
// resolves it when it creates the definition's DRBD layer: the count the
// request names (requested, zero for none), else the group's peer_slots
// (CtrlRscDfnApiCallHandler.createRscDfn), else PeerSlotsNewResource on
// the definition, its group and the controller in that order, else 7
// (RscDrbdLayerHelper.getPeerSlotsForNewResource). A count rd already
// carries, as a clone or a migration sets it, is kept.
//
// A group that cannot be read is skipped, as a missing one is: the
// definition-create paths check the group themselves. A
// PeerSlotsNewResource that is not a count fails the creation, as it
// does in LINSTOR.
func ResolvePeerSlots(ctx context.Context, st Store, rd *apiv1.ResourceDefinition, requested int32) error {
	if requested != 0 {
		err := drbd.ValidatePeerSlots(int64(requested))
		if err != nil {
			return err //nolint:wrapcheck // the message is LINSTOR's, for the caller to show
		}

		rd.PeerSlots = requested

		return nil
	}

	if rd.PeerSlots > 0 {
		return nil
	}

	var group *apiv1.ResourceGroup

	if rd.ResourceGroupName != "" {
		rg, err := st.ResourceGroups().Get(ctx, rd.ResourceGroupName)
		if err == nil {
			group = &rg
		} else if !errors.Is(err, ErrNotFound) {
			return errors.Wrapf(err, "read resource group %q for the peer slots of %q", rd.ResourceGroupName, rd.Name)
		}
	}

	if group != nil && group.PeerSlots > 0 {
		rd.PeerSlots = group.PeerSlots

		return nil
	}

	ctrl, err := st.ControllerProps().Get(ctx)
	if err != nil {
		return errors.Wrapf(err, "read controller properties for the peer slots of %q", rd.Name)
	}

	var groupProps map[string]string
	if group != nil {
		groupProps = group.Props
	}

	for _, props := range []map[string]string{rd.Props, groupProps, ctrl} {
		value, ok := props[drbd.PeerSlotsNewResourceProp]
		if !ok {
			continue
		}

		slots, err := drbd.ParsePeerSlotsProp(value)
		if err != nil {
			return errors.Wrapf(err, "resource definition %q", rd.Name)
		}

		rd.PeerSlots = slots

		return nil
	}

	rd.PeerSlots = drbd.DefaultPeerSlots

	return nil
}

// ErrInsufficientPeerSlots reports a replica with a disk its resource
// definition has no peer slot left for.
var ErrInsufficientPeerSlots = drbd.ErrInsufficientPeerSlots

// CheckPeerSlotsForNewPeer refuses a replica with a disk on node when a
// replica of the definition, the new one included, would be left with
// fewer peer slots than peers with a disk (drbd.CheckPeerSlotsForNewPeer).
// It runs before the replica is created, or before a diskless one on
// node turns diskful, as LINSTOR's resource create and toggle-disk do.
//
// A replica is counted with its recorded count, or with the definition's
// when the controller has not copied it on yet, the way the satellite
// reads it (ReplicaPeerSlots). A diskless replica on node that turns
// diskful keeps its own count; a replica not there yet gets the
// definition's.
// A definition that uses no DRBD keeps no bitmaps and is not checked.
func CheckPeerSlotsForNewPeer(ctx context.Context, st Store, rdName, node string) error {
	rd, err := st.ResourceDefinitions().Get(ctx, rdName)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil
		}

		return errors.Wrapf(err, "read resource definition %q for its peer slots", rdName)
	}

	if len(rd.LayerStack) > 0 && !slices.Contains(rd.LayerStack, apiv1.LayerKindDRBD) {
		return nil
	}

	replicas, err := st.Resources().ListByDefinition(ctx, rdName)
	if err != nil {
		return errors.Wrapf(err, "list the replicas of %q for their peer slots", rdName)
	}

	existing := make([]drbd.PeerSlotReplica, 0, len(replicas))

	// A replica that turns diskful, as on toggle-disk, already carries
	// the count its metadata will be created with; a new one gets the
	// definition's.
	newPeerSlots := ReplicaPeerSlots(0, rd.PeerSlots)

	for i := range replicas {
		replica := &replicas[i]
		slots := ReplicaPeerSlots(replica.PeerSlots, rd.PeerSlots)

		if naming.Same(replica.NodeName, node) {
			newPeerSlots = slots
		}

		existing = append(existing, drbd.PeerSlotReplica{
			Node:      replica.NodeName,
			Diskful:   !replicaIsDiskless(replica.Flags),
			PeerSlots: int(slots),
		})
	}

	err = drbd.CheckPeerSlotsForNewPeer(existing, node, int(newPeerSlots))
	if err != nil {
		return errors.Wrapf(err, "resource definition %q", rdName)
	}

	return nil
}

// ReplicaPeerSlots is the peer-slot count of a replica: its recorded
// count, else its definition's, which the controller copies onto it,
// else the 15 a replica of a definition without a count was created
// with. Zero stands for no count.
func ReplicaPeerSlots(replica, definition int32) int32 {
	switch {
	case replica > 0:
		return replica
	case definition > 0:
		return definition
	default:
		return drbd.LegacyPeerSlots
	}
}

// replicaIsDiskless reports a replica that keeps no disk: DISKLESS, or a
// TIE_BREAKER witness, which is diskless too.
func replicaIsDiskless(flags []string) bool {
	for _, flag := range flags {
		if flag == apiv1.ResourceFlagDiskless || flag == apiv1.ResourceFlagTieBreaker {
			return true
		}
	}

	return false
}
