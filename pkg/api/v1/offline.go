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
	"slices"

	"github.com/cozystack/blockstor/pkg/naming"
)

// liveSatellites maps each node (by naming.Key) to the satellite
// process whose reports a read may show: none when the heartbeat
// watchdog has marked the node OFFLINE.
func liveSatellites(nodes []Node) map[string]liveSatellite {
	out := make(map[string]liveSatellite, len(nodes))

	for i := range nodes {
		out[naming.Key(nodes[i].Name)] = liveSatellite{
			offline:     nodes[i].ConnectionStatus == NodeTypeOffline,
			incarnation: nodes[i].SatelliteIncarnation,
		}
	}

	return out
}

type liveSatellite struct {
	offline     bool
	incarnation string
}

// reported tells whether the state stored on the replica comes from
// the satellite now running on its node.
//
// An incarnation missing on either side says nothing: a satellite
// older than the field does not stamp it, and a replica no satellite
// has reported to since carries nothing to forget.
func (l liveSatellite) reported(r *Resource) bool {
	if l.offline {
		return false
	}

	return l.incarnation == "" || r.SatelliteIncarnation == "" || r.SatelliteIncarnation == l.incarnation
}

// ForgetSatelliteState drops from a replica everything LINSTOR answers
// from the satellite's live state: the role, every volume's disk and
// replication state, and the DRBD layer's peer links.
//
// The layer volumes' own disk_state stays. LINSTOR fills it from the
// layer data the controller merged from the satellite's last apply
// (CtrlRscLayerDataMerger) and keeps that when the satellite goes
// offline, so its answer still carries it.
//
// The store keeps the satellite's last report after the satellite
// goes away, so a replica on a dead node would keep reading UpToDate
// and connected. LINSTOR forgets a satellite's state when the
// connection to it closes and answers that node's replicas with no
// state at all, which linstor-client shows as a yellow Unknown that
// `--faulty` lists. This gives the same answer. The stored report
// itself is left alone; only the copy being answered changes.
//
// The slices and layers it changes are copied first, since a store
// may hand back data it still holds.
func ForgetSatelliteState(r *Resource) {
	r.State.InUse = nil
	r.State.DrbdState = ""
	r.Volumes = forgetVolumeStates(r.Volumes)
	r.LayerObject = forgetLayerState(r.LayerObject)
}

// ForgetStaleSatelliteState applies ForgetSatelliteState to every
// replica whose stored state the satellite now running on its node did
// not report: the node is OFFLINE, or the report came from an earlier
// satellite process.
//
// LINSTOR shows a satellite's state only from the connection it holds
// to it now. Once the node is back ONLINE, a rebooted satellite brings
// its replicas up one by one, minutes apart on a busy node, and until
// it reports one the store still holds the report from before the
// crash. Showing that would read a replica that is not even up yet as
// UpToDate and connected.
//
// A restart of the satellite alone hides the node's replicas too, as
// LINSTOR shows them Unknown until the reconnected satellite has sent
// their state. The kernel still holds them then, so the new satellite
// writes them all right after the initial events2 state, and that
// lasts seconds.
//
// So does a replica whose DRBD resource the satellite saw destroyed
// while the node stays up (`drbdsetup down`, a failed bring-up): the
// LINSTOR satellite closes that resource's state stream on the destroy
// event (DrbdEventPublisher.resourceDestroyed) and the controller then
// answers it with no state. The satellite here stores the departure as
// a cleared status that still reads Secondary, since the stored role
// cannot be unset, so a diskful replica that held no data and no peer
// link read Unused, and `?faulty=true` passed over it.
func ForgetStaleSatelliteState(resources []Resource, nodes []Node) {
	live := liveSatellites(nodes)

	for i := range resources {
		sat, ok := live[naming.Key(resources[i].NodeName)]
		if resources[i].KernelSlotDestroyed || (ok && !sat.reported(&resources[i])) {
			ForgetSatelliteState(&resources[i])
		}
	}
}

func forgetVolumeStates(vols []Volume) []Volume {
	if vols == nil {
		return nil
	}

	out := slices.Clone(vols)
	for i := range out {
		out[i].State = VolumeState{}
	}

	return out
}

func forgetLayerState(layer *ResourceLayer) *ResourceLayer {
	if layer == nil {
		return nil
	}

	out := *layer

	if len(layer.Children) > 0 {
		out.Children = make([]ResourceLayer, len(layer.Children))
		for i := range layer.Children {
			out.Children[i] = *forgetLayerState(&layer.Children[i])
		}
	}

	if layer.Drbd != nil {
		drbd := *layer.Drbd
		drbd.Connections = nil
		out.Drbd = &drbd
	}

	return &out
}
