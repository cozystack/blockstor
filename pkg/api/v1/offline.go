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

// OfflineNodes returns the keys (naming.Key) of the nodes whose
// satellite the heartbeat watchdog has marked OFFLINE.
func OfflineNodes(nodes []Node) map[string]struct{} {
	out := make(map[string]struct{})

	for i := range nodes {
		if nodes[i].ConnectionStatus == NodeTypeOffline {
			out[naming.Key(nodes[i].Name)] = struct{}{}
		}
	}

	return out
}

// OnOfflineNode reports whether the replica lives on one of the nodes
// OfflineNodes returned.
func OnOfflineNode(r *Resource, offline map[string]struct{}) bool {
	_, ok := offline[naming.Key(r.NodeName)]

	return ok
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

// ForgetStateOnOfflineNodes applies ForgetSatelliteState to every
// replica on a node that is OFFLINE.
func ForgetStateOnOfflineNodes(resources []Resource, nodes []Node) {
	offline := OfflineNodes(nodes)
	if len(offline) == 0 {
		return
	}

	for i := range resources {
		if OnOfflineNode(&resources[i], offline) {
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
