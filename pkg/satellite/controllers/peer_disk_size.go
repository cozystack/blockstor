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

package controllers

import (
	"fmt"
	"slices"
	"strings"

	"github.com/cockroachdb/errors"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/naming"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
)

// errPeerDiskTooSmall is the refusal DRBD makes in its size handshake
// (receive_sizes): a device that holds data, Outdated or better, is never
// shrunk to connect a peer whose disk is smaller, so the kernel logs "The
// peer's disk size is too small!" and drops the connection, and does so
// again on every attempt. Nothing in the replica's state says why: both
// replicas report a successful apply and the connection only reads
// StandAlone or Connecting.
var errPeerDiskTooSmall = errors.New("DRBD refuses peers whose disk is smaller than this replica's device")

// peerDiskTooSmallReason is the Event reason for errPeerDiskTooSmall.
const peerDiskTooSmallReason = "PeerDiskTooSmall"

// diskStatesHoldingData are the disk states DRBD never shrinks a device
// in to connect a peer (D_OUTDATED and above).
//
//nolint:gochecknoglobals // a fixed vocabulary, matched in one place
var diskStatesHoldingData = []string{"Outdated", "Consistent", "UpToDate"}

// peerSizeRefusal is one diskful peer whose disk is smaller than this
// replica's device on one volume.
type peerSizeRefusal struct {
	peer            string
	volume          int32
	peerKib, ownKib int64
}

// peerSizeRefusals lists the peers the kernel of res refuses for their
// disk size: on some volume res holds data on a device larger than the
// device the peer reports, and neither side shows their connection
// established. A peer still connected is not refused, and a peer whose
// own record still shows it connected is one whose report is stale, as a
// node that went down before a grow, not one being refused.
func peerSizeRefusals(res *blockstoriov1alpha1.Resource, peers []blockstoriov1alpha1.Resource) []peerSizeRefusal {
	var refusals []peerSizeRefusal

	for _, own := range res.Status.Volumes {
		if own.DeviceSizeKib <= 0 || !slices.Contains(diskStatesHoldingData, own.DiskState) {
			continue
		}

		for i := range peers {
			peer := &peers[i]
			if naming.Same(peer.Spec.NodeName, res.Spec.NodeName) || slices.Contains(peer.Spec.Flags, apiv1.ResourceFlagDiskless) {
				continue
			}

			theirs, ok := volumeStatus(peer, own.VolumeNumber)
			if !ok || theirs.DeviceSizeKib <= 0 || theirs.DeviceSizeKib >= own.DeviceSizeKib {
				continue
			}

			if connectedTo(res, peer.Spec.NodeName) || connectedTo(peer, res.Spec.NodeName) {
				continue
			}

			refusals = append(refusals, peerSizeRefusal{
				peer: peer.Spec.NodeName, volume: own.VolumeNumber,
				peerKib: theirs.DeviceSizeKib, ownKib: own.DeviceSizeKib,
			})
		}
	}

	return refusals
}

// volumeStatus is the status res reports for volume vn.
func volumeStatus(res *blockstoriov1alpha1.Resource, vn int32) (blockstoriov1alpha1.ResourceVolumeStatus, bool) {
	for _, vol := range res.Status.Volumes {
		if vol.VolumeNumber == vn {
			return vol, true
		}
	}

	return blockstoriov1alpha1.ResourceVolumeStatus{}, false
}

// connectedTo reports whether res shows its connection to node
// established.
func connectedTo(res *blockstoriov1alpha1.Resource, node string) bool {
	for _, conn := range res.Status.Connections {
		if naming.Same(conn.PeerNodeName, node) {
			return conn.Connected
		}
	}

	return false
}

// refusePeersTooSmall fails an apply that went through on a replica whose
// kernel refuses peers for their disk size (peerSizeRefusals), so the
// replica reports the refusal (Status.ApplyFailure, a Warning Event)
// rather than a success. The apply cannot cure it: the peer's disk has
// to grow, or the larger replica be recreated at the definition's size.
func refusePeersTooSmall(res *blockstoriov1alpha1.Resource, peers []blockstoriov1alpha1.Resource, results []*intent.ResourceApplyResult) {
	if len(results) == 0 || !results[0].GetOk() {
		return
	}

	refusals := peerSizeRefusals(res, peers)
	if len(refusals) == 0 {
		return
	}

	parts := make([]string, 0, len(refusals))
	for _, refusal := range refusals {
		parts = append(parts, fmt.Sprintf("%s (volume %d: %d KiB, this replica's device %d KiB)",
			refusal.peer, refusal.volume, refusal.peerKib, refusal.ownKib))
	}

	results[0].Ok = false
	results[0].Message = fmt.Sprintf("%s: %s; a device holding data is never shrunk to connect them",
		errPeerDiskTooSmall.Error(), strings.Join(parts, ", "))
}

// selfIn is the target's own entry in the uncached peer list, which
// carries its freshest status, else the target as the caller holds it.
func selfIn(res *blockstoriov1alpha1.Resource, peers []blockstoriov1alpha1.Resource) *blockstoriov1alpha1.Resource {
	for i := range peers {
		if naming.Same(peers[i].Spec.NodeName, res.Spec.NodeName) {
			return &peers[i]
		}
	}

	return res
}
