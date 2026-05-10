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
	"strconv"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// TypedDRBDOptionsToProps re-emits a typed DRBDOptions (the
// hierarchy resolver's output) as a flat upstream-LINSTOR-shaped
// `props["DrbdOptions/<Section>/<Key>"]` map. Used by the controller
// to feed the dispatcher and the satellite-side .res renderer
// without changing the transport — typed proto comes in a later
// phase. Nil → empty map. Phase 10.3 step 5.
func TypedDRBDOptionsToProps(opts *blockstoriov1alpha1.DRBDOptions) map[string]string {
	out := map[string]string{}
	if opts == nil {
		return out
	}

	emitTypedNet(out, opts.Net)
	emitTypedDisk(out, opts.Disk)
	emitTypedPeerDevice(out, opts.PeerDevice)
	emitTypedResource(out, opts.Resource)
	emitTypedHandlers(out, opts.Handlers)

	return out
}

func emitTypedNet(out map[string]string, net *blockstoriov1alpha1.DRBDNetOptions) {
	if net == nil {
		return
	}

	if net.Protocol != "" {
		out["DrbdOptions/Net/protocol"] = net.Protocol
	}

	if net.MaxBuffers != nil {
		out["DrbdOptions/Net/max-buffers"] = strconv.FormatInt(int64(*net.MaxBuffers), 10)
	}

	if net.AllowTwoPrimaries != nil {
		out["DrbdOptions/Net/allow-two-primaries"] = boolToWire(*net.AllowTwoPrimaries)
	}

	if net.AfterSb0Pri != "" {
		out["DrbdOptions/Net/after-sb-0pri"] = net.AfterSb0Pri
	}

	if net.AfterSb1Pri != "" {
		out["DrbdOptions/Net/after-sb-1pri"] = net.AfterSb1Pri
	}

	if net.AfterSb2Pri != "" {
		out["DrbdOptions/Net/after-sb-2pri"] = net.AfterSb2Pri
	}
}

func emitTypedDisk(out map[string]string, disk *blockstoriov1alpha1.DRBDDiskOptions) {
	if disk == nil {
		return
	}

	if disk.OnIOError != "" {
		out["DrbdOptions/Disk/on-io-error"] = disk.OnIOError
	}

	if disk.ALExtents != nil {
		out["DrbdOptions/Disk/al-extents"] = strconv.FormatInt(int64(*disk.ALExtents), 10)
	}
}

func emitTypedPeerDevice(out map[string]string, peerDev *blockstoriov1alpha1.DRBDPeerDeviceOptions) {
	if peerDev == nil {
		return
	}

	if peerDev.CMaxRate != "" {
		out["DrbdOptions/PeerDevice/c-max-rate"] = peerDev.CMaxRate
	}
}

func emitTypedResource(out map[string]string, res *blockstoriov1alpha1.DRBDResourceOptions) {
	if res == nil {
		return
	}

	if res.AutoPromote != nil {
		out["DrbdOptions/Resource/auto-promote"] = boolToWire(*res.AutoPromote)
	}

	if res.Quorum != "" {
		out["DrbdOptions/Resource/quorum"] = res.Quorum
	}

	if res.OnNoQuorum != "" {
		out["DrbdOptions/Resource/on-no-quorum"] = res.OnNoQuorum
	}
}

func emitTypedHandlers(out, handlers map[string]string) {
	for name, script := range handlers {
		out["DrbdOptions/Handlers/"+name] = script
	}
}

// LINSTOR-native bool literals — output side only. golinstor pushes
// other forms ("true"/"1"/"yes") on input; we always emit `yes`/`no`.
const (
	wireYes = "yes"
	wireNo  = "no"
)

// boolToWire matches LINSTOR's wire shape (`yes`/`no`).
func boolToWire(b bool) string {
	if b {
		return wireYes
	}

	return wireNo
}
