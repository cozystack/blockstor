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
	"bytes"
	"context"
	"encoding/json"
	"slices"
	"strings"

	"github.com/cockroachdb/errors"
)

// KernelResource is one resource of `drbdsetup status --json`, reduced to
// what the satellite's DRBD workarounds read: who holds the device open,
// the role and forced I/O failures, and how each peer's resync is going.
type KernelResource struct {
	Name            string             `json:"name"`
	Role            string             `json:"role"`
	ForceIOFailures yesNo              `json:"force-io-failures"`
	Devices         []KernelDevice     `json:"devices"`
	Connections     []KernelConnection `json:"connections"`
}

// KernelDevice is one local volume of a KernelResource.
type KernelDevice struct {
	Volume    int32  `json:"volume"`
	DiskState string `json:"disk-state"`
	Open      yesNo  `json:"open"`
}

// KernelConnection is one peer of a KernelResource.
type KernelConnection struct {
	Name        string             `json:"name"`
	PeerDevices []KernelPeerDevice `json:"peer_devices"`
}

// KernelPeerDevice is one volume of a peer connection. PercentInSync is
// nil when drbd-utils did not report it.
type KernelPeerDevice struct {
	Volume           int32    `json:"volume"`
	ReplicationState string   `json:"replication-state"`
	ResyncSuspended  string   `json:"resync-suspended"`
	PercentInSync    *float64 `json:"percent-in-sync"`
}

// yesNo is a drbd-utils flag: a JSON boolean, or "yes"/"no" as some
// releases print it.
type yesNo bool

// UnmarshalJSON accepts true/false and "yes"/"no"/"true"/"false".
func (b *yesNo) UnmarshalJSON(raw []byte) error {
	switch strings.Trim(string(bytes.TrimSpace(raw)), `"`) {
	case "true", wireYes:
		*b = true
	default:
		*b = false
	}

	return nil
}

// KernelStatus reads every DRBD resource the kernel holds in one
// `drbdsetup status --json` call. A kernel with none reads as empty.
func (a *Adm) KernelStatus(ctx context.Context) ([]KernelResource, error) {
	out, err := a.exec.Run(ctx, "drbdsetup", "status", "--json")
	if err != nil {
		if isResourceNotLoadedErr(err, out) {
			return nil, nil
		}

		return nil, errors.Wrap(err, "drbdsetup status --json")
	}

	var resources []KernelResource

	if len(bytes.TrimSpace(out)) == 0 {
		return nil, nil
	}

	err = json.Unmarshal(out, &resources)
	if err != nil {
		return nil, errors.Wrap(err, "parse drbdsetup status --json")
	}

	return resources, nil
}

// DisconnectPeer drops this node's connection to peer for resource,
// `drbdadm disconnect <resource>:<peer>`. Nothing on disk changes.
func (a *Adm) DisconnectPeer(ctx context.Context, resource, peer string) error {
	return a.run(ctx, "disconnect", resource+":"+peer)
}

// ConnectPeer connects this node to peer for resource again, `drbdadm
// connect <resource>:<peer>`.
func (a *Adm) ConnectPeer(ctx context.Context, resource, peer string) error {
	return a.run(ctx, "connect", resource+":"+peer)
}

// IdlePrimary reports a resource that is Primary on this node while none
// of its volumes is open: nobody uses it, so demoting it loses nothing.
func (r *KernelResource) IdlePrimary() bool {
	return r.Role == "Primary" && len(r.Devices) > 0 && r.unused()
}

// UnusedSecondaryFailingIO reports a Secondary that still forces I/O
// failures while none of its volumes is open.
func (r *KernelResource) UnusedSecondaryFailingIO() bool {
	return r.Role == "Secondary" && bool(r.ForceIOFailures) && len(r.Devices) > 0 && r.unused()
}

// ResyncSource is the peer a local Inconsistent volume of the resource
// is resyncing from, with how far that resync got, while the resync from
// another peer waits on it (resync-suspended:dependency). ok is false
// when the resource is not in that shape.
func (r *KernelResource) ResyncSource() (ResyncFrom, bool) {
	inconsistent := slices.ContainsFunc(r.Devices, func(d KernelDevice) bool {
		return d.DiskState == "Inconsistent"
	})
	if !inconsistent {
		return ResyncFrom{}, false
	}

	var (
		from    ResyncFrom
		found   bool
		waiting bool
	)

	for _, conn := range r.Connections {
		for _, peerDevice := range conn.PeerDevices {
			if slices.Contains(strings.Split(peerDevice.ResyncSuspended, ","), "dependency") {
				waiting = true
			}

			if !found && peerDevice.ReplicationState == "SyncTarget" && peerDevice.PercentInSync != nil {
				from, found = ResyncFrom{Peer: conn.Name, PercentInSync: *peerDevice.PercentInSync}, true
			}
		}
	}

	return from, found && waiting
}

// ResyncFrom is the peer a resync pulls from and how far it got.
type ResyncFrom struct {
	Peer          string
	PercentInSync float64
}

// unused reports whether no local volume of the resource is open.
func (r *KernelResource) unused() bool {
	return !slices.ContainsFunc(r.Devices, func(d KernelDevice) bool { return bool(d.Open) })
}
