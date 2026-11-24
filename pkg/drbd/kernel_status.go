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
	Name            string             `json:"name"`
	ConnectionState string             `json:"connection-state"`
	PeerRole        string             `json:"peer-role"`
	PeerDevices     []KernelPeerDevice `json:"peer_devices"`
}

// KernelPeerDevice is one volume of a peer connection. PercentInSync and
// OutOfSync are nil when drbd-utils did not report them. Received and
// OutOfSync are in KiB: the kernel reports both in sectors
// (peer_dev_received is recv_cnt in drbd_nl.c), and drbdsetup halves
// them when it prints the JSON (user/v9/drbdsetup.c). Pending counts the
// requests this node sent the peer that wait for its answer, Unacked the
// ones the peer sent this node that this node has not completed yet (the
// kernel's ap_pending+rs_pending and unacked_cnt).
type KernelPeerDevice struct {
	Volume           int32    `json:"volume"`
	ReplicationState string   `json:"replication-state"`
	PeerDiskState    string   `json:"peer-disk-state"`
	ResyncSuspended  string   `json:"resync-suspended"`
	PercentInSync    *float64 `json:"percent-in-sync"`
	Received         int64    `json:"received"`
	OutOfSync        *int64   `json:"out-of-sync"`
	Pending          int64    `json:"pending"`
	Unacked          int64    `json:"unacked"`
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

// SoleUpToDateOfAPrimaryPeer reports a resource a connected peer is
// Primary of while, for some volume, this node's disk is UpToDate and no
// connected peer reports an UpToDate disk of its own: that Primary's
// I/O depends on this replica, so taking it down, even for a moment, can
// leave the Primary without good data or quorum.
func (r *KernelResource) SoleUpToDateOfAPrimaryPeer() bool {
	primaryPeer := slices.ContainsFunc(r.Connections, func(c KernelConnection) bool {
		return c.ConnectionState == string(ConnectionStateConnected) && c.PeerRole == string(RolePrimary)
	})
	if !primaryPeer {
		return false
	}

	return slices.ContainsFunc(r.Devices, func(d KernelDevice) bool {
		return d.DiskState == string(DiskStateUpToDate) && !r.peerUpToDate(d.Volume)
	})
}

// Inconsistent reports whether some local volume of the resource is
// Inconsistent.
func (r *KernelResource) Inconsistent() bool {
	return slices.ContainsFunc(r.Devices, func(d KernelDevice) bool {
		return d.DiskState == string(DiskStateInconsistent)
	})
}

// InconsistentSecondary reports whether the resource is Secondary here
// and every local volume is Inconsistent. Such a node cannot be the
// SyncSource of a handshake it makes, a resync only ever takes an
// Inconsistent disk as its target, so a reconnect it runs cannot make it
// the source of a Secondary while a Primary writes (LINBIT/drbd#153).
func (r *KernelResource) InconsistentSecondary() bool {
	return r.Role == string(RoleSecondary) && len(r.Devices) > 0 &&
		!slices.ContainsFunc(r.Devices, func(d KernelDevice) bool {
			return d.DiskState != string(DiskStateInconsistent)
		})
}

// ResyncDependency is the shape of a resync that can stall on another
// peer: a local Inconsistent volume of the resource is SyncTarget of one
// peer (Active) while its resync from another one (Paused) waits on it
// (resync-suspended:dependency). ok is false when the resource is not in
// that shape, or drbd-utils did not report out-of-sync for the active
// peer.
func (r *KernelResource) ResyncDependency() (ResyncDependency, bool) {
	for _, dev := range r.Devices {
		if dev.DiskState != string(DiskStateInconsistent) {
			continue
		}

		dep := ResyncDependency{Volume: dev.Volume}

		for _, conn := range r.Connections {
			for _, peerDevice := range conn.PeerDevices {
				if peerDevice.Volume != dev.Volume {
					continue
				}

				switch {
				case dep.Active == "" && peerDevice.ReplicationState == "SyncTarget" && peerDevice.OutOfSync != nil:
					dep.Active = conn.Name
					dep.OutOfSync = *peerDevice.OutOfSync
					dep.Received = peerDevice.Received
					dep.Outstanding = peerDevice.Pending + peerDevice.Unacked

					if peerDevice.PercentInSync != nil {
						dep.PercentInSync = *peerDevice.PercentInSync
					}
				case dep.Paused == "" && slices.Contains(strings.Split(peerDevice.ResyncSuspended, ","), "dependency"):
					dep.Paused = conn.Name
				}
			}
		}

		if dep.Active != "" && dep.Paused != "" {
			dep.ActiveOnlyCopy = !r.peerUpToDateExcept(dev.Volume, dep.Active)

			return dep, true
		}
	}

	return ResyncDependency{}, false
}

// ResyncDependency is one volume resyncing from Active while its resync
// from Paused waits on it, with how far the resync from Active got.
// OutOfSync and Received are in KiB; Outstanding is the resync requests
// and replies of the Active link not completed yet (pending + unacked).
// ActiveOnlyCopy is whether Active holds the only UpToDate disk of the
// volume this node sees, the Paused peer lacking the data too: the shape
// of an initial sync, where the data exists on its source alone.
type ResyncDependency struct {
	Volume         int32
	Active         string
	Paused         string
	PercentInSync  float64
	OutOfSync      int64
	Received       int64
	Outstanding    int64
	ActiveOnlyCopy bool
}

// PrimaryRuledOut reports whether this node can tell that no replica of
// the resource is Primary: it is Secondary itself, and every configured
// connection is Connected to a Secondary. A peer that is not connected
// may be Primary for all this node knows.
func (r *KernelResource) PrimaryRuledOut() bool {
	return r.PrimaryRuledOutBesides("")
}

// PrimaryRuledOutBesides is PrimaryRuledOut over every connection but the
// one to peer: the peer a reconnect would connect, whose role cannot be
// read while it is StandAlone. The Primary that matters to that reconnect
// is a third node writing to both ends, and any other link that is not
// Connected, StandAlone ones included, may lead to one.
func (r *KernelResource) PrimaryRuledOutBesides(peer string) bool {
	return r.Role == string(RoleSecondary) && !slices.ContainsFunc(r.Connections, func(c KernelConnection) bool {
		return c.Name != peer &&
			(c.ConnectionState != string(ConnectionStateConnected) || c.PeerRole != string(RoleSecondary))
	})
}

// StandAlone reports whether this node's connection to peer is
// configured and StandAlone: dropped, and connected again by nothing but
// a `drbdadm connect` or adjust.
func (r *KernelResource) StandAlone(peer string) bool {
	return slices.ContainsFunc(r.Connections, func(c KernelConnection) bool {
		return c.Name == peer && c.ConnectionState == string(ConnectionStateStandAlone)
	})
}

// StandAlonePeers is the peers whose connection is StandAlone.
func (r *KernelResource) StandAlonePeers() []string {
	var peers []string

	for _, c := range r.Connections {
		if c.ConnectionState == string(ConnectionStateStandAlone) {
			peers = append(peers, c.Name)
		}
	}

	return peers
}

// peerUpToDate reports whether a connected peer has an UpToDate disk of
// volume.
func (r *KernelResource) peerUpToDate(volume int32) bool {
	return r.peerUpToDateExcept(volume, "")
}

// peerUpToDateExcept reports whether a connected peer other than skip has
// an UpToDate disk of volume.
func (r *KernelResource) peerUpToDateExcept(volume int32, skip string) bool {
	return slices.ContainsFunc(r.Connections, func(c KernelConnection) bool {
		return c.Name != skip && c.ConnectionState == string(ConnectionStateConnected) &&
			slices.ContainsFunc(c.PeerDevices, func(p KernelPeerDevice) bool {
				return p.Volume == volume && p.PeerDiskState == string(DiskStateUpToDate)
			})
	})
}

// unused reports whether no local volume of the resource is open.
func (r *KernelResource) unused() bool {
	return !slices.ContainsFunc(r.Devices, func(d KernelDevice) bool { return bool(d.Open) })
}
