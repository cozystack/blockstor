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
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
)

// KernelSlot is the satellite-facing view of one peer connection slot
// the DRBD kernel currently holds for a resource. Captured at probe
// time by Adm.Show via `drbdsetup show -j <rd>`. Used by the Bug 342
// three-pass diff in pkg/satellite/reconciler.go::reconcilePeers to
// detect zombie slots (Connecting / StandAlone with no peer-device
// configured) and re-incarnation under the same name (Bug 342 root
// cause: the K8s Resource was deleted + re-created sub-second; same
// node name, new metadata.uid, kernel slot bound to the OLD identity's
// PSK / handshake state).
type KernelSlot struct {
	// Name is the peer node name. Mirrors the .res `on <name> {` block.
	Name string

	// NodeID is the peer's DRBD-9 node-id observed by the kernel
	// (not what K8s currently desires — those can disagree under the
	// node-id-reuse race; see TestReconcilePeers_NodeIDReused_*).
	NodeID int32

	// ConnectionState is the .connections[].connection token from
	// drbdsetup show -j: "Connected", "Connecting", "StandAlone",
	// "BrokenPipe", "NetworkFailure", "Timeout", etc. The
	// reconcilePeers Pass-3 zombie probe only acts on Connecting /
	// StandAlone (debounced via LastStateChangeTime).
	ConnectionState string

	// LastStateChangeTime is the wall-clock at which this kernel
	// slot's connection-state last transitioned (when available;
	// otherwise time.Time{}). Sourced from events2 stream or the
	// satellite's probe time as a fallback — used only by the
	// zombie-probe debounce, which falls open (zombie cleanup
	// proceeds) when the timestamp is unknown.
	LastStateChangeTime time.Time

	// PeerDevicesByVolNum is the kernel's per-volume peer-device
	// registration table for this peer slot. Keyed by volume number.
	// Empty when the kernel has the connection slot but no
	// peer-device registered for any volume — that's the zombie
	// signature (Bug 342) the Pass-3 probe targets.
	PeerDevicesByVolNum map[int32]KernelPeerDevice

	// SharedSecret is the in-kernel PSK for this connection slot,
	// surfaced via `.connections[].net.shared-secret`. The adoption-
	// mode gate cross-checks this against Spec.NetSecret; mismatch
	// triggers a no-tear-down adjust to rotate the kernel's PSK to
	// the K8s-Spec value without disrupting connections. Empty when
	// the .res renderer hasn't emitted a `shared-secret` yet (.res
	// parity is a separate workstream — until then the adoption-mode
	// gate degrades gracefully and skips the PSK check).
	SharedSecret string
}

// KernelPeerDevice is the kernel's per-volume peer-device entry for
// a given (peer, volume) pair. Present in `.connections[].peer_devices[]`
// when DRBD has completed the per-volume handshake; absent during
// the in-flight window OR forever if the connection wedged (Bug 342).
type KernelPeerDevice struct {
	// VolumeNumber mirrors the parent Resource's VolumeNumber.
	VolumeNumber int32

	// DiskState is the kernel-observed peer disk state from the
	// .replication / .peer-disk-state token: "UpToDate", "Outdated",
	// "Inconsistent", "Diskless", "DUnknown".
	DiskState string

	// Configured signals presence — true means the kernel has the
	// peer-device registered for this volume. Always true for slots
	// the parser surfaces (the parser only emits entries that exist
	// in the JSON); the field exists so callers can use a zero-value
	// KernelPeerDevice (Configured=false) as the "absent" sentinel
	// in lookup helpers like HasAnyPeerDeviceConfigured.
	Configured bool
}

// HasAnyPeerDeviceConfigured reports whether the kernel currently
// has at least one of the named volumes registered as a peer-device
// for this slot. Used by the Pass-3 zombie probe to debounce: a slot
// with one configured peer-device (out of N volumes) is a
// partial-handshake-in-progress, not a zombie — let DRBD finish.
//
// nil volumes → returns false (no volumes to check; callers should
// treat that as the no-zombie case to avoid false positives on
// volume-less RDs).
func (s *KernelSlot) HasAnyPeerDeviceConfigured(volumes []int32) bool {
	if s == nil || len(volumes) == 0 || len(s.PeerDevicesByVolNum) == 0 {
		return false
	}

	for _, v := range volumes {
		if pd, ok := s.PeerDevicesByVolNum[v]; ok && pd.Configured {
			return true
		}
	}

	return false
}

// IsConnectingOrStandalone returns true iff the slot is in a state
// where the zombie probe is allowed to act. Other states ("Connected",
// "WFConnection" mid-flight transitions) are off-limits to the probe
// — DRBD is mid-handshake or already healthy, and tearing down would
// flap the connection.
func (s *KernelSlot) IsConnectingOrStandalone() bool {
	if s == nil {
		return false
	}

	switch s.ConnectionState {
	case "Connecting", "StandAlone":
		return true
	default:
		return false
	}
}

// drbdsetupShowRoot is the top-level shape of `drbdsetup show -j <rd>`.
// drbd-utils emits one array element per resource named on the
// command line; the satellite always invokes with a single resource,
// so we take the first element.
type drbdsetupShowRoot []drbdsetupShowResource

// drbdsetupShowResource models the `_this_host` / `connections` /
// `_my_node_id` shape of one resource in drbdsetup show -j output.
// We intentionally model ONLY the fields reconcilePeers consumes —
// drbd-utils adds tail fields freely, and a strict full-shape parser
// would break on every minor utils bump.
type drbdsetupShowResource struct {
	Name        string                    `json:"_name"`
	MyNodeID    int32                     `json:"_my_node_id"`
	Connections []drbdsetupShowConnection `json:"connections"`
	ThisHost    *drbdsetupShowThisHost    `json:"_this_host,omitempty"`
}

// drbdsetupShowThisHost models the local host's volume set so the
// parser can correlate volume numbers across `_this_host.volumes[]`
// and each connection's `peer_devices[]`. We only read the volume
// numbers; the rest is ignored.
type drbdsetupShowThisHost struct {
	NodeID  int32                     `json:"node_id"`
	Volumes []drbdsetupShowThisVolume `json:"volumes,omitempty"`
}

type drbdsetupShowThisVolume struct {
	VolumeNumber int32 `json:"volume_nr"`
}

// drbdsetupShowConnection models one peer connection from drbdsetup
// show -j: peer node-id, peer name (the `_peer_node_name` field
// drbd-utils 9.x added; falls back to the host map walk if absent),
// the connection state, optional per-peer-device registrations, and
// the negotiated net section (including shared-secret).
type drbdsetupShowConnection struct {
	PeerNodeID    int32                     `json:"peer_node_id"`
	PeerName      string                    `json:"_peer_node_name"`
	Net           drbdsetupShowNet          `json:"net"`
	PeerDevices   []drbdsetupShowPeerDevice `json:"peer_devices"`
	ConnectionStr string                    `json:"connection"`
	LastChangeNs  int64                     `json:"_last_state_change_ns"`
}

type drbdsetupShowNet struct {
	SharedSecret string `json:"shared-secret"`
}

type drbdsetupShowPeerDevice struct {
	VolumeNumber  int32  `json:"volume_nr"`
	PeerDiskState string `json:"peer-disk-state"`
}

// Show runs `drbdsetup show -j <resource>` and parses the output into
// a map keyed by peer node name. Errors are surfaced wrapped; the
// caller (reconcilePeers) treats any error as "kernel state unknown"
// and falls through to UID-only diff — degradation is preferred over
// a wedged reconcile.
//
// Missing kernel slot for the named resource → empty map + nil error
// (the "kernel module loaded but resource not present" steady state
// is a valid pre-up condition, not a failure). Mirrors the convention
// IsLoaded uses for the same case. nil-map is the documented empty
// signal; the caller treats nil and the zero-length map identically.
//
//nolint:nilnil // a nil map IS the empty-result signal; sentinel error would force every caller to branch on errors.Is which adds friction without value
func (a *Adm) Show(ctx context.Context, resource string) (map[string]KernelSlot, error) {
	out, err := a.exec.Run(ctx, "drbdsetup", "show", "-j", resource)
	if err != nil {
		// "No currently configured DRBD found" / "Unknown resource"
		// are the verbatim drbd-utils messages for the "resource
		// not loaded" branch — return empty map + nil so callers
		// can treat absence and "show failed" identically.
		errText := err.Error() + " " + string(out)
		if strings.Contains(errText, "No currently configured DRBD") ||
			strings.Contains(errText, "Unknown resource") {
			return nil, nil
		}

		return nil, errors.Wrapf(err, "drbdsetup show -j %s", resource)
	}

	return parseShowJSON(out, resource)
}

// parseShowJSON does the JSON-unmarshal half of Show, pulled out so
// the top-level method stays under funlen and so unit tests can feed
// golden fixtures directly without invoking the FakeExec round-trip.
//
//nolint:nilnil // see Show — nil map IS the empty result
func parseShowJSON(out []byte, resource string) (map[string]KernelSlot, error) {
	body := strings.TrimSpace(string(out))
	if body == "" {
		return nil, nil
	}

	var root drbdsetupShowRoot

	parseErr := json.Unmarshal([]byte(body), &root)
	if parseErr != nil {
		return nil, errors.Wrapf(parseErr, "parse drbdsetup show -j %s", resource)
	}

	if len(root) == 0 {
		return nil, nil
	}

	res := root[0]
	slots := make(map[string]KernelSlot, len(res.Connections))

	for i := range res.Connections {
		slot := connectionToSlot(&res.Connections[i])
		// Skip nameless slots — drbd-utils versions before
		// `_peer_node_name` emit only the node-id, leaving callers
		// to walk a side-table. The reconciler keys on peer-name
		// for the K8s sibling join, so a nameless slot can't drive
		// either pass safely; we'd rather fall through to UID-only
		// diff than emit ambiguous entries.
		if slot.Name == "" {
			continue
		}

		slots[slot.Name] = slot
	}

	return slots, nil
}

// connectionToSlot turns one parsed drbdsetup show connection into
// the satellite-facing KernelSlot shape.
func connectionToSlot(conn *drbdsetupShowConnection) KernelSlot {
	slot := KernelSlot{
		Name:            conn.PeerName,
		NodeID:          conn.PeerNodeID,
		ConnectionState: conn.ConnectionStr,
		SharedSecret:    conn.Net.SharedSecret,
	}

	if conn.LastChangeNs > 0 {
		slot.LastStateChangeTime = time.Unix(0, conn.LastChangeNs)
	}

	if len(conn.PeerDevices) > 0 {
		slot.PeerDevicesByVolNum = make(map[int32]KernelPeerDevice, len(conn.PeerDevices))
		for _, pd := range conn.PeerDevices {
			slot.PeerDevicesByVolNum[pd.VolumeNumber] = KernelPeerDevice{
				VolumeNumber: pd.VolumeNumber,
				DiskState:    pd.PeerDiskState,
				Configured:   true,
			}
		}
	}

	return slot
}
