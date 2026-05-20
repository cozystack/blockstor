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

	"github.com/cockroachdb/errors"
)

// KernelSlot is the satellite-facing view of one peer connection slot
// the DRBD kernel currently holds for a resource. Captured at probe
// time by Adm.Show via `drbdsetup show -j <rd>`. Consumed by the Bug
// 342 v3 proactive prune in pkg/satellite/kernel_cleanup.go: two
// state-only passes (peer-not-expected and stuck-Connecting/StandAlone-
// with-no-peer-device) drive `drbdadm del-peer` + `drbdmeta forget-peer`
// without touching the previously-rendered .res file (which may not
// exist on a fresh satellite-pod for a relocated replica — the Bug 342
// root scenario).
//
// We model only the fields the v3 prune actually consumes; drbd-utils
// adds tail fields freely and a strict full-shape parser would break
// on every minor bump. Specifically: no PSK, no GI tracking, no
// last-state-change timestamp — debounce lives in-memory in the
// Reconciler, not in the kernel-side observation.
type KernelSlot struct {
	// Name is the peer node name. Mirrors the .res `on <name> {`
	// block. Empty entries (drbd-utils <9.x missing _peer_node_name)
	// are filtered out by the parser — the v3 prune keys on peer
	// name and can't safely act on nameless slots.
	Name string

	// NodeID is the peer's DRBD-9 node-id observed by the kernel.
	// Used as the `--node-id` argument to `drbdmeta forget-peer`
	// when clearing the per-volume metadata slot.
	NodeID int32

	// ConnectionState is the `.connections[].connection` token from
	// drbdsetup show -j: "Connected", "Connecting", "StandAlone",
	// "BrokenPipe", "NetworkFailure", "Timeout", etc. The Pass-3
	// stuck-slot probe only acts on Connecting / StandAlone.
	ConnectionState string

	// PeerDevicesByVolNum is a presence set of volume numbers the
	// kernel has registered as peer-devices for this slot. The
	// Pass-3 stuck-slot probe targets the empty case: a connection
	// slot with NO peer-device registered for any volume — that's
	// the Bug 342 signature.
	PeerDevicesByVolNum map[int32]struct{}
}

// IsConnectingOrStandalone reports whether the slot is in a state
// where the Pass-3 stuck-slot probe is allowed to act. Other states
// ("Connected", in-flight transitions) are off-limits — DRBD is
// either healthy or mid-handshake, and tearing down would flap a
// live connection.
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

// HasAnyPeerDeviceConfigured reports whether the kernel has at least
// one of the named volumes registered as a peer-device for this slot.
// Used by the Pass-3 stuck-slot probe to debounce: a slot with one
// configured peer-device (out of N volumes) is a partial-handshake-
// in-progress, not a zombie — let DRBD finish.
//
// nil / empty volumes → returns false; callers should treat that as
// "no zombie" to avoid false positives on volume-less RDs.
func (s *KernelSlot) HasAnyPeerDeviceConfigured(volumes []int32) bool {
	if s == nil || len(volumes) == 0 || len(s.PeerDevicesByVolNum) == 0 {
		return false
	}

	for _, v := range volumes {
		if _, ok := s.PeerDevicesByVolNum[v]; ok {
			return true
		}
	}

	return false
}

// drbdsetupShowRoot is the top-level shape of `drbdsetup show -j <rd>`.
// drbd-utils emits one array element per resource named on the
// command line; the satellite always invokes with a single resource,
// so we read the first element only.
type drbdsetupShowRoot []drbdsetupShowResource

// drbdsetupShowResource is one resource block from drbdsetup show -j.
// We model only the fields the v3 prune consumes.
type drbdsetupShowResource struct {
	Connections []drbdsetupShowConnection `json:"connections"`
}

// drbdsetupShowConnection is one peer connection slot.
type drbdsetupShowConnection struct {
	PeerNodeID    int32                     `json:"peer_node_id"`
	PeerName      string                    `json:"_peer_node_name"`
	ConnectionStr string                    `json:"connection"`
	PeerDevices   []drbdsetupShowPeerDevice `json:"peer_devices"`
}

type drbdsetupShowPeerDevice struct {
	VolumeNumber int32 `json:"volume_nr"`
}

// Show runs `drbdsetup show -j <resource>` and parses the output into
// a map keyed by peer node name.
//
// Tolerant of:
//   - resource not present in kernel (drbdsetup non-zero with
//     "No currently configured DRBD" / "Unknown resource" /
//     "no resources defined") → nil map + nil error
//   - blank stdout / empty `[]` array → nil map + nil error
//   - malformed JSON → nil map + nil error (degrade to no-op rather
//     than wedge the reconcile path: the v3 prune is best-effort)
//
//nolint:nilnil // nil map IS the empty-result signal; sentinel error would force callers to branch on errors.Is without value.
func (a *Adm) Show(ctx context.Context, resource string) (map[string]KernelSlot, error) {
	out, err := a.exec.Run(ctx, "drbdsetup", "show", "-j", resource)
	if err != nil {
		// "No currently configured DRBD found" / "Unknown resource"
		// / "no resources defined" are the verbatim drbd-utils
		// messages for the "resource not loaded" branch — treat
		// absence as empty so callers don't need to branch.
		errText := err.Error() + " " + string(out)
		if strings.Contains(errText, "No currently configured DRBD") ||
			strings.Contains(errText, "Unknown resource") ||
			strings.Contains(errText, "no resources defined") {
			return nil, nil
		}

		return nil, errors.Wrapf(err, "drbdsetup show -j %s", resource)
	}

	return parseShowJSON(out), nil
}

// parseShowJSON does the JSON-unmarshal half of Show. Pulled out so
// unit tests can feed fixtures directly without the FakeExec round-trip.
// Malformed JSON degrades to nil (no-op) — the v3 prune is best-effort
// and we'd rather skip cleanup than wedge a reconcile.
func parseShowJSON(out []byte) map[string]KernelSlot {
	body := strings.TrimSpace(string(out))
	if body == "" {
		return nil
	}

	var root drbdsetupShowRoot

	err := json.Unmarshal([]byte(body), &root)
	if err != nil {
		return nil
	}

	if len(root) == 0 {
		return nil
	}

	res := root[0]
	slots := make(map[string]KernelSlot, len(res.Connections))

	for i := range res.Connections {
		c := &res.Connections[i]
		// Skip nameless slots — drbd-utils versions before
		// `_peer_node_name` emit only the node-id. The v3 prune
		// keys on peer-name to cross-reference K8s expected
		// peers, so an entry with no name is useless to us.
		if c.PeerName == "" {
			continue
		}

		pdv := make(map[int32]struct{}, len(c.PeerDevices))
		for j := range c.PeerDevices {
			pdv[c.PeerDevices[j].VolumeNumber] = struct{}{}
		}

		slots[c.PeerName] = KernelSlot{
			Name:                c.PeerName,
			NodeID:              c.PeerNodeID,
			ConnectionState:     c.ConnectionStr,
			PeerDevicesByVolNum: pdv,
		}
	}

	return slots
}
