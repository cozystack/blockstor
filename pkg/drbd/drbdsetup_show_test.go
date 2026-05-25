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

package drbd_test

import (
	"testing"

	"github.com/cockroachdb/errors"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/storage"
)

// errShowResourceAbsent is the sentinel paired with each absent-
// resource fixture. err113 forbids dynamic errors.New inside test
// bodies, so the verbatim drbd-utils stderr message rides on the
// FakeResponse.Stdout channel instead — the Show wrapper joins
// err.Error() + " " + string(out) before substring-matching, so the
// channel split doesn't change the match semantics.
var errShowResourceAbsent = errors.New("drbdsetup exited non-zero")

// sampleShowTwoPeers is a `drbdsetup status -j pvc-1` fixture with two
// peer connections: worker-1 (Connected, one peer-device on volume 0)
// and worker-2 (Connecting, no peer-device — the Bug 342 zombie
// signature). The field shape is captured verbatim from a real
// satellite (`kubectl exec ds/blockstor-satellite -- drbdsetup status
// -j <res>` on the QEMU stand, drbd-utils 9.x): peer name is
// `connections[].name`, peer node-id is `connections[].peer-node-id`,
// connection state is `connections[].connection-state`, and the
// per-volume peer-device array nests under `peer_devices[].volume`.
// The previous fabricated fixture used `_peer_node_name` / `connection`
// / `peer_node_id` / `volume_nr`, none of which exist in real output —
// which is exactly why PruneStaleKernelSlots was dead code (Bug B).
const sampleShowTwoPeers = `[
  {
    "name": "pvc-1",
    "node-id": 0,
    "role": "Secondary",
    "devices": [
      { "volume": 0, "minor": 20000, "disk-state": "UpToDate" }
    ],
    "connections": [
      {
        "peer-node-id": 1,
        "name": "worker-1",
        "connection-state": "Connected",
        "peer-role": "Secondary",
        "peer_devices": [
          { "volume": 0, "replication-state": "Established", "peer-disk-state": "UpToDate" }
        ]
      },
      {
        "peer-node-id": 2,
        "name": "worker-2",
        "connection-state": "Connecting",
        "peer-role": "Unknown",
        "peer_devices": []
      }
    ]
  }
]`

// TestAdmShowParsesTwoPeers exercises the happy path: a resource with
// two peer slots, one healthy and one zombie. The parser must return
// both, distinguishable by ConnectionState and the empty peer-device
// presence set.
func TestAdmShowParsesTwoPeers(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("drbdsetup status -j pvc-1", storage.FakeResponse{
		Stdout: []byte(sampleShowTwoPeers),
	})
	adm := drbd.NewAdm(fx)

	slots, err := adm.Show(t.Context(), "pvc-1")
	if err != nil {
		t.Fatalf("Show: %v", err)
	}

	if len(slots) != 2 {
		t.Fatalf("expected 2 slots, got %d: %+v", len(slots), slots)
	}

	w1, ok := slots["worker-1"]
	if !ok {
		t.Fatalf("missing worker-1 slot")
	}

	if w1.NodeID != 1 {
		t.Errorf("worker-1 NodeID = %d, want 1", w1.NodeID)
	}

	if w1.ConnectionState != "Connected" {
		t.Errorf("worker-1 ConnectionState = %q, want Connected", w1.ConnectionState)
	}

	if !w1.HasAnyPeerDeviceConfigured([]int32{0}) {
		t.Errorf("worker-1 should have peer-device for volume 0")
	}

	if w1.IsConnectingOrStandalone() {
		t.Errorf("worker-1 (Connected) MUST NOT match IsConnectingOrStandalone — only Connecting/StandAlone are zombie candidates")
	}

	w2, ok := slots["worker-2"]
	if !ok {
		t.Fatalf("missing worker-2 slot")
	}

	if w2.ConnectionState != "Connecting" {
		t.Errorf("worker-2 ConnectionState = %q, want Connecting", w2.ConnectionState)
	}

	if w2.HasAnyPeerDeviceConfigured([]int32{0}) {
		t.Errorf("worker-2 (zombie) MUST report no peer-device for volume 0")
	}

	if !w2.IsConnectingOrStandalone() {
		t.Errorf("worker-2 (Connecting) MUST match IsConnectingOrStandalone")
	}
}

// TestAdmShowAbsentResourceReturnsNilNil pins the tolerance contract:
// drbdsetup non-zero exit with "No currently configured DRBD" /
// "Unknown resource" is the verbatim "resource not loaded" branch
// — must collapse to (nil, nil) so callers don't branch on absence.
func TestAdmShowAbsentResourceReturnsNilNil(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		stderr string
	}{
		{"no currently configured drbd", "No currently configured DRBD found"},
		{"unknown resource", "drbdsetup: Unknown resource pvc-x"},
		{"no resources defined", "no resources defined!"},
		// Bug 350: exit-10 transient teardown window — the slot was
		// just `drbdadm down`-ed. Must tolerate so PruneStaleKernelSlots
		// treats the absent slot as nothing-to-prune instead of bubbling.
		{"no such resource exit 10", "No such resource: pvc-x"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			fx := storage.NewFakeExec()
			// Show concatenates err.Error() + " " + string(out)
			// before substring matching; carry the verbatim
			// drbd-utils message on Stdout while pairing with
			// the err113-friendly static sentinel error.
			fx.Expect("drbdsetup status -j pvc-x", storage.FakeResponse{
				Stdout: []byte(tc.stderr),
				Err:    errShowResourceAbsent,
			})

			slots, err := drbd.NewAdm(fx).Show(t.Context(), "pvc-x")
			if err != nil {
				t.Fatalf("Show: expected nil err on absent resource, got %v", err)
			}

			if slots != nil {
				t.Errorf("Show: expected nil slots on absent resource, got %+v", slots)
			}
		})
	}
}

// TestAdmShowBlankAndMalformedDegradeToNil — drbd-utils can emit
// blank stdout when the resource is newly down; the parser must
// surface (nil, nil) in that case. Malformed JSON likewise degrades
// to (nil, nil) so a transient drbd-utils format quirk doesn't wedge
// the reconcile path.
func TestAdmShowBlankAndMalformedDegradeToNil(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		stdout string
	}{
		{"blank stdout", ""},
		{"whitespace-only stdout", "   \n\t  \n"},
		{"empty array", "[]"},
		{"malformed json", "{ not_valid_json"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			fx := storage.NewFakeExec()
			fx.Expect("drbdsetup status -j pvc-z", storage.FakeResponse{
				Stdout: []byte(tc.stdout),
			})

			slots, err := drbd.NewAdm(fx).Show(t.Context(), "pvc-z")
			if err != nil {
				t.Fatalf("Show: expected nil err, got %v", err)
			}

			if slots != nil {
				t.Errorf("Show: expected nil slots, got %+v", slots)
			}
		})
	}
}

// TestAdmShowSkipsNamelessSlots — a connection still mid-negotiation
// can surface with an empty `name`. The v3 prune keys on peer name to
// cross-reference K8s expected peers, so nameless slots are useless and
// must be filtered out.
func TestAdmShowSkipsNamelessSlots(t *testing.T) {
	const namelessFixture = `[
  {
    "connections": [
      { "peer-node-id": 1, "name": "", "connection-state": "Connecting", "peer_devices": [] },
      { "peer-node-id": 2, "name": "worker-2", "connection-state": "Connected", "peer_devices": [ { "volume": 0 } ] }
    ]
  }
]`

	fx := storage.NewFakeExec()
	fx.Expect("drbdsetup status -j pvc-1", storage.FakeResponse{
		Stdout: []byte(namelessFixture),
	})

	slots, err := drbd.NewAdm(fx).Show(t.Context(), "pvc-1")
	if err != nil {
		t.Fatalf("Show: %v", err)
	}

	if len(slots) != 1 {
		t.Fatalf("expected 1 slot (nameless filtered), got %d: %+v", len(slots), slots)
	}

	if _, ok := slots["worker-2"]; !ok {
		t.Errorf("worker-2 should survive the nameless filter")
	}
}

// TestKernelSlotHelpersOnNilReceiver: both predicates must accept a
// nil receiver and return false. The Pass-3 stuck-slot probe iterates
// over kernel slots; callers shouldn't need to nil-check before each
// invocation.
func TestKernelSlotHelpersOnNilReceiver(t *testing.T) {
	t.Parallel()

	var s *drbd.KernelSlot

	if s.IsConnectingOrStandalone() {
		t.Errorf("nil KernelSlot must report IsConnectingOrStandalone=false")
	}

	if s.HasAnyPeerDeviceConfigured([]int32{0, 1}) {
		t.Errorf("nil KernelSlot must report HasAnyPeerDeviceConfigured=false")
	}
}

// TestHasAnyPeerDeviceConfigured_EmptyVolumes pins the false-on-empty
// contract: callers pass `vols` from rd.Spec.Volumes, which can be
// empty on volume-less RDs. The probe must not false-trip in that
// case (no volumes = nothing to register = no zombie).
func TestHasAnyPeerDeviceConfigured_EmptyVolumes(t *testing.T) {
	t.Parallel()

	s := drbd.KernelSlot{
		PeerDevicesByVolNum: map[int32]struct{}{0: {}},
	}

	if s.HasAnyPeerDeviceConfigured(nil) {
		t.Errorf("nil vols slice must return false")
	}

	if s.HasAnyPeerDeviceConfigured([]int32{}) {
		t.Errorf("empty vols slice must return false")
	}
}
