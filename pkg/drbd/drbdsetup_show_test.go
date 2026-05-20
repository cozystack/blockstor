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
	"errors"
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/storage"
)

// errExit10 is a static sentinel mirroring the non-zero exit
// `drbdsetup show -j` produces when the named resource isn't loaded.
// Static so the err113 linter is satisfied — the parser only cares
// that an error WAS returned, not its identity.
var errExit10 = errors.New("exit 10")

// drbdsetupShowJSONTwoPeers is a golden capture mirroring
// drbd-utils 9.27's `drbdsetup show -j <rd>` output for a 3-replica
// RD with two peer connections, both Connected with a UpToDate
// peer-device on vol 0.
const drbdsetupShowJSONTwoPeers = `[
  {
    "_name": "pvc-1",
    "_my_node_id": 0,
    "_this_host": {
      "node_id": 0,
      "volumes": [
        { "volume_nr": 0 }
      ]
    },
    "connections": [
      {
        "peer_node_id": 1,
        "_peer_node_name": "n2",
        "net": { "shared-secret": "psk-1" },
        "connection": "Connected",
        "_last_state_change_ns": 0,
        "peer_devices": [
          { "volume_nr": 0, "peer-disk-state": "UpToDate" }
        ]
      },
      {
        "peer_node_id": 2,
        "_peer_node_name": "n3",
        "net": { "shared-secret": "psk-1" },
        "connection": "Connecting",
        "_last_state_change_ns": 1234567890000000000,
        "peer_devices": []
      }
    ]
  }
]`

// TestAdmShowParsesTwoPeers covers the steady-state shape: two peer
// slots, each with the satellite-relevant fields (name, node-id,
// connection state, optional peer-devices, shared-secret).
func TestAdmShowParsesTwoPeers(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Responses["drbdsetup show -j pvc-1"] = storage.FakeResponse{
		Stdout: []byte(drbdsetupShowJSONTwoPeers),
	}

	adm := drbd.NewAdm(fx)

	slots, err := adm.Show(t.Context(), "pvc-1")
	if err != nil {
		t.Fatalf("Show: %v", err)
	}

	if len(slots) != 2 {
		t.Fatalf("slots=%d, want 2 (keys=%v)", len(slots), keys(slots))
	}

	n2 := slots["n2"]
	if n2.NodeID != 1 {
		t.Errorf("n2.NodeID=%d, want 1", n2.NodeID)
	}

	if n2.ConnectionState != "Connected" {
		t.Errorf("n2.ConnectionState=%q, want Connected", n2.ConnectionState)
	}

	if !n2.HasAnyPeerDeviceConfigured([]int32{0}) {
		t.Errorf("n2 should have peer-device for vol 0")
	}

	if n2.SharedSecret != "psk-1" {
		t.Errorf("n2.SharedSecret=%q, want psk-1", n2.SharedSecret)
	}

	n3 := slots["n3"]
	if !n3.IsConnectingOrStandalone() {
		t.Errorf("n3 should be Connecting or StandAlone, got %q", n3.ConnectionState)
	}

	if n3.HasAnyPeerDeviceConfigured([]int32{0}) {
		t.Errorf("n3 has empty peer_devices — must NOT be HasAny (zombie shape)")
	}

	if n3.LastStateChangeTime.IsZero() {
		t.Errorf("n3.LastStateChangeTime zero; want from _last_state_change_ns")
	}
}

// TestAdmShowEmptyOnAbsentResource pins the "kernel module loaded but
// resource not present" branch — drbdsetup exits non-zero with
// "No currently configured DRBD found" / "Unknown resource"; Show
// returns nil + nil so the reconciler treats it identically to a
// freshly-created (no kernel state) replica.
func TestAdmShowEmptyOnAbsentResource(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Responses["drbdsetup show -j pvc-1"] = storage.FakeResponse{
		Stdout: []byte("No currently configured DRBD found.\n"),
		Err:    errExit10,
	}

	adm := drbd.NewAdm(fx)

	slots, err := adm.Show(t.Context(), "pvc-1")
	if err != nil {
		t.Fatalf("Show on absent: %v; want nil", err)
	}

	if len(slots) != 0 {
		t.Errorf("slots=%v, want empty on absent resource", slots)
	}
}

// TestAdmShowEmptyOnBlankStdout pins the alternate "show returns OK
// but empty stdout" shape — some drbd-utils variants exit 0 with no
// bytes when the resource is absent. Treat identically to absent.
func TestAdmShowEmptyOnBlankStdout(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Responses["drbdsetup show -j pvc-1"] = storage.FakeResponse{
		Stdout: []byte("   \n"),
	}

	adm := drbd.NewAdm(fx)

	slots, err := adm.Show(t.Context(), "pvc-1")
	if err != nil {
		t.Fatalf("Show on blank stdout: %v", err)
	}

	if len(slots) != 0 {
		t.Errorf("slots=%v, want empty on blank stdout", slots)
	}
}

// TestAdmShowParseError surfaces malformed JSON as a wrapped error
// so the reconciler can fall through to UID-only diff with logging.
func TestAdmShowParseError(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Responses["drbdsetup show -j pvc-1"] = storage.FakeResponse{
		Stdout: []byte("{not valid json"),
	}

	adm := drbd.NewAdm(fx)

	_, err := adm.Show(t.Context(), "pvc-1")
	if err == nil {
		t.Fatal("expected parse error, got nil")
	}

	if !strings.Contains(err.Error(), "parse") {
		t.Errorf("err=%v, want to mention parse", err)
	}
}

// TestKernelSlotHasAnyPeerDeviceConfiguredMultiVol verifies the
// partial-handshake debounce: kernel has peer-device for vol 0 but
// not vol 1 → HasAny returns true → Pass-3 zombie probe declines
// teardown (DRBD still finishing handshake for vol 1).
func TestKernelSlotHasAnyPeerDeviceConfiguredMultiVol(t *testing.T) {
	slot := drbd.KernelSlot{
		Name:            "n2",
		NodeID:          1,
		ConnectionState: "Connecting",
		PeerDevicesByVolNum: map[int32]drbd.KernelPeerDevice{
			0: {VolumeNumber: 0, Configured: true},
			// vol 1 absent
		},
	}

	if !slot.HasAnyPeerDeviceConfigured([]int32{0, 1}) {
		t.Error("multi-vol partial handshake should report HasAny=true")
	}

	if slot.HasAnyPeerDeviceConfigured(nil) {
		t.Error("nil volumes should return false")
	}

	zombie := drbd.KernelSlot{
		Name:                "n3",
		ConnectionState:     "Connecting",
		PeerDevicesByVolNum: map[int32]drbd.KernelPeerDevice{},
	}

	if zombie.HasAnyPeerDeviceConfigured([]int32{0, 1}) {
		t.Error("empty PeerDevicesByVolNum should report HasAny=false (zombie shape)")
	}
}

// TestKernelSlotIsConnectingOrStandaloneStates pins the small state
// set the Pass-3 zombie probe acts on — anything else (Connected,
// WFConnection, BrokenPipe transitional states) is off-limits.
func TestKernelSlotIsConnectingOrStandaloneStates(t *testing.T) {
	for _, tc := range []struct {
		state string
		want  bool
	}{
		{"Connecting", true},
		{"StandAlone", true},
		{"Connected", false},
		{"WFConnection", false},
		{"BrokenPipe", false},
		{"", false},
	} {
		slot := drbd.KernelSlot{ConnectionState: tc.state}
		if got := slot.IsConnectingOrStandalone(); got != tc.want {
			t.Errorf("%q: got %v, want %v", tc.state, got, tc.want)
		}
	}
}

// TestAdmShowSkipsNamelessSlots pins the drbd-utils-pre-9.x branch:
// older versions emit `peer_node_id` without `_peer_node_name`. The
// parser can't safely key those slots by name (the reconciler's
// K8s sibling join is by name) so it skips them; the reconciler
// falls through to UID-only diff for those edge clusters.
func TestAdmShowSkipsNamelessSlots(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Responses["drbdsetup show -j pvc-1"] = storage.FakeResponse{
		Stdout: []byte(`[{"_name":"pvc-1","connections":[{"peer_node_id":1,"connection":"Connected"}]}]`),
	}

	adm := drbd.NewAdm(fx)

	slots, err := adm.Show(t.Context(), "pvc-1")
	if err != nil {
		t.Fatalf("Show: %v", err)
	}

	if len(slots) != 0 {
		t.Errorf("nameless slot should be skipped, got %d entries", len(slots))
	}
}

func keys(m map[string]drbd.KernelSlot) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}

	return out
}
