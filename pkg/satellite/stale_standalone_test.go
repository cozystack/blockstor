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

package satellite

import (
	"slices"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
)

const staleStandAlonePlan = "drbdadm -d adjust " + portInUseRes

// staleStandAloneStatus is the kernel of a replica whose connection to
// n2 the kernel dropped: n2 was set up as a diskless peer, came up
// diskful, and the handshake failed with "The peer is configured to be
// diskless but presents Inconsistent". The connection to n0 is fine.
func staleStandAloneStatus() storage.FakeResponse {
	return storage.FakeResponse{Stdout: []byte(`[
  {
    "name": "` + portInUseRes + `",
    "connections": [
      {"peer-node-id": 0, "name": "n0", "connection-state": "Connected", "peer_devices": [{"volume": 0}]},
      {"peer-node-id": 2, "name": "n2", "connection-state": "StandAlone", "peer_devices": [{"volume": 0}]}
    ]
  }
]
`)}
}

// A fresh replica's peer that was a diskless witness when this node
// brought the resource up, and that the placement took over as a
// diskful replica, is left StandAlone by the kernel with the diskless
// peer's options. Nobody disconnected it: the adjust that the drift
// check asks for, the pass after the .res renders the peer diskful
// and the pass after a satellite restart all have to be full adjusts,
// which reset the peer's options and connect it. With --skip-net every
// one of them succeeded without touching the connection, and the
// replica stayed StandAlone until an operator ran drbdadm adjust.
func TestAdjustReconnectsAStandAlonePeerWithStaleConfig(t *testing.T) {
	t.Parallel()

	fx := portInUseExec()
	fx.Expect("drbdsetup status -j "+portInUseRes, staleStandAloneStatus())
	fx.Expect(staleStandAlonePlan, storage.FakeResponse{Stdout: []byte(
		"drbdsetup peer-device-options " + portInUseRes + " 2 0 --set-defaults\n" +
			"drbdsetup connect " + portInUseRes + " 2\n" +
			"drbdsetup wait-connect-connection " + portInUseRes + " 2\n",
	)})

	for _, pass := range []string{"drift check", "after a satellite restart"} {
		fx.Reset()

		rec := NewReconciler(ReconcilerConfig{Adm: drbd.NewAdm(fx), StateDir: t.TempDir(), NodeName: "n1"})
		result := &intent.ResourceApplyResult{Name: portInUseRes}

		err := rec.adjustResource(withApplyResult(t.Context(), result), portInUseDesired(), false)
		if err != nil {
			t.Fatalf("%s: adjust: %v", pass, err)
		}

		assertAdjustedInFull(t, fx.CommandLines())

		if got := result.GetReconnectedPeers(); !slices.Equal(got, []string{"n2"}) {
			t.Errorf("%s: reconnected peers = %v, want [n2] reported for the Event", pass, got)
		}
	}
}

// A peer the operator disconnected is still kept: the plan of a full
// adjust only connects it again, so the adjust skips the network and
// reports nothing.
func TestAdjustKeepsAnOperatorDisconnectThePlanOnlyConnects(t *testing.T) {
	t.Parallel()

	fx := portInUseExec()
	fx.Expect("drbdsetup status -j "+portInUseRes, staleStandAloneStatus())
	fx.Expect(staleStandAlonePlan, storage.FakeResponse{Stdout: []byte(
		"drbdsetup connect " + portInUseRes + " 2\n" +
			"drbdsetup wait-connect-connection " + portInUseRes + " 2\n",
	)})

	rec := NewReconciler(ReconcilerConfig{Adm: drbd.NewAdm(fx), StateDir: t.TempDir(), NodeName: "n1"})
	result := &intent.ResourceApplyResult{Name: portInUseRes}

	err := rec.adjustResource(withApplyResult(t.Context(), result), portInUseDesired(), false)
	if err != nil {
		t.Fatalf("adjust: %v", err)
	}

	if cmds := fx.CommandLines(); !slices.Contains(cmds, portInUseSkipNet) || slices.Contains(cmds, portInUseBare) {
		t.Errorf("an operator's disconnect must be kept with --skip-net; got %v", cmds)
	}

	if got := result.GetReconnectedPeers(); len(got) != 0 {
		t.Errorf("reconnected peers = %v, want none for an operator's disconnect", got)
	}
}

// A plan drbdadm cannot produce leaves the decision where it was: the
// StandAlone peer is kept with --skip-net, as before the plan was read.
func TestAdjustKeepsStandAloneWhenThePlanCannotBeRead(t *testing.T) {
	t.Parallel()

	fx := portInUseExec()
	fx.Expect("drbdsetup status -j "+portInUseRes, staleStandAloneStatus())
	fx.Expect(staleStandAlonePlan, storage.FakeResponse{Err: errLowerDevTooSmall})

	rec := NewReconciler(ReconcilerConfig{Adm: drbd.NewAdm(fx), StateDir: t.TempDir(), NodeName: "n1"})

	err := rec.adjustResource(t.Context(), portInUseDesired(), false)
	if err != nil {
		t.Fatalf("adjust: %v", err)
	}

	if cmds := fx.CommandLines(); !slices.Contains(cmds, portInUseSkipNet) {
		t.Errorf("without a plan the StandAlone peer must be kept with --skip-net; got %v", cmds)
	}
}
