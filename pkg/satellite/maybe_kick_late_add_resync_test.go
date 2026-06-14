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
	"context"
	"slices"
	"testing"
	"time"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
)

// Regression pins for the BUG-048 late-add resync-KICK self-heal wiring.
// maybeKickLateAddResync must disconnect+adjust a late-added volume whose
// resync wedged in a paused / bitmap-exchange state that never advances
// (the ≥3-replica concurrent-add convergence wedge), and must NOT act on
// a diskless replica, a healthy progressing sync, or a converged set.

const kickStatusKey = "drbdsetup status pvc-ka --json"

// vol-0/vol-1 UpToDate, the late vol-2 Inconsistent locally, a peer is
// PausedSyncS/resync-suspended:dependency and the partner WFBitMapT/peer
// — the stand-observed dependency-deadlock wedge. No Primary.
const kickStatusWedged = `[{
  "name":"pvc-ka","node-id":0,"role":"Secondary",
  "devices":[
    {"volume":0,"disk-state":"UpToDate"},
    {"volume":1,"disk-state":"UpToDate"},
    {"volume":2,"disk-state":"Inconsistent"}
  ],
  "connections":[
    {"peer-node-id":1,"name":"n2","connection-state":"Connected","peer-role":"Secondary",
     "peer_devices":[
       {"volume":2,"peer-disk-state":"Inconsistent","replication-state":"PausedSyncS","resync-suspended":"dependency"}
     ]},
    {"peer-node-id":2,"name":"n3","connection-state":"Connected","peer-role":"Secondary",
     "peer_devices":[
       {"volume":2,"peer-disk-state":"Inconsistent","replication-state":"WFBitMapT","resync-suspended":"peer"}
     ]}
  ]
}]`

func kickDR() *intent.DesiredResource {
	return &intent.DesiredResource{
		Name:     "pvc-ka",
		NodeName: "n1",
		Volumes: []*intent.DesiredVolume{
			{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
			{VolumeNumber: 1, SizeKib: 1024 * 1024, StoragePool: "thin1"},
			{VolumeNumber: 2, SizeKib: 1024 * 1024, StoragePool: "thin1"},
		},
		Peers: []intent.DesiredPeer{{Name: "n2"}, {Name: "n3"}},
		DrbdOptions: map[string]string{
			"port": "7000", "node-id": "0", "address": "10.0.0.1", "minor": "1000",
		},
	}
}

// The wedge fires the disconnect+adjust kick — but ONLY after the stall
// has persisted beyond the dwell window. The FIRST observation starts the
// dwell (no kick); once the recorded stall is older than lateAddStallDwell
// the kick fires.
func TestMaybeKickLateAddResync_FiresForStalledResync(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect(kickStatusKey, storage.FakeResponse{Stdout: []byte(kickStatusWedged)})

	rec := NewReconciler(ReconcilerConfig{Adm: drbd.NewAdm(fx), NodeName: "n1"})

	// First pass starts the dwell window — must NOT kick yet.
	if err := rec.maybeKickLateAddResync(context.Background(), kickDR(), false); err != nil {
		t.Fatalf("maybeKickLateAddResync pass 1: %v", err)
	}
	if slices.Contains(fx.CommandLines(), "drbdadm disconnect pvc-ka") {
		t.Fatalf("must NOT kick on the first observation (dwell not elapsed), got: %v", fx.CommandLines())
	}

	// Backdate the recorded stall so the dwell has elapsed.
	rec.mu.Lock()
	rec.firstLateAddStallAt["pvc-ka"] = time.Now().Add(-2 * lateAddStallDwell)
	rec.mu.Unlock()

	if err := rec.maybeKickLateAddResync(context.Background(), kickDR(), false); err != nil {
		t.Fatalf("maybeKickLateAddResync pass 2: %v", err)
	}

	cmds := fx.CommandLines()
	if !slices.Contains(cmds, "drbdadm disconnect pvc-ka") {
		t.Errorf("expected `drbdadm disconnect pvc-ka` after dwell, got: %v", cmds)
	}
	if !slices.Contains(cmds, "drbdadm adjust pvc-ka") {
		t.Errorf("expected `drbdadm adjust pvc-ka` (reconnect after disconnect), got: %v", cmds)
	}
}

// A stall that CLEARS (predicate goes false) resets the dwell window — a
// subsequent fresh stall must start its dwell from scratch rather than
// inherit the earlier timestamp and kick immediately.
func TestMaybeKickLateAddResync_DwellResetsWhenStallClears(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect(kickStatusKey, storage.FakeResponse{Stdout: []byte(kickStatusWedged)})

	rec := NewReconciler(ReconcilerConfig{Adm: drbd.NewAdm(fx), NodeName: "n1"})

	// Start the dwell window.
	if err := rec.maybeKickLateAddResync(context.Background(), kickDR(), false); err != nil {
		t.Fatalf("maybeKickLateAddResync pass 1: %v", err)
	}

	// Stall clears (converged) — predicate goes false, dwell is dropped.
	fx.Expect(kickStatusKey, storage.FakeResponse{Stdout: []byte(`[{
	  "name":"pvc-ka","node-id":0,"role":"Secondary",
	  "devices":[{"volume":0,"disk-state":"UpToDate"}],
	  "connections":[{"peer-node-id":1,"name":"n2","connection-state":"Connected","peer-role":"Secondary",
	    "peer_devices":[{"volume":0,"peer-disk-state":"UpToDate","replication-state":"Established","resync-suspended":"no"}]}]
	}]`)})
	if err := rec.maybeKickLateAddResync(context.Background(), kickDR(), false); err != nil {
		t.Fatalf("maybeKickLateAddResync pass 2: %v", err)
	}

	rec.mu.Lock()
	_, stillTracked := rec.firstLateAddStallAt["pvc-ka"]
	rec.mu.Unlock()
	if stillTracked {
		t.Errorf("dwell must be cleared once the stall clears")
	}
}

// A diskless replica has no local resync to kick — skip outright.
func TestMaybeKickLateAddResync_SkipsDiskless(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect(kickStatusKey, storage.FakeResponse{Stdout: []byte(kickStatusWedged)})

	rec := NewReconciler(ReconcilerConfig{Adm: drbd.NewAdm(fx), NodeName: "n1"})

	if err := rec.maybeKickLateAddResync(context.Background(), kickDR(), true); err != nil {
		t.Fatalf("maybeKickLateAddResync: %v", err)
	}

	if slices.Contains(fx.CommandLines(), "drbdadm disconnect pvc-ka") {
		t.Errorf("diskless replica must NOT be kicked, got: %v", fx.CommandLines())
	}
}

// A healthy progressing SyncSource (resync-suspended "no") must NOT be
// kicked — the kick would abort a sync that finishes on its own.
func TestMaybeKickLateAddResync_SkipsHealthySync(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect(kickStatusKey, storage.FakeResponse{Stdout: []byte(`[{
	  "name":"pvc-ka","node-id":0,"role":"Secondary",
	  "devices":[
	    {"volume":0,"disk-state":"UpToDate"},
	    {"volume":2,"disk-state":"Inconsistent"}
	  ],
	  "connections":[{
	    "peer-node-id":1,"name":"n2","connection-state":"Connected","peer-role":"Secondary",
	    "peer_devices":[
	      {"volume":2,"peer-disk-state":"Inconsistent","replication-state":"SyncSource","resync-suspended":"no"}
	    ]
	  }]
	}]`)})

	rec := NewReconciler(ReconcilerConfig{Adm: drbd.NewAdm(fx), NodeName: "n1"})

	if err := rec.maybeKickLateAddResync(context.Background(), kickDR(), false); err != nil {
		t.Fatalf("maybeKickLateAddResync: %v", err)
	}

	if slices.Contains(fx.CommandLines(), "drbdadm disconnect pvc-ka") {
		t.Errorf("must NOT kick a healthy progressing SyncSource, got: %v", fx.CommandLines())
	}
}

// The kick is throttled by the shared recoveryPromoteThrottle so a
// still-converging resync isn't churned: a second back-to-back call
// inside the window must NOT re-disconnect.
func TestMaybeKickLateAddResync_Throttled(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect(kickStatusKey, storage.FakeResponse{Stdout: []byte(kickStatusWedged)})

	rec := NewReconciler(ReconcilerConfig{Adm: drbd.NewAdm(fx), NodeName: "n1"})

	// Pre-satisfy the dwell so both passes are past the dwell gate and the
	// throttle is the only thing limiting the kick.
	rec.mu.Lock()
	rec.firstLateAddStallAt["pvc-ka"] = time.Now().Add(-2 * lateAddStallDwell)
	rec.mu.Unlock()

	for i := range 2 {
		if err := rec.maybeKickLateAddResync(context.Background(), kickDR(), false); err != nil {
			t.Fatalf("maybeKickLateAddResync pass %d: %v", i, err)
		}
	}

	disconnects := 0
	for _, c := range fx.CommandLines() {
		if c == "drbdadm disconnect pvc-ka" {
			disconnects++
		}
	}
	if disconnects != 1 {
		t.Errorf("expected exactly 1 disconnect within the throttle window, got %d", disconnects)
	}
}
