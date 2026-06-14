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
// maybeKickLateAddResync must `drbdadm invalidate <res>/<vol>` each stalled
// late-added volume that has an UpToDate peer (re-pull FROM it), and must
// NOT act on a diskless replica, an actively-pulling SyncTarget, or a
// volume with no UpToDate peer.

const kickStatusKey = "drbdsetup status pvc-ka --json"

// vol-0 UpToDate, late vol-2 Inconsistent locally with an UpToDate peer
// whose resync is stalled (PausedSyncT/dependency) — the recoverable
// straggler. No Primary.
const kickStatusWedged = `[{
  "name":"pvc-ka","node-id":2,"role":"Secondary",
  "devices":[
    {"volume":0,"disk-state":"UpToDate"},
    {"volume":2,"disk-state":"Inconsistent"}
  ],
  "connections":[{
    "peer-node-id":0,"name":"n1","connection-state":"Connected","peer-role":"Secondary",
    "peer_devices":[
      {"volume":0,"peer-disk-state":"UpToDate","replication-state":"Established","resync-suspended":"no"},
      {"volume":2,"peer-disk-state":"UpToDate","replication-state":"PausedSyncT","resync-suspended":"dependency"}
    ]
  }]
}]`

func kickDR() *intent.DesiredResource {
	return &intent.DesiredResource{
		Name:     "pvc-ka",
		NodeName: "n3",
		Volumes: []*intent.DesiredVolume{
			{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
			{VolumeNumber: 2, SizeKib: 1024 * 1024, StoragePool: "thin1"},
		},
		Peers: []intent.DesiredPeer{{Name: "n1"}},
		DrbdOptions: map[string]string{
			"port": "7000", "node-id": "2", "address": "10.0.0.3", "minor": "1000",
		},
	}
}

// The wedge fires the per-volume invalidate — but ONLY after the stall
// has persisted beyond the dwell window.
func TestMaybeKickLateAddResync_InvalidatesStalledVolume(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect(kickStatusKey, storage.FakeResponse{Stdout: []byte(kickStatusWedged)})

	rec := NewReconciler(ReconcilerConfig{Adm: drbd.NewAdm(fx), NodeName: "n3"})

	// First pass starts the dwell window — must NOT invalidate yet.
	if err := rec.maybeKickLateAddResync(context.Background(), kickDR(), false); err != nil {
		t.Fatalf("maybeKickLateAddResync pass 1: %v", err)
	}
	if slices.Contains(fx.CommandLines(), "drbdadm invalidate pvc-ka/2") {
		t.Fatalf("must NOT invalidate on the first observation (dwell not elapsed), got: %v", fx.CommandLines())
	}

	// Backdate the recorded stall so the dwell has elapsed.
	rec.mu.Lock()
	rec.firstLateAddStallAt["pvc-ka"] = time.Now().Add(-2 * lateAddStallDwell)
	rec.mu.Unlock()

	if err := rec.maybeKickLateAddResync(context.Background(), kickDR(), false); err != nil {
		t.Fatalf("maybeKickLateAddResync pass 2: %v", err)
	}

	if !slices.Contains(fx.CommandLines(), "drbdadm invalidate pvc-ka/2") {
		t.Errorf("expected `drbdadm invalidate pvc-ka/2` after dwell, got: %v", fx.CommandLines())
	}
	// Must NOT use the abandoned disconnect/adjust handshake kick.
	if slices.Contains(fx.CommandLines(), "drbdadm disconnect pvc-ka") {
		t.Errorf("must NOT disconnect (re-handshake abandons the bitmap), got: %v", fx.CommandLines())
	}
}

// A stall that CLEARS (no volume returned) resets the dwell window.
func TestMaybeKickLateAddResync_DwellResetsWhenStallClears(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect(kickStatusKey, storage.FakeResponse{Stdout: []byte(kickStatusWedged)})

	rec := NewReconciler(ReconcilerConfig{Adm: drbd.NewAdm(fx), NodeName: "n3"})

	if err := rec.maybeKickLateAddResync(context.Background(), kickDR(), false); err != nil {
		t.Fatalf("maybeKickLateAddResync pass 1: %v", err)
	}

	// Converged — no stalled volume returned, dwell dropped.
	fx.Expect(kickStatusKey, storage.FakeResponse{Stdout: []byte(`[{
	  "name":"pvc-ka","node-id":2,"role":"Secondary",
	  "devices":[{"volume":0,"disk-state":"UpToDate"},{"volume":2,"disk-state":"UpToDate"}],
	  "connections":[{"peer-node-id":0,"name":"n1","connection-state":"Connected","peer-role":"Secondary",
	    "peer_devices":[
	      {"volume":0,"peer-disk-state":"UpToDate","replication-state":"Established","resync-suspended":"no"},
	      {"volume":2,"peer-disk-state":"UpToDate","replication-state":"Established","resync-suspended":"no"}]}]
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

// A diskless replica has no local copy to invalidate — skip outright.
func TestMaybeKickLateAddResync_SkipsDiskless(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect(kickStatusKey, storage.FakeResponse{Stdout: []byte(kickStatusWedged)})

	rec := NewReconciler(ReconcilerConfig{Adm: drbd.NewAdm(fx), NodeName: "n3"})

	if err := rec.maybeKickLateAddResync(context.Background(), kickDR(), true); err != nil {
		t.Fatalf("maybeKickLateAddResync: %v", err)
	}

	if slices.Contains(fx.CommandLines(), "drbdadm invalidate pvc-ka/2") {
		t.Errorf("diskless replica must NOT be invalidated, got: %v", fx.CommandLines())
	}
}

// A live, unsuspended SyncTarget must NOT be invalidated — it is actively
// pulling and finishes on its own.
func TestMaybeKickLateAddResync_SkipsActiveSyncTarget(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect(kickStatusKey, storage.FakeResponse{Stdout: []byte(`[{
	  "name":"pvc-ka","node-id":2,"role":"Secondary",
	  "devices":[{"volume":0,"disk-state":"UpToDate"},{"volume":2,"disk-state":"Inconsistent"}],
	  "connections":[{"peer-node-id":0,"name":"n1","connection-state":"Connected","peer-role":"Secondary",
	    "peer_devices":[
	      {"volume":2,"peer-disk-state":"UpToDate","replication-state":"SyncTarget","resync-suspended":"no"}]}]
	}]`)})

	rec := NewReconciler(ReconcilerConfig{Adm: drbd.NewAdm(fx), NodeName: "n3"})

	if err := rec.maybeKickLateAddResync(context.Background(), kickDR(), false); err != nil {
		t.Fatalf("maybeKickLateAddResync: %v", err)
	}

	if slices.Contains(fx.CommandLines(), "drbdadm invalidate pvc-ka/2") {
		t.Errorf("must NOT invalidate a live SyncTarget, got: %v", fx.CommandLines())
	}
}

// Throttled by the shared recoveryPromoteThrottle: a second back-to-back
// pass inside the window (dwell pre-satisfied) must NOT re-invalidate.
func TestMaybeKickLateAddResync_Throttled(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect(kickStatusKey, storage.FakeResponse{Stdout: []byte(kickStatusWedged)})

	rec := NewReconciler(ReconcilerConfig{Adm: drbd.NewAdm(fx), NodeName: "n3"})

	rec.mu.Lock()
	rec.firstLateAddStallAt["pvc-ka"] = time.Now().Add(-2 * lateAddStallDwell)
	rec.mu.Unlock()

	for i := range 2 {
		if err := rec.maybeKickLateAddResync(context.Background(), kickDR(), false); err != nil {
			t.Fatalf("maybeKickLateAddResync pass %d: %v", i, err)
		}
	}

	invals := 0
	for _, c := range fx.CommandLines() {
		if c == "drbdadm invalidate pvc-ka/2" {
			invals++
		}
	}
	if invals != 1 {
		t.Errorf("expected exactly 1 invalidate within the throttle window, got %d", invals)
	}
}
