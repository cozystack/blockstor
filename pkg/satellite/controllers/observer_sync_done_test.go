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

package controllers

import (
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
)

// TestObserverKeepsResyncDonePerPeer replays the events2 frames of a
// replica that resyncs from two peers: a small bitmap resync from one
// finished, a full resync from the other is half way. drbdsetup prints
// `done` on the peer-device frame of the running resync only
// (drbdsetup_events2.c print_peer_device_statistics, from rs_total,
// drbd_nl.c), and the kernel re-broadcasts that frame at most every
// 3 s while the resync clears bits (RS_PROGRESS, drbd_actlog.c).
//
// The progress must stay with its peer. The volume-wide OutOfSyncKib
// is whichever peer's frame came last, which is how `r l --faulty`
// showed `Inconsistent(99%)` against a kernel `done:50.31`. And a
// frame without `done` ends the resync for that peer, as LINSTOR's
// DrbdVolume.update treats it, so a finished resync shows no stale
// progress.
func TestObserverKeepsResyncDonePerPeer(t *testing.T) {
	t.Parallel()

	o := &ObserverRunnable{}

	feed := func(line string) {
		t.Helper()

		ev, err := drbd.ParseEvent(line)
		if err != nil {
			t.Fatalf("ParseEvent(%q): %v", line, err)
		}

		obs, ok := translatePeerDeviceEvent(ev)
		if !ok {
			t.Fatalf("translatePeerDeviceEvent rejected %q", line)
		}

		o.mergeConnections(&obs)
	}

	doneOf := func(peer string) string {
		t.Helper()

		snap := o.snapshotFor("pvc-x")
		for _, c := range buildObserverConnectionStatus(&snap) {
			if c.PeerNodeName != peer {
				continue
			}

			for _, pv := range c.PeerVolumes {
				if pv.VolumeNumber == 0 {
					return pv.Done
				}
			}
		}

		t.Fatalf("no volume 0 entry for peer %s in %+v", peer, snap.Connections)

		return ""
	}

	feed("change peer-device name:pvc-x peer-node-id:0 conn-name:node-a volume:0 " +
		"replication:SyncTarget peer-disk:UpToDate peer-client:no resync-suspended:no " +
		"done:50.31 received:62914560 sent:0 out-of-sync:65011712 pending:0 unacked:0")
	feed("change peer-device name:pvc-x peer-node-id:1 conn-name:node-b volume:0 " +
		"replication:Established peer-disk:UpToDate peer-client:no resync-suspended:no " +
		"received:19424 sent:0 out-of-sync:1258291 pending:0 unacked:0")

	if got := doneOf("node-a"); got != "50.31" {
		t.Errorf("node-a done = %q, want %q (the running full resync)", got, "50.31")
	}

	if got := doneOf("node-b"); got != "" {
		t.Errorf("node-b done = %q, want none (no resync with node-b)", got)
	}

	feed("change peer-device name:pvc-x peer-node-id:0 conn-name:node-a volume:0 " +
		"replication:Established peer-disk:UpToDate peer-client:no resync-suspended:no " +
		"received:125829120 sent:0 out-of-sync:0 pending:0 unacked:0")

	if got := doneOf("node-a"); got != "" {
		t.Errorf("node-a done = %q after the resync ended, want none", got)
	}
}

// TestObserverKeepsReplicationPerVolume replays a two-volume resource
// whose volume 0 resyncs from a peer while volume 1, on the same
// connection, is paused. DRBD reports replication per peer and per
// volume, and LINSTOR keeps it so (DrbdVolume.volReplState); the
// connection-level state is whichever volume's frame came last. Each
// volume must keep its own, or volume 0's `done` would be read against
// volume 1's PausedSyncT and the other way round.
//
// A frame without `peer-disk` still states the volume's replication
// and ends its `done` when it carries none, so no stale progress is
// left behind on such a frame.
func TestObserverKeepsReplicationPerVolume(t *testing.T) {
	t.Parallel()

	o := &ObserverRunnable{}

	feed := func(line string) {
		t.Helper()

		ev, err := drbd.ParseEvent(line)
		if err != nil {
			t.Fatalf("ParseEvent(%q): %v", line, err)
		}

		obs, ok := translatePeerDeviceEvent(ev)
		if !ok {
			t.Fatalf("translatePeerDeviceEvent rejected %q", line)
		}

		o.mergeConnections(&obs)
	}

	volumeOf := func(vol int32) (string, string) {
		t.Helper()

		snap := o.snapshotFor("pvc-x")
		for _, c := range buildObserverConnectionStatus(&snap) {
			for _, pv := range c.PeerVolumes {
				if pv.VolumeNumber == vol {
					return pv.ReplicationState, pv.Done
				}
			}
		}

		t.Fatalf("no volume %d entry in %+v", vol, snap.Connections)

		return "", ""
	}

	feed("change peer-device name:pvc-x peer-node-id:0 conn-name:node-a volume:0 " +
		"replication:SyncTarget peer-disk:UpToDate peer-client:no resync-suspended:no " +
		"done:50.31 received:62914560 sent:0 out-of-sync:65011712 pending:0 unacked:0")
	feed("change peer-device name:pvc-x peer-node-id:0 conn-name:node-a volume:1 " +
		"replication:PausedSyncT peer-disk:UpToDate peer-client:no resync-suspended:dependency " +
		"done:3.00 received:0 sent:0 out-of-sync:1048576 pending:0 unacked:0")

	if repl, done := volumeOf(0); repl != "SyncTarget" || done != "50.31" {
		t.Errorf("volume 0 = (%q, %q), want (SyncTarget, 50.31)", repl, done)
	}

	if repl, done := volumeOf(1); repl != "PausedSyncT" || done != "3.00" {
		t.Errorf("volume 1 = (%q, %q), want (PausedSyncT, 3.00)", repl, done)
	}

	feed("change peer-device name:pvc-x peer-node-id:0 conn-name:node-a volume:0 " +
		"replication:Established")

	if repl, done := volumeOf(0); repl != "Established" || done != "" {
		t.Errorf("volume 0 after a frame without peer-disk = (%q, %q), want (Established, none)", repl, done)
	}
}

// TestObserverFollowsAResyncThatMovesToAnotherPeer replays the frames
// a replica's node sees when its resync starts from one peer and DRBD
// moves it to another in the same state change: the first peer goes
// to PausedSyncT, the second to SyncTarget, both at done 0.00. The
// paused peer must not be left reading SyncTarget, and the running
// one keeps the figure of its last frame until the next one: DRBD
// sends progress only when bits are cleared and at most every 3 s,
// so a resync that stalls before its first report keeps 0.00, which
// is also what LINSTOR shows (DrbdVolume.update).
func TestObserverFollowsAResyncThatMovesToAnotherPeer(t *testing.T) {
	t.Parallel()

	o := &ObserverRunnable{}

	feed := func(line string) {
		t.Helper()

		ev, err := drbd.ParseEvent(line)
		if err != nil {
			t.Fatalf("ParseEvent(%q): %v", line, err)
		}

		obs, ok := translatePeerDeviceEvent(ev)
		if !ok {
			t.Fatalf("translatePeerDeviceEvent rejected %q", line)
		}

		o.mergeConnections(&obs)
	}

	stateOf := func(peer string) (string, string) {
		t.Helper()

		snap := o.snapshotFor("pvc-x")
		for _, c := range buildObserverConnectionStatus(&snap) {
			if c.PeerNodeName != peer {
				continue
			}

			for _, pv := range c.PeerVolumes {
				if pv.VolumeNumber == 0 {
					return pv.ReplicationState, pv.Done
				}
			}
		}

		t.Fatalf("no volume 0 entry for peer %s in %+v", peer, snap.Connections)

		return "", ""
	}

	feed("change peer-device name:pvc-x peer-node-id:0 conn-name:node-a volume:0 " +
		"replication:SyncTarget peer-disk:UpToDate peer-client:no resync-suspended:no " +
		"done:0.00 received:0 sent:0 out-of-sync:172032 pending:0 unacked:0")
	feed("change peer-device name:pvc-x peer-node-id:2 conn-name:node-b volume:0 " +
		"replication:PausedSyncT peer-disk:UpToDate peer-client:no resync-suspended:peer " +
		"done:0.00 received:0 sent:0 out-of-sync:107104 pending:0 unacked:0")
	feed("change peer-device name:pvc-x peer-node-id:0 conn-name:node-a volume:0 " +
		"replication:PausedSyncT peer-disk:UpToDate peer-client:no resync-suspended:dependency " +
		"done:0.00 received:0 sent:0 out-of-sync:172032 pending:0 unacked:0")
	feed("change peer-device name:pvc-x peer-node-id:2 conn-name:node-b volume:0 " +
		"replication:SyncTarget peer-disk:UpToDate peer-client:no resync-suspended:no " +
		"done:0.00 received:0 sent:0 out-of-sync:107104 pending:0 unacked:0")

	if repl, done := stateOf("node-a"); repl != "PausedSyncT" || done != "0.00" {
		t.Errorf("node-a after the switch = (%q, %q), want (PausedSyncT, 0.00)", repl, done)
	}

	if repl, done := stateOf("node-b"); repl != "SyncTarget" || done != "0.00" {
		t.Errorf("node-b after the switch = (%q, %q), want (SyncTarget, 0.00)", repl, done)
	}

	feed("change peer-device name:pvc-x peer-node-id:2 conn-name:node-b volume:0 " +
		"replication:SyncTarget peer-disk:UpToDate peer-client:no resync-suspended:no " +
		"done:65.45 received:70144 sent:0 out-of-sync:37284 pending:0 unacked:0")

	if repl, done := stateOf("node-b"); repl != "SyncTarget" || done != "65.45" {
		t.Errorf("node-b after its progress frame = (%q, %q), want (SyncTarget, 65.45)", repl, done)
	}

	if repl, _ := stateOf("node-a"); repl != "PausedSyncT" {
		t.Errorf("node-a after node-b's progress frame = %q, want PausedSyncT", repl)
	}
}
