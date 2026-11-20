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
// drbd_nl.c), and the kernel re-broadcasts that frame every few
// seconds while it runs (RS_PROGRESS, drbd_actlog.c).
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
