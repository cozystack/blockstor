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
	"strconv"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
)

func peerDeviceFrame(t *testing.T, peer, repl string, oos int64) observation {
	t.Helper()

	fields := map[string]string{
		"name": "pvc-two-peers", "volume": "0", "conn-name": peer, "replication": repl,
	}
	if oos >= 0 {
		fields["out-of-sync"] = strconv.FormatInt(oos, 10)
	}

	ev, ok := translatePeerDeviceEvent(drbd.Event{Action: eventActionChange, Kind: eventKindPeerDevice, Fields: fields})
	if !ok {
		t.Fatalf("peer-device frame from %s rejected", peer)
	}

	return ev
}

// A volume's out-of-sync count is the largest over its peers, not the
// one of whichever peer's frame came last. With the last frame's, a
// replica in sync with one peer and not with another flipped between
// zero and the count on every statistics frame, and every flip was a
// status write. The count of a peer whose connection is destroyed no
// longer counts.
func TestObserverOutOfSyncIsTheLargestOverPeers(t *testing.T) {
	o := &ObserverRunnable{}

	ingest := func(ev observation) (bool, int64) {
		o.mergeVolumes(&ev)
		o.mergeConnections(&ev)

		return len(ev.Volumes) > 0, o.volCache["pvc-two-peers"][0].OutOfSyncKib
	}

	if _, got := ingest(peerDeviceFrame(t, "n2", "Established", 4096)); got != 4096 {
		t.Fatalf("first count: %d, want 4096", got)
	}

	steps := []struct {
		what    string
		ev      observation
		changed bool
		want    int64
	}{
		{what: "a peer in sync", ev: peerDeviceFrame(t, "n3", "Established", 0), want: 4096},
		{what: "a peer in sync without a count", ev: peerDeviceFrame(t, "n3", "Established", -1), want: 4096},
		{what: "the stuck peer drifts", ev: peerDeviceFrame(t, "n2", "Established", 4100), changed: true, want: 4100},
		{what: "the peer in sync again", ev: peerDeviceFrame(t, "n3", "Established", 0), want: 4100},
		{what: "the other peer falls behind further", ev: peerDeviceFrame(t, "n3", "Established", 8192), changed: true, want: 8192},
		{
			what:    "that peer's connection is destroyed",
			ev:      observation{ResourceName: "pvc-two-peers", Connections: []connectionObservation{{PeerNodeName: "n3", Removed: true}}},
			changed: true,
			want:    4100,
		},
	}

	for _, step := range steps {
		changed, got := ingest(step.ev)
		if changed != step.changed || got != step.want {
			t.Fatalf("%s: changed=%v count=%d, want changed=%v count=%d", step.what, changed, got, step.changed, step.want)
		}
	}
}
