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
	"sort"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
)

// TestTranslateEventConnection pins the events2 → connectionObservation
// path that drives `linstor r list --faulty`. drbd-9 emits one frame
// per peer per state-transition; we must reject malformed frames
// (missing conn-name / connection) so writeStatus doesn't claim an
// empty peer slot via SSA.
func TestTranslateEventConnection(t *testing.T) {
	cases := []struct {
		name      string
		fields    map[string]string
		wantOK    bool
		wantRes   string
		wantPeer  string
		wantConn  bool
		wantState string
	}{
		{
			name: "connected peer",
			fields: map[string]string{
				"name":         "pvc-1",
				"peer-node-id": "1",
				"conn-name":    "node-b",
				"connection":   "Connected",
				"role":         "Secondary",
			},
			wantOK: true, wantRes: "pvc-1", wantPeer: "node-b",
			wantConn: true, wantState: "Connected",
		},
		{
			name: "broken peer",
			fields: map[string]string{
				"name":       "pvc-1",
				"conn-name":  "node-c",
				"connection": "BrokenPipe",
			},
			wantOK: true, wantRes: "pvc-1", wantPeer: "node-c",
			wantConn: false, wantState: "BrokenPipe",
		},
		{
			name: "connecting transitional state — not Connected, so not connected",
			fields: map[string]string{
				"name":       "pvc-1",
				"conn-name":  "node-c",
				"connection": "Connecting",
			},
			wantOK: true, wantRes: "pvc-1", wantPeer: "node-c",
			wantConn: false, wantState: "Connecting",
		},
		{
			name:   "missing conn-name → reject",
			fields: map[string]string{"name": "pvc-1", "connection": "Connected"},
			wantOK: false,
		},
		{
			name:   "missing connection state → reject",
			fields: map[string]string{"name": "pvc-1", "conn-name": "node-b"},
			wantOK: false,
		},
		{
			name:   "missing resource name → reject",
			fields: map[string]string{"conn-name": "node-b", "connection": "Connected"},
			wantOK: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			obs, ok := translateEvent(drbd.Event{
				Action: "change",
				Kind:   "connection",
				Fields: tc.fields,
			})

			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v (obs=%+v)", ok, tc.wantOK, obs)
			}

			if !tc.wantOK {
				return
			}

			if obs.ResourceName != tc.wantRes {
				t.Errorf("ResourceName = %q, want %q", obs.ResourceName, tc.wantRes)
			}

			if len(obs.Connections) != 1 {
				t.Fatalf("Connections len = %d, want 1: %+v", len(obs.Connections), obs.Connections)
			}

			c := obs.Connections[0]
			if c.PeerNodeName != tc.wantPeer {
				t.Errorf("PeerNodeName = %q, want %q", c.PeerNodeName, tc.wantPeer)
			}

			if c.Connected != tc.wantConn {
				t.Errorf("Connected = %v, want %v", c.Connected, tc.wantConn)
			}

			if c.Message != tc.wantState {
				t.Errorf("Message = %q, want %q", c.Message, tc.wantState)
			}
		})
	}
}

// TestMergeConnectionsSnapshotsAllPeers pins the SSA-overwrite fix:
// every connection-event apply MUST emit the full peer snapshot,
// not just the peer the event named. Otherwise SSA with the same
// FieldOwner drops every peer not present in the new apply, and
// `Status.Connections` collapses to a single entry per event.
//
// Reproduces the original bug observed on e2e1: events for
// worker-2 then worker-3 left only one peer in Status.
func TestMergeConnectionsSnapshotsAllPeers(t *testing.T) {
	o := &ObserverRunnable{}

	// First event: worker-2 connects.
	first := &observation{
		ResourceName: "pvc-1",
		Connections: []connectionObservation{
			{PeerNodeName: "worker-2", Connected: true, Message: "Connected"},
		},
	}
	o.mergeConnections(first)

	gotPeers := peerSet(first.Connections)
	if !sameSet(gotPeers, []string{"worker-2"}) {
		t.Errorf("first apply peers = %v, want [worker-2]", gotPeers)
	}

	// Second event: worker-3 in Connecting state. Must NOT erase
	// worker-2 from the apply slice — that was the bug.
	second := &observation{
		ResourceName: "pvc-1",
		Connections: []connectionObservation{
			{PeerNodeName: "worker-3", Connected: false, Message: "Connecting"},
		},
	}
	o.mergeConnections(second)

	gotPeers = peerSet(second.Connections)
	if !sameSet(gotPeers, []string{"worker-2", "worker-3"}) {
		t.Errorf("second apply peers = %v, want [worker-2 worker-3]", gotPeers)
	}

	// Third event: worker-2 transitions to BrokenPipe. The merge
	// updates the existing entry, the snapshot still has both peers.
	third := &observation{
		ResourceName: "pvc-1",
		Connections: []connectionObservation{
			{PeerNodeName: "worker-2", Connected: false, Message: "BrokenPipe"},
		},
	}
	o.mergeConnections(third)

	got := connByPeer(third.Connections)
	if got["worker-2"].Message != "BrokenPipe" || got["worker-2"].Connected {
		t.Errorf("worker-2 not updated: %+v", got["worker-2"])
	}

	if got["worker-3"].Message != "Connecting" {
		t.Errorf("worker-3 not preserved: %+v", got["worker-3"])
	}
}

// TestMergeConnectionsIsolatesResources guards against cross-resource
// pollution: two RDs share the observer's connCache map and must
// not see each other's peers.
func TestMergeConnectionsIsolatesResources(t *testing.T) {
	o := &ObserverRunnable{}

	o.mergeConnections(&observation{
		ResourceName: "pvc-A",
		Connections: []connectionObservation{
			{PeerNodeName: "worker-2", Connected: true, Message: "Connected"},
		},
	})

	ev := &observation{
		ResourceName: "pvc-B",
		Connections: []connectionObservation{
			{PeerNodeName: "worker-3", Connected: true, Message: "Connected"},
		},
	}
	o.mergeConnections(ev)

	gotPeers := peerSet(ev.Connections)
	if !sameSet(gotPeers, []string{"worker-3"}) {
		t.Errorf("pvc-B leaked pvc-A's peers: got %v, want [worker-3]", gotPeers)
	}
}

// TestMergeConnectionsNoopForNonConnectionEvents — disk/role events
// arrive without a Connections slice; the merge must leave the
// observation untouched (we don't want every kernel frame to
// re-broadcast the entire cached snapshot, that would explode SSA
// traffic).
func TestMergeConnectionsNoopForNonConnectionEvents(t *testing.T) {
	o := &ObserverRunnable{}

	// Seed the cache with one peer.
	o.mergeConnections(&observation{
		ResourceName: "pvc-1",
		Connections: []connectionObservation{
			{PeerNodeName: "worker-2", Connected: true},
		},
	})

	// Volume / role event — no connection data.
	ev := &observation{
		ResourceName: "pvc-1",
		InUse:        true,
		DrbdState:    "UpToDate",
	}
	o.mergeConnections(ev)

	if len(ev.Connections) != 0 {
		t.Errorf("non-connection event got Connections populated: %+v", ev.Connections)
	}
}

// TestBuildObserverConnectionStatus pins the wire-side projection
// the satellite SSA patch consumes. Nil/empty in → nil out (so the
// apply object stays narrow and doesn't claim an empty slot).
func TestBuildObserverConnectionStatus(t *testing.T) {
	if got := buildObserverConnectionStatus(&observation{}); got != nil {
		t.Errorf("empty observation: got %v, want nil", got)
	}

	got := buildObserverConnectionStatus(&observation{
		Connections: []connectionObservation{
			{PeerNodeName: "n2", Connected: true, Message: "Connected"},
			{PeerNodeName: "n3", Connected: false, Message: "BrokenPipe"},
		},
	})

	if len(got) != 2 {
		t.Fatalf("len = %d, want 2: %+v", len(got), got)
	}

	byPeer := map[string]struct {
		conn bool
		msg  string
	}{}

	for _, c := range got {
		byPeer[c.PeerNodeName] = struct {
			conn bool
			msg  string
		}{c.Connected, c.Message}
	}

	if byPeer["n2"].msg != "Connected" || !byPeer["n2"].conn {
		t.Errorf("n2 wrong: %+v", byPeer["n2"])
	}

	if byPeer["n3"].msg != "BrokenPipe" || byPeer["n3"].conn {
		t.Errorf("n3 wrong: %+v", byPeer["n3"])
	}
}

func peerSet(cs []connectionObservation) []string {
	out := make([]string, len(cs))
	for i := range cs {
		out[i] = cs[i].PeerNodeName
	}

	sort.Strings(out)

	return out
}

func connByPeer(cs []connectionObservation) map[string]connectionObservation {
	out := map[string]connectionObservation{}
	for i := range cs {
		out[cs[i].PeerNodeName] = cs[i]
	}

	return out
}

func sameSet(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}

	sort.Strings(got)
	sort.Strings(want)

	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}

	return true
}

// TestTranslateResourceEventHasResource pins the HasResource flag on
// the resource-kind observation. mergeResource keys off this flag
// when deciding whether to cache InUse — without HasResource=true,
// connection-kind events would clobber the cached value with their
// zero-default and the apiserver-side f:inUse claim disappears.
func TestTranslateResourceEventHasResource(t *testing.T) {
	cases := []struct {
		name      string
		role      string
		wantInUse bool
	}{
		{"primary => InUse=true", "Primary", true},
		{"secondary => InUse=false", "Secondary", false},
		{"unknown role still emits HasResource", "Connecting", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ev := drbd.Event{
				Kind:   "resource",
				Action: "change",
				Fields: map[string]string{
					"name": "pvc-0",
					"role": tc.role,
				},
			}

			obs, ok := translateResourceEvent(ev)
			if !ok {
				t.Fatalf("translateResourceEvent rejected %+v", ev)
			}

			if !obs.HasResource {
				t.Errorf("HasResource: got false, want true (always set on resource-kind events)")
			}

			if obs.InUse != tc.wantInUse {
				t.Errorf("InUse: got %v, want %v", obs.InUse, tc.wantInUse)
			}
		})
	}
}

// TestMergeResourceCachesInUseAcrossNonResourceEvents pins the
// session-fix root cause: after a resource-kind event sets
// InUse=true, a subsequent connection-kind event would carry
// InUse=false (zero-default). Without mergeResource caching the
// last HasResource value, the second event's apply strips the
// f:inUse claim and the apiserver deletes the field.
//
// Auto-diskful + two-primaries-live-migration both regress when
// this caching breaks.
func TestMergeResourceCachesInUseAcrossNonResourceEvents(t *testing.T) {
	o := &ObserverRunnable{}

	// 1. resource-kind event flips the replica to Primary.
	primary := observation{
		ResourceName: "pvc-0",
		InUse:        true,
		HasResource:  true,
	}
	o.mergeResource(&primary)

	if !primary.InUse {
		t.Fatalf("primary event: InUse got %v, want true", primary.InUse)
	}

	// 2. Connection-kind event ~10ms later carries InUse=false
	//    (zero-default) and HasResource=false. mergeResource must
	//    re-emit the cached true value.
	connEvent := observation{
		ResourceName: "pvc-0",
		// InUse unset (zero) — simulating non-resource event
		Connections: []connectionObservation{{
			PeerNodeName: "peer-a",
			Connected:    true,
			Message:      "Connected",
		}},
	}
	o.mergeResource(&connEvent)

	if !connEvent.InUse {
		t.Errorf("connection event after Primary: InUse re-emit got %v, want true", connEvent.InUse)
	}

	// 3. Explicit Secondary transition (HasResource=true,
	//    InUse=false) replaces the cache.
	secondary := observation{
		ResourceName: "pvc-0",
		InUse:        false,
		HasResource:  true,
	}
	o.mergeResource(&secondary)

	if secondary.InUse {
		t.Errorf("secondary event: InUse got %v, want false (HasResource overrides cache)", secondary.InUse)
	}

	// 4. Subsequent non-resource event must NOT spring back to
	//    the previously-cached true — Secondary is now the
	//    authoritative state.
	connAfterSecondary := observation{
		ResourceName: "pvc-0",
		Connections: []connectionObservation{{
			PeerNodeName: "peer-a",
			Message:      "Connected",
		}},
	}
	o.mergeResource(&connAfterSecondary)

	if connAfterSecondary.InUse {
		t.Errorf("connection event after Secondary: InUse got %v, want false", connAfterSecondary.InUse)
	}
}

// TestMergeResourceCachesDrbdStateAcrossNonResourceEvents pins the
// same re-emit guarantee for DrbdState. Disk transitions only fire
// on device-kind events; without the cache, connection events
// between disk transitions would strip the f:drbdState claim.
func TestMergeResourceCachesDrbdStateAcrossNonResourceEvents(t *testing.T) {
	o := &ObserverRunnable{}

	deviceUpToDate := observation{
		ResourceName: "pvc-0",
		DrbdState:    "UpToDate",
	}
	o.mergeResource(&deviceUpToDate)

	if deviceUpToDate.DrbdState != "UpToDate" {
		t.Fatalf("device event: DrbdState got %q, want UpToDate", deviceUpToDate.DrbdState)
	}

	connEvent := observation{
		ResourceName: "pvc-0",
		Connections: []connectionObservation{{
			PeerNodeName: "peer-a",
		}},
	}
	o.mergeResource(&connEvent)

	if connEvent.DrbdState != "UpToDate" {
		t.Errorf("connection event: DrbdState re-emit got %q, want UpToDate", connEvent.DrbdState)
	}
}

// TestMergeResourceIsolatesResources pins the per-resource keying.
// The InUse=true on pvc-0 must not leak into pvc-1's observations.
func TestMergeResourceIsolatesResources(t *testing.T) {
	o := &ObserverRunnable{}

	o.mergeResource(&observation{
		ResourceName: "pvc-0",
		InUse:        true,
		HasResource:  true,
	})

	other := observation{
		ResourceName: "pvc-1",
		Connections: []connectionObservation{{
			PeerNodeName: "peer-a",
		}},
	}
	o.mergeResource(&other)

	if other.InUse {
		t.Errorf("pvc-1 inherited pvc-0's InUse=true (got %v, want false)", other.InUse)
	}
}

// TestMergeResourceEmptyResourceNameNoop guards the early return.
// Events without a resource name must NOT populate the cache —
// otherwise a malformed event would corrupt the per-resource
// state map.
func TestMergeResourceEmptyResourceNameNoop(t *testing.T) {
	o := &ObserverRunnable{}

	o.mergeResource(&observation{HasResource: true, InUse: true})

	realEv := observation{
		ResourceName: "pvc-0",
		Connections:  []connectionObservation{{PeerNodeName: "peer-a"}},
	}
	o.mergeResource(&realEv)

	if realEv.InUse {
		t.Errorf("malformed event leaked into pvc-0 cache: InUse got %v, want false", realEv.InUse)
	}
}
