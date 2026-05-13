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
	"context"
	"sort"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/storage"
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

// TestObserverReportsPausedSyncS (scenario 5.25) pins the peer-device
// translation path for `replication:PausedSyncS`. DRBD-9 emits this
// token when a SyncTarget has been paused — typically because the
// resync was suspended via `resync-suspended:dependency` (another
// peer holds the only UpToDate copy of a region we need, and DRBD
// blocks the resync until that peer comes back). Operators recover
// from this by `drbdadm disconnect <r>:<peer>` + `drbdadm connect
// <r>:<peer>` to force a fresh handshake to the Primary; the
// reconciler MUST NOT auto-resume by doing `drbdadm adjust` (that
// re-renders connection-config and the kernel restarts the resync
// from 0, which is exactly Bug 8's failure mode).
//
// This test exercises only the observer side: the events2
// `peer-device` frame carrying `replication:PausedSyncS` must
// surface intact on the observation so the satellite SSA patch
// projects it onto Resource.Status.Connections[*].replicationState
// — that's how the controller (and the operator running
// `linstor v l`) sees the paused state and decides to apply the
// recipe.
//
// Variants:
//   - Bare PausedSyncS frame (DRBD-9 abbreviated form).
//   - PausedSyncS together with out-of-sync stats (the long form
//     drbdsetup emits when `--statistics` is on; covers the path
//     where a single event carries both volume and connection
//     observations).
//   - Surrounding states the operator can also observe during a
//     dependency pause (`PausedSyncT`, `Behind`) — same translation
//     contract, recorded here so a future refactor of the
//     replication-state passthrough doesn't accidentally
//     special-case PausedSyncS while dropping its cousins.
func TestObserverReportsPausedSyncS(t *testing.T) {
	cases := []struct {
		name     string
		fields   map[string]string
		wantRepl string
		wantOos  int64
		wantHas  bool
	}{
		{
			name: "bare PausedSyncS frame",
			fields: map[string]string{
				"name":         "pvc-1",
				"peer-node-id": "1",
				"volume":       "0",
				"conn-name":    "node-b",
				"replication":  "PausedSyncS",
			},
			wantRepl: "PausedSyncS",
		},
		{
			name: "PausedSyncS with out-of-sync stats",
			fields: map[string]string{
				"name":         "pvc-1",
				"peer-node-id": "1",
				"volume":       "0",
				"conn-name":    "node-b",
				"replication":  "PausedSyncS",
				"out-of-sync":  "524288", // 512 MiB still to ship
			},
			wantRepl: "PausedSyncS",
			wantOos:  524288,
			wantHas:  true,
		},
		{
			name: "PausedSyncT (peer-side paused) — same translation contract",
			fields: map[string]string{
				"name":         "pvc-1",
				"peer-node-id": "1",
				"volume":       "0",
				"conn-name":    "node-b",
				"replication":  "PausedSyncT",
			},
			wantRepl: "PausedSyncT",
		},
		{
			name: "Behind (dependency-pause sibling) — pass through",
			fields: map[string]string{
				"name":         "pvc-1",
				"peer-node-id": "1",
				"volume":       "0",
				"conn-name":    "node-b",
				"replication":  "Behind",
			},
			wantRepl: "Behind",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			obs, ok := translatePeerDeviceEvent(drbd.Event{
				Action: "change",
				Kind:   "peer-device",
				Fields: tc.fields,
			})

			if !ok {
				t.Fatalf("translatePeerDeviceEvent rejected %+v", tc.fields)
			}

			if obs.ResourceName != "pvc-1" {
				t.Errorf("ResourceName = %q, want pvc-1", obs.ResourceName)
			}

			if len(obs.Connections) != 1 {
				t.Fatalf("Connections len = %d, want 1: %+v", len(obs.Connections), obs.Connections)
			}

			c := obs.Connections[0]
			if c.PeerNodeName != "node-b" {
				t.Errorf("PeerNodeName = %q, want node-b", c.PeerNodeName)
			}

			if c.ReplicationState != tc.wantRepl {
				t.Errorf("ReplicationState = %q, want %q", c.ReplicationState, tc.wantRepl)
			}

			// Connected/Message belong to the connection-kind path; the
			// peer-device translator must not synthesize them — only the
			// kernel's `connection:<state>` frame is authoritative for
			// connection liveness. A PausedSyncS resync runs on top of a
			// Connected link, so claiming Connected=true here would
			// double-write the per-peer status entry and clobber the
			// real connection-kind frame's claim under SSA.
			if c.Connected {
				t.Errorf("Connected = true, want false (peer-device must not set Connected)")
			}

			if c.Message != "" {
				t.Errorf("Message = %q, want empty (peer-device must not set Message)", c.Message)
			}

			// Out-of-sync passthrough: when the frame carries
			// `out-of-sync:<kib>`, the same observation must also surface
			// the per-volume Volumes entry. Without it, the operator
			// can't see resync-progress alongside the PausedSyncS state
			// — both flow from the same event under --statistics.
			if tc.wantHas {
				if len(obs.Volumes) != 1 {
					t.Fatalf("Volumes len = %d, want 1: %+v", len(obs.Volumes), obs.Volumes)
				}

				if obs.Volumes[0].OutOfSyncKib != tc.wantOos {
					t.Errorf("OutOfSyncKib = %d, want %d", obs.Volumes[0].OutOfSyncKib, tc.wantOos)
				}

				if !obs.Volumes[0].HasSync {
					t.Errorf("HasSync = false, want true (out-of-sync was present)")
				}
			} else if len(obs.Volumes) != 0 {
				t.Errorf("Volumes populated without out-of-sync: %+v", obs.Volumes)
			}
		})
	}
}

// TestMergeConnectionsPreservesPausedSyncS pins the cache-merge half
// of the PausedSyncS path. The peer-device-kind event sets only
// `ReplicationState`; the connection-kind event sets
// `Connected`/`Message` independently. Both must coexist on the
// merged per-peer entry — without this, an interleaved
// `connection:Connected` frame would zero the just-recorded
// `replication:PausedSyncS` (or vice-versa), and the operator's
// `linstor v l` Repl column would flicker.
//
// Mirrors the SSA contract documented on writeStatus: connection-
// kind frames own f:connected/f:message; peer-device-kind frames
// own f:replicationState. Both apply under the same FieldOwner so
// the merge logic — not SSA — has to keep both contributions live
// across event arrival order.
func TestMergeConnectionsPreservesPausedSyncS(t *testing.T) {
	o := &ObserverRunnable{}

	// 1. peer-device frame arrives first: replication:PausedSyncS.
	pd := &observation{
		ResourceName: "pvc-1",
		Connections: []connectionObservation{
			{PeerNodeName: "node-b", ReplicationState: "PausedSyncS"},
		},
	}
	o.mergeConnections(pd)

	got := connByPeer(pd.Connections)
	if got["node-b"].ReplicationState != "PausedSyncS" {
		t.Fatalf("first apply: ReplicationState = %q, want PausedSyncS",
			got["node-b"].ReplicationState)
	}

	// 2. connection-kind frame arrives next: connection:Connected.
	//    Must NOT erase the PausedSyncS already in cache.
	conn := &observation{
		ResourceName: "pvc-1",
		Connections: []connectionObservation{
			{PeerNodeName: "node-b", Connected: true, Message: "Connected"},
		},
	}
	o.mergeConnections(conn)

	got = connByPeer(conn.Connections)
	if got["node-b"].ReplicationState != "PausedSyncS" {
		t.Errorf("Connected event erased PausedSyncS: ReplicationState = %q",
			got["node-b"].ReplicationState)
	}

	if !got["node-b"].Connected || got["node-b"].Message != "Connected" {
		t.Errorf("Connected/Message dropped: %+v", got["node-b"])
	}

	// 3. peer-device frame transitions to Established (resync
	//    resumed, dependency cleared). The cache must replace
	//    PausedSyncS but keep Connected/Message intact.
	resumed := &observation{
		ResourceName: "pvc-1",
		Connections: []connectionObservation{
			{PeerNodeName: "node-b", ReplicationState: "Established"},
		},
	}
	o.mergeConnections(resumed)

	got = connByPeer(resumed.Connections)
	if got["node-b"].ReplicationState != "Established" {
		t.Errorf("post-resume: ReplicationState = %q, want Established",
			got["node-b"].ReplicationState)
	}

	if !got["node-b"].Connected || got["node-b"].Message != "Connected" {
		t.Errorf("post-resume: Connected/Message lost: %+v", got["node-b"])
	}
}

// TestObserverWritesSkipDiskOnFailed (scenario 5.11) pins the
// observer's response to `change device disk:Failed` from
// drbdsetup events2: alongside the auto-detach (which transitions
// the volume to Diskless so the kernel stops issuing I/O at the
// dead lower disk), the observer MUST stamp
// `DrbdOptions/SkipDisk=True` onto the matching Resource's
// Spec.Props. Without that prop, the next `drbdadm adjust` would
// re-attempt disk attachment and fail.
//
// Mirrors upstream linstor's StateSequenceDetector which auto-
// stamps the same prop on Failed→Diskless (controller/.../event/
// handler/StateSequenceDetector.java:67) — implementing it in the
// satellite (not controller) here because our event observer
// runs on the satellite and we'd rather not synthesise the
// transition on the controller from the SSA Status update.
//
// Verifies:
//   - The Resource.Spec.Props["DrbdOptions/SkipDisk"] key lands
//     with value "True" (case-sensitive on write; upstream reads
//     case-insensitively).
//   - Pre-existing Spec.Props entries are preserved — SSA's
//     per-key map merge must not collapse the bag down to just
//     SkipDisk.
//   - The required Spec scalars (resourceDefinitionName,
//     nodeName) survive unchanged.
//   - `drbdadm detach --force <rsc>` still ran. The two side-
//     effects are independent and both must converge.
//   - NotFound on the Get is silenced (convergence-pending case
//     where the Resource CRD hasn't been created yet — same
//     contract writeStatus already honours).
func TestObserverWritesSkipDiskOnFailed(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = blockstoriov1alpha1.AddToScheme(scheme)

	existing := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.n1"},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "pvc-1",
			NodeName:               "n1",
			Props: map[string]string{
				// Pre-existing entry the dispatcher landed: the SkipDisk
				// SSA must NOT clobber it. SSA's per-key merge on the
				// map keeps both keys alive when the new owner only
				// claims SkipDisk.
				"StorPoolName": "thin1",
			},
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(existing).
		Build()

	fx := storage.NewFakeExec()

	o := &ObserverRunnable{
		Client:   cli,
		Exec:     fx,
		NodeName: "n1",
	}

	adm := drbd.NewAdm(fx)

	// The events2 frame: `change device name:pvc-1 disk:Failed`.
	// Drives both side-effects — detach + SkipDisk prop write.
	ev := &observation{
		ResourceName: "pvc-1",
		DrbdState:    drbdDiskStateFailed,
		Volumes: []volumeObservation{
			{VolumeNumber: 0, DiskState: drbdDiskStateFailed},
		},
	}

	o.handleObservation(context.Background(), adm, ev)

	// 1. drbdadm detach --force ran (the auto-detach branch).
	wantDetach := "drbdadm detach --force pvc-1"

	var sawDetach bool

	for _, line := range fx.CommandLines() {
		if line == wantDetach {
			sawDetach = true

			break
		}
	}

	if !sawDetach {
		t.Errorf("expected %q in commands; got %v", wantDetach, fx.CommandLines())
	}

	// 2. SkipDisk prop landed on Resource.Spec.Props with the
	//    canonical "True" value.
	var got blockstoriov1alpha1.Resource

	err := cli.Get(context.Background(), client.ObjectKey{Name: "pvc-1.n1"}, &got)
	if err != nil {
		t.Fatalf("get Resource: %v", err)
	}

	if got.Spec.Props[skipDiskPropKey] != skipDiskPropValue {
		t.Errorf("Spec.Props[%q] = %q, want %q",
			skipDiskPropKey, got.Spec.Props[skipDiskPropKey], skipDiskPropValue)
	}

	// 3. Pre-existing prop preserved — SSA's per-key merge must
	//    not collapse the map.
	if got.Spec.Props["StorPoolName"] != "thin1" {
		t.Errorf("pre-existing StorPoolName lost: got %q, want thin1",
			got.Spec.Props["StorPoolName"])
	}

	// 4. Required scalars unchanged. ForceOwnership on a value-
	//    unchanged field is a no-op for ownership tracking; the
	//    fields must survive intact.
	if got.Spec.ResourceDefinitionName != "pvc-1" {
		t.Errorf("ResourceDefinitionName: got %q, want pvc-1",
			got.Spec.ResourceDefinitionName)
	}

	if got.Spec.NodeName != "n1" {
		t.Errorf("NodeName: got %q, want n1", got.Spec.NodeName)
	}
}

// TestObserverSkipDiskNoopOnNonFailedState guards against false
// positives: the SkipDisk write MUST NOT fire for healthy
// DiskState values. A bug here would auto-set SkipDisk on every
// UpToDate/Inconsistent/Outdated transition, wedging the cluster
// into perpetual `--skip-disk` mode and blocking all real recovery
// paths.
func TestObserverSkipDiskNoopOnNonFailedState(t *testing.T) {
	t.Parallel()

	for _, diskState := range []string{
		"UpToDate", "Inconsistent", "Outdated", "Attaching",
		"Diskless", "Negotiating", "", // omitted disk field
	} {
		t.Run(diskState, func(t *testing.T) {
			t.Parallel()

			scheme := runtime.NewScheme()
			_ = blockstoriov1alpha1.AddToScheme(scheme)

			existing := &blockstoriov1alpha1.Resource{
				ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.n1"},
				Spec: blockstoriov1alpha1.ResourceSpec{
					ResourceDefinitionName: "pvc-1",
					NodeName:               "n1",
				},
			}

			cli := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(existing).
				Build()

			fx := storage.NewFakeExec()

			o := &ObserverRunnable{
				Client:   cli,
				Exec:     fx,
				NodeName: "n1",
			}

			adm := drbd.NewAdm(fx)

			o.handleObservation(context.Background(), adm, &observation{
				ResourceName: "pvc-1",
				DrbdState:    diskState,
				Volumes: []volumeObservation{
					{VolumeNumber: 0, DiskState: diskState},
				},
			})

			var got blockstoriov1alpha1.Resource

			err := cli.Get(context.Background(), client.ObjectKey{Name: "pvc-1.n1"}, &got)
			if err != nil {
				t.Fatalf("get Resource: %v", err)
			}

			if v := got.Spec.Props[skipDiskPropKey]; v != "" {
				t.Errorf("DiskState=%q triggered SkipDisk write: got %q, want empty",
					diskState, v)
			}

			// Sibling guard: detach must also not fire for non-Failed
			// states (it's the same `DrbdState == "Failed"` branch).
			for _, line := range fx.CommandLines() {
				if line == "drbdadm detach --force pvc-1" {
					t.Errorf("DiskState=%q triggered detach: cmds=%v",
						diskState, fx.CommandLines())
				}
			}
		})
	}
}

// TestObserverSkipDiskSilencesNotFound pins the convergence-pending
// contract: when handleObservation fires for a Resource CRD that
// doesn't exist yet (the satellite's events2 observer can race
// the controller's CRD creation), the SkipDisk write must surface
// NotFound but NOT bubble it as a fatal error. handleObservation
// silences NotFound the same way writeStatus does so a fresh
// Resource's first observed event doesn't spam the logs.
func TestObserverSkipDiskSilencesNotFound(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	_ = blockstoriov1alpha1.AddToScheme(scheme)

	// Empty client — no Resource CRD for pvc-1.n1.
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()

	o := &ObserverRunnable{
		Client:   cli,
		NodeName: "n1",
	}

	err := o.writeSkipDiskProp(context.Background(), "pvc-1")
	if err == nil {
		t.Fatalf("expected NotFound, got nil")
	}

	// Defer to the caller (handleObservation) to silence the
	// NotFound — this method MUST return it so the caller can
	// distinguish "Resource not yet created" from "apiserver
	// rejected the write".
	if !isNotFoundErr(err) {
		t.Errorf("expected NotFound; got %v", err)
	}
}

func isNotFoundErr(err error) bool {
	type notFound interface{ Status() metav1.Status }

	var nf notFound

	for unwrapErr := err; unwrapErr != nil; {
		if asNF, ok := unwrapErr.(notFound); ok {
			nf = asNF

			break
		}

		// errors.Unwrap chain
		un, ok := unwrapErr.(interface{ Unwrap() error })
		if !ok {
			break
		}

		unwrapErr = un.Unwrap()
	}

	if nf == nil {
		return false
	}

	return nf.Status().Reason == metav1.StatusReasonNotFound
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

// TestObserverSyncGateCoversDownUpWindow (scenario 5.33) pins the
// IsResourceSyncing-style defer-gate contract for the operator's
// `drbdadm down + up` recovery recipe applied to a stuck
// SyncTarget. The state machine the operator drives the kernel
// through is:
//
//	SyncTarget                     (kernel reports stuck resync)
//	→ drbdadm down                 (operator clears kernel slot)
//	→ kernel state: <destroyed>    (observer sees `destroy` frame)
//	→ drbdadm up                   (operator brings slot back)
//	→ SyncTarget                   (resync resumes from bitmap)
//	→ UpToDate                     (sync completes)
//
// The reconciler MUST stay quiet during the brief window when the
// kernel slot is gone but the operator's recipe is in flight. Two
// concrete things the reconciler must NOT do:
//
//  1. Re-render .res while the operator is mid-recipe. The .res
//     content is unchanged across the down+up (same peers, same
//     port, same volumes) so a re-render is a no-op on the wire,
//     but a `drbdadm adjust` driven by the re-render would race
//     the operator's `drbdadm up` and either fail with "Unknown
//     resource" (slot still down) or re-adjust mid-up (kernel
//     drops the in-flight resync state, restart from 0%).
//  2. Auto-revert by issuing its own `drbdadm up` between the
//     operator's down and up. That would be Bug 8's failure mode
//     under a different trigger — the reconciler picks the slot
//     back up with a default `adjust` invocation, which clobbers
//     the in-flight resync.
//
// The defer-gate the unit test pins: while ANY peer for this
// resource has been observed in {SyncSource, SyncTarget,
// PausedSyncS, PausedSyncT, VerifyS, VerifyT} within the last
// observation cycle, OR while the resource itself just emitted a
// `destroy` frame (i.e. the kernel slot is mid-cycle), the
// reconciler's apply path must skip its `drbdadm adjust` / `up`
// call and let the operator's recipe finish.
//
// This test exercises the OBSERVER side of the contract: it
// drives the full state-machine sequence through translateEvent
// + the merge caches and verifies the observer's per-resource
// cached state correctly reflects each stage, so the reconciler's
// IsResourceSyncing probe has the data it needs to gate on. The
// reconciler-side defer is pinned by the existing 5.16 / 5.25
// tests in reconciler_drbd_test.go (currently t.Skip'd, same as
// this one, awaiting the Bug 8 kernel-state-probe + gate landing
// in pkg/satellite/reconciler.go::applyDRBD).
func TestObserverSyncGateCoversDownUpWindow(t *testing.T) {
	t.Skip("Bug 8 sync-gate not yet wired into applyDRBD — once the " +
		"reconciler probes kernel state (or the observer's mid-cycle " +
		"cache) before issuing drbdadm adjust/up, drop the Skip and " +
		"this test pins the down+up window invariant for scenario 5.33.")

	o := &ObserverRunnable{}

	// Stage 1: kernel reports peer in SyncTarget (the wedged-resync
	// starting point). The observer caches the replication state on
	// the connection observation; mergeVolumes also records the
	// HasSync flag with whatever out-of-sync stats arrived.
	syncTargetEv, ok := translatePeerDeviceEvent(drbd.Event{
		Action: "change",
		Kind:   eventKindPeerDevice,
		Fields: map[string]string{
			"name":         "pvc-stuck",
			"peer-node-id": "1",
			"volume":       "0",
			"conn-name":    "peer-a",
			"replication":  "SyncTarget",
			"out-of-sync":  "524288", // 512 MiB still to ship
		},
	})
	if !ok {
		t.Fatalf("translatePeerDeviceEvent rejected SyncTarget frame")
	}

	o.mergeConnections(&syncTargetEv)
	o.mergeVolumes(&syncTargetEv)
	o.mergeResource(&syncTargetEv)

	cached := o.snapshotFor("pvc-stuck")
	if len(cached.Connections) != 1 || cached.Connections[0].ReplicationState != "SyncTarget" {
		t.Fatalf("stage 1: ReplicationState not cached as SyncTarget; got %+v",
			cached.Connections)
	}

	// Contract assertion: a reconciler probing IsResourceSyncing at
	// this point MUST see "syncing" so it skips its `drbdadm adjust`.
	// The probe surface is the observer's snapshotFor cache, keyed by
	// the replicating-state token set.
	if !observerIndicatesSyncing(&cached) {
		t.Errorf("stage 1 (SyncTarget): observer cache must indicate syncing for gate; got %+v",
			cached.Connections)
	}

	// Stage 2: operator runs `drbdadm down`. The kernel emits a
	// `destroy` frame on the connection (peer goes away) and the
	// device-level state for the local volume is cleared. The
	// observer must NOT race ahead and report UpToDate here — the
	// resource is mid-cycle, not mid-sync-complete.
	destroyEv, ok := translateEvent(drbd.Event{
		Action: eventActionDestroy,
		Kind:   eventKindConnection,
		Fields: map[string]string{
			"name":      "pvc-stuck",
			"conn-name": "peer-a",
		},
	})
	if !ok {
		t.Fatalf("translateEvent rejected destroy frame")
	}

	o.mergeConnections(&destroyEv)
	o.mergeVolumes(&destroyEv)
	o.mergeResource(&destroyEv)

	// Stage 2 invariant: the resource just lost its kernel slot.
	// The reconciler's gate MUST defer here — any `drbdadm adjust`
	// would fail with `Unknown resource` and any `drbdadm up` driven
	// by the satellite would race the operator's pending `up`.
	// Surfacing this is what the IsResourceSyncing-style gate must
	// cover: "not just SyncTarget — also the destroy-then-blank
	// window that the down+up recipe creates".
	cached = o.snapshotFor("pvc-stuck")
	if observerCacheShowsResourceUp(&cached) {
		t.Errorf("stage 2 (post-down): observer must NOT report resource as up "+
			"during destroy-window; got %+v", cached)
	}

	// Stage 3: operator runs `drbdadm up`. Kernel re-emits the
	// peer-device frame in SyncTarget (bitmap-fed resync resumes
	// from where it stalled — DRBD durably persisted the bitmap, so
	// no full re-sync).
	resumedEv, ok := translatePeerDeviceEvent(drbd.Event{
		Action: "change",
		Kind:   eventKindPeerDevice,
		Fields: map[string]string{
			"name":         "pvc-stuck",
			"peer-node-id": "1",
			"volume":       "0",
			"conn-name":    "peer-a",
			"replication":  "SyncTarget",
			"out-of-sync":  "262144", // bitmap shrunk by half during stall
		},
	})
	if !ok {
		t.Fatalf("translatePeerDeviceEvent rejected resumed SyncTarget frame")
	}

	o.mergeConnections(&resumedEv)
	o.mergeVolumes(&resumedEv)
	o.mergeResource(&resumedEv)

	cached = o.snapshotFor("pvc-stuck")
	if !observerIndicatesSyncing(&cached) {
		t.Errorf("stage 3 (post-up, SyncTarget resumed): observer cache "+
			"must indicate syncing; got %+v", cached.Connections)
	}

	// Stage 4: sync completes — peer transitions to Established and
	// the local volume to UpToDate. The gate must now RELEASE so
	// the reconciler's next pass can run `drbdadm adjust` to pick
	// up any pending prop / .res changes.
	establishedEv, ok := translatePeerDeviceEvent(drbd.Event{
		Action: "change",
		Kind:   eventKindPeerDevice,
		Fields: map[string]string{
			"name":         "pvc-stuck",
			"peer-node-id": "1",
			"volume":       "0",
			"conn-name":    "peer-a",
			"replication":  "Established",
		},
	})
	if !ok {
		t.Fatalf("translatePeerDeviceEvent rejected Established frame")
	}

	o.mergeConnections(&establishedEv)
	o.mergeVolumes(&establishedEv)
	o.mergeResource(&establishedEv)

	uptodateEv, ok := translateDeviceEvent(drbd.Event{
		Action: "change",
		Kind:   eventKindDevice,
		Fields: map[string]string{
			"name":   "pvc-stuck",
			"volume": "0",
			"disk":   "UpToDate",
		},
	})
	if !ok {
		t.Fatalf("translateDeviceEvent rejected UpToDate frame")
	}

	o.mergeConnections(&uptodateEv)
	o.mergeVolumes(&uptodateEv)
	o.mergeResource(&uptodateEv)

	cached = o.snapshotFor("pvc-stuck")
	if observerIndicatesSyncing(&cached) {
		t.Errorf("stage 4 (Established+UpToDate): observer cache must NOT "+
			"indicate syncing; gate must release. got %+v", cached.Connections)
	}
}

// observerIndicatesSyncing reports whether any peer in the cached
// snapshot for a resource is in a replication state that demands
// the reconciler defer its `drbdadm adjust` / `up` call (Bug 8 +
// scenario 5.33 gate). Mirrors the set the reconciler's kernel-
// state probe will check once it lands. Kept private to the test
// file so production code can ship a different (kernel-probe-
// based) implementation without locking us into the cache-based
// shape.
func observerIndicatesSyncing(obs *observation) bool {
	syncStates := map[string]bool{
		"SyncSource":  true,
		"SyncTarget":  true,
		"PausedSyncS": true,
		"PausedSyncT": true,
		"VerifyS":     true,
		"VerifyT":     true,
	}

	for _, c := range obs.Connections {
		if syncStates[c.ReplicationState] {
			return true
		}
	}

	return false
}

// observerCacheShowsResourceUp reports whether the cached snapshot
// has positive evidence the kernel slot is up — i.e. either a peer
// in Established or a volume in UpToDate. The down+up window
// invariant relies on the opposite: between the operator's `down`
// and `up`, neither is present, so the gate stays armed.
func observerCacheShowsResourceUp(obs *observation) bool {
	for _, c := range obs.Connections {
		if c.ReplicationState == "Established" {
			return true
		}
	}

	for _, v := range obs.Volumes {
		if v.DiskState == "UpToDate" {
			return true
		}
	}

	return false
}
