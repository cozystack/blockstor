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

package drbd_test

import (
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/storage"
)

// Regression pins for the BUG-048 late-add-promote predicate
// (NeedsLateAddPromote). It exists to unstick a LATE-ADDED volume that
// wedged Inconsistent on EVERY diskful replica with no SyncSource (the
// concurrent two-VD add on a ≥3-diskful RD): the deterministic
// lowest-node-id replica force-primaries to become the SyncSource. It
// must fire ONLY in that genuine wedge and never when a peer holds data
// (SyncTarget instead), a resync is already running, a Primary exists,
// or this node is not the lowest-id promoter.

const lateAddPromoteKey = "drbdsetup status pvc-late --json"

func admWithLateAddStatus(t *testing.T, json string) *drbd.Adm {
	t.Helper()

	fx := storage.NewFakeExec()
	fx.Responses[lateAddPromoteKey] = storage.FakeResponse{Stdout: []byte(json)}

	return drbd.NewAdm(fx)
}

// The genuine BUG-048 wedge: vol-0/vol-1 UpToDate, the late vol-2 is
// Inconsistent locally AND on every diskful peer, no peer holds data for
// vol-2, no Primary, and we (node-id 0) are the lowest → promote.
func TestNeedsLateAddPromote_WedgedLateVolume(t *testing.T) {
	adm := admWithLateAddStatus(t, `[{
	  "name":"pvc-late","node-id":0,"role":"Secondary",
	  "devices":[
	    {"volume":0,"disk-state":"UpToDate"},
	    {"volume":1,"disk-state":"UpToDate"},
	    {"volume":2,"disk-state":"Inconsistent"}
	  ],
	  "connections":[{
	    "peer-node-id":1,"name":"n2","connection-state":"Connected",
	    "peer-role":"Secondary",
	    "peer_devices":[
	      {"volume":0,"peer-disk-state":"UpToDate","replication-state":"Established","resync-suspended":"no"},
	      {"volume":1,"peer-disk-state":"UpToDate","replication-state":"Established","resync-suspended":"no"},
	      {"volume":2,"peer-disk-state":"Inconsistent","replication-state":"Established","resync-suspended":"no"}
	    ]
	  }]
	}]`)

	if !adm.NeedsLateAddPromote(t.Context(), "pvc-late") {
		t.Fatal("late vol-2 Inconsistent on every replica with no SyncSource must trigger late-add-promote")
	}
}

// Not the lowest node-id: a peer with a LOWER node-id is also wedged on
// vol-2, so it is the elected promoter — we must defer (no split-brain).
// (vol-0 UpToDate everywhere → past day0, peer Connected → gates 1+2 OK.)
func TestNeedsLateAddPromote_DefersToLowerNodeID(t *testing.T) {
	adm := admWithLateAddStatus(t, `[{
	  "name":"pvc-late","node-id":2,"role":"Secondary",
	  "devices":[
	    {"volume":0,"disk-state":"UpToDate"},
	    {"volume":2,"disk-state":"Inconsistent"}
	  ],
	  "connections":[{
	    "peer-node-id":0,"name":"n1","connection-state":"Connected",
	    "peer-role":"Secondary",
	    "peer_devices":[
	      {"volume":0,"peer-disk-state":"UpToDate","replication-state":"Established","resync-suspended":"no"},
	      {"volume":2,"peer-disk-state":"Inconsistent","replication-state":"Established","resync-suspended":"no"}
	    ]
	  }]
	}]`)

	if adm.NeedsLateAddPromote(t.Context(), "pvc-late") {
		t.Fatal("a lower-node-id wedged peer is the promoter; this node (id 2) must defer")
	}
}

// A peer already holds committed data for vol-2 (UpToDate) — the correct
// action is to SyncTarget from it, NEVER force-primary (Bug 342
// unrelated-data guard). Must not fire.
func TestNeedsLateAddPromote_PeerHasDataVeto(t *testing.T) {
	adm := admWithLateAddStatus(t, `[{
	  "name":"pvc-late","node-id":0,"role":"Secondary",
	  "devices":[
	    {"volume":0,"disk-state":"UpToDate"},
	    {"volume":2,"disk-state":"Inconsistent"}
	  ],
	  "connections":[{
	    "peer-node-id":1,"name":"n2","connection-state":"Connected",
	    "peer-role":"Secondary",
	    "peer_devices":[
	      {"volume":0,"peer-disk-state":"UpToDate","replication-state":"Established","resync-suspended":"no"},
	      {"volume":2,"peer-disk-state":"UpToDate","replication-state":"SyncTarget","resync-suspended":"no"}
	    ]
	  }]
	}]`)

	if adm.NeedsLateAddPromote(t.Context(), "pvc-late") {
		t.Fatal("a peer holding data for the volume must veto force-primary (SyncTarget instead)")
	}
}

// A live resync is already driving vol-2 toward UpToDate from this node —
// let it finish rather than churn a promote.
func TestNeedsLateAddPromote_ActiveResyncDefers(t *testing.T) {
	adm := admWithLateAddStatus(t, `[{
	  "name":"pvc-late","node-id":0,"role":"Secondary",
	  "devices":[
	    {"volume":0,"disk-state":"UpToDate"},
	    {"volume":2,"disk-state":"Inconsistent"}
	  ],
	  "connections":[{
	    "peer-node-id":1,"name":"n2","connection-state":"Connected",
	    "peer-role":"Secondary",
	    "peer_devices":[
	      {"volume":0,"peer-disk-state":"UpToDate","replication-state":"Established","resync-suspended":"no"},
	      {"volume":2,"peer-disk-state":"Inconsistent","replication-state":"SyncSource","resync-suspended":"no"}
	    ]
	  }]
	}]`)

	if adm.NeedsLateAddPromote(t.Context(), "pvc-late") {
		t.Fatal("a volume already being actively resynced must NOT trigger a promote")
	}
}

// A Primary exists somewhere — it already drives the sync; never disturb.
func TestNeedsLateAddPromote_PeerPrimaryVeto(t *testing.T) {
	adm := admWithLateAddStatus(t, `[{
	  "name":"pvc-late","node-id":0,"role":"Secondary",
	  "devices":[
	    {"volume":0,"disk-state":"UpToDate"},
	    {"volume":2,"disk-state":"Inconsistent"}
	  ],
	  "connections":[{
	    "peer-node-id":1,"name":"n2","connection-state":"Connected",
	    "peer-role":"Primary",
	    "peer_devices":[
	      {"volume":0,"peer-disk-state":"UpToDate","replication-state":"Established","resync-suspended":"no"},
	      {"volume":2,"peer-disk-state":"Inconsistent","replication-state":"Established","resync-suspended":"no"}
	    ]
	  }]
	}]`)

	if adm.NeedsLateAddPromote(t.Context(), "pvc-late") {
		t.Fatal("a Primary peer must veto the late-add-promote")
	}
}

// SPLIT-BRAIN SAFETY gate 1: a pure day0 bootstrap — EVERY volume
// transiently Inconsistent, NO UpToDate sibling — must NOT fire, even
// though "all replicas Inconsistent, no SyncSource" superficially looks
// like the wedge. The normal day0 winner-election + auto-promote own
// this; misfiring here is exactly what split-brained vol-0 on the stand.
func TestNeedsLateAddPromote_Day0BootstrapNoFire(t *testing.T) {
	adm := admWithLateAddStatus(t, `[{
	  "name":"pvc-late","node-id":0,"role":"Secondary",
	  "devices":[{"volume":0,"disk-state":"Inconsistent"}],
	  "connections":[{
	    "peer-node-id":1,"name":"n2","connection-state":"Connected",
	    "peer-role":"Secondary",
	    "peer_devices":[{"volume":0,"peer-disk-state":"Inconsistent",
	      "replication-state":"Established","resync-suspended":"no"}]
	  }]
	}]`)

	if adm.NeedsLateAddPromote(t.Context(), "pvc-late") {
		t.Fatal("day0 bootstrap (no UpToDate sibling) must NOT trigger late-add-promote — normal winner election owns it")
	}
}

// SPLIT-BRAIN SAFETY gate 2: a peer that is NOT fully Connected (the
// day0 t+1s StandAlone/Connecting state) means the lowest-node-id
// election would run over a partial peer set. Defer until every peer is
// connected, so EXACTLY ONE node promotes — never two simultaneously.
func TestNeedsLateAddPromote_PartialConnectivityNoFire(t *testing.T) {
	adm := admWithLateAddStatus(t, `[{
	  "name":"pvc-late","node-id":1,"role":"Secondary",
	  "devices":[
	    {"volume":0,"disk-state":"UpToDate"},
	    {"volume":2,"disk-state":"Inconsistent"}
	  ],
	  "connections":[
	    {"peer-node-id":0,"name":"n1","connection-state":"StandAlone","peer-role":"Unknown",
	     "peer_devices":[{"volume":2,"peer-disk-state":"DUnknown","replication-state":"Off","resync-suspended":"no"}]},
	    {"peer-node-id":2,"name":"n3","connection-state":"Connected","peer-role":"Secondary",
	     "peer_devices":[{"volume":2,"peer-disk-state":"Inconsistent","replication-state":"Established","resync-suspended":"no"}]}
	  ]
	}]`)

	if adm.NeedsLateAddPromote(t.Context(), "pvc-late") {
		t.Fatal("a not-fully-connected peer set must defer the promote (incomplete election → split-brain risk)")
	}
}

// No Inconsistent local volume — nothing to promote. Must not fire (this
// is the healthy steady state every converged reconcile passes through).
func TestNeedsLateAddPromote_AllUpToDateNoOp(t *testing.T) {
	adm := admWithLateAddStatus(t, `[{
	  "name":"pvc-late","node-id":0,"role":"Secondary",
	  "devices":[
	    {"volume":0,"disk-state":"UpToDate"},
	    {"volume":1,"disk-state":"UpToDate"},
	    {"volume":2,"disk-state":"UpToDate"}
	  ],
	  "connections":[{
	    "peer-node-id":1,"name":"n2","connection-state":"Connected",
	    "peer-role":"Secondary",
	    "peer_devices":[
	      {"volume":2,"peer-disk-state":"UpToDate","replication-state":"Established","resync-suspended":"no"}
	    ]
	  }]
	}]`)

	if adm.NeedsLateAddPromote(t.Context(), "pvc-late") {
		t.Fatal("a fully-converged RD must NOT trigger any promote")
	}
}

// BUG-048 (≥3-replica double-promoter wedge). A LOWER-node-id diskful peer
// whose freshly late-added volume is still in a TRANSIENT bring-up state
// (DUnknown — connection negotiated, peer disk not yet reported) is the
// rightful promoter. The old election only counted a lower-id peer as a
// competitor when it observed it as Inconsistent, so this node (id 2) saw
// the lower-id peer mid-bring-up, concluded "I am lowest", and force-
// primaried — and once the lower-id peer settled it ALSO promoted →
// divergent current-UUIDs → PausedSyncS / StandAlone wedge (stand-observed
// on 3-diskful: node-id 1 and node-id 0 both force-primaried the same late
// volume). This node MUST defer to the lower-id peer regardless of its
// transient per-volume state.
func TestNeedsLateAddPromote_DefersToLowerNodeIDStillBringingUp(t *testing.T) {
	for _, tc := range []struct {
		name      string
		peerState string
	}{
		{"DUnknown", "DUnknown"},
		{"Negotiating", "Negotiating"},
		{"Attaching", "Attaching"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			adm := admWithLateAddStatus(t, `[{
			  "name":"pvc-late","node-id":2,"role":"Secondary",
			  "devices":[
			    {"volume":0,"disk-state":"UpToDate"},
			    {"volume":2,"disk-state":"Inconsistent"}
			  ],
			  "connections":[{
			    "peer-node-id":0,"name":"n1","connection-state":"Connected",
			    "peer-role":"Secondary",
			    "peer_devices":[
			      {"volume":0,"peer-disk-state":"UpToDate","replication-state":"Established","resync-suspended":"no"},
			      {"volume":2,"peer-disk-state":"`+tc.peerState+`","replication-state":"Established","resync-suspended":"no"}
			    ]
			  }]
			}]`)

			if adm.NeedsLateAddPromote(t.Context(), "pvc-late") {
				t.Fatalf("a lower-node-id diskful peer still bringing up the volume (%s) is the elected promoter; "+
					"this node (id 2) must defer or both promote and wedge the volume (BUG-048)", tc.peerState)
			}
		})
	}
}

// BUG-048 companion: deferring to a transient lower-id peer must NOT
// deadlock when the lower-id peer is a PERMANENT diskless witness
// (tiebreaker). A diskless peer never becomes a diskful promoter, so a
// lower-id Diskless peer must be IGNORED (not deferred to) — this node
// (id 2), the lowest DISKFUL replica, still promotes. (The witness shows
// steady-state "Diskless"; a diskful peer mid-bring-up shows DUnknown /
// Negotiating / Attaching, handled by the test above.)
func TestNeedsLateAddPromote_PromotesOverLowerIDDisklessWitness(t *testing.T) {
	adm := admWithLateAddStatus(t, `[{
	  "name":"pvc-late","node-id":2,"role":"Secondary",
	  "devices":[
	    {"volume":0,"disk-state":"UpToDate"},
	    {"volume":2,"disk-state":"Inconsistent"}
	  ],
	  "connections":[{
	    "peer-node-id":0,"name":"tiebreaker","connection-state":"Connected",
	    "peer-role":"Secondary",
	    "peer_devices":[
	      {"volume":0,"peer-disk-state":"Diskless","replication-state":"Established","resync-suspended":"no"},
	      {"volume":2,"peer-disk-state":"Diskless","replication-state":"Established","resync-suspended":"no"}
	    ]
	  }]
	}]`)

	if !adm.NeedsLateAddPromote(t.Context(), "pvc-late") {
		t.Fatal("a lower-id DISKLESS witness never promotes; this node (the lowest DISKFUL replica) " +
			"must still force-primary to seed the SyncSource (deferring would deadlock the volume forever)")
	}
}

// BUG-048 companion: the true-lowest diskful node still promotes even when
// a HIGHER-id peer is mid-bring-up (DUnknown). A higher-id transient peer
// is not a competitor — this node (id 0) is lowest and must seed.
func TestNeedsLateAddPromote_LowestPromotesDespiteHigherIDBringingUp(t *testing.T) {
	adm := admWithLateAddStatus(t, `[{
	  "name":"pvc-late","node-id":0,"role":"Secondary",
	  "devices":[
	    {"volume":0,"disk-state":"UpToDate"},
	    {"volume":2,"disk-state":"Inconsistent"}
	  ],
	  "connections":[{
	    "peer-node-id":1,"name":"n2","connection-state":"Connected",
	    "peer-role":"Secondary",
	    "peer_devices":[
	      {"volume":0,"peer-disk-state":"UpToDate","replication-state":"Established","resync-suspended":"no"},
	      {"volume":2,"peer-disk-state":"DUnknown","replication-state":"Established","resync-suspended":"no"}
	    ]
	  }]
	}]`)

	if !adm.NeedsLateAddPromote(t.Context(), "pvc-late") {
		t.Fatal("the lowest diskful node must promote even when a higher-id peer is still bringing up the volume")
	}
}
