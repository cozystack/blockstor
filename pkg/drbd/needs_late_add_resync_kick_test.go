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
	"errors"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/storage"
)

var errKickProbe = errors.New("drbdsetup status: exit 1")

// Regression pins for the BUG-048 late-add resync-KICK predicate
// (NeedsLateAddResyncKick). It exists to unstick a late-added volume
// whose resync EXISTS but wedged in a paused / bitmap-exchange state
// that never advances — the ≥3-replica concurrent-add convergence wedge.
// It must fire ONLY on that stalled-resync signature and never on a
// healthy progressing sync, a day0 bootstrap, a Primary-held resource,
// or a not-fully-connected peer set.

const lateAddKickKey = "drbdsetup status pvc-kick --json"

func admWithKickStatus(t *testing.T, json string) *drbd.Adm {
	t.Helper()

	fx := storage.NewFakeExec()
	fx.Responses[lateAddKickKey] = storage.FakeResponse{Stdout: []byte(json)}

	return drbd.NewAdm(fx)
}

// Stand-observed signature 1 — "dependency deadlock" (B-3r RUN2/RUN8):
// vol-0/vol-1 UpToDate, the late vol-2 Inconsistent locally, a peer is
// PausedSyncS/resync-suspended:dependency (an elected source whose
// resync is paused on a stale dependency) and the partner waits in
// WFBitMapT/resync-suspended:peer. oos frozen at full. No Primary.
func TestNeedsLateAddResyncKick_DependencyDeadlock(t *testing.T) {
	adm := admWithKickStatus(t, `[{
	  "name":"pvc-kick","node-id":0,"role":"Secondary",
	  "devices":[
	    {"volume":0,"disk-state":"UpToDate"},
	    {"volume":1,"disk-state":"UpToDate"},
	    {"volume":2,"disk-state":"Inconsistent"}
	  ],
	  "connections":[
	    {"peer-node-id":1,"name":"n2","connection-state":"Connected","peer-role":"Secondary",
	     "peer_devices":[
	       {"volume":0,"peer-disk-state":"UpToDate","replication-state":"Established","resync-suspended":"no"},
	       {"volume":1,"peer-disk-state":"UpToDate","replication-state":"Established","resync-suspended":"no"},
	       {"volume":2,"peer-disk-state":"Inconsistent","replication-state":"PausedSyncS","resync-suspended":"dependency"}
	     ]},
	    {"peer-node-id":2,"name":"n3","connection-state":"Connected","peer-role":"Secondary",
	     "peer_devices":[
	       {"volume":0,"peer-disk-state":"UpToDate","replication-state":"Established","resync-suspended":"no"},
	       {"volume":1,"peer-disk-state":"UpToDate","replication-state":"Established","resync-suspended":"no"},
	       {"volume":2,"peer-disk-state":"Inconsistent","replication-state":"WFBitMapT","resync-suspended":"peer"}
	     ]}
	  ]
	}]`)

	if !adm.NeedsLateAddResyncKick(t.Context(), "pvc-kick") {
		t.Fatal("expected kick for the dependency-deadlock wedge")
	}
}

// Stand-observed signature 2 — "partial stall" (B-3r RUN9): one peer
// reached UpToDate (a real SyncSource exists) but a SECOND peer is stuck
// WFBitMapT / peerdisk Outdated and never finalises. The waiting peer's
// resync-suspended is "peer". No Primary.
func TestNeedsLateAddResyncKick_PartialStall(t *testing.T) {
	adm := admWithKickStatus(t, `[{
	  "name":"pvc-kick","node-id":2,"role":"Secondary",
	  "devices":[
	    {"volume":0,"disk-state":"UpToDate"},
	    {"volume":1,"disk-state":"UpToDate"},
	    {"volume":2,"disk-state":"Outdated"}
	  ],
	  "connections":[
	    {"peer-node-id":1,"name":"n2","connection-state":"Connected","peer-role":"Secondary",
	     "peer_devices":[
	       {"volume":0,"peer-disk-state":"UpToDate","replication-state":"Established","resync-suspended":"no"},
	       {"volume":1,"peer-disk-state":"UpToDate","replication-state":"Established","resync-suspended":"no"},
	       {"volume":2,"peer-disk-state":"UpToDate","replication-state":"WFBitMapT","resync-suspended":"peer"}
	     ]},
	    {"peer-node-id":0,"name":"n1","connection-state":"Connected","peer-role":"Secondary",
	     "peer_devices":[
	       {"volume":0,"peer-disk-state":"UpToDate","replication-state":"Established","resync-suspended":"no"},
	       {"volume":1,"peer-disk-state":"UpToDate","replication-state":"Established","resync-suspended":"no"},
	       {"volume":2,"peer-disk-state":"UpToDate","replication-state":"Established","resync-suspended":"no"}
	     ]}
	  ]
	}]`)

	if !adm.NeedsLateAddResyncKick(t.Context(), "pvc-kick") {
		t.Fatal("expected kick for the partial-stall wedge")
	}
}

// A HEALTHY progressing initial sync (SyncSource / SyncTarget with
// resync-suspended "no") must NOT be kicked — it advances on its own,
// and a kick would needlessly abort it.
func TestNeedsLateAddResyncKick_HealthySyncNotKicked(t *testing.T) {
	adm := admWithKickStatus(t, `[{
	  "name":"pvc-kick","node-id":0,"role":"Secondary",
	  "devices":[
	    {"volume":0,"disk-state":"UpToDate"},
	    {"volume":1,"disk-state":"Inconsistent"}
	  ],
	  "connections":[{
	    "peer-node-id":1,"name":"n2","connection-state":"Connected","peer-role":"Secondary",
	    "peer_devices":[
	      {"volume":0,"peer-disk-state":"UpToDate","replication-state":"Established","resync-suspended":"no"},
	      {"volume":1,"peer-disk-state":"Inconsistent","replication-state":"SyncSource","resync-suspended":"no"}
	    ]
	  }]
	}]`)

	if adm.NeedsLateAddResyncKick(t.Context(), "pvc-kick") {
		t.Fatal("must NOT kick a healthy progressing SyncSource")
	}
}

// A transient bitmap-exchange step with resync-suspended "no" (a sync
// about to start, not wedged) must NOT be kicked.
func TestNeedsLateAddResyncKick_WFBitMapNotSuspendedNotKicked(t *testing.T) {
	adm := admWithKickStatus(t, `[{
	  "name":"pvc-kick","node-id":0,"role":"Secondary",
	  "devices":[
	    {"volume":0,"disk-state":"UpToDate"},
	    {"volume":1,"disk-state":"Inconsistent"}
	  ],
	  "connections":[{
	    "peer-node-id":1,"name":"n2","connection-state":"Connected","peer-role":"Secondary",
	    "peer_devices":[
	      {"volume":0,"peer-disk-state":"UpToDate","replication-state":"Established","resync-suspended":"no"},
	      {"volume":1,"peer-disk-state":"Inconsistent","replication-state":"WFBitMapT","resync-suspended":"no"}
	    ]
	  }]
	}]`)

	if adm.NeedsLateAddResyncKick(t.Context(), "pvc-kick") {
		t.Fatal("must NOT kick a WFBitMapT that is not suspended (sync starting)")
	}
}

// Day0 bootstrap: NO local UpToDate volume yet (every volume transiently
// Inconsistent while the fresh-RD winner election runs). A paused sync
// here must NOT be kicked — the gate requires a local UpToDate sibling so
// this can never misfire during first activation.
func TestNeedsLateAddResyncKick_Day0NotKicked(t *testing.T) {
	adm := admWithKickStatus(t, `[{
	  "name":"pvc-kick","node-id":0,"role":"Secondary",
	  "devices":[
	    {"volume":0,"disk-state":"Inconsistent"}
	  ],
	  "connections":[{
	    "peer-node-id":1,"name":"n2","connection-state":"Connected","peer-role":"Secondary",
	    "peer_devices":[
	      {"volume":0,"peer-disk-state":"Inconsistent","replication-state":"PausedSyncS","resync-suspended":"dependency"}
	    ]
	  }]
	}]`)

	if adm.NeedsLateAddResyncKick(t.Context(), "pvc-kick") {
		t.Fatal("must NOT kick during day0 (no local UpToDate volume)")
	}
}

// A Primary anywhere (local or peer) vetoes the kick — never disconnect a
// resource an application is actively writing.
func TestNeedsLateAddResyncKick_PrimaryVetoes(t *testing.T) {
	adm := admWithKickStatus(t, `[{
	  "name":"pvc-kick","node-id":0,"role":"Primary",
	  "devices":[
	    {"volume":0,"disk-state":"UpToDate"},
	    {"volume":1,"disk-state":"Inconsistent"}
	  ],
	  "connections":[{
	    "peer-node-id":1,"name":"n2","connection-state":"Connected","peer-role":"Secondary",
	    "peer_devices":[
	      {"volume":0,"peer-disk-state":"UpToDate","replication-state":"Established","resync-suspended":"no"},
	      {"volume":1,"peer-disk-state":"Inconsistent","replication-state":"PausedSyncS","resync-suspended":"dependency"}
	    ]
	  }]
	}]`)

	if adm.NeedsLateAddResyncKick(t.Context(), "pvc-kick") {
		t.Fatal("must NOT kick while a Primary holds the resource")
	}
}

// A not-fully-connected peer (Connecting / StandAlone) defers the kick:
// the resync-state read is only authoritative over a settled connection.
func TestNeedsLateAddResyncKick_PeerNotConnectedDefers(t *testing.T) {
	adm := admWithKickStatus(t, `[{
	  "name":"pvc-kick","node-id":0,"role":"Secondary",
	  "devices":[
	    {"volume":0,"disk-state":"UpToDate"},
	    {"volume":1,"disk-state":"Inconsistent"}
	  ],
	  "connections":[
	    {"peer-node-id":1,"name":"n2","connection-state":"Connected","peer-role":"Secondary",
	     "peer_devices":[
	       {"volume":1,"peer-disk-state":"Inconsistent","replication-state":"PausedSyncS","resync-suspended":"dependency"}
	     ]},
	    {"peer-node-id":2,"name":"n3","connection-state":"Connecting","peer-role":"Unknown",
	     "peer_devices":[]}
	  ]
	}]`)

	if adm.NeedsLateAddResyncKick(t.Context(), "pvc-kick") {
		t.Fatal("must NOT kick when a peer is not fully Connected")
	}
}

// Fully converged (every peer Established, resync-suspended "no") — no
// stalled resync remains, so the predicate stops holding (self-limiting).
func TestNeedsLateAddResyncKick_ConvergedNotKicked(t *testing.T) {
	adm := admWithKickStatus(t, `[{
	  "name":"pvc-kick","node-id":0,"role":"Secondary",
	  "devices":[
	    {"volume":0,"disk-state":"UpToDate"},
	    {"volume":1,"disk-state":"UpToDate"},
	    {"volume":2,"disk-state":"UpToDate"}
	  ],
	  "connections":[{
	    "peer-node-id":1,"name":"n2","connection-state":"Connected","peer-role":"Secondary",
	    "peer_devices":[
	      {"volume":0,"peer-disk-state":"UpToDate","replication-state":"Established","resync-suspended":"no"},
	      {"volume":1,"peer-disk-state":"UpToDate","replication-state":"Established","resync-suspended":"no"},
	      {"volume":2,"peer-disk-state":"UpToDate","replication-state":"Established","resync-suspended":"no"}
	    ]
	  }]
	}]`)

	if adm.NeedsLateAddResyncKick(t.Context(), "pvc-kick") {
		t.Fatal("must NOT kick a fully converged resource")
	}
}

// Probe failure (drbdsetup errors) → conservative false, no kick.
func TestNeedsLateAddResyncKick_ProbeFailureFalse(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Responses[lateAddKickKey] = storage.FakeResponse{Err: errKickProbe}

	adm := drbd.NewAdm(fx)
	if adm.NeedsLateAddResyncKick(t.Context(), "pvc-kick") {
		t.Fatal("must return false on a probe failure")
	}
}
