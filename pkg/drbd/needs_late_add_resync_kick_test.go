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
	"slices"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/storage"
)

// Regression pins for the BUG-048 late-add resync-KICK predicate
// (LateAddResyncKickVolumes). It returns the LOCAL volume numbers whose
// late-add resync is STALLED and recoverable by `drbdadm invalidate
// <res>/<vol>` — a local copy below UpToDate, an UpToDate peer to pull
// from, and no live resync already running. It must return ONLY genuine
// stragglers and never an UpToDate local, a volume with no UpToDate peer,
// a live SyncTarget, a day0 bootstrap, or a Primary-held resource.

var errKickProbe = errors.New("drbdsetup status: exit 1")

const lateAddKickKey = "drbdsetup status pvc-kick --json"

func admWithKickStatus(t *testing.T, json string) *drbd.Adm {
	t.Helper()

	fx := storage.NewFakeExec()
	fx.Responses[lateAddKickKey] = storage.FakeResponse{Stdout: []byte(json)}

	return drbd.NewAdm(fx)
}

// Stand-observed "partial stall" (B-3r RUN9): vol-0/vol-1 UpToDate, the
// late vol-2 stuck Inconsistent locally while a peer (n2) reached
// UpToDate for vol-2 — but this replica is stuck WFBitMapT and never
// finalises. An UpToDate peer exists ⇒ invalidate vol-2 locally.
func TestLateAddResyncKickVolumes_PartialStall(t *testing.T) {
	adm := admWithKickStatus(t, `[{
	  "name":"pvc-kick","node-id":2,"role":"Secondary",
	  "devices":[
	    {"volume":0,"disk-state":"UpToDate"},
	    {"volume":1,"disk-state":"UpToDate"},
	    {"volume":2,"disk-state":"Inconsistent"}
	  ],
	  "connections":[
	    {"peer-node-id":1,"name":"n2","connection-state":"Connected","peer-role":"Secondary",
	     "peer_devices":[
	       {"volume":2,"peer-disk-state":"UpToDate","replication-state":"WFBitMapT","resync-suspended":"peer"}
	     ]},
	    {"peer-node-id":0,"name":"n1","connection-state":"Connected","peer-role":"Secondary",
	     "peer_devices":[
	       {"volume":2,"peer-disk-state":"UpToDate","replication-state":"Established","resync-suspended":"no"}
	     ]}
	  ]
	}]`)

	got := adm.LateAddResyncKickVolumes(t.Context(), "pvc-kick")
	if !slices.Equal(got, []int32{2}) {
		t.Fatalf("expected to invalidate [2], got %v", got)
	}
}

// Post-promote dependency case: a peer is now UpToDate (the promoted
// source) but this Inconsistent replica's resync stays paused
// (resync-suspended:dependency). An UpToDate peer exists ⇒ invalidate.
func TestLateAddResyncKickVolumes_PostPromoteDependency(t *testing.T) {
	adm := admWithKickStatus(t, `[{
	  "name":"pvc-kick","node-id":2,"role":"Secondary",
	  "devices":[
	    {"volume":0,"disk-state":"UpToDate"},
	    {"volume":2,"disk-state":"Inconsistent"}
	  ],
	  "connections":[{
	    "peer-node-id":0,"name":"n1","connection-state":"Connected","peer-role":"Secondary",
	    "peer_devices":[
	      {"volume":2,"peer-disk-state":"UpToDate","replication-state":"PausedSyncT","resync-suspended":"dependency"}
	    ]
	  }]
	}]`)

	got := adm.LateAddResyncKickVolumes(t.Context(), "pvc-kick")
	if !slices.Equal(got, []int32{2}) {
		t.Fatalf("expected to invalidate [2], got %v", got)
	}
}

// No UpToDate peer for the stalled volume (all-Inconsistent dependency
// deadlock BEFORE promote): invalidate has no source, so this predicate
// must NOT return the volume — maybeLateAddPromote owns minting the
// source first.
func TestLateAddResyncKickVolumes_NoUpToDatePeerSkipped(t *testing.T) {
	adm := admWithKickStatus(t, `[{
	  "name":"pvc-kick","node-id":0,"role":"Secondary",
	  "devices":[
	    {"volume":0,"disk-state":"UpToDate"},
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
	}]`)

	got := adm.LateAddResyncKickVolumes(t.Context(), "pvc-kick")
	if len(got) != 0 {
		t.Fatalf("must NOT invalidate when no peer is UpToDate, got %v", got)
	}
}

// A live, unsuspended SyncTarget (resync-suspended "no") must NOT be
// invalidated — it is actively pulling and finishes on its own.
func TestLateAddResyncKickVolumes_ActiveSyncTargetSkipped(t *testing.T) {
	adm := admWithKickStatus(t, `[{
	  "name":"pvc-kick","node-id":2,"role":"Secondary",
	  "devices":[
	    {"volume":0,"disk-state":"UpToDate"},
	    {"volume":2,"disk-state":"Inconsistent"}
	  ],
	  "connections":[{
	    "peer-node-id":0,"name":"n1","connection-state":"Connected","peer-role":"Secondary",
	    "peer_devices":[
	      {"volume":2,"peer-disk-state":"UpToDate","replication-state":"SyncTarget","resync-suspended":"no"}
	    ]
	  }]
	}]`)

	got := adm.LateAddResyncKickVolumes(t.Context(), "pvc-kick")
	if len(got) != 0 {
		t.Fatalf("must NOT invalidate a live SyncTarget, got %v", got)
	}
}

// An UpToDate local volume is authoritative — never invalidate it even if
// a peer-device shows a stalled state.
func TestLateAddResyncKickVolumes_UpToDateLocalSkipped(t *testing.T) {
	adm := admWithKickStatus(t, `[{
	  "name":"pvc-kick","node-id":0,"role":"Secondary",
	  "devices":[
	    {"volume":0,"disk-state":"UpToDate"},
	    {"volume":2,"disk-state":"UpToDate"}
	  ],
	  "connections":[{
	    "peer-node-id":1,"name":"n2","connection-state":"Connected","peer-role":"Secondary",
	    "peer_devices":[
	      {"volume":2,"peer-disk-state":"UpToDate","replication-state":"PausedSyncS","resync-suspended":"dependency"}
	    ]
	  }]
	}]`)

	got := adm.LateAddResyncKickVolumes(t.Context(), "pvc-kick")
	if len(got) != 0 {
		t.Fatalf("must NOT invalidate an UpToDate local volume, got %v", got)
	}
}

// Day0 bootstrap: NO local UpToDate volume yet. Even with an Inconsistent
// local + UpToDate peer, the gate refuses (RD not past first activation).
func TestLateAddResyncKickVolumes_Day0Skipped(t *testing.T) {
	adm := admWithKickStatus(t, `[{
	  "name":"pvc-kick","node-id":0,"role":"Secondary",
	  "devices":[
	    {"volume":0,"disk-state":"Inconsistent"}
	  ],
	  "connections":[{
	    "peer-node-id":1,"name":"n2","connection-state":"Connected","peer-role":"Secondary",
	    "peer_devices":[
	      {"volume":0,"peer-disk-state":"UpToDate","replication-state":"PausedSyncT","resync-suspended":"dependency"}
	    ]
	  }]
	}]`)

	got := adm.LateAddResyncKickVolumes(t.Context(), "pvc-kick")
	if len(got) != 0 {
		t.Fatalf("must NOT invalidate during day0 (no local UpToDate volume), got %v", got)
	}
}

// A Primary anywhere (local or peer) vetoes invalidate — never discard a
// volume an application may be writing.
func TestLateAddResyncKickVolumes_PrimaryVetoes(t *testing.T) {
	adm := admWithKickStatus(t, `[{
	  "name":"pvc-kick","node-id":2,"role":"Secondary",
	  "devices":[
	    {"volume":0,"disk-state":"UpToDate"},
	    {"volume":2,"disk-state":"Inconsistent"}
	  ],
	  "connections":[{
	    "peer-node-id":0,"name":"n1","connection-state":"Connected","peer-role":"Primary",
	    "peer_devices":[
	      {"volume":2,"peer-disk-state":"UpToDate","replication-state":"PausedSyncT","resync-suspended":"dependency"}
	    ]
	  }]
	}]`)

	got := adm.LateAddResyncKickVolumes(t.Context(), "pvc-kick")
	if len(got) != 0 {
		t.Fatalf("must NOT invalidate while a Primary holds the resource, got %v", got)
	}
}

// A not-fully-connected peer (Connecting / StandAlone) defers — the
// resync-state read is only authoritative over a settled connection.
func TestLateAddResyncKickVolumes_PeerNotConnectedDefers(t *testing.T) {
	adm := admWithKickStatus(t, `[{
	  "name":"pvc-kick","node-id":2,"role":"Secondary",
	  "devices":[
	    {"volume":0,"disk-state":"UpToDate"},
	    {"volume":2,"disk-state":"Inconsistent"}
	  ],
	  "connections":[
	    {"peer-node-id":0,"name":"n1","connection-state":"Connected","peer-role":"Secondary",
	     "peer_devices":[
	       {"volume":2,"peer-disk-state":"UpToDate","replication-state":"PausedSyncT","resync-suspended":"dependency"}
	     ]},
	    {"peer-node-id":1,"name":"n2","connection-state":"Connecting","peer-role":"Unknown",
	     "peer_devices":[]}
	  ]
	}]`)

	got := adm.LateAddResyncKickVolumes(t.Context(), "pvc-kick")
	if len(got) != 0 {
		t.Fatalf("must NOT invalidate when a peer is not fully Connected, got %v", got)
	}
}

// Fully converged (every volume UpToDate) — nothing to invalidate.
func TestLateAddResyncKickVolumes_ConvergedEmpty(t *testing.T) {
	adm := admWithKickStatus(t, `[{
	  "name":"pvc-kick","node-id":0,"role":"Secondary",
	  "devices":[
	    {"volume":0,"disk-state":"UpToDate"},
	    {"volume":2,"disk-state":"UpToDate"}
	  ],
	  "connections":[{
	    "peer-node-id":1,"name":"n2","connection-state":"Connected","peer-role":"Secondary",
	    "peer_devices":[
	      {"volume":0,"peer-disk-state":"UpToDate","replication-state":"Established","resync-suspended":"no"},
	      {"volume":2,"peer-disk-state":"UpToDate","replication-state":"Established","resync-suspended":"no"}
	    ]
	  }]
	}]`)

	got := adm.LateAddResyncKickVolumes(t.Context(), "pvc-kick")
	if len(got) != 0 {
		t.Fatalf("must return nothing for a converged resource, got %v", got)
	}
}

// Probe failure (drbdsetup errors) → conservative empty.
func TestLateAddResyncKickVolumes_ProbeFailureEmpty(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Responses[lateAddKickKey] = storage.FakeResponse{Err: errKickProbe}

	adm := drbd.NewAdm(fx)
	if got := adm.LateAddResyncKickVolumes(t.Context(), "pvc-kick"); len(got) != 0 {
		t.Fatalf("must return empty on a probe failure, got %v", got)
	}
}
