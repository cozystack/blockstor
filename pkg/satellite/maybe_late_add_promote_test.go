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

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
)

// Regression pins for the BUG-048 late-add-promote self-heal wiring.
// maybeLateAddPromote must force-primary the lowest-node-id replica when
// a late-added volume wedged Inconsistent on every diskful replica with
// no SyncSource — WITHOUT the dispatcher's auto-primary (which is absent
// on the initialized RD every late-add lands on) — and must NOT promote
// a diskless replica.

const lateAddStatusKey = "drbdsetup status pvc-la --json"

// vol-0/vol-1 UpToDate, the late vol-2 Inconsistent locally and on the
// peer, no peer data for vol-2, no Primary, local is the lowest node-id.
const lateAddStatusWedged = `[{
  "name":"pvc-la","node-id":0,"role":"Secondary",
  "devices":[
    {"volume":0,"minor":1000,"disk-state":"UpToDate"},
    {"volume":1,"minor":1001,"disk-state":"UpToDate"},
    {"volume":2,"minor":1002,"disk-state":"Inconsistent"}
  ],
  "connections":[{
    "peer-node-id":1,"name":"n2","connection-state":"Connected","peer-role":"Secondary",
    "peer_devices":[
      {"volume":0,"peer-disk-state":"UpToDate","replication-state":"Established","resync-suspended":"no"},
      {"volume":1,"peer-disk-state":"UpToDate","replication-state":"Established","resync-suspended":"no"},
      {"volume":2,"peer-disk-state":"Inconsistent","replication-state":"Established","resync-suspended":"no"}
    ]
  }]
}]`

func lateAddDR() *intent.DesiredResource {
	return &intent.DesiredResource{
		Name:     "pvc-la",
		NodeName: "n1",
		Volumes: []*intent.DesiredVolume{
			{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
			{VolumeNumber: 1, SizeKib: 1024 * 1024, StoragePool: "thin1"},
			{VolumeNumber: 2, SizeKib: 1024 * 1024, StoragePool: "thin1"},
		},
		Peers: []intent.DesiredPeer{{Name: "n2"}},
		DrbdOptions: map[string]string{
			"port": "7000", "node-id": "0", "address": "10.0.0.1", "minor": "1000",
		},
	}
}

// The wedge mints an UpToDate source via disconnect + per-volume
// new-current-uuid --clear-bitmap + reconnect (NOT primary --force, which
// the kernel rejects while a volume is Inconsistent).
func TestMaybeLateAddPromote_FiresForWedgedLateVolume(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect(lateAddStatusKey, storage.FakeResponse{Stdout: []byte(lateAddStatusWedged)})

	rec := NewReconciler(ReconcilerConfig{Adm: drbd.NewAdm(fx), NodeName: "n1"})

	if err := rec.maybeLateAddPromote(context.Background(), lateAddDR(), false); err != nil {
		t.Fatalf("maybeLateAddPromote: %v", err)
	}

	cmds := fx.CommandLines()
	if !slices.Contains(cmds, "drbdadm disconnect pvc-la") {
		t.Errorf("expected `drbdadm disconnect pvc-la`, got: %v", cmds)
	}
	if !slices.Contains(cmds, "drbdsetup new-current-uuid --clear-bitmap 1002") {
		t.Errorf("expected `drbdsetup new-current-uuid --clear-bitmap 1002` (the Inconsistent vol-2 minor), got: %v", cmds)
	}
	if !slices.Contains(cmds, "drbdadm adjust pvc-la") {
		t.Errorf("expected `drbdadm adjust pvc-la` (reconnect after mint), got: %v", cmds)
	}
	// Must NOT touch the UpToDate sibling volumes' minors.
	if slices.Contains(cmds, "drbdsetup new-current-uuid --clear-bitmap 1000") ||
		slices.Contains(cmds, "drbdsetup new-current-uuid --clear-bitmap 1001") {
		t.Errorf("must NOT clear-bitmap UpToDate sibling volumes, got: %v", cmds)
	}
	// The rejected resource-wide primary --force is gone.
	if slices.Contains(cmds, "drbdadm primary --force pvc-la") {
		t.Errorf("must NOT use the kernel-rejected `primary --force` on the late-add path, got: %v", cmds)
	}
}

// A diskless replica has no disk to promote — skip outright.
func TestMaybeLateAddPromote_SkipsDiskless(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect(lateAddStatusKey, storage.FakeResponse{Stdout: []byte(lateAddStatusWedged)})

	rec := NewReconciler(ReconcilerConfig{Adm: drbd.NewAdm(fx), NodeName: "n1"})

	if err := rec.maybeLateAddPromote(context.Background(), lateAddDR(), true); err != nil {
		t.Fatalf("maybeLateAddPromote: %v", err)
	}

	if slices.Contains(fx.CommandLines(), "drbdadm disconnect pvc-la") {
		t.Errorf("diskless replica must NOT mint a source, got: %v", fx.CommandLines())
	}
}

// A peer holding committed data for the volume vetoes the promote — the
// replica must SyncTarget from it instead (Bug 342 unrelated-data guard).
func TestMaybeLateAddPromote_SkipsWhenPeerHasData(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect(lateAddStatusKey, storage.FakeResponse{Stdout: []byte(`[{
	  "name":"pvc-la","node-id":0,"role":"Secondary",
	  "devices":[
	    {"volume":0,"disk-state":"UpToDate"},
	    {"volume":2,"disk-state":"Inconsistent"}
	  ],
	  "connections":[{
	    "peer-node-id":1,"name":"n2","connection-state":"Connected","peer-role":"Secondary",
	    "peer_devices":[
	      {"volume":0,"peer-disk-state":"UpToDate","replication-state":"Established","resync-suspended":"no"},
	      {"volume":2,"peer-disk-state":"UpToDate","replication-state":"SyncTarget","resync-suspended":"no"}
	    ]
	  }]
	}]`)})

	rec := NewReconciler(ReconcilerConfig{Adm: drbd.NewAdm(fx), NodeName: "n1"})

	if err := rec.maybeLateAddPromote(context.Background(), lateAddDR(), false); err != nil {
		t.Fatalf("maybeLateAddPromote: %v", err)
	}

	if slices.Contains(fx.CommandLines(), "drbdadm disconnect pvc-la") {
		t.Errorf("must NOT mint a source when a peer holds data, got: %v", fx.CommandLines())
	}
}
