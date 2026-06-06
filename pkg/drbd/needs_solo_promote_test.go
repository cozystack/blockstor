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

// Regression pin for the solo diskless→diskful toggle wedge (r-full
// Phase 6). NeedsSoloPromote must return true ONLY for a lone, peerless
// diskful replica that is below UpToDate — the exact kernel shape a
// `r td -s <pool>` on the last/only replica of an initialized RD lands
// in, where the dispatcher's auto-primary suppression and the
// offline-safety seed-refusal leave it Inconsistent with no SyncSource.

const soloPromoteKey = "drbdsetup status pvc-solo --json"

// errSoloProbe is the static probe-failure sentinel for the
// conservative-on-error case.
var errSoloProbe = errors.New("drbdsetup status: exit 1")

func admWithStatus(t *testing.T, json string) (*drbd.Adm, *storage.FakeExec) {
	t.Helper()

	fx := storage.NewFakeExec()
	fx.Responses[soloPromoteKey] = storage.FakeResponse{Stdout: []byte(json)}

	return drbd.NewAdm(fx), fx
}

// TestNeedsSoloPromote_LoneInconsistent: a single resource block, zero
// connections, Secondary role, local disk Inconsistent → promote needed.
func TestNeedsSoloPromote_LoneInconsistent(t *testing.T) {
	adm, _ := admWithStatus(t, `[{
	  "name":"pvc-solo","node-id":0,"role":"Secondary",
	  "devices":[{"volume":0,"disk-state":"Inconsistent"}],
	  "connections":[]
	}]`)

	if !adm.NeedsSoloPromote(t.Context(), "pvc-solo") {
		t.Fatal("expected NeedsSoloPromote=true for a lone Inconsistent diskful replica")
	}
}

// TestNeedsSoloPromote_LoneConsistent: Consistent (below UpToDate) is
// also a promote target for a peerless replica.
func TestNeedsSoloPromote_LoneConsistent(t *testing.T) {
	adm, _ := admWithStatus(t, `[{
	  "name":"pvc-solo","node-id":1,"role":"Secondary",
	  "devices":[{"volume":0,"disk-state":"Consistent"}],
	  "connections":[]
	}]`)

	if !adm.NeedsSoloPromote(t.Context(), "pvc-solo") {
		t.Fatal("expected NeedsSoloPromote=true for a lone Consistent diskful replica")
	}
}

// TestNeedsSoloPromote_HasPeer: a peer connection exists → NOT solo;
// the recovery-promote / SyncTarget paths own convergence, and a
// force-primary against a peer could mint a divergent Current UUID.
func TestNeedsSoloPromote_HasPeer(t *testing.T) {
	adm, _ := admWithStatus(t, `[{
	  "name":"pvc-solo","node-id":0,"role":"Secondary",
	  "devices":[{"volume":0,"disk-state":"Inconsistent"}],
	  "connections":[{"peer-node-id":1,"name":"n2","connection-state":"Connected","peer-role":"Secondary",
	    "peer_devices":[{"volume":0,"peer-disk-state":"UpToDate"}]}]
	}]`)

	if adm.NeedsSoloPromote(t.Context(), "pvc-solo") {
		t.Fatal("expected NeedsSoloPromote=false when a peer connection exists")
	}
}

// TestNeedsSoloPromote_AlreadyUpToDate: nothing to promote.
func TestNeedsSoloPromote_AlreadyUpToDate(t *testing.T) {
	adm, _ := admWithStatus(t, `[{
	  "name":"pvc-solo","node-id":0,"role":"Secondary",
	  "devices":[{"volume":0,"disk-state":"UpToDate"}],
	  "connections":[]
	}]`)

	if adm.NeedsSoloPromote(t.Context(), "pvc-solo") {
		t.Fatal("expected NeedsSoloPromote=false when the local replica is already UpToDate")
	}
}

// TestNeedsSoloPromote_AlreadyPrimary: never disturb a Primary slot.
func TestNeedsSoloPromote_AlreadyPrimary(t *testing.T) {
	adm, _ := admWithStatus(t, `[{
	  "name":"pvc-solo","node-id":0,"role":"Primary",
	  "devices":[{"volume":0,"disk-state":"Inconsistent"}],
	  "connections":[]
	}]`)

	if adm.NeedsSoloPromote(t.Context(), "pvc-solo") {
		t.Fatal("expected NeedsSoloPromote=false when the local replica is already Primary")
	}
}

// TestNeedsSoloPromote_Diskless: a diskless local has no disk to promote.
func TestNeedsSoloPromote_Diskless(t *testing.T) {
	adm, _ := admWithStatus(t, `[{
	  "name":"pvc-solo","node-id":0,"role":"Secondary",
	  "devices":[{"volume":0,"disk-state":"Diskless"}],
	  "connections":[]
	}]`)

	if adm.NeedsSoloPromote(t.Context(), "pvc-solo") {
		t.Fatal("expected NeedsSoloPromote=false for a diskless local replica")
	}
}

// TestNeedsSoloPromote_MultiVolumeAllBelow: every diskful volume below
// UpToDate qualifies; one already-UpToDate volume does not block the
// others — but a Diskless volume disqualifies the whole replica.
func TestNeedsSoloPromote_MultiVolumeMixed(t *testing.T) {
	// vol0 Inconsistent, vol1 UpToDate → mixed: at least one volume is
	// already UpToDate, so the replica is not uniformly below UpToDate.
	adm, _ := admWithStatus(t, `[{
	  "name":"pvc-solo","node-id":0,"role":"Secondary",
	  "devices":[{"volume":0,"disk-state":"Inconsistent"},{"volume":1,"disk-state":"UpToDate"}],
	  "connections":[]
	}]`)

	if adm.NeedsSoloPromote(t.Context(), "pvc-solo") {
		t.Fatal("expected NeedsSoloPromote=false when any volume is already UpToDate")
	}
}

// TestNeedsSoloPromote_ProbeError: a failed drbdsetup probe is
// conservative (false).
func TestNeedsSoloPromote_ProbeError(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Responses[soloPromoteKey] = storage.FakeResponse{Err: errSoloProbe}
	adm := drbd.NewAdm(fx)

	if adm.NeedsSoloPromote(t.Context(), "pvc-solo") {
		t.Fatal("expected NeedsSoloPromote=false on probe error")
	}
}
