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

// Regression pins for the BUG-028 latch-free mkfs-retry promote-safety
// predicate (SafeForMkfsRetryPromote). The predicate authorises a
// promote→mkfs→demote retry WITHOUT the dispatcher's auto-primary
// blessing, so it must be provably conservative: it may only return
// true when every replica is Secondary and every connected peer-device
// is a lock-step UpToDate sibling (or an intentional Diskless witness)
// — i.e. when `primary --force` cannot mint an unrelated UUID against
// anyone and the mkfs writes replicate to bit-identical copies.

const mkfsRetryStatusKey = "drbdsetup status pvc-b028 --json"

func admWithMkfsRetryStatus(t *testing.T, json string) *drbd.Adm {
	t.Helper()

	fx := storage.NewFakeExec()
	fx.Responses[mkfsRetryStatusKey] = storage.FakeResponse{Stdout: []byte(json)}

	return drbd.NewAdm(fx)
}

// TestSafeForMkfsRetryPromote_AllSecondaryLockStepUpToDate: the exact
// BUG-028 terminal state between two drbd-reactor promote cycles —
// local Secondary UpToDate, diskful peer Secondary UpToDate, diskless
// tiebreaker — must authorise the retry.
func TestSafeForMkfsRetryPromote_AllSecondaryLockStepUpToDate(t *testing.T) {
	adm := admWithMkfsRetryStatus(t, `[{
	  "name":"pvc-b028","node-id":0,"role":"Secondary",
	  "devices":[{"volume":0,"disk-state":"UpToDate"},{"volume":1,"disk-state":"UpToDate"}],
	  "connections":[{
	    "peer-node-id":1,"name":"n2","connection-state":"Connected",
	    "peer-role":"Secondary",
	    "peer_devices":[{"volume":0,"peer-disk-state":"UpToDate"},{"volume":1,"peer-disk-state":"UpToDate"}]
	  },{
	    "peer-node-id":2,"name":"n3","connection-state":"Connected",
	    "peer-role":"Secondary",
	    "peer_devices":[{"volume":0,"peer-disk-state":"Diskless"},{"volume":1,"peer-disk-state":"Diskless"}]
	  }]
	}]`)

	if !adm.SafeForMkfsRetryPromote(t.Context(), "pvc-b028") {
		t.Fatal("all-Secondary lock-step UpToDate set (with diskless witness) must authorise the latch-free mkfs retry")
	}
}

// TestSafeForMkfsRetryPromote_ForeignPrimaryDefers: drbd-reactor (or
// any external promoter) currently holds the device on a peer →
// refuse; the caller retries on a later reconcile once it demoted.
func TestSafeForMkfsRetryPromote_ForeignPrimaryDefers(t *testing.T) {
	adm := admWithMkfsRetryStatus(t, `[{
	  "name":"pvc-b028","node-id":0,"role":"Secondary",
	  "devices":[{"volume":0,"disk-state":"UpToDate"}],
	  "connections":[{
	    "peer-node-id":1,"name":"n2","connection-state":"Connected",
	    "peer-role":"Primary",
	    "peer_devices":[{"volume":0,"peer-disk-state":"UpToDate"}]
	  }]
	}]`)

	if adm.SafeForMkfsRetryPromote(t.Context(), "pvc-b028") {
		t.Fatal("a foreign Primary peer must defer the latch-free mkfs retry")
	}
}

// TestSafeForMkfsRetryPromote_LocalPrimaryRefuses: the local slot is
// already Primary (a consumer or a previous dance holds the device) →
// refuse.
func TestSafeForMkfsRetryPromote_LocalPrimaryRefuses(t *testing.T) {
	adm := admWithMkfsRetryStatus(t, `[{
	  "name":"pvc-b028","node-id":0,"role":"Primary",
	  "devices":[{"volume":0,"disk-state":"UpToDate"}],
	  "connections":[{
	    "peer-node-id":1,"name":"n2","connection-state":"Connected",
	    "peer-role":"Secondary",
	    "peer_devices":[{"volume":0,"peer-disk-state":"UpToDate"}]
	  }]
	}]`)

	if adm.SafeForMkfsRetryPromote(t.Context(), "pvc-b028") {
		t.Fatal("a local Primary role must refuse the latch-free mkfs retry")
	}
}

// TestSafeForMkfsRetryPromote_DisconnectedPeerRefuses: a peer whose
// disk state is DUnknown (connection down) could be an OFFLINE DATA
// HOLDER — promoting against it is the Bug 342 unrelated-data wedge,
// and mkfs could overwrite real data once it reconnects. Refuse.
func TestSafeForMkfsRetryPromote_DisconnectedPeerRefuses(t *testing.T) {
	adm := admWithMkfsRetryStatus(t, `[{
	  "name":"pvc-b028","node-id":0,"role":"Secondary",
	  "devices":[{"volume":0,"disk-state":"UpToDate"}],
	  "connections":[{
	    "peer-node-id":1,"name":"n2","connection-state":"Connecting",
	    "peer-role":"Unknown",
	    "peer_devices":[{"volume":0,"peer-disk-state":"DUnknown"}]
	  }]
	}]`)

	if adm.SafeForMkfsRetryPromote(t.Context(), "pvc-b028") {
		t.Fatal("a disconnected (DUnknown) peer must refuse the latch-free mkfs retry — it could be an offline data holder")
	}
}

// TestSafeForMkfsRetryPromote_InconsistentPeerRefuses: a peer still
// Inconsistent is not in lock-step with the local copy; the retry must
// wait (or let the Bug 366 recovery-promote own that state).
func TestSafeForMkfsRetryPromote_InconsistentPeerRefuses(t *testing.T) {
	adm := admWithMkfsRetryStatus(t, `[{
	  "name":"pvc-b028","node-id":0,"role":"Secondary",
	  "devices":[{"volume":0,"disk-state":"UpToDate"}],
	  "connections":[{
	    "peer-node-id":1,"name":"n2","connection-state":"Connected",
	    "peer-role":"Secondary",
	    "peer_devices":[{"volume":0,"peer-disk-state":"Inconsistent","replication-state":"Established","resync-suspended":"no"}]
	  }]
	}]`)

	if adm.SafeForMkfsRetryPromote(t.Context(), "pvc-b028") {
		t.Fatal("an Inconsistent peer must refuse the latch-free mkfs retry")
	}
}

// TestSafeForMkfsRetryPromote_LocalNotUpToDateRefuses: the retry adds
// a missing filesystem to a HEALTHY converged replica — it must never
// promote an Inconsistent local copy.
func TestSafeForMkfsRetryPromote_LocalNotUpToDateRefuses(t *testing.T) {
	adm := admWithMkfsRetryStatus(t, `[{
	  "name":"pvc-b028","node-id":0,"role":"Secondary",
	  "devices":[{"volume":0,"disk-state":"Inconsistent"}],
	  "connections":[{
	    "peer-node-id":1,"name":"n2","connection-state":"Connected",
	    "peer-role":"Secondary",
	    "peer_devices":[{"volume":0,"peer-disk-state":"UpToDate"}]
	  }]
	}]`)

	if adm.SafeForMkfsRetryPromote(t.Context(), "pvc-b028") {
		t.Fatal("a non-UpToDate local volume must refuse the latch-free mkfs retry")
	}
}

// TestSafeForMkfsRetryPromote_ProbeFailureRefuses: any probe / parse
// failure must be conservative (false) — the retry just waits for the
// next reconcile.
func TestSafeForMkfsRetryPromote_ProbeFailureRefuses(t *testing.T) {
	adm := admWithMkfsRetryStatus(t, `not-json`)

	if adm.SafeForMkfsRetryPromote(t.Context(), "pvc-b028") {
		t.Fatal("a malformed status probe must refuse the latch-free mkfs retry")
	}
}

// Day0SiblingSetConnected pins (BUG-028 bypass coverage probe). Same
// conservatism contract as SafeForMkfsRetryPromote, with ONE deliberate
// relaxation: a not-yet-handshaken (DUnknown) peer is tolerated when it
// is a configured diskless witness — it carries no data by construction
// and must not cost the one-shot first-activation mkfs.

// TestDay0SiblingSetConnected_DisklessWitnessStillConnecting: the day0
// race shape — diskful sibling Connected+UpToDate, tiebreaker witness
// still handshaking → covered.
func TestDay0SiblingSetConnected_DisklessWitnessStillConnecting(t *testing.T) {
	adm := admWithMkfsRetryStatus(t, `[{
	  "name":"pvc-b028","node-id":0,"role":"Secondary",
	  "devices":[{"volume":0,"disk-state":"UpToDate"}],
	  "connections":[{
	    "peer-node-id":1,"name":"n2","connection-state":"Connected",
	    "peer-role":"Secondary",
	    "peer_devices":[{"volume":0,"peer-disk-state":"UpToDate"}]
	  },{
	    "peer-node-id":2,"name":"n3","connection-state":"Connecting",
	    "peer-role":"Unknown",
	    "peer_devices":[{"volume":0,"peer-disk-state":"DUnknown"}]
	  }]
	}]`)

	if !adm.Day0SiblingSetConnected(t.Context(), "pvc-b028", map[string]bool{"n3": true}) {
		t.Fatal("a still-connecting DISKLESS witness must not block the day0 bypass coverage")
	}
}

// TestDay0SiblingSetConnected_DiskfulPeerStillConnecting: the same
// DUnknown peer WITHOUT the diskless marking is a potential offline
// data holder → refuse.
func TestDay0SiblingSetConnected_DiskfulPeerStillConnecting(t *testing.T) {
	adm := admWithMkfsRetryStatus(t, `[{
	  "name":"pvc-b028","node-id":0,"role":"Secondary",
	  "devices":[{"volume":0,"disk-state":"UpToDate"}],
	  "connections":[{
	    "peer-node-id":1,"name":"n2","connection-state":"Connected",
	    "peer-role":"Secondary",
	    "peer_devices":[{"volume":0,"peer-disk-state":"UpToDate"}]
	  },{
	    "peer-node-id":2,"name":"n3","connection-state":"Connecting",
	    "peer-role":"Unknown",
	    "peer_devices":[{"volume":0,"peer-disk-state":"DUnknown"}]
	  }]
	}]`)

	if adm.Day0SiblingSetConnected(t.Context(), "pvc-b028", map[string]bool{}) {
		t.Fatal("a not-yet-handshaken DISKFUL peer must refuse the day0 bypass coverage")
	}
}

// TestDay0SiblingSetConnected_ForeignPrimaryRefuses: an external
// promoter already holds the device → defer to the latch-free retry.
func TestDay0SiblingSetConnected_ForeignPrimaryRefuses(t *testing.T) {
	adm := admWithMkfsRetryStatus(t, `[{
	  "name":"pvc-b028","node-id":0,"role":"Secondary",
	  "devices":[{"volume":0,"disk-state":"UpToDate"}],
	  "connections":[{
	    "peer-node-id":1,"name":"n2","connection-state":"Connected",
	    "peer-role":"Primary",
	    "peer_devices":[{"volume":0,"peer-disk-state":"UpToDate"}]
	  }]
	}]`)

	if adm.Day0SiblingSetConnected(t.Context(), "pvc-b028", map[string]bool{}) {
		t.Fatal("a foreign Primary must refuse the day0 bypass coverage")
	}
}

// TestDay0SiblingSetConnected_InconsistentPeerRefuses: an Inconsistent
// peer-device is not a lock-step day0 sibling → refuse.
func TestDay0SiblingSetConnected_InconsistentPeerRefuses(t *testing.T) {
	adm := admWithMkfsRetryStatus(t, `[{
	  "name":"pvc-b028","node-id":0,"role":"Secondary",
	  "devices":[{"volume":0,"disk-state":"UpToDate"}],
	  "connections":[{
	    "peer-node-id":1,"name":"n2","connection-state":"Connected",
	    "peer-role":"Secondary",
	    "peer_devices":[{"volume":0,"peer-disk-state":"Inconsistent","replication-state":"Established","resync-suspended":"no"}]
	  }]
	}]`)

	if adm.Day0SiblingSetConnected(t.Context(), "pvc-b028", map[string]bool{}) {
		t.Fatal("an Inconsistent peer must refuse the day0 bypass coverage")
	}
}
