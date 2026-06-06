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

// Regression pins for the Bug 366 recovery-promote predicate
// (NeedsRecoveryPromote). The predicate exists to unstick a fresh RD
// whose initial sync WEDGED (an Inconsistent peer that nothing is
// driving to UpToDate, no Primary anywhere). It must NOT fire on a
// HEALTHY progressing initial sync — an Inconsistent peer already
// being served by an active SyncSource — where the repeated
// `primary --force` → `secondary` churn (one cycle per throttle
// window for the whole sync) is pure noise. Caught live on a fresh
// FILE_THIN 512M create: the recovery-promote fired every ~10 s for
// the entire ~2 min resync.

const recoveryPromoteKey = "drbdsetup status pvc-rec --json"

func admWithRecoveryStatus(t *testing.T, json string) *drbd.Adm {
	t.Helper()

	fx := storage.NewFakeExec()
	fx.Responses[recoveryPromoteKey] = storage.FakeResponse{Stdout: []byte(json)}

	return drbd.NewAdm(fx)
}

// TestNeedsRecoveryPromote_WedgedInconsistentPeer: the genuine Bug 366
// shape — local UpToDate, a diskful peer stuck Inconsistent with NO
// resync running toward it (replication Established), no Primary
// anywhere, and we hold the lowest node-id → promote.
func TestNeedsRecoveryPromote_WedgedInconsistentPeer(t *testing.T) {
	adm := admWithRecoveryStatus(t, `[{
	  "name":"pvc-rec","node-id":0,"role":"Secondary",
	  "devices":[{"volume":0,"disk-state":"UpToDate"}],
	  "connections":[{
	    "peer-node-id":1,"name":"n2","connection-state":"Connected",
	    "peer-role":"Secondary",
	    "peer_devices":[{"volume":0,"peer-disk-state":"Inconsistent",
	      "replication-state":"Established","resync-suspended":"no"}]
	  }]
	}]`)

	if !adm.NeedsRecoveryPromote(t.Context(), "pvc-rec") {
		t.Fatal("wedged Inconsistent peer (Established, no sync running) must trigger the recovery-promote")
	}
}

// TestNeedsRecoveryPromote_HealthyProgressingSync: the same peer is
// Inconsistent but a live resync is already serving it (SyncSource,
// resync-suspended no) → leave it alone. This is the churn-suppression
// pin: every fresh create that legitimately resyncs (e.g. a relocate
// SyncTarget) must complete WITHOUT recovery-promote interference.
func TestNeedsRecoveryPromote_HealthyProgressingSync(t *testing.T) {
	adm := admWithRecoveryStatus(t, `[{
	  "name":"pvc-rec","node-id":0,"role":"Secondary",
	  "devices":[{"volume":0,"disk-state":"UpToDate"}],
	  "connections":[{
	    "peer-node-id":1,"name":"n2","connection-state":"Connected",
	    "peer-role":"Secondary",
	    "peer_devices":[{"volume":0,"peer-disk-state":"Inconsistent",
	      "replication-state":"SyncSource","resync-suspended":"no"}]
	  }]
	}]`)

	if adm.NeedsRecoveryPromote(t.Context(), "pvc-rec") {
		t.Fatal("actively progressing SyncSource must NOT trigger the recovery-promote (promote churn)")
	}
}

// TestNeedsRecoveryPromote_BitmapExchangeIsHealthy: WFBitMapS is the
// bitmap-exchange step immediately before SyncSource — the sync
// machinery is running; suppress the promote there too.
func TestNeedsRecoveryPromote_BitmapExchangeIsHealthy(t *testing.T) {
	adm := admWithRecoveryStatus(t, `[{
	  "name":"pvc-rec","node-id":0,"role":"Secondary",
	  "devices":[{"volume":0,"disk-state":"UpToDate"}],
	  "connections":[{
	    "peer-node-id":1,"name":"n2","connection-state":"Connected",
	    "peer-role":"Secondary",
	    "peer_devices":[{"volume":0,"peer-disk-state":"Inconsistent",
	      "replication-state":"WFBitMapS","resync-suspended":"no"}]
	  }]
	}]`)

	if adm.NeedsRecoveryPromote(t.Context(), "pvc-rec") {
		t.Fatal("WFBitMapS (bitmap exchange before SyncSource) must NOT trigger the recovery-promote")
	}
}

// TestNeedsRecoveryPromote_SuspendedSyncSourceStillFires: the original
// Bug 366 wedge — dual SyncSource collapsed into resync-suspended:peer
// at done:0.00. replication-state still reads SyncSource but the sync
// is NOT progressing; the recovery-promote must still fire.
func TestNeedsRecoveryPromote_SuspendedSyncSourceStillFires(t *testing.T) {
	adm := admWithRecoveryStatus(t, `[{
	  "name":"pvc-rec","node-id":0,"role":"Secondary",
	  "devices":[{"volume":0,"disk-state":"UpToDate"}],
	  "connections":[{
	    "peer-node-id":1,"name":"n2","connection-state":"Connected",
	    "peer-role":"Secondary",
	    "peer_devices":[{"volume":0,"peer-disk-state":"Inconsistent",
	      "replication-state":"SyncSource","resync-suspended":"peer"}]
	  }]
	}]`)

	if !adm.NeedsRecoveryPromote(t.Context(), "pvc-rec") {
		t.Fatal("suspended SyncSource (the dual-SyncSource Bug 366 wedge) must still trigger the recovery-promote")
	}
}

// TestNeedsRecoveryPromote_PeerPrimaryVeto: any Primary anywhere
// already drives the sync — never promote over it (unchanged guard,
// re-pinned here alongside the new suppressor).
func TestNeedsRecoveryPromote_PeerPrimaryVeto(t *testing.T) {
	adm := admWithRecoveryStatus(t, `[{
	  "name":"pvc-rec","node-id":0,"role":"Secondary",
	  "devices":[{"volume":0,"disk-state":"UpToDate"}],
	  "connections":[{
	    "peer-node-id":1,"name":"n2","connection-state":"Connected",
	    "peer-role":"Primary",
	    "peer_devices":[{"volume":0,"peer-disk-state":"Inconsistent",
	      "replication-state":"Established","resync-suspended":"no"}]
	  }]
	}]`)

	if adm.NeedsRecoveryPromote(t.Context(), "pvc-rec") {
		t.Fatal("a peer Primary must veto the recovery-promote")
	}
}
