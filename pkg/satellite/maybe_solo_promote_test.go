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

// Regression pins for the solo diskless→diskful toggle wedge (r-full
// Phase 6). maybeSoloPromote must force-primary a lone, peerless diskful
// replica wedged below UpToDate — and must NOT promote when peers exist
// or the replica is diskless.

const soloStatusKey = "drbdsetup status pvc-solo --json"

const soloStatusInconsistent = `[{
  "name":"pvc-solo","node-id":0,"role":"Secondary",
  "devices":[{"volume":0,"disk-state":"Inconsistent"}],
  "connections":[]
}]`

func soloDR(peers ...intent.DesiredPeer) *intent.DesiredResource {
	return &intent.DesiredResource{
		Name:     "pvc-solo",
		NodeName: "n1",
		Volumes: []*intent.DesiredVolume{
			{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
		},
		Peers: peers,
		DrbdOptions: map[string]string{
			"port": "7000", "node-id": "0", "address": "10.0.0.1", "minor": "1000",
		},
	}
}

// TestMaybeSoloPromote_FiresForLonePeerlessInconsistent: a solo replica
// (no peers) below UpToDate must be force-primaried, then demoted.
func TestMaybeSoloPromote_FiresForLonePeerlessInconsistent(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect(soloStatusKey, storage.FakeResponse{Stdout: []byte(soloStatusInconsistent)})

	rec := NewReconciler(ReconcilerConfig{
		Adm:      drbd.NewAdm(fx),
		NodeName: "n1",
	})

	if err := rec.maybeSoloPromote(context.Background(), soloDR(), false); err != nil {
		t.Fatalf("maybeSoloPromote: %v", err)
	}

	cmds := fx.CommandLines()
	if !slices.Contains(cmds, "drbdadm primary --force pvc-solo") {
		t.Errorf("expected `drbdadm primary --force pvc-solo`, got: %v", cmds)
	}

	if !slices.Contains(cmds, "drbdadm secondary pvc-solo") {
		t.Errorf("expected `drbdadm secondary pvc-solo` (demote after promote), got: %v", cmds)
	}
}

// TestMaybeSoloPromote_SkipsWhenPeersPresent: a replica with peers in its
// desired set is not solo — the recovery / SyncTarget paths own
// convergence and a force-primary could mint a divergent Current UUID.
func TestMaybeSoloPromote_SkipsWhenPeersPresent(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect(soloStatusKey, storage.FakeResponse{Stdout: []byte(soloStatusInconsistent)})

	rec := NewReconciler(ReconcilerConfig{
		Adm:      drbd.NewAdm(fx),
		NodeName: "n1",
	})

	if err := rec.maybeSoloPromote(context.Background(), soloDR(intent.DesiredPeer{Name: "n2"}), false); err != nil {
		t.Fatalf("maybeSoloPromote: %v", err)
	}

	if slices.Contains(fx.CommandLines(), "drbdadm primary --force pvc-solo") {
		t.Errorf("expected NO promote when a desired peer exists, got: %v", fx.CommandLines())
	}
}

// TestMaybeSoloPromote_SkipsDiskless: a diskless replica has no disk to
// promote.
func TestMaybeSoloPromote_SkipsDiskless(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect(soloStatusKey, storage.FakeResponse{Stdout: []byte(soloStatusInconsistent)})

	rec := NewReconciler(ReconcilerConfig{
		Adm:      drbd.NewAdm(fx),
		NodeName: "n1",
	})

	if err := rec.maybeSoloPromote(context.Background(), soloDR(), true); err != nil {
		t.Fatalf("maybeSoloPromote: %v", err)
	}

	if slices.Contains(fx.CommandLines(), "drbdadm primary --force pvc-solo") {
		t.Errorf("expected NO promote for a diskless replica, got: %v", fx.CommandLines())
	}
}

// TestMaybeSoloPromote_ThrottlesRepeatFire: the second back-to-back call
// is throttled (recoveryPromoteDue), so the kernel resync from the first
// promote is not starved by a hot-loop.
func TestMaybeSoloPromote_ThrottlesRepeatFire(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect(soloStatusKey, storage.FakeResponse{Stdout: []byte(soloStatusInconsistent)})

	rec := NewReconciler(ReconcilerConfig{
		Adm:      drbd.NewAdm(fx),
		NodeName: "n1",
	})

	dr := soloDR()
	if err := rec.maybeSoloPromote(context.Background(), dr, false); err != nil {
		t.Fatalf("maybeSoloPromote #1: %v", err)
	}

	before := len(fx.CommandLines())

	if err := rec.maybeSoloPromote(context.Background(), dr, false); err != nil {
		t.Fatalf("maybeSoloPromote #2: %v", err)
	}

	// The throttle must suppress a second promote — only the probe
	// (NeedsSoloPromote's drbdsetup status) may run again, never another
	// `primary --force`.
	after := fx.CommandLines()[before:]
	if slices.Contains(after, "drbdadm primary --force pvc-solo") {
		t.Errorf("expected throttle to suppress the immediate second promote, got: %v", after)
	}
}
