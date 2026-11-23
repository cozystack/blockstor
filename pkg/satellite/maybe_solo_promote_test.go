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
	"os"
	"path/filepath"
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

// TestMaybeSoloPromote_FormatsOnlyABornEmptyReplica: the solo-promote
// runs the same promote, mkfs, demote sequence as the first activation.
// On a Filesystem RD whose replica blockstor did not create empty (an
// adopted LINSTOR volume is stamped SkipInitialSync=false; nil is not
// stamped yet) the `.mkfs.done` marker is always missing, so only the
// post-promote probe stood between mkfs and the adopted data. Such a
// replica is still promoted and demoted, but never formatted, even when
// that probe reads the device as empty.
func TestMaybeSoloPromote_FormatsOnlyABornEmptyReplica(t *testing.T) {
	yes, no := true, false

	cases := map[string]struct {
		skip *bool
		mkfs bool
	}{
		"born empty":      {skip: &yes, mkfs: true},
		"adopted":         {skip: &no},
		"not yet stamped": {skip: nil},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			fx := storage.NewFakeExec()
			fx.Expect(soloStatusKey, storage.FakeResponse{Stdout: []byte(soloStatusInconsistent)})
			fx.Expect("blkid -p -o export /dev/drbd1000", storage.FakeResponse{Err: storage.FakeExitError{Code: 2}})

			dir := t.TempDir()
			rec := NewReconciler(ReconcilerConfig{
				Adm:      drbd.NewAdm(fx),
				Exec:     fx,
				StateDir: dir,
				NodeName: "n1",
			})

			dr := soloDR()
			dr.Props = map[string]string{"FileSystem/Type": "ext4"}
			dr.SkipInitialSync = tc.skip

			if err := rec.maybeSoloPromote(context.Background(), dr, false); err != nil {
				t.Fatalf("maybeSoloPromote: %v", err)
			}

			cmds := fx.CommandLines()
			if !slices.Contains(cmds, "drbdadm primary --force pvc-solo") || !slices.Contains(cmds, "drbdadm secondary pvc-solo") {
				t.Errorf("the solo-promote itself must still run: %v", cmds)
			}

			if got := slices.Contains(cmds, "mkfs.ext4 /dev/drbd1000"); got != tc.mkfs {
				t.Errorf("mkfs.ext4 ran = %v, want %v: %v", got, tc.mkfs, cmds)
			}

			_, statErr := os.Stat(filepath.Join(dir, "pvc-solo.mkfs.done"))
			if (statErr == nil) != tc.mkfs {
				t.Errorf(".mkfs.done written = %v, want %v", statErr == nil, tc.mkfs)
			}
		})
	}
}
