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
	"errors"
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

// soloReconciler wires a reconciler over fx with a state dir, recording
// the replica as born solo when bornSolo is set: what ensureMetadata
// leaves behind for a replica whose metadata it created with no other
// copy around.
func soloReconciler(t *testing.T, fx *storage.FakeExec, bornSolo bool) (*Reconciler, string) {
	t.Helper()

	dir := t.TempDir()
	if bornSolo {
		err := os.WriteFile(filepath.Join(dir, "pvc-solo"+bornSoloSuffix), nil, 0o600)
		if err != nil {
			t.Fatalf("seed born-solo record: %v", err)
		}
	}

	return NewReconciler(ReconcilerConfig{
		Adm:      drbd.NewAdm(fx),
		NodeName: "n1",
		StateDir: dir,
	}), dir
}

// TestMaybeSoloPromote_FiresForLonePeerlessInconsistent: a solo replica
// (no peers) below UpToDate must be force-primaried, then demoted.
func TestMaybeSoloPromote_FiresForLonePeerlessInconsistent(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect(soloStatusKey, storage.FakeResponse{Stdout: []byte(soloStatusInconsistent)})

	rec, dir := soloReconciler(t, fx, true)

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

	// One shot: the record is spent by the promote, so a later drop
	// below UpToDate (data synced from a peer since) is never forced.
	if _, err := os.Stat(filepath.Join(dir, "pvc-solo"+bornSoloSuffix)); !os.IsNotExist(err) {
		t.Errorf("born-solo record must be spent by the promote, stat err=%v", err)
	}
}

// TestMaybeSoloPromote_SkipsWhenPeersPresent: a replica with peers in its
// desired set is not solo — the recovery / SyncTarget paths own
// convergence and a force-primary could mint a divergent Current UUID.
func TestMaybeSoloPromote_SkipsWhenPeersPresent(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect(soloStatusKey, storage.FakeResponse{Stdout: []byte(soloStatusInconsistent)})

	rec, _ := soloReconciler(t, fx, true)

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

	rec, _ := soloReconciler(t, fx, true)

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

	rec, _ := soloReconciler(t, fx, true)

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
			// The solo-promote fires only for a replica born alone.
			err := os.WriteFile(filepath.Join(dir, "pvc-solo"+bornSoloSuffix), nil, 0o600)
			if err != nil {
				t.Fatalf("seed born-solo record: %v", err)
			}

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

// Regression pins for the delete-mid-sync promote: a replica that was
// syncing from peers which then went is Inconsistent because the data is
// elsewhere, and LINSTOR never forces primary on it. Each case below has
// zero desired peers and a kernel slot with zero connections, which is
// all the solo-promote used to check.

func assertNoSoloPromote(t *testing.T, fx *storage.FakeExec) {
	t.Helper()

	if slices.Contains(fx.CommandLines(), "drbdadm primary --force pvc-solo") {
		t.Errorf("expected NO promote, got: %v", fx.CommandLines())
	}
}

// TestMaybeSoloPromote_SkipsReplicaBeingDeleted: the replica or its
// definition is being deleted. Losing every peer is the end state of the
// delete, not a wedge, even for a replica that was born alone.
func TestMaybeSoloPromote_SkipsReplicaBeingDeleted(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect(soloStatusKey, storage.FakeResponse{Stdout: []byte(soloStatusInconsistent)})

	rec, _ := soloReconciler(t, fx, true)

	dr := soloDR()
	dr.Deleting = true

	if err := rec.maybeSoloPromote(context.Background(), dr, false); err != nil {
		t.Fatalf("maybeSoloPromote: %v", err)
	}

	assertNoSoloPromote(t, fx)
}

// TestMaybeSoloPromote_SkipsReplicaNotBornSolo: the replica's metadata
// was created while it had peers (the issue's restored volume, or a
// replica whose UpToDate peer was later forced out). With no solo-birth
// record it is never promoted, deletion or not.
func TestMaybeSoloPromote_SkipsReplicaNotBornSolo(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect(soloStatusKey, storage.FakeResponse{Stdout: []byte(soloStatusInconsistent)})

	rec, _ := soloReconciler(t, fx, false)

	if err := rec.maybeSoloPromote(context.Background(), soloDR(), false); err != nil {
		t.Fatalf("maybeSoloPromote: %v", err)
	}

	assertNoSoloPromote(t, fx)
}

// TestMaybeSoloPromote_SkipsWhenAnotherDiskfulReplicaExists: a diskful
// replica the desired peers leave out (deactivated, being deleted, not
// allocated yet) may hold newer data. No promote, and the solo-birth
// record goes: from now on this replica was not alone.
func TestMaybeSoloPromote_SkipsWhenAnotherDiskfulReplicaExists(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect(soloStatusKey, storage.FakeResponse{Stdout: []byte(soloStatusInconsistent)})

	rec, dir := soloReconciler(t, fx, true)

	dr := soloDR()
	dr.OtherDiskfulReplica = true

	// The apply drops the record before its first step, here the
	// storage layer, which fails because no provider serves the pool.
	applyFailing(t, rec, dr)

	if _, err := os.Stat(filepath.Join(dir, "pvc-solo"+bornSoloSuffix)); !os.IsNotExist(err) {
		t.Errorf("born-solo record must be dropped once another diskful replica appeared, stat err=%v", err)
	}

	if err := rec.maybeSoloPromote(context.Background(), dr, false); err != nil {
		t.Fatalf("maybeSoloPromote: %v", err)
	}

	assertNoSoloPromote(t, fx)

	// The peer goes again: the replica stays unpromoted.
	if err := rec.maybeSoloPromote(context.Background(), soloDR(), false); err != nil {
		t.Fatalf("maybeSoloPromote after the peer left: %v", err)
	}

	assertNoSoloPromote(t, fx)
}

// TestMaybeSoloPromote_SkipsWhenPeerHasData: a peer is known to hold
// data, so this replica is not the only copy.
func TestMaybeSoloPromote_SkipsWhenPeerHasData(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect(soloStatusKey, storage.FakeResponse{Stdout: []byte(soloStatusInconsistent)})

	rec, _ := soloReconciler(t, fx, true)

	dr := soloDR()
	dr.PeerHasData = true

	if err := rec.maybeSoloPromote(context.Background(), dr, false); err != nil {
		t.Fatalf("maybeSoloPromote: %v", err)
	}

	assertNoSoloPromote(t, fx)
}

// TestMaybeSoloPromote_SkipsWithoutStateDir: with nowhere to keep the
// solo-birth record the promote has no evidence and stays off.
func TestMaybeSoloPromote_SkipsWithoutStateDir(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect(soloStatusKey, storage.FakeResponse{Stdout: []byte(soloStatusInconsistent)})

	rec := NewReconciler(ReconcilerConfig{Adm: drbd.NewAdm(fx), NodeName: "n1"})

	if err := rec.maybeSoloPromote(context.Background(), soloDR(), false); err != nil {
		t.Fatalf("maybeSoloPromote: %v", err)
	}

	assertNoSoloPromote(t, fx)
}

// TestEnsureMetadata_RecordsSoloBirth: fresh metadata created with no
// other copy around (the lone diskless-to-diskful toggle) is recorded as
// a solo birth; fresh metadata created with a peer, with another diskful
// replica elsewhere, or during a delete is not, and clears an older
// record left under the same name.
func TestEnsureMetadata_RecordsSoloBirth(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(dr *intent.DesiredResource)
		want   bool
	}{
		{name: "lone", mutate: func(*intent.DesiredResource) {}, want: true},
		{name: "with peer", mutate: func(dr *intent.DesiredResource) {
			dr.Peers = []intent.DesiredPeer{{Name: "n2"}}
			dr.DrbdOptions["peer.n2.address"] = "10.0.0.2"
			dr.DrbdOptions["peer.n2.node-id"] = "1"
			dr.DrbdOptions["peer.n2.port"] = "7000"
		}, want: false},
		{name: "other diskful replica", mutate: func(dr *intent.DesiredResource) {
			dr.OtherDiskfulReplica = true
		}, want: false},
		{name: "deleting", mutate: func(dr *intent.DesiredResource) {
			dr.Deleting = true
		}, want: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fx := storage.NewFakeExec()
			fx.Expect("drbdadm dump-md pvc-solo/0", storage.FakeResponse{Err: errDumpMdNoMeta})

			rec, dir := soloReconciler(t, fx, true)

			dr := soloDR()
			tc.mutate(dr)

			err := rec.ensureMetadata(context.Background(), dr, map[int32]string{0: "/dev/zvol/pvc-solo_00000"},
				filepath.Join(dir, "pvc-solo.md-created"), false)
			if err != nil {
				t.Fatalf("ensureMetadata: %v", err)
			}

			if !slices.Contains(fx.CommandLines(), "drbdadm create-md --force --max-peers=15 pvc-solo/0") {
				t.Fatalf("precondition: fresh metadata was not created: %v", fx.CommandLines())
			}

			if got := rec.bornSolo("pvc-solo"); got != tc.want {
				t.Errorf("bornSolo = %v, want %v", got, tc.want)
			}
		})
	}
}

// applyFailing runs one Apply pass that fails in the storage layer: no
// provider serves the replica's pool.
func applyFailing(t *testing.T, rec *Reconciler, dr *intent.DesiredResource) {
	t.Helper()

	results, err := rec.Apply(context.Background(), []*intent.DesiredResource{dr})
	if err != nil || len(results) != 1 || results[0].GetOk() {
		t.Fatalf("precondition: apply must fail in the storage layer, got %+v, %v", results, err)
	}
}

// TestApply_DropsSoloBirthBeforeAFailingStep: a replica born alone
// gains a desired peer, and its apply fails before the self-heals run.
// The record must still go, or the peer could later sync part of its
// data into this replica, leave, and the solo-promote would declare
// the partial copy UpToDate.
func TestApply_DropsSoloBirthBeforeAFailingStep(t *testing.T) {
	fx := storage.NewFakeExec()
	rec, dir := soloReconciler(t, fx, true)

	applyFailing(t, rec, soloDR(intent.DesiredPeer{Name: "n2"}))

	if _, err := os.Stat(filepath.Join(dir, "pvc-solo"+bornSoloSuffix)); !os.IsNotExist(err) {
		t.Errorf("born-solo record survived an apply with a desired peer, stat err=%v", err)
	}

	// A lone replica's failing apply leaves its own record alone.
	rec, _ = soloReconciler(t, fx, true)

	applyFailing(t, rec, soloDR())

	if !rec.bornSolo("pvc-solo") {
		t.Error("a lone replica must keep its born-solo record")
	}
}

var errCreateMDCrash = errors.New("satellite died here")

// TestEnsureMetadata_RecordsSoloBirthBeforeCreateMD: the record is in
// place before create-md runs. Were it written after, a crash between
// the two would find the metadata present on restart, never record the
// birth, and strand the lone replica Inconsistent.
func TestEnsureMetadata_RecordsSoloBirthBeforeCreateMD(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("drbdadm dump-md pvc-solo/0", storage.FakeResponse{Err: errDumpMdNoMeta})
	fx.Expect("drbdadm create-md --force --max-peers=15 pvc-solo/0",
		storage.FakeResponse{Err: errCreateMDCrash})

	rec, dir := soloReconciler(t, fx, false)

	err := rec.ensureMetadata(context.Background(), soloDR(), map[int32]string{0: "/dev/zvol/pvc-solo_00000"},
		filepath.Join(dir, "pvc-solo.md-created"), false)
	if err == nil {
		t.Fatal("precondition: create-md must fail")
	}

	if !rec.bornSolo("pvc-solo") {
		t.Error("born-solo record must be written before create-md runs")
	}
}

// TestEnsureMetadata_KeepsSoloBirthOnAdoptedMetadata: metadata that was
// already there was not created now, so the record is left alone in
// either direction: an adopted replica never gains one.
func TestEnsureMetadata_KeepsSoloBirthOnAdoptedMetadata(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("drbdadm dump-md pvc-solo/0",
		storage.FakeResponse{Stdout: []byte("version \"v09\";\nla-size-sect 2048;\n")})

	rec, dir := soloReconciler(t, fx, false)

	err := rec.ensureMetadata(context.Background(), soloDR(), map[int32]string{0: "/dev/zvol/pvc-solo_00000"},
		filepath.Join(dir, "pvc-solo.md-created"), false)
	if err != nil {
		t.Fatalf("ensureMetadata: %v", err)
	}

	if rec.bornSolo("pvc-solo") {
		t.Error("adopted metadata must not be recorded as a solo birth")
	}
}

// TestDeleteResource_DropsSoloBirth: the record goes with the replica,
// so a definition re-created under the same name with peers is not
// promoted on the strength of an older, lone incarnation.
func TestDeleteResource_DropsSoloBirth(t *testing.T) {
	fx := storage.NewFakeExec()
	rec, dir := soloReconciler(t, fx, true)

	resp, err := rec.DeleteResource(context.Background(), &intent.DeleteResourceRequest{Name: "pvc-solo"})
	if err != nil || !resp.GetOk() {
		t.Fatalf("DeleteResource: %v, %+v", err, resp)
	}

	if _, err := os.Stat(filepath.Join(dir, "pvc-solo"+bornSoloSuffix)); !os.IsNotExist(err) {
		t.Errorf("born-solo record survived DeleteResource, stat err=%v", err)
	}
}
