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
	"errors"
	"slices"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
)

const (
	portInUseRes = "pvc-port-in-use"

	portInUseBare    = "drbdadm adjust " + portInUseRes
	portInUseSkipNet = "drbdadm adjust --skip-net " + portInUseRes
	portInUseUp      = "drbdadm up " + portInUseRes
)

var errPortInUse = errors.New("drbdadm adjust: Failure: (103) Remote address(port) already in use. " +
	"Command 'drbdsetup new-path " + portInUseRes + " 0 ipv4:10.0.0.1:7001 ipv4:10.0.0.3:7003' terminated with exit code 10")

var errUnknownResource = errors.New(portInUseRes + ": Failure: (158) Unknown resource\n" +
	"additional info from kernel:\nunknown resource")

var errLowerDevTooSmall = errors.New("drbdadm adjust: Failure: (111) Low.dev. smaller than requested DRBD-dev. size")

// portInUseStatus is the kernel a failed adjust of a new replica leaves:
// the connections and their peer devices exist, nothing connected them.
// The same status follows an operator's `drbdadm disconnect`.
func portInUseStatus() storage.FakeResponse {
	return storage.FakeResponse{Stdout: []byte(`[
  {
    "name": "` + portInUseRes + `",
    "connections": [
      {"peer-node-id": 0, "name": "n0", "connection-state": "StandAlone", "peer_devices": [{"volume": 0}]},
      {"peer-node-id": 2, "name": "n2", "connection-state": "StandAlone", "peer_devices": [{"volume": 0}]}
    ]
  }
]
`)}
}

func portInUseExec() *storage.FakeExec {
	fx := storage.NewFakeExec()

	// Local disk attached, so neither SkipDisk path fires.
	fx.Expect("drbdsetup status --verbose "+portInUseRes, storage.FakeResponse{
		Stdout: []byte(portInUseRes + " node-id:1 role:Secondary suspended:quorum\n" +
			"  volume:0 minor:1000 disk:Inconsistent backing_dev:/dev/zvol/tank/" + portInUseRes + "_00000 quorum:no\n"),
	})

	return fx
}

func portInUseDesired() *intent.DesiredResource {
	return &intent.DesiredResource{
		Name:     portInUseRes,
		NodeName: "n1",
		Props:    map[string]string{},
		Volumes:  []*intent.DesiredVolume{{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "zfs"}},
	}
}

// A replica whose adjust failed half way, the new-path to one peer
// refused because a stale kernel resource held the port, is left with
// StandAlone connections that carry peer devices. The retry must be a
// full adjust: with --skip-net it succeeds without touching the network
// and the replica stays StandAlone, suspended on quorum, for good. Once a
// full adjust went through, a StandAlone peer is an operator's
// disconnect again and is kept.
func TestAdjustRetriesAFailedAdjustInFull(t *testing.T) {
	t.Parallel()

	fx := portInUseExec()
	fx.ExpectSequence("drbdsetup status -j "+portInUseRes,
		storage.FakeResponse{Stdout: []byte("[]")}, portInUseStatus(), portInUseStatus(), portInUseStatus())
	fx.ExpectSequence(portInUseBare,
		storage.FakeResponse{Err: errPortInUse}, storage.FakeResponse{Err: errPortInUse}, storage.FakeResponse{})

	rec := NewReconciler(ReconcilerConfig{Adm: drbd.NewAdm(fx), StateDir: t.TempDir(), NodeName: "n1"})
	dr := portInUseDesired()

	if err := rec.adjustResource(t.Context(), dr, false); err == nil {
		t.Fatal("the first adjust hit the port in use and must fail")
	}

	// The conflict is still there: the retry fails again rather than
	// reporting a success it did not have.
	fx.Reset()

	if err := rec.adjustResource(t.Context(), dr, false); err == nil {
		t.Fatal("a retry while the port is still in use must fail")
	}

	assertAdjustedInFull(t, fx.CommandLines())

	// The conflict is gone: the retry connects the peers.
	fx.Reset()

	if err := rec.adjustResource(t.Context(), dr, false); err != nil {
		t.Fatalf("retry after the conflict went: %v", err)
	}

	assertAdjustedInFull(t, fx.CommandLines())

	// The operator disconnects a peer of the healthy replica.
	fx.Reset()

	if err := rec.adjustResource(t.Context(), dr, false); err != nil {
		t.Fatalf("adjust after an operator's disconnect: %v", err)
	}

	if cmds := fx.CommandLines(); !slices.Contains(cmds, portInUseSkipNet) || slices.Contains(cmds, portInUseBare) {
		t.Errorf("after a full adjust went through a StandAlone peer is the operator's and must be kept with --skip-net; got %v", cmds)
	}
}

// The record of a failed adjust outlives the satellite: a restart
// between the failure and the retry must not turn the retry into a
// --skip-net adjust.
func TestAdjustRetriesAFailedAdjustInFullAfterARestart(t *testing.T) {
	t.Parallel()

	stateDir := t.TempDir()

	fx := portInUseExec()
	fx.ExpectSequence("drbdsetup status -j "+portInUseRes, storage.FakeResponse{Stdout: []byte("[]")}, portInUseStatus())
	fx.ExpectSequence(portInUseBare, storage.FakeResponse{Err: errPortInUse}, storage.FakeResponse{})

	dr := portInUseDesired()

	before := NewReconciler(ReconcilerConfig{Adm: drbd.NewAdm(fx), StateDir: stateDir, NodeName: "n1"})
	if err := before.adjustResource(t.Context(), dr, false); err == nil {
		t.Fatal("the first adjust hit the port in use and must fail")
	}

	fx.Reset()

	after := NewReconciler(ReconcilerConfig{Adm: drbd.NewAdm(fx), StateDir: stateDir, NodeName: "n1"})
	if err := after.adjustResource(t.Context(), dr, false); err != nil {
		t.Fatalf("retry after the restart: %v", err)
	}

	assertAdjustedInFull(t, fx.CommandLines())

	if after.adjustFailedBefore(portInUseRes) {
		t.Error("a full adjust that went through must clear the record")
	}
}

// `drbdadm up` runs the same new-peer and new-path steps as a full
// adjust, so one refused at new-path leaves the same StandAlone
// connections with peer devices behind: after a reboot, an operator's
// down or the my-node-id self-heal. The next pass is an adjust, since
// the slot is loaded now, and it must be a full one.
func TestAdjustAfterAFailedUpIsAFullAdjust(t *testing.T) {
	t.Parallel()

	fx := portInUseExec()
	fx.Expect(portInUseUp, storage.FakeResponse{Err: errPortInUse})
	fx.Expect("drbdsetup status -j "+portInUseRes, portInUseStatus())

	rec := NewReconciler(ReconcilerConfig{Adm: drbd.NewAdm(fx), StateDir: t.TempDir(), NodeName: "n1"})
	dr := portInUseDesired()

	if err := rec.bringUpResource(t.Context(), dr); err == nil {
		t.Fatal("the up hit the port in use and must fail")
	}

	fx.Reset()

	if err := rec.adjustResource(t.Context(), dr, false); err != nil {
		t.Fatalf("adjust after the failed up: %v", err)
	}

	assertAdjustedInFull(t, fx.CommandLines())
}

// The Bug-287 fallback runs `drbdadm up` from inside an adjust that may
// have skipped the network. An up that fails there half built the
// connections just the same, so the next adjust is a full one.
func TestAdjustAfterAFailedUpFallbackIsAFullAdjust(t *testing.T) {
	t.Parallel()

	fx := portInUseExec()
	fx.Expect("drbdsetup status -j "+portInUseRes, portInUseStatus())
	fx.ExpectSequence(portInUseSkipNet, storage.FakeResponse{Err: errUnknownResource})
	fx.Expect(portInUseUp, storage.FakeResponse{Err: errPortInUse})

	rec := NewReconciler(ReconcilerConfig{Adm: drbd.NewAdm(fx), StateDir: t.TempDir(), NodeName: "n1"})
	dr := portInUseDesired()

	if err := rec.adjustResource(t.Context(), dr, false); err == nil {
		t.Fatal("the fallback up hit the port in use and must fail")
	}

	fx.Reset()

	if err := rec.adjustResource(t.Context(), dr, false); err != nil {
		t.Fatalf("adjust after the failed fallback up: %v", err)
	}

	assertAdjustedInFull(t, fx.CommandLines())
}

// An adjust run with --skip-net never touched the connections, so its
// failure, a disk-side one, is no reason to configure the network on
// the retry: that would connect a peer the operator disconnected as the
// first step of a split-brain recovery.
func TestFailedSkipNetAdjustKeepsTheOperatorsDisconnect(t *testing.T) {
	t.Parallel()

	fx := portInUseExec()
	fx.Expect("drbdsetup status -j "+portInUseRes, portInUseStatus())
	fx.ExpectSequence(portInUseSkipNet, storage.FakeResponse{Err: errLowerDevTooSmall})

	rec := NewReconciler(ReconcilerConfig{Adm: drbd.NewAdm(fx), StateDir: t.TempDir(), NodeName: "n1"})
	dr := portInUseDesired()

	if err := rec.adjustResource(t.Context(), dr, false); err == nil {
		t.Fatal("the disk-side failure must surface")
	}

	if rec.adjustFailedBefore(portInUseRes) {
		t.Error("a failed --skip-net adjust must not record a failed adjust")
	}

	fx.Reset()

	if err := rec.adjustResource(t.Context(), dr, false); err != nil {
		t.Fatalf("retry: %v", err)
	}

	if cmds := fx.CommandLines(); !slices.Contains(cmds, portInUseSkipNet) || slices.Contains(cmds, portInUseBare) {
		t.Errorf("the retry must keep the operator's disconnect with --skip-net; got %v", cmds)
	}
}

func assertAdjustedInFull(t *testing.T, cmds []string) {
	t.Helper()

	if !slices.Contains(cmds, portInUseBare) {
		t.Errorf("the retry of a failed adjust must be a full %q; got %v", portInUseBare, cmds)
	}

	if slices.Contains(cmds, portInUseSkipNet) {
		t.Errorf("the retry of a failed adjust must not skip the network; got %v", cmds)
	}
}
