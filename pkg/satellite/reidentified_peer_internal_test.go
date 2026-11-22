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
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
)

var (
	errUnknownConnection = errors.New("unknown connection")
	errForgetPeer        = errors.New("forget-peer failed")
)

// kernelWithPeer is `drbdsetup status -j` for a resource holding one
// connection, to n2 under node-id 1.
const kernelWithPeer = `[{"name":"pvc-reid","node-id":0,"connections":[` +
	`{"peer-node-id":1,"name":"n2","connection-state":"Connecting","peer_devices":[]}]}]`

func reidReconciler(t *testing.T) (*Reconciler, *storage.FakeExec) {
	t.Helper()

	fx := storage.NewFakeExec()
	fx.Expect("drbdsetup status -j pvc-reid", storage.FakeResponse{Stdout: []byte(kernelWithPeer)})

	return NewReconciler(ReconcilerConfig{Adm: drbd.NewAdm(fx), StateDir: t.TempDir(), NodeName: "n1"}), fx
}

// resWithPeer writes the .res this node rendered before n2 left: n2
// under node-id 1, as the kernel still has it.
func resWithPeer(t *testing.T, rec *Reconciler) string {
	t.Helper()

	path := filepath.Join(rec.cfg.StateDir, "pvc-reid.res")

	body := "resource pvc-reid {\n  on n1 {\n    node-id 0;\n  }\n  on n2 {\n    node-id 1;\n  }\n}\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write .res: %v", err)
	}

	return path
}

func peerAt(name string, id int32) intent.DesiredPeer {
	return intent.DesiredPeer{Name: name, NodeID: &id}
}

// n2 was deleted and came back as a tiebreaker under node-id 3 before
// this node dropped the old connection. The old one goes by node-id and
// its slot is forgotten, so the adjust that follows can add n2 again.
func TestReidentifiedPeerIsRemovedByItsOldNodeID(t *testing.T) {
	t.Parallel()

	rec, fx := reidReconciler(t)
	dr := &intent.DesiredResource{Name: "pvc-reid", NodeName: "n1", Peers: []intent.DesiredPeer{peerAt("n2", 3)}}

	err := rec.tearDownReidentifiedPeers(context.Background(), dr, resWithPeer(t, rec), map[int32]string{0: "/dev/vg/pvc-reid_00000"})
	if err != nil {
		t.Fatalf("tearDownReidentifiedPeers: %v", err)
	}

	calls := fx.CommandLines()

	del := slices.Index(calls, "drbdsetup del-peer pvc-reid 1")
	forget := slices.Index(calls, "drbdsetup forget-peer pvc-reid 1")

	if del < 0 || forget < del {
		t.Fatalf("want del-peer, then forget-peer of node-id 1; got %v", calls)
	}
}

// A peer the kernel knows under the node-id it still has, or one the
// controller has not given a node-id yet, is left alone. No .res here,
// as on a cold start, so the kernel is read every time.
func TestPeerUnderItsOwnNodeIDIsKept(t *testing.T) {
	t.Parallel()

	for name, peer := range map[string]intent.DesiredPeer{
		"same node-id": peerAt("n2", 1),
		"no node-id":   {Name: "n2"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			rec, fx := reidReconciler(t)
			dr := &intent.DesiredResource{Name: "pvc-reid", NodeName: "n1", Peers: []intent.DesiredPeer{peer}}

			err := rec.tearDownReidentifiedPeers(context.Background(), dr, filepath.Join(t.TempDir(), "pvc-reid.res"), map[int32]string{0: "/dev/vg/pvc-reid_00000"})
			if err != nil {
				t.Fatalf("tearDownReidentifiedPeers: %v", err)
			}

			for _, c := range fx.CommandLines() {
				if strings.Contains(c, "del-peer") || strings.Contains(c, "forget-peer") {
					t.Errorf("a current peer was torn down: %s", c)
				}
			}
		})
	}
}

// reidentifiedDiskless is a diskless replica on n1 whose peer n2 now has
// node-id 3.
func reidentifiedDiskless() *intent.DesiredResource {
	return &intent.DesiredResource{
		Name:     "pvc-reid",
		NodeName: "n1",
		Flags:    []string{"DISKLESS"},
		Volumes:  []*intent.DesiredVolume{{VolumeNumber: 0, SizeKib: 1024 * 1024}},
		Peers:    []intent.DesiredPeer{peerAt("n2", 3)},
		DrbdOptions: map[string]string{
			"port":            "7000",
			"node-id":         "0",
			"address":         "10.0.0.1",
			"minor":           "1000",
			"peer.n2.address": "10.0.0.2",
			"peer.n2.node-id": "3",
			"peer.n2.port":    "7000",
		},
	}
}

// Through the full apply, the old connection goes before the adjust that
// adds the peer back, so that adjust's new-peer does not fail on it.
func TestApplyRemovesAReidentifiedPeerBeforeAdjust(t *testing.T) {
	t.Parallel()

	rec, fx := reidReconciler(t)
	resWithPeer(t, rec)

	_, err := rec.Apply(t.Context(), []*intent.DesiredResource{reidentifiedDiskless()})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	calls := fx.CommandLines()

	del := slices.Index(calls, "drbdsetup del-peer pvc-reid 1")
	adjust := slices.IndexFunc(calls, func(c string) bool { return strings.HasPrefix(c, "drbdadm adjust") })

	if del < 0 || adjust < del {
		t.Fatalf("want del-peer of node-id 1 before adjust; got %v", calls)
	}
}

// A resource whose .res already names every peer under the node-id it
// has costs no kernel read on a pass.
func TestSteadyPeersReadNoKernelState(t *testing.T) {
	t.Parallel()

	rec, fx := reidReconciler(t)
	dr := &intent.DesiredResource{Name: "pvc-reid", NodeName: "n1", Peers: []intent.DesiredPeer{peerAt("n2", 1)}}

	err := rec.tearDownReidentifiedPeers(context.Background(), dr, resWithPeer(t, rec), nil)
	if err != nil {
		t.Fatalf("tearDownReidentifiedPeers: %v", err)
	}

	if calls := fx.CommandLines(); len(calls) != 0 {
		t.Errorf("want no command for unchanged peers; got %v", calls)
	}
}

// A connection that is up under a node-id the controller no longer gives
// the peer is not the leftover of a removed replica: the ids disagree
// some other way, and dropping it would cut a working peer.
func TestConnectedPeerUnderAnOldNodeIDIsKept(t *testing.T) {
	t.Parallel()

	fx := storage.NewFakeExec()
	fx.Expect("drbdsetup status -j pvc-reid", storage.FakeResponse{Stdout: []byte(`[{"name":"pvc-reid","node-id":0,"connections":[` +
		`{"peer-node-id":1,"name":"n2","connection-state":"Connected","peer_devices":[{"volume":0}]}]}]`)})
	rec := NewReconciler(ReconcilerConfig{Adm: drbd.NewAdm(fx), StateDir: t.TempDir(), NodeName: "n1"})

	dr := &intent.DesiredResource{Name: "pvc-reid", NodeName: "n1", Peers: []intent.DesiredPeer{peerAt("n2", 3)}}

	err := rec.tearDownReidentifiedPeers(context.Background(), dr, resWithPeer(t, rec), map[int32]string{0: "/dev/vg/pvc-reid_00000"})
	if err != nil {
		t.Fatalf("tearDownReidentifiedPeers: %v", err)
	}

	for _, c := range fx.CommandLines() {
		if strings.Contains(c, "del-peer") || strings.Contains(c, "forget-peer") {
			t.Errorf("a Connected peer was torn down: %s", c)
		}
	}
}

// The old connection can go on its own between the kernel read and the
// del-peer, which then fails with "unknown connection". As in LINSTOR's
// satellite, that is no error once forget-peer succeeds or the kernel no
// longer holds the node-id; a del-peer failure with the connection still
// there is.
func TestDelPeerOfAConnectionAlreadyGone(t *testing.T) {
	t.Parallel()

	unknown := storage.FakeResponse{Err: errUnknownConnection}
	gone := storage.FakeResponse{Stdout: []byte(`[{"name":"pvc-reid","node-id":0,"connections":[]}]`)}
	still := storage.FakeResponse{Stdout: []byte(kernelWithPeer)}

	for name, tc := range map[string]struct {
		devices   map[int32]string
		forgetErr error
		reread    storage.FakeResponse
		wantErr   bool
	}{
		"forget-peer succeeds":          {devices: map[int32]string{0: "/dev/vg/pvc-reid_00000"}, reread: still},
		"diskless, connection gone":     {reread: gone},
		"forget fails, connection gone": {devices: map[int32]string{0: "/dev/vg/pvc-reid_00000"}, forgetErr: errForgetPeer, reread: gone},
		"diskless, connection still up": {reread: still, wantErr: true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			fx := storage.NewFakeExec()
			fx.ExpectSequence("drbdsetup status -j pvc-reid", still, tc.reread)
			fx.Expect("drbdsetup del-peer pvc-reid 1", unknown)
			fx.Expect("drbdsetup forget-peer pvc-reid 1", storage.FakeResponse{Err: tc.forgetErr})
			rec := NewReconciler(ReconcilerConfig{Adm: drbd.NewAdm(fx), StateDir: t.TempDir(), NodeName: "n1"})

			dr := &intent.DesiredResource{Name: "pvc-reid", NodeName: "n1", Peers: []intent.DesiredPeer{peerAt("n2", 3)}}

			err := rec.tearDownReidentifiedPeers(context.Background(), dr, resWithPeer(t, rec), tc.devices)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, want error %v; calls %v", err, tc.wantErr, fx.CommandLines())
			}
		})
	}
}
