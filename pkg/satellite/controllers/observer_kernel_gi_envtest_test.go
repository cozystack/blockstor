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

package controllers

import (
	"slices"
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/storage"
)

const (
	kernelTestPeerID = 2
	kernelTestGI     = "1C9E5B3D7A2F4E80"
	kernelTestMinted = "5B7D2E9A1C3F4D60"
)

// kernelGetGI is the drbdsetup line that reads the test volume's GI
// from the kernel through its peer.
const kernelGetGI = "drbdsetup get-gi " + statusTestRD + " 2 0"

// peerConnectedWithID is peerConnected carrying the peer's DRBD
// node-id, as the events2 connection frames do.
func peerConnectedWithID(peer string, nodeID int32) *observation {
	ev := peerConnected(peer)
	ev.Connections[0].PeerDRBDNodeID = &nodeID

	return ev
}

// metaGIReads counts the drbdmeta get-gi runs fx has seen: the reads
// that open the backing device and draw udev's change uevents.
func metaGIReads(fx *storage.FakeExec) int {
	n := 0

	for _, line := range fx.CommandLines() {
		if strings.HasPrefix(line, "drbdmeta ") && strings.Contains(line, " get-gi ") {
			n++
		}
	}

	return n
}

// A replica with a peer has its GI read from the kernel on every
// resync, not from the backing device. drbdmeta opens the device
// read-write and udev answers each close with change uevents; done for
// every replica every 5 s that held a busy satellite at its CPU limit.
func TestObserverReadsTheGIFromTheKernel(t *testing.T) {
	cli := startStatusEnvtest(t)
	name := createStatusReplica(t, cli)

	fx := statusTestExec()
	fx.Expect(kernelGetGI, storage.FakeResponse{Stdout: []byte(kernelTestGI + ":0:0:0:1:1:0:0\n")})

	o := &ObserverRunnable{Client: cli, Exec: fx, NodeName: statusTestNode}
	adm := drbd.NewAdm(fx)
	ctx := t.Context()

	o.handleObservation(ctx, adm, peerConnectedWithID("n2", kernelTestPeerID))
	o.handleObservation(ctx, adm, deviceUpToDate())

	for range 3 {
		o.resyncOnce(ctx)
	}

	if got := metaGIReads(fx); got != 0 {
		t.Fatalf("read the GI of a replica with a peer from its backing device %d times", got)
	}

	if !slices.Contains(fx.CommandLines(), kernelGetGI) {
		t.Fatalf("no %q in %v", kernelGetGI, fx.CommandLines())
	}

	if got := getStatusReplica(t, cli, name).Volumes[0].CurrentGI; got != kernelTestGI {
		t.Fatalf("published GI %q, want the kernel's %q", got, kernelTestGI)
	}
}

// kernelGIReads counts the drbdsetup get-gi runs fx has seen.
func kernelGIReads(fx *storage.FakeExec) int {
	n := 0

	for _, line := range fx.CommandLines() {
		if line == kernelGetGI {
			n++
		}
	}

	return n
}

// peerWithRole is peerConnectedWithID with the peer's role, as the
// events2 connection frames carry it.
func peerWithRole(peer string, nodeID int32, role string) *observation {
	ev := peerConnectedWithID(peer, nodeID)
	ev.Connections[0].PeerRole = role

	return ev
}

// kernelAnswers makes the kernel report gi as the test volume's GI.
func kernelAnswers(fx *storage.FakeExec, gi string) {
	fx.Expect(kernelGetGI, storage.FakeResponse{Stdout: []byte(gi + ":0:0:0\n")})
}

// DRBD moves a GI with no events2 frame: a replica promoted mints a new
// current UUID on its first write after, which can come long after the
// last frame, and its connected peers take it then. While the GI is
// still day0 and a replica is Primary, the published GI has to follow
// on the next resync, or the seed gates take the written replica for a
// day0 sibling and let a new replica skip its initial sync.
func TestObserverFollowsADay0GIThatMovesWithoutAFrame(t *testing.T) {
	cli := startStatusEnvtest(t)
	name := createStatusReplica(t, cli)

	day0 := drbd.Day0GIFor(statusTestRD, 0)

	fx := statusTestExec()
	kernelAnswers(fx, day0)

	o := &ObserverRunnable{Client: cli, Exec: fx, NodeName: statusTestNode}
	adm := drbd.NewAdm(fx)
	ctx := t.Context()

	o.handleObservation(ctx, adm, peerWithRole("n2", kernelTestPeerID, drbdRolePrimary))
	o.handleObservation(ctx, adm, deviceUpToDate())

	// Long enough for any window after the last frame to have closed.
	for range 10 {
		o.resyncOnce(ctx)
	}

	if got := getStatusReplica(t, cli, name).Volumes[0].CurrentGI; got != day0 {
		t.Fatalf("published GI %q, want day0 %q", got, day0)
	}

	// The Primary's first write mints a new current UUID.
	kernelAnswers(fx, kernelTestMinted)

	o.resyncOnce(ctx)

	if got := getStatusReplica(t, cli, name).Volumes[0].CurrentGI; got != kernelTestMinted {
		t.Fatalf("published GI %q after a write minted %q with no frame", got, kernelTestMinted)
	}
}

// A GI that left day0 never comes back to it, and the seed gates treat
// every other GI alike; a day0 GI with no replica Primary cannot move.
// Neither is read again until a frame changes the replica: reading
// every data-bearing volume on every resync cost a drbdsetup run per
// volume every 5 s, twenty a second on a node with a hundred replicas,
// while nothing happened.
func TestObserverLeavesASettledGIAlone(t *testing.T) {
	for _, tc := range []struct {
		name string
		gi   string
		peer *observation
	}{
		{"left day0", kernelTestGI, peerWithRole("n2", kernelTestPeerID, drbdRolePrimary)},
		{"day0 with no replica primary", drbd.Day0GIFor(statusTestRD, 0), peerWithRole("n2", kernelTestPeerID, "Secondary")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cli := startStatusEnvtest(t)
			name := createStatusReplica(t, cli)

			fx := statusTestExec()
			kernelAnswers(fx, tc.gi)

			o := &ObserverRunnable{Client: cli, Exec: fx, NodeName: statusTestNode}
			adm := drbd.NewAdm(fx)
			ctx := t.Context()

			o.handleObservation(ctx, adm, tc.peer)
			o.handleObservation(ctx, adm, deviceUpToDate())

			if got := getStatusReplica(t, cli, name).Volumes[0].CurrentGI; got != tc.gi {
				t.Fatalf("published GI %q, want %q", got, tc.gi)
			}

			before := kernelGIReads(fx)

			for range 10 {
				o.resyncOnce(ctx)
			}

			if n := kernelGIReads(fx) - before; n != 0 {
				t.Errorf("%d GI reads in 10 resyncs of a replica whose GI cannot move", n)
			}

			if got := getStatusReplica(t, cli, name).Volumes[0].CurrentGI; got != tc.gi {
				t.Errorf("published GI %q after the resyncs, want %q", got, tc.gi)
			}
		})
	}
}

// A Primary can write and step down between two resyncs. Once it has,
// no replica is Primary and the resync no longer reads the day0 GI, so
// the frame that reports the role change reads it.
func TestObserverReadsTheGIWhenARoleChanges(t *testing.T) {
	cli := startStatusEnvtest(t)
	name := createStatusReplica(t, cli)

	fx := statusTestExec()
	kernelAnswers(fx, drbd.Day0GIFor(statusTestRD, 0))

	o := &ObserverRunnable{Client: cli, Exec: fx, NodeName: statusTestNode}
	adm := drbd.NewAdm(fx)
	ctx := t.Context()

	o.handleObservation(ctx, adm, peerWithRole("n2", kernelTestPeerID, drbdRolePrimary))
	o.handleObservation(ctx, adm, deviceUpToDate())

	// The peer writes, minting a new GI that reaches this replica, and
	// steps down before the next resync.
	kernelAnswers(fx, kernelTestMinted)
	o.handleObservation(ctx, adm, peerWithRole("n2", kernelTestPeerID, "Secondary"))

	if got := getStatusReplica(t, cli, name).Volumes[0].CurrentGI; got != kernelTestMinted {
		t.Fatalf("published GI %q after the peer stepped down, want %q", got, kernelTestMinted)
	}
}

// A replica with no peer has no peer device for drbdsetup to name the
// volume by, and is read from the backing device, as before. One whose
// kernel read fails publishes no GI rather than read the backing device
// the kernel holds attached: the read ran drbdmeta --force against an
// in-use Primary each time a replica of it was deleted.
func TestObserverReadsTheGIFromDiskOnlyWithoutAPeer(t *testing.T) {
	for _, tc := range []struct {
		name      string
		frames    []*observation
		kernel    *storage.FakeResponse
		want      string
		readsDisk bool
	}{
		{name: "no peer", frames: []*observation{deviceUpToDate()}, want: statusTestGI, readsDisk: true},
		{
			name:   "kernel read fails",
			frames: []*observation{peerConnectedWithID("n2", kernelTestPeerID), deviceUpToDate()},
			kernel: &storage.FakeResponse{Err: storage.FakeExitError{Code: 10}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cli := startStatusEnvtest(t)
			name := createStatusReplica(t, cli)

			fx := statusTestExec()
			if tc.kernel != nil {
				fx.Expect(kernelGetGI, *tc.kernel)
			}

			o := &ObserverRunnable{Client: cli, Exec: fx, NodeName: statusTestNode}
			adm := drbd.NewAdm(fx)
			ctx := t.Context()

			for _, ev := range tc.frames {
				o.handleObservation(ctx, adm, ev)
			}

			o.resyncOnce(ctx)

			if got := getStatusReplica(t, cli, name).Volumes[0].CurrentGI; got != tc.want {
				t.Fatalf("published GI %q, want %q", got, tc.want)
			}

			if got := metaGIReads(fx); (got > 0) != tc.readsDisk {
				t.Fatalf("read the backing device %d times, want reads: %v", got, tc.readsDisk)
			}
		})
	}
}
