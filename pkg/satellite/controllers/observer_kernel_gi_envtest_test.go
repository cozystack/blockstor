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

// DRBD moves a GI with no events2 frame: a Primary that lost a peer
// mints its new current UUID on the first write after, which can come
// long after the last frame. The published GI has to follow on the next
// resync, or the seed gates take the written replica for a day0
// sibling and let a new replica skip its initial sync.
func TestObserverFollowsAGIThatMovesWithoutAFrame(t *testing.T) {
	cli := startStatusEnvtest(t)
	name := createStatusReplica(t, cli)

	fx := statusTestExec()
	fx.Expect(kernelGetGI, storage.FakeResponse{Stdout: []byte(kernelTestGI + ":0:0:0\n")})

	o := &ObserverRunnable{Client: cli, Exec: fx, NodeName: statusTestNode}
	adm := drbd.NewAdm(fx)
	ctx := t.Context()

	o.handleObservation(ctx, adm, peerConnectedWithID("n2", kernelTestPeerID))
	o.handleObservation(ctx, adm, deviceUpToDate())

	// Long enough for any window after the last frame to have closed.
	for range 10 {
		o.resyncOnce(ctx)
	}

	if got := getStatusReplica(t, cli, name).Volumes[0].CurrentGI; got != kernelTestGI {
		t.Fatalf("published GI %q, want %q", got, kernelTestGI)
	}

	// The first write after the peer left mints a new current UUID.
	fx.Expect(kernelGetGI, storage.FakeResponse{Stdout: []byte(kernelTestMinted + ":0:" + kernelTestGI + ":0\n")})

	o.resyncOnce(ctx)

	if got := getStatusReplica(t, cli, name).Volumes[0].CurrentGI; got != kernelTestMinted {
		t.Fatalf("published GI %q after a write minted %q with no frame", got, kernelTestMinted)
	}
}

// A replica with no peer has no peer device for drbdsetup to name the
// volume by, and one whose kernel read fails has nothing from it; both
// are read from the backing device, as before.
func TestObserverReadsTheGIFromDiskWithoutAKernelAnswer(t *testing.T) {
	for _, tc := range []struct {
		name   string
		frames []*observation
		kernel *storage.FakeResponse
	}{
		{name: "no peer", frames: []*observation{deviceUpToDate()}},
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

			if got := getStatusReplica(t, cli, name).Volumes[0].CurrentGI; got != statusTestGI {
				t.Fatalf("published GI %q, want the one on disk %q", got, statusTestGI)
			}
		})
	}
}
