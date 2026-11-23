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
	"testing"
	"time"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/storage"
)

// adoptedMeshFixture is a healthy three-replica mesh seen from
// worker-0: both peers Connected with their peer-device up. It is what
// a node LINSTOR ran looks like when blockstor adopts it.
const adoptedMeshFixture = `[
  {
    "name": "pvc-1",
    "connections": [
      {
        "peer-node-id": 1,
        "name": "worker-1",
        "connection-state": "Connected",
        "peer_devices": [ { "volume": 0 } ]
      },
      {
        "peer-node-id": 2,
        "name": "worker-2",
        "connection-state": "Connected",
        "peer_devices": [ { "volume": 0 } ]
      }
    ]
  }
]`

// departedPeerFixture has worker-1 healthy and worker-2 Connecting
// with no peer-device: the slot a peer leaves behind once its own
// satellite tore the replica down.
const departedPeerFixture = kernelCleanupFixture

// newPruneReconciler wires a Reconciler with a StateDir to a FakeExec
// answering the status probe with fixture.
func newPruneReconciler(t *testing.T, fixture string) (*Reconciler, *storage.FakeExec, string) {
	t.Helper()

	fx := storage.NewFakeExec()
	fx.Expect("drbdsetup status -j pvc-1", storage.FakeResponse{Stdout: []byte(fixture)})

	dir := t.TempDir()

	r := NewReconciler(ReconcilerConfig{
		NodeName: "worker-0",
		Adm:      drbd.NewAdm(fx),
		StateDir: dir,
	})

	return r, fx, dir
}

func prune(t *testing.T, r *Reconciler, expect PeerExpectation) {
	t.Helper()

	err := r.PruneStaleKernelSlots(t.Context(), "pvc-1", expect, []int32{0},
		map[int32]string{0: "/dev/zvol/pool/pvc-1"})
	if err != nil {
		t.Fatalf("PruneStaleKernelSlots: %v", err)
	}
}

func mustNotDelPeer(t *testing.T, fx *storage.FakeExec) {
	t.Helper()

	for _, c := range fx.CommandLines() {
		if len(c) >= len("drbdadm del-peer") && c[:len("drbdadm del-peer")] == "drbdadm del-peer" {
			t.Errorf("no peer may be pruned here; got %q in %v", c, fx.CommandLines())
		}
	}
}

// TestPruneStaleKernelSlots_AdoptionPartialSetKeepsConnectedPeers is
// the issue #266 scenario: a bulk adoption delivered worker-0's
// Resource before its siblings', so the observed set has no peers,
// while the kernel still runs the mesh LINSTOR built. blockstor has not
// rendered this resource here yet (no .owned), and the Connected
// peers must survive even though the local replica is allocated.
func TestPruneStaleKernelSlots_AdoptionPartialSetKeepsConnectedPeers(t *testing.T) {
	r, fx, _ := newPruneReconciler(t, adoptedMeshFixture)

	prune(t, r, PeerExpectation{Allocated: true, AllocatedNodeIDs: []int32{0}})

	mustNotDelPeer(t, fx)
}

// TestPruneStaleKernelSlots_AdoptedSlotWaitsForRender: an adopted slot
// is left alone, both passes included, for as long as blockstor has
// not rendered it, even a peer that looks departed and has outlived
// the Pass-3 grace window. Once the allocation-gated render stamps
// .owned and drops the peer from the .res it wrote earlier, the same
// peer is pruned.
func TestPruneStaleKernelSlots_AdoptedSlotWaitsForRender(t *testing.T) {
	r, fx, dir := newPruneReconciler(t, departedPeerFixture)

	r.seenStuckAt["pvc-1/worker-2"] = time.Now().Add(-2 * defaultStuckSlotGrace)

	prune(t, r, PeerExpectation{
		PeerNames:        []string{"worker-1"},
		Allocated:        true,
		AllocatedNodeIDs: []int32{0, 1},
	})
	mustNotDelPeer(t, fx)

	prune(t, r, PeerExpectation{
		PeerNames:        []string{"worker-1", "worker-2"},
		Allocated:        true,
		AllocatedNodeIDs: []int32{0, 1, 2},
	})
	mustNotDelPeer(t, fx)

	writeOwnedSlot(t, dir, "pvc-1", map[string]int32{"worker-1": 1, "worker-2": 2})

	fx.Reset()
	fx.Expect("drbdsetup status -j pvc-1", storage.FakeResponse{Stdout: []byte(departedPeerFixture)})

	prune(t, r, PeerExpectation{
		PeerNames:        []string{"worker-1"},
		Allocated:        true,
		AllocatedNodeIDs: []int32{0, 1},
	})

	cmds := fx.CommandLines()
	mustContain(t, cmds, "drbdadm del-peer worker-2:pvc-1")
	mustNotContain(t, cmds, "drbdadm del-peer worker-1:pvc-1")
}

// TestPruneStaleKernelSlots_OwnedRenderedPeerPrunedWhileConnected: a
// peer blockstor rendered and has since dropped is stale whatever its
// connection state, and whether or not the replacement is allocated —
// the relocate waits on that allocation, and this prune exists to run
// before it.
func TestPruneStaleKernelSlots_OwnedRenderedPeerPrunedWhileConnected(t *testing.T) {
	r, fx, dir := newPruneReconciler(t, adoptedMeshFixture)

	writeOwnedSlot(t, dir, "pvc-1", map[string]int32{"worker-1": 1, "worker-2": 2})

	prune(t, r, expectPeers("worker-1", "worker-3"))

	cmds := fx.CommandLines()
	mustContain(t, cmds, "drbdadm del-peer worker-2:pvc-1")
	mustNotContain(t, cmds, "drbdadm del-peer worker-1:pvc-1")
}

// TestPruneStaleKernelSlots_OwnedUnrecordedPeer covers an owned slot
// whose .res does not name the peer, as after the cold-start wipe of
// an unhealthy resource's .res: without the record, only an allocated
// observed set that leaves the peer's node-id free, with the peer not
// Connected, makes it stale.
func TestPruneStaleKernelSlots_OwnedUnrecordedPeer(t *testing.T) {
	cases := []struct {
		name    string
		fixture string
		expect  PeerExpectation
		pruned  bool
	}{
		{
			name:    "complete set without the node-id prunes",
			fixture: departedPeerFixture,
			expect: PeerExpectation{
				PeerNames:        []string{"worker-1"},
				Allocated:        true,
				AllocatedNodeIDs: []int32{0, 1},
			},
			pruned: true,
		},
		{
			name:    "partial set keeps the peer",
			fixture: departedPeerFixture,
			expect: PeerExpectation{
				PeerNames:        []string{"worker-1"},
				Allocated:        false,
				AllocatedNodeIDs: []int32{0},
			},
		},
		{
			name:    "Connected peer is kept on a complete set",
			fixture: adoptedMeshFixture,
			expect: PeerExpectation{
				PeerNames:        []string{"worker-1"},
				Allocated:        true,
				AllocatedNodeIDs: []int32{0, 1},
			},
		},
		{
			name:    "node-id still held by an observed replica keeps the peer",
			fixture: departedPeerFixture,
			expect: PeerExpectation{
				PeerNames:        []string{"worker-1"},
				Allocated:        true,
				AllocatedNodeIDs: []int32{0, 1, 2},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, fx, dir := newPruneReconciler(t, tc.fixture)

			writeOwnedSlot(t, dir, "pvc-1", nil)

			prune(t, r, tc.expect)

			cmds := fx.CommandLines()
			mustNotContain(t, cmds, "drbdadm del-peer worker-1:pvc-1")

			if tc.pruned {
				mustContain(t, cmds, "drbdadm del-peer worker-2:pvc-1")
				mustContain(t, cmds, "drbdsetup forget-peer pvc-1 2")

				return
			}

			mustNotDelPeer(t, fx)
		})
	}
}

// TestPruneStaleKernelSlots_RenderedPeerMatchedThroughNaming: the .res
// spells a node the way its row spelled it when rendered, the kernel
// and the Resource may spell it otherwise; the same node is neither
// unexpected nor unrecorded.
func TestPruneStaleKernelSlots_RenderedPeerMatchedThroughNaming(t *testing.T) {
	r, fx, dir := newPruneReconciler(t, departedPeerFixture)

	writeOwnedSlot(t, dir, "pvc-1", map[string]int32{"Worker-1": 1, "Worker-2": 2})

	prune(t, r, expectPeers("WORKER-1"))

	cmds := fx.CommandLines()
	mustContain(t, cmds, "drbdadm del-peer worker-2:pvc-1")
	mustNotContain(t, cmds, "drbdadm del-peer worker-1:pvc-1")
}
