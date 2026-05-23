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
	"sync"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
)

// fakePeerForgetAckStamper captures every (resourceName, peerNodeName)
// pair the satellite reconciler calls StampPeerForgetAck with. Used
// to pin spec §4.2 / §6: the stamp MUST fire once per departed peer
// after the kernel-side del-peer / forget-peer has run.
type fakePeerForgetAckStamper struct {
	mu    sync.Mutex
	calls []fakeAckCall
}

type fakeAckCall struct {
	resourceName string
	peerNodeName string
}

func (f *fakePeerForgetAckStamper) StampPeerForgetAck(_ context.Context, resourceName, peerNodeName string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.calls = append(f.calls, fakeAckCall{resourceName: resourceName, peerNodeName: peerNodeName})

	return nil
}

func (f *fakePeerForgetAckStamper) snapshot() []fakeAckCall {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := make([]fakeAckCall, len(f.calls))
	copy(out, f.calls)

	return out
}

// TestTearDownRemovedPeersStampsAck pins spec §4.2 + §6: after
// `tearDownRemovedPeers` issues `drbdadm del-peer` (and
// `drbdmeta forget-peer` for diskful local replicas) against a
// peer that has departed from the rendered .res, the satellite
// MUST stamp the per-peer ACK annotation
// (`blockstor.io/peer-forget-acked.<peerNode>`) onto the local
// Resource CRD so the REST handler's `waitForPeerDeletionAcks` can
// proceed with the physical reap of the doomed Resource row. The
// stamp is what gates the controller's "phase 2: confirm and reap"
// step of the two-phase delete protocol.
//
// Test shape:
//
//   - Pre-existing .res names peer worker-3 with node-id 1
//     (simulates a 3-replica RD that just lost worker-3).
//   - The DesiredResource passed to tearDownRemovedPeers contains
//     NO worker-3 peer (the controller's phase-1 mark + dispatcher
//     filter has already dropped it).
//   - FakeExec replies success to the `drbdadm disconnect` and
//     `del-peer` commands.
//   - The stamper records the (resource, peer) tuple.
//
// Assertions:
//
//   - exactly one stamp call landed
//   - its resourceName matches `<rd>.<localNode>` (the CRD naming
//     convention from the Resource type's CEL validation)
//   - its peerNodeName matches the departed peer (worker-3).
func TestTearDownRemovedPeersStampsAck(t *testing.T) {
	fx := storage.NewFakeExec()

	// del-peer cascade: drbdadm disconnect (best-effort, errors
	// ignored) then drbdadm del-peer. Match both so the FakeExec
	// doesn't fall through to an unexpected-command error.
	fx.Expect("drbdadm disconnect worker-3:pvc-spec-§4.2-tear", storage.FakeResponse{})
	fx.Expect("drbdadm del-peer worker-3:pvc-spec-§4.2-tear", storage.FakeResponse{})

	stamper := &fakePeerForgetAckStamper{}

	rec := NewReconciler(ReconcilerConfig{
		Adm:                  drbd.NewAdm(fx),
		NodeName:             "worker-1",
		PeerForgetAckStamper: stamper,
	})

	stateDir := t.TempDir()
	resPath := filepath.Join(stateDir, "pvc-spec-§4.2-tear.res")

	// Pre-render the .res with worker-1 (local) + worker-3 (peer).
	// The on-block parser reads `on <name>` and `node-id N;` —
	// keep the format minimal.
	resBody := `resource pvc-spec-§4.2-tear {
  on worker-1 {
    node-id 0;
  }
  on worker-3 {
    node-id 1;
  }
}
`

	err := os.WriteFile(resPath, []byte(resBody), 0o600)
	if err != nil {
		t.Fatalf("seed .res: %v", err)
	}

	// DesiredResource carries NO peers — the dispatcher already
	// filtered worker-3 out because its CRD is flagged DELETE.
	dr := &intent.DesiredResource{
		Name:     "pvc-spec-§4.2-tear",
		NodeName: "worker-1",
		Peers:    nil,
	}

	// Diskless local for simplicity — devices map is empty, the
	// forget-peer half is skipped, only del-peer fires, ACK still
	// stamps (per spec §4.1: diskless local issues del-peer only,
	// and the REST poller only needs the kernel-side cleanup
	// confirmation, not the metadata half).
	err = rec.tearDownRemovedPeers(context.Background(), dr, resPath, nil)
	if err != nil {
		t.Fatalf("tearDownRemovedPeers: %v", err)
	}

	got := stamper.snapshot()
	if len(got) != 1 {
		t.Fatalf("stamper calls: got %d, want 1; calls=%+v", len(got), got)
	}

	wantResource := "pvc-spec-§4.2-tear.worker-1"
	if got[0].resourceName != wantResource {
		t.Errorf("stamper resourceName: got=%q want=%q", got[0].resourceName, wantResource)
	}

	if got[0].peerNodeName != "worker-3" {
		t.Errorf("stamper peerNodeName: got=%q want=%q", got[0].peerNodeName, "worker-3")
	}
}

// TestTearDownRemovedPeersAckSkippedWhenNoPeersDeparted pins the
// negative case: if NO peer departed from the .res (the typical
// steady-state reconcile), the stamper MUST NOT fire — otherwise the
// REST poller would see stale ACKs from prior cleanup cycles and
// pre-ack a fresh doomed peer it never actually cleaned up.
func TestTearDownRemovedPeersAckSkippedWhenNoPeersDeparted(t *testing.T) {
	fx := storage.NewFakeExec()
	// No del-peer commands expected — the loop short-circuits at
	// computeRemovedPeers returning empty.

	stamper := &fakePeerForgetAckStamper{}

	rec := NewReconciler(ReconcilerConfig{
		Adm:                  drbd.NewAdm(fx),
		NodeName:             "worker-1",
		PeerForgetAckStamper: stamper,
	})

	stateDir := t.TempDir()
	resPath := filepath.Join(stateDir, "pvc-no-churn.res")

	// .res mentions worker-1 + worker-3; desired also keeps both.
	// No peer departed.
	resBody := `resource pvc-no-churn {
  on worker-1 {
    node-id 0;
  }
  on worker-3 {
    node-id 1;
  }
}
`

	err := os.WriteFile(resPath, []byte(resBody), 0o600)
	if err != nil {
		t.Fatalf("seed .res: %v", err)
	}

	dr := &intent.DesiredResource{
		Name:     "pvc-no-churn",
		NodeName: "worker-1",
		Peers:    []intent.DesiredPeer{{Name: "worker-3", NodeID: 1}},
	}

	err = rec.tearDownRemovedPeers(context.Background(), dr, resPath, nil)
	if err != nil {
		t.Fatalf("tearDownRemovedPeers: %v", err)
	}

	if got := stamper.snapshot(); len(got) != 0 {
		t.Fatalf("stamper fired on steady-state reconcile: %+v", got)
	}
}

// TestTearDownRemovedPeersNoStamperNoPanic pins the nil-stamper
// path: the legacy/test wiring may construct a Reconciler without a
// PeerForgetAckStamper (unit tests that don't wire an apiserver
// client). The tear-down must complete normally and not panic.
func TestTearDownRemovedPeersNoStamperNoPanic(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("drbdadm disconnect worker-3:pvc-no-stamper", storage.FakeResponse{})
	fx.Expect("drbdadm del-peer worker-3:pvc-no-stamper", storage.FakeResponse{})

	rec := NewReconciler(ReconcilerConfig{
		Adm:      drbd.NewAdm(fx),
		NodeName: "worker-1",
		// Note: PeerForgetAckStamper deliberately left nil.
	})

	stateDir := t.TempDir()
	resPath := filepath.Join(stateDir, "pvc-no-stamper.res")
	resBody := `resource pvc-no-stamper {
  on worker-1 { node-id 0; }
  on worker-3 { node-id 1; }
}
`

	err := os.WriteFile(resPath, []byte(resBody), 0o600)
	if err != nil {
		t.Fatalf("seed .res: %v", err)
	}

	dr := &intent.DesiredResource{
		Name:     "pvc-no-stamper",
		NodeName: "worker-1",
	}

	err = rec.tearDownRemovedPeers(context.Background(), dr, resPath, nil)
	if err != nil {
		t.Fatalf("tearDownRemovedPeers: %v", err)
	}
}
