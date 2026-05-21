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
	"os"
	"path/filepath"
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
)

// resFileFixture is the .res-file body the Bug 342 v15
// tearDownRemovedPeers tests read off disk to compute the
// removed-peers diff. Two peers (worker-1 + worker-2) are
// declared so the test can drop one from the DesiredResource's
// Peers list and exercise the per-peer cleanup branch.
const resFileV15Fixture = `resource pvc-v15 {
  net {
    cram-hmac-alg sha1;
  }
  on worker-0 {
    node-id 0;
    address 10.0.0.10:7000;
    volume 0 {
      device minor 1000;
      disk /dev/zvol/pool/pvc-v15;
      meta-disk internal;
    }
  }
  on worker-1 {
    node-id 1;
    address 10.0.0.11:7000;
    volume 0 {
      device minor 1000;
      disk /dev/zvol/pool/pvc-v15;
      meta-disk internal;
    }
  }
  on worker-2 {
    node-id 2;
    address 10.0.0.12:7000;
    volume 0 {
      device minor 1000;
      disk /dev/zvol/pool/pvc-v15;
      meta-disk internal;
    }
  }
}
`

// writeV15Res writes the v15 fixture to <dir>/pvc-v15.res and
// returns the path. The tearDown call reads this file to discover
// the OLD peer set against which the new DesiredResource.Peers
// diff is computed.
func writeV15Res(t *testing.T, dir string) string {
	t.Helper()

	path := filepath.Join(dir, "pvc-v15.res")
	if err := os.WriteFile(path, []byte(resFileV15Fixture), 0o600); err != nil {
		t.Fatalf("WriteFile %s: %v", path, err)
	}

	return path
}

// TestTearDownRemovedPeers_DisklessPeerRunsForgetPeer pins the
// Bug 342 v15 forget-peer discriminator's TIE_BREAKER /
// DISKLESS branch: when the departed peer's last-known state
// (Status.PeerDiskless / DesiredResource.PeerDiskless) was
// `true`, tearDownRemovedPeers MUST issue `drbdmeta forget-peer`
// in addition to `drbdadm del-peer`. This clears the stale GI /
// bitmap slot the witness incarnation left behind so a fresh
// diskful replacement on the same node name can handshake
// without an unrelated-data UUID mismatch — the core Phase 3 fix.
func TestTearDownRemovedPeers_DisklessPeerRunsForgetPeer(t *testing.T) {
	dir := t.TempDir()
	resPath := writeV15Res(t, dir)

	fx := storage.NewFakeExec()
	rec := NewReconciler(ReconcilerConfig{
		NodeName: "worker-0",
		Adm:      drbd.NewAdm(fx),
		StateDir: dir,
	})

	dr := &intent.DesiredResource{
		Name:     "pvc-v15",
		NodeName: "worker-0",
		// Only worker-1 remains; worker-2 has departed.
		Peers: []intent.DesiredPeer{{Name: "worker-1"}},
		PeerDiskless: map[string]bool{
			// Prior incarnation was TIE_BREAKER.
			"worker-2": true,
		},
	}

	devices := map[int32]string{0: "/dev/zvol/pool/pvc-v15"}

	if err := rec.tearDownRemovedPeers(t.Context(), dr, resPath, devices); err != nil {
		t.Fatalf("tearDownRemovedPeers: %v", err)
	}

	cmds := fx.CommandLines()

	mustContain(t, cmds, "drbdadm del-peer worker-2:pvc-v15")
	mustContain(t, cmds, "drbdmeta --force pvc-v15/0 v09 /dev/zvol/pool/pvc-v15 internal forget-peer 2")
}

// TestTearDownRemovedPeers_DiskfulPeerSkipsForgetPeer pins the
// Bug 342 v15 forget-peer discriminator's diskful branch: when
// the departed peer's last-known state was `false` (diskful),
// tearDownRemovedPeers MUST issue `drbdadm del-peer` (cheap,
// removes the runtime slot) but MUST NOT issue `drbdmeta
// forget-peer`. forget-peer on a diskful slot wipes the
// per-peer bitmap mid-handshake and wedges the new replica at
// `Unconnected` / `StandAlone` — the v8/v9/v10/v12c regression.
// DRBD-9 adjust + UUID compare handles the fresh handshake on
// the next reconcile pass.
func TestTearDownRemovedPeers_DiskfulPeerSkipsForgetPeer(t *testing.T) {
	dir := t.TempDir()
	resPath := writeV15Res(t, dir)

	fx := storage.NewFakeExec()
	rec := NewReconciler(ReconcilerConfig{
		NodeName: "worker-0",
		Adm:      drbd.NewAdm(fx),
		StateDir: dir,
	})

	dr := &intent.DesiredResource{
		Name:     "pvc-v15",
		NodeName: "worker-0",
		Peers:    []intent.DesiredPeer{{Name: "worker-1"}},
		PeerDiskless: map[string]bool{
			// Prior incarnation was diskful.
			"worker-2": false,
		},
	}

	devices := map[int32]string{0: "/dev/zvol/pool/pvc-v15"}

	if err := rec.tearDownRemovedPeers(t.Context(), dr, resPath, devices); err != nil {
		t.Fatalf("tearDownRemovedPeers: %v", err)
	}

	cmds := fx.CommandLines()

	// del-peer must still fire (correctness — leaks live connection
	// otherwise).
	mustContain(t, cmds, "drbdadm del-peer worker-2:pvc-v15")

	// forget-peer must NOT fire — this is the whole purpose of v15.
	mustNotContain(t, cmds, "drbdmeta --force pvc-v15/0 v09 /dev/zvol/pool/pvc-v15 internal forget-peer 2")
}

// TestTearDownRemovedPeers_MissingEntryDefaultsToForgetPeer pins
// the v15 default-to-true safety net: when PeerDiskless has no
// entry for the departed peer (rollout window before the v15
// stamper landed, or Status was wiped by a restore),
// tearDownRemovedPeers MUST default to "true" (run forget-peer).
// A stale-slot wipe is cheap; a missed wipe wedges the Phase 3
// relocate forever — the safer bet.
func TestTearDownRemovedPeers_MissingEntryDefaultsToForgetPeer(t *testing.T) {
	dir := t.TempDir()
	resPath := writeV15Res(t, dir)

	fx := storage.NewFakeExec()
	rec := NewReconciler(ReconcilerConfig{
		NodeName: "worker-0",
		Adm:      drbd.NewAdm(fx),
		StateDir: dir,
	})

	dr := &intent.DesiredResource{
		Name:     "pvc-v15",
		NodeName: "worker-0",
		Peers:    []intent.DesiredPeer{{Name: "worker-1"}},
		// PeerDiskless intentionally nil — simulates the rollout
		// window before the v15 stamper started populating the map.
		PeerDiskless: nil,
	}

	devices := map[int32]string{0: "/dev/zvol/pool/pvc-v15"}

	if err := rec.tearDownRemovedPeers(t.Context(), dr, resPath, devices); err != nil {
		t.Fatalf("tearDownRemovedPeers: %v", err)
	}

	cmds := fx.CommandLines()

	mustContain(t, cmds, "drbdadm del-peer worker-2:pvc-v15")
	mustContain(t, cmds, "drbdmeta --force pvc-v15/0 v09 /dev/zvol/pool/pvc-v15 internal forget-peer 2")
}

// TestTearDownRemovedPeers_MixedPeersDiscriminateIndependently
// pins the per-peer independence of the v15 discriminator: when
// two peers depart in the same reconcile pass, the cleanup
// decision is made independently for each peer based on its own
// last-known PeerDiskless value. The diskful peer skips
// forget-peer; the TIE_BREAKER peer runs it.
func TestTearDownRemovedPeers_MixedPeersDiscriminateIndependently(t *testing.T) {
	dir := t.TempDir()
	resPath := writeV15Res(t, dir)

	fx := storage.NewFakeExec()
	rec := NewReconciler(ReconcilerConfig{
		NodeName: "worker-0",
		Adm:      drbd.NewAdm(fx),
		StateDir: dir,
	})

	dr := &intent.DesiredResource{
		Name:     "pvc-v15",
		NodeName: "worker-0",
		// Both worker-1 and worker-2 depart this reconcile pass.
		Peers: nil,
		PeerDiskless: map[string]bool{
			"worker-1": false, // was diskful — skip forget-peer
			"worker-2": true,  // was TIE_BREAKER — run forget-peer
		},
	}

	devices := map[int32]string{0: "/dev/zvol/pool/pvc-v15"}

	if err := rec.tearDownRemovedPeers(t.Context(), dr, resPath, devices); err != nil {
		t.Fatalf("tearDownRemovedPeers: %v", err)
	}

	cmds := fx.CommandLines()

	// Both peers get del-peer.
	mustContain(t, cmds, "drbdadm del-peer worker-1:pvc-v15")
	mustContain(t, cmds, "drbdadm del-peer worker-2:pvc-v15")

	// Only the TIE_BREAKER peer gets forget-peer.
	mustNotContain(t, cmds, "drbdmeta --force pvc-v15/0 v09 /dev/zvol/pool/pvc-v15 internal forget-peer 1")
	mustContain(t, cmds, "drbdmeta --force pvc-v15/0 v09 /dev/zvol/pool/pvc-v15 internal forget-peer 2")
}
