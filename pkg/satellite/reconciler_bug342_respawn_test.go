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
	"strings"
	"testing"
	"time"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/drbd"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
)

// twoPeerOldResFile renders a 3-replica .res body the
// `tearDownRemovedPeers` parsers (extractResFilePeers +
// extractResFilePeerNodeIDs) can chew on. The fixture mirrors the
// shape `renderResFile` emits in production: one top-level
// `resource <name> { ... }` with three `on <node> { node-id N; }`
// blocks. Used by the Fix B tests to drive a delta where the
// local node `n1` stays while peers `n2` and/or `n3` get torn
// down.
const twoPeerOldResFile = `resource pvc-respawn {
  on n1 {
    node-id 0;
    volume 0 { device minor 1000; disk /dev/zd0; meta-disk internal; }
  }
  on n2 {
    node-id 1;
    volume 0 { device minor 1000; disk /dev/zd0; meta-disk internal; }
  }
  on n3 {
    node-id 2;
    volume 0 { device minor 1000; disk /dev/zd0; meta-disk internal; }
  }
}
`

// writeOldRes writes the fixture .res body to a fresh per-test
// state directory and returns (statedir, resPath). Mirrors the
// other satellite tests' pattern of stashing the file under
// t.TempDir().
func writeOldRes(t *testing.T, name, body string) (string, string) {
	t.Helper()

	stateDir := t.TempDir()
	resPath := filepath.Join(stateDir, name+".res")

	if err := os.WriteFile(resPath, []byte(body), 0o600); err != nil {
		t.Fatalf("write .res fixture: %v", err)
	}

	return stateDir, resPath
}

// TestTearDownRemovedPeersSkipsForgetPeerOnRespawn pins the Bug 342
// Fix B Option 2 invariant on the satellite-side reader: when the
// parent RD's annotations carry a non-expired
// `blockstor.io/peer-respawning-<peer>` deadline for a peer that
// disappeared from the .res, `tearDownRemovedPeers` MUST run
// `drbdadm del-peer` but SKIP the per-volume `drbdmeta forget-peer`.
//
// Without this skip, the Phase-2 `r d` + `r c` same-node respawn
// in e2e/scenarios/cli-matrix/r-full-lifecycle.sh wipes the
// per-volume v09 GI / bitmap metadata on the surviving siblings
// before the new incarnation's adjust runs — the kernel slot
// then wedges in Connecting / DUnknown forever.
func TestTearDownRemovedPeersSkipsForgetPeerOnRespawn(t *testing.T) {
	fx := storage.NewFakeExec()

	stateDir, resPath := writeOldRes(t, "pvc-respawn", twoPeerOldResFile)

	rec := NewReconciler(ReconcilerConfig{
		Adm:      drbd.NewAdm(fx),
		StateDir: stateDir,
		NodeName: "n1",
	})

	// Desired: n1 stays, n3 stays, n2 vanished. Stamped on the
	// parent RD with a deadline well in the future — the
	// satellite must therefore skip n2's forget-peer.
	dr := &intent.DesiredResource{
		Name:     "pvc-respawn",
		NodeName: "n1",
		Peers: []intent.DesiredPeer{
			{Name: "n3", NodeID: 2},
		},
		RDAnnotations: map[string]string{
			apiv1.PeerRespawningAnnotationKey("n2"): time.Now().
				Add(30 * time.Second).UTC().Format(time.RFC3339Nano),
		},
	}

	// Devices map carries a non-empty path so the per-volume
	// loop wouldn't naturally skip — the annotation skip is the
	// only mechanism that can suppress forget-peer here.
	devices := map[int32]string{0: "/dev/zd0"}

	if err := rec.tearDownRemovedPeers(context.Background(), dr, resPath, devices); err != nil {
		t.Fatalf("tearDownRemovedPeers: %v", err)
	}

	cmds := strings.Join(fx.CommandLines(), "\n")

	// del-peer must still run — the runtime kernel connection
	// MUST be severed regardless of operator intent, otherwise
	// the new incarnation's handshake collides with the stale
	// kernel slot.
	if !strings.Contains(cmds, "drbdadm del-peer n2:pvc-respawn") {
		t.Errorf("del-peer n2 must always run; commands=\n%s", cmds)
	}

	// forget-peer for n2 MUST be skipped (this is the whole
	// point of Fix B Option 2). A regression that removed the
	// skip would emit `drbdmeta --force pvc-respawn/0 v09 ...
	// forget-peer 1`.
	if strings.Contains(cmds, "forget-peer 1") ||
		strings.Contains(cmds, "forget-peer 1\n") {
		t.Errorf("forget-peer for n2 (node-id 1) must be skipped while respawn pending; commands=\n%s", cmds)
	}
}

// TestTearDownRemovedPeersRunsForgetPeerWithoutAnnotation pins the
// counter-case: a peer that vanished without a respawn stamp on
// the parent RD takes the full `del-peer` + `forget-peer` path —
// the steady-state cleanup that recycles the per-volume v09
// MaxPeers-1 slot budget for genuinely-departed peers.
//
// A regression that universally skipped forget-peer would leak a
// metadata slot per node-replace cycle and eventually wedge
// `drbdadm create-md` on `running out of room`.
func TestTearDownRemovedPeersRunsForgetPeerWithoutAnnotation(t *testing.T) {
	fx := storage.NewFakeExec()

	stateDir, resPath := writeOldRes(t, "pvc-respawn", twoPeerOldResFile)

	rec := NewReconciler(ReconcilerConfig{
		Adm:      drbd.NewAdm(fx),
		StateDir: stateDir,
		NodeName: "n1",
	})

	// Desired: only n1 (local) — both n2 and n3 vanished. No
	// annotations → both peers fall through to the full
	// del-peer + forget-peer chain.
	dr := &intent.DesiredResource{
		Name:     "pvc-respawn",
		NodeName: "n1",
		Peers:    nil,
		// RDAnnotations deliberately nil — the steady-state
		// shape for a genuine departure.
	}

	devices := map[int32]string{0: "/dev/zd0"}

	if err := rec.tearDownRemovedPeers(context.Background(), dr, resPath, devices); err != nil {
		t.Fatalf("tearDownRemovedPeers: %v", err)
	}

	cmds := fx.CommandLines()

	// Both del-peer and forget-peer must run for n2 (node-id 1).
	if !slices.Contains(cmds, "drbdadm del-peer n2:pvc-respawn") {
		t.Errorf("del-peer n2 missing; commands=%v", cmds)
	}

	if !slices.Contains(cmds, "drbdmeta --force pvc-respawn/0 v09 /dev/zd0 internal forget-peer 1") {
		t.Errorf("forget-peer for n2 (node-id 1) missing; commands=%v", cmds)
	}

	// And for n3 (node-id 2).
	if !slices.Contains(cmds, "drbdadm del-peer n3:pvc-respawn") {
		t.Errorf("del-peer n3 missing; commands=%v", cmds)
	}

	if !slices.Contains(cmds, "drbdmeta --force pvc-respawn/0 v09 /dev/zd0 internal forget-peer 2") {
		t.Errorf("forget-peer for n3 (node-id 2) missing; commands=%v", cmds)
	}
}

// TestTearDownRemovedPeersExpiredAnnotationFallsThroughToForgetPeer
// pins the deadline-expired branch: a stale `peer-respawning`
// stamp whose RFC3339Nano deadline is already in the past MUST
// fall through to the normal forget-peer path. Without this fall-
// through, a never-cleaned annotation would permanently leak the
// v09 metadata slot for the named peer.
func TestTearDownRemovedPeersExpiredAnnotationFallsThroughToForgetPeer(t *testing.T) {
	fx := storage.NewFakeExec()

	stateDir, resPath := writeOldRes(t, "pvc-respawn", twoPeerOldResFile)

	rec := NewReconciler(ReconcilerConfig{
		Adm:      drbd.NewAdm(fx),
		StateDir: stateDir,
		NodeName: "n1",
	})

	dr := &intent.DesiredResource{
		Name:     "pvc-respawn",
		NodeName: "n1",
		Peers: []intent.DesiredPeer{
			{Name: "n3", NodeID: 2},
		},
		RDAnnotations: map[string]string{
			// Deadline 1h in the past — expired stamp.
			apiv1.PeerRespawningAnnotationKey("n2"): time.Now().
				Add(-time.Hour).UTC().Format(time.RFC3339Nano),
		},
	}

	devices := map[int32]string{0: "/dev/zd0"}

	if err := rec.tearDownRemovedPeers(context.Background(), dr, resPath, devices); err != nil {
		t.Fatalf("tearDownRemovedPeers: %v", err)
	}

	cmds := fx.CommandLines()

	if !slices.Contains(cmds, "drbdmeta --force pvc-respawn/0 v09 /dev/zd0 internal forget-peer 1") {
		t.Errorf("expired annotation must NOT block forget-peer; commands=%v", cmds)
	}
}

// TestPeerRespawnPendingMatrix exercises the small parser
// directly: parse failures, expired deadlines, missing keys, and
// happy-path future deadlines all return the documented value.
// Pinned because every degraded case must fall through to the
// normal forget-peer path — a regression that inverted any
// branch would either leak metadata slots forever (false
// positives) or defeat Fix B entirely (false negatives).
func TestPeerRespawnPendingMatrix(t *testing.T) {
	now := time.Now()

	tests := map[string]struct {
		annotations map[string]string
		peer        string
		want        bool
	}{
		"nil map":       {nil, "n2", false},
		"empty map":     {map[string]string{}, "n2", false},
		"missing key":   {map[string]string{"unrelated": "x"}, "n2", false},
		"empty value":   {map[string]string{apiv1.PeerRespawningAnnotationKey("n2"): ""}, "n2", false},
		"unparseable":   {map[string]string{apiv1.PeerRespawningAnnotationKey("n2"): "not-a-time"}, "n2", false},
		"past deadline": {map[string]string{apiv1.PeerRespawningAnnotationKey("n2"): now.Add(-time.Hour).UTC().Format(time.RFC3339Nano)}, "n2", false},
		"future":        {map[string]string{apiv1.PeerRespawningAnnotationKey("n2"): now.Add(30 * time.Second).UTC().Format(time.RFC3339Nano)}, "n2", true},
		"wrong peer":    {map[string]string{apiv1.PeerRespawningAnnotationKey("n3"): now.Add(30 * time.Second).UTC().Format(time.RFC3339Nano)}, "n2", false},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := peerRespawnPending(tc.annotations, tc.peer, now)
			if got != tc.want {
				t.Errorf("peerRespawnPending(%v, %q): got %v, want %v",
					tc.annotations, tc.peer, got, tc.want)
			}
		})
	}
}
