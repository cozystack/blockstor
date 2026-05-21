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
	"testing"

	"github.com/cozystack/blockstor/pkg/drbd"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
)

// fakeClearedPeersStamper records every StampClearedPeer call so the
// Bug 342 v12c test can assert (a) the ACK fires after a successful
// teardown and (b) it does NOT fire when del-peer surfaces an error.
type fakeClearedPeersStamper struct {
	calls    []clearedPeerCall
	returnUp error
}

type clearedPeerCall struct {
	resourceName string
	departedPeer string
	stamp        string
}

func (f *fakeClearedPeersStamper) StampClearedPeer(_ context.Context, resourceName, departedPeer, stamp string) error {
	f.calls = append(f.calls, clearedPeerCall{
		resourceName: resourceName,
		departedPeer: departedPeer,
		stamp:        stamp,
	})

	return f.returnUp
}

// writeOldResFile writes a minimal `.res` body so
// computeRemovedPeers + extractResFilePeerNodeIDs see the prior peer
// set. The renderer normally writes a much fatter file; only the
// `on <node> {` blocks + `node-id` lines are required by these
// helpers.
func writeOldResFile(t *testing.T, dir, rd string, peers []string) string {
	t.Helper()

	body := ""
	for i, p := range peers {
		body += "  on " + p + " {\n"
		body += "    node-id " + intToStr(int32(i)) + ";\n" //nolint:gosec // small loop bound
		body += "  }\n"
	}

	path := filepath.Join(dir, rd+".res")
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil { //nolint:gosec // test fixture
		t.Fatalf("write .res: %v", err)
	}

	return path
}

func intToStr(v int32) string {
	switch v {
	case 0:
		return "0"
	case 1:
		return "1"
	case 2:
		return "2"
	case 3:
		return "3"
	default:
		return "0" // not exercised in v12c tests; keep helper trivial
	}
}

// TestBug342V12CTearDownStampsClearedPeerOnSuccess pins the happy
// path: a peer drops out of the desired set, del-peer succeeds,
// per-volume forget-peer is dispatched, and the new stamper records
// an ACK entry for that peer name.
func TestBug342V12CTearDownStampsClearedPeerOnSuccess(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()

	// Old .res had {n1 (local), n2, n3}; new desired has {n1, n2} —
	// so n3 is the removed peer.
	resPath := writeOldResFile(t, tmp, "rdA", []string{"n1", "n2", "n3"})

	fx := storage.NewFakeExec()
	fx.Expect("drbdadm disconnect n3:rdA", storage.FakeResponse{})
	fx.Expect("drbdadm del-peer n3:rdA", storage.FakeResponse{})
	fx.Expect(
		"drbdmeta --force rdA/0 v09 /dev/vg/rdA_00000 internal forget-peer 2",
		storage.FakeResponse{},
	)

	stamper := &fakeClearedPeersStamper{}

	rec := NewReconciler(ReconcilerConfig{
		Adm:                 drbd.NewAdm(fx),
		NodeName:            "n1",
		StateDir:            tmp,
		ClearedPeersStamper: stamper,
	})

	dr := &intent.DesiredResource{
		Name:     "rdA",
		NodeName: "n1",
		Peers: []intent.DesiredPeer{
			{Name: "n2"},
		},
	}

	devices := map[int32]string{0: "/dev/vg/rdA_00000"}

	if err := rec.tearDownRemovedPeers(context.Background(), dr, resPath, devices); err != nil {
		t.Fatalf("tearDownRemovedPeers: %v", err)
	}

	if len(stamper.calls) != 1 {
		t.Fatalf("expected 1 StampClearedPeer call; got %d (%v)", len(stamper.calls), stamper.calls)
	}

	got := stamper.calls[0]
	if got.resourceName != "rdA.n1" || got.departedPeer != "n3" {
		t.Errorf("stamp call: got %+v; want resourceName=rdA.n1 departedPeer=n3", got)
	}

	if got.stamp == "" {
		t.Errorf("stamp value must be non-empty (RFC3339Nano now())")
	}
}

// TestBug342V12CTearDownNoStampWhenNoRemovedPeers pins the steady-
// state: when nothing changed in the peer set, tearDownRemovedPeers
// returns early WITHOUT calling the stamper — otherwise every
// healthy reconcile would churn the Resource Status subresource.
func TestBug342V12CTearDownNoStampWhenNoRemovedPeers(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()

	// Old .res had {n1 (local), n2}; new desired has {n1, n2} —
	// no peers removed.
	resPath := writeOldResFile(t, tmp, "rdB", []string{"n1", "n2"})

	stamper := &fakeClearedPeersStamper{}

	rec := NewReconciler(ReconcilerConfig{
		Adm:                 drbd.NewAdm(storage.NewFakeExec()),
		NodeName:            "n1",
		StateDir:            tmp,
		ClearedPeersStamper: stamper,
	})

	dr := &intent.DesiredResource{
		Name:     "rdB",
		NodeName: "n1",
		Peers: []intent.DesiredPeer{
			{Name: "n2"},
		},
	}

	if err := rec.tearDownRemovedPeers(context.Background(), dr, resPath, map[int32]string{0: "/dev/vg/rdB_00000"}); err != nil {
		t.Fatalf("tearDownRemovedPeers: %v", err)
	}

	if len(stamper.calls) != 0 {
		t.Errorf("expected 0 stamps on steady state; got %d (%v)", len(stamper.calls), stamper.calls)
	}
}

// TestBug342V12CTearDownNilStamperIsSafe pins the unit-test
// compatibility surface: a Reconciler constructed without
// ClearedPeersStamper (the FakeExec / no-apiserver unit-test
// scenario) MUST still successfully run the teardown — only the ACK
// half degrades to a no-op.
func TestBug342V12CTearDownNilStamperIsSafe(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	resPath := writeOldResFile(t, tmp, "rdC", []string{"n1", "n2", "n3"})

	fx := storage.NewFakeExec()
	fx.Expect("drbdadm disconnect n3:rdC", storage.FakeResponse{})
	fx.Expect("drbdadm del-peer n3:rdC", storage.FakeResponse{})
	fx.Expect(
		"drbdmeta --force rdC/0 v09 /dev/vg/rdC_00000 internal forget-peer 2",
		storage.FakeResponse{},
	)

	rec := NewReconciler(ReconcilerConfig{
		Adm:      drbd.NewAdm(fx),
		NodeName: "n1",
		StateDir: tmp,
		// ClearedPeersStamper intentionally left nil.
	})

	dr := &intent.DesiredResource{
		Name:     "rdC",
		NodeName: "n1",
		Peers: []intent.DesiredPeer{
			{Name: "n2"},
		},
	}

	if err := rec.tearDownRemovedPeers(context.Background(), dr, resPath, map[int32]string{0: "/dev/vg/rdC_00000"}); err != nil {
		t.Fatalf("tearDownRemovedPeers with nil stamper: %v", err)
	}
}
