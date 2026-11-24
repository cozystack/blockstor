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
	"os"
	"path/filepath"
	"testing"
	"time"

	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
)

// renderResFileFixture builds the minimum DesiredResource +
// devices map that drives buildResFile through a non-empty
// render. No fake exec is needed: buildResFile is pure
// in-memory templating against the supplied devices map, so the
// test stays focused on the file-write contract.
func renderResFileFixture(name string) (*intent.DesiredResource, map[int32]string) {
	dr := &intent.DesiredResource{
		Name:     name,
		NodeName: "n1",
		Volumes: []*intent.DesiredVolume{
			{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: "thin1"},
		},
		Peers: []intent.DesiredPeer{{Name: "n2"}},
		DrbdOptions: map[string]string{
			"port":            "7000",
			"node-id":         "0",
			"address":         "10.0.0.1",
			"minor":           "1000",
			"peer.n2.address": "10.0.0.2",
			"peer.n2.node-id": "1",
			"peer.n2.port":    "7000",
		},
	}
	devices := map[int32]string{0: "/dev/vg/" + name + "_00000"}
	return dr, devices
}

// TestRenderResFileWritesBody: on a fresh StateDir, renderResFile
// must produce the .res file with the same body buildResFile
// would render. This pins the helper's basic write contract — the
// FSM dispatch path (Phase 11.2.c Stage 2) and the legacy chain
// both call this helper, and both expect a stable on-disk
// artefact afterwards.
func TestRenderResFileWritesBody(t *testing.T) {
	dir := t.TempDir()
	rec := NewReconciler(ReconcilerConfig{
		StateDir:     dir,
		NodeName:     "n1",
		LocalAddress: "10.0.0.1",
	})

	dr, devices := renderResFileFixture("pvc-render")

	if err := rec.renderResFile(context.Background(), dr, devices); err != nil {
		t.Fatalf("renderResFile: %v", err)
	}

	resPath := filepath.Join(dir, "pvc-render.res")
	got, err := os.ReadFile(resPath)
	if err != nil {
		t.Fatalf("ReadFile(%s): %v", resPath, err)
	}

	autoDisk := rec.autoDiskOptionsForResource(context.Background(), dr, devices)

	want, err := buildResFile(dr, rec.cfg.NodeName, rec.cfg.LocalAddress, devices, autoDisk, nil)
	if err != nil {
		t.Fatalf("buildResFile: %v", err)
	}

	if string(got) != want {
		t.Fatalf(".res body mismatch:\non disk: %s\nwant:    %s", got, want)
	}
}

// TestRenderResFileIdempotent: pins Bug 315's content-idempotent
// invariant on the extracted helper. Two renderResFile calls with
// the same DesiredResource must leave the .res mtime unchanged on
// the second pass — the writer short-circuits on bytes.Equal
// before os.WriteFile so drbdadm's config-file-watcher does not
// see a spurious mtime bump.
func TestRenderResFileIdempotent(t *testing.T) {
	dir := t.TempDir()
	rec := NewReconciler(ReconcilerConfig{
		StateDir:     dir,
		NodeName:     "n1",
		LocalAddress: "10.0.0.1",
	})

	dr, devices := renderResFileFixture("pvc-idem")

	if err := rec.renderResFile(context.Background(), dr, devices); err != nil {
		t.Fatalf("renderResFile (1st): %v", err)
	}

	resPath := filepath.Join(dir, "pvc-idem.res")
	info1, err := os.Stat(resPath)
	if err != nil {
		t.Fatalf("Stat (1st): %v", err)
	}

	// Sleep past coarse filesystem mtime granularity (HFS+/ext4
	// truncate to 1s) so a rewrite would be observable.
	time.Sleep(1100 * time.Millisecond)

	if err := rec.renderResFile(context.Background(), dr, devices); err != nil {
		t.Fatalf("renderResFile (2nd): %v", err)
	}

	info2, err := os.Stat(resPath)
	if err != nil {
		t.Fatalf("Stat (2nd): %v", err)
	}

	if !info1.ModTime().Equal(info2.ModTime()) {
		t.Errorf("mtime advanced on identical-content reconcile: before=%v after=%v — helper is not content-idempotent",
			info1.ModTime(), info2.ModTime())
	}
}

// TestRenderResFileReplacesTheFileWhole pins that a changed .res is
// renamed into place rather than rewritten in place. drbdadm reads every
// .res on the node for any command, and the satellite applies several
// resources at once, so a drbdadm for another resource must never read
// this one empty or half written. A reader that opened the old file keeps
// the old body whole: here a hard link stands in for it, and it would show
// the new body if the file had been truncated and rewritten.
func TestRenderResFileReplacesTheFileWhole(t *testing.T) {
	dir := t.TempDir()
	rec := NewReconciler(ReconcilerConfig{
		StateDir:     dir,
		NodeName:     "n1",
		LocalAddress: "10.0.0.1",
	})

	dr, devices := renderResFileFixture("pvc-swap")
	resPath := filepath.Join(dir, "pvc-swap.res")

	const old = "# the body a concurrent drbdadm is reading\n"

	err := os.WriteFile(resPath, []byte(old), 0o644)
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	reader := filepath.Join(t.TempDir(), "open-old-file")

	err = os.Link(resPath, reader)
	if err != nil {
		t.Skipf("hard links not available here: %v", err)
	}

	err = rec.renderResFile(context.Background(), dr, devices)
	if err != nil {
		t.Fatalf("renderResFile: %v", err)
	}

	seen, err := os.ReadFile(reader)
	if err != nil {
		t.Fatalf("read the old file: %v", err)
	}

	if string(seen) != old {
		t.Errorf("the open .res changed under its reader: the new body was written in place, not renamed in")
	}

	got, err := os.ReadFile(resPath)
	if err != nil || string(got) == old {
		t.Errorf("the .res was not replaced: %q, %v", got, err)
	}

	leftovers, _ := filepath.Glob(filepath.Join(dir, "*"+ResFileCandidateSuffix))
	if len(leftovers) != 0 {
		t.Errorf("candidate left behind: %v", leftovers)
	}
}

// A crash between writing the candidate and renaming it leaves the
// candidate behind. Deleting the resource takes it along with the .res, so
// nothing of a deleted resource stays in the state dir.
func TestDeleteResourceRemovesALeftoverCandidate(t *testing.T) {
	rec := uncleanReconciler(t)

	candidate := filepath.Join(rec.cfg.StateDir, "pvc-unclean"+ResFileCandidateSuffix)
	if err := os.WriteFile(candidate, []byte("resource pvc-unclean {}\n"), 0o600); err != nil {
		t.Fatalf("write the candidate: %v", err)
	}

	resp, err := rec.DeleteResource(context.Background(), &intent.DeleteResourceRequest{Name: "pvc-unclean"})
	if err != nil || !resp.GetOk() {
		t.Fatalf("DeleteResource: %v, %+v", err, resp)
	}

	if _, err := os.Stat(candidate); !os.IsNotExist(err) {
		t.Errorf("the deleted resource left %s behind (stat: %v)", filepath.Base(candidate), err)
	}
}
