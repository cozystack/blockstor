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
		Peers: []string{"n2"},
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

	want, err := buildResFile(dr, rec.cfg.NodeName, rec.cfg.LocalAddress, devices)
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
