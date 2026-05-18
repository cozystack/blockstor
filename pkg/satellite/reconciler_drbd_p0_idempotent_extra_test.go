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

package satellite_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/satellite"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/lvm"
)

// mkIdempotencyDesired returns a stable DesiredResource shape used by
// every test in this file so the first-Apply→.res-render produces the
// same bytes across cases. Anything load-bearing (port, node-id,
// peers, options) is fixed; callers tweak the volume size or props
// when a test needs to drive a content change.
func mkIdempotencyDesired() []*intent.DesiredResource {
	return []*intent.DesiredResource{
		{
			Name:     "pvc-idem",
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
		},
	}
}

// mkIdempotencyReconciler wires the bare minimum stack the .res
// idempotency tests need: a LVM thin provider over FakeExec, an
// Adm wrapper over the same FakeExec, and a temp StateDir.
func mkIdempotencyReconciler(t *testing.T, dir string) (*satellite.Reconciler, *storage.FakeExec) {
	t.Helper()

	fx := storage.NewFakeExec()
	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": thin},
		Adm:       drbd.NewAdm(fx),
		StateDir:  dir,
		NodeName:  "n1",
	})

	return rec, fx
}

// expectIdempotencyStorage seeds the FakeExec response for the
// applyStorage pickup pre-flight that every Apply pass issues.
// Callers pass either an empty stdout (first activation, no LV yet)
// or the LV name (pickup case: LV already materialised).
func expectIdempotencyStorage(fx *storage.FakeExec, lvName string) {
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-idem_00000",
		storage.FakeResponse{Stdout: []byte(lvName)})
}

// TestApplyDRBDWritesWhenContentDiffers pins the inverse of
// TestApplyDRBDSkipsWriteWhenResUnchanged: when the DesiredResource
// produces a DIFFERENT rendered body (volume size grew, a prop
// changed, a peer was added) the .res file MUST be re-written and the
// new bytes must hit disk. A regression that compared mtime-only or
// skipped the write on any second-Apply would silently freeze .res
// content at the first-pass shape — `drbdadm adjust` would then keep
// reading stale config.
func TestApplyDRBDWritesWhenContentDiffers(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	rec, fx := mkIdempotencyReconciler(t, dir)
	expectIdempotencyStorage(fx, "")

	desired := mkIdempotencyDesired()

	if _, err := rec.Apply(t.Context(), desired); err != nil {
		t.Fatalf("first Apply: %v", err)
	}

	resPath := filepath.Join(dir, "pvc-idem.res")

	bodyFirst, err := os.ReadFile(resPath)
	if err != nil {
		t.Fatalf("read after first Apply: %v", err)
	}

	first, err := os.Stat(resPath)
	if err != nil {
		t.Fatalf("stat after first Apply: %v", err)
	}

	// Drive a real content change: bump the peer's port. Peer port
	// flows through buildResFile into the rendered `on <peer>` block's
	// address line, so the on-disk .res MUST diverge across this pass.
	time.Sleep(50 * time.Millisecond)
	desired[0].DrbdOptions["peer.n2.port"] = "7001"

	expectIdempotencyStorage(fx, "pvc-idem_00000")

	if _, err := rec.Apply(t.Context(), desired); err != nil {
		t.Fatalf("second Apply (port change): %v", err)
	}

	bodyAfter, err := os.ReadFile(resPath)
	if err != nil {
		t.Fatalf("read after second Apply: %v", err)
	}

	if string(bodyFirst) == string(bodyAfter) {
		t.Errorf("size change did not alter rendered .res: body unchanged " +
			"(idempotency over-fired and skipped a legitimate re-write)")
	}

	second, err := os.Stat(resPath)
	if err != nil {
		t.Fatalf("stat after second Apply: %v", err)
	}

	if !second.ModTime().After(first.ModTime()) {
		t.Errorf(".res mtime did not advance across content-changing Apply passes: "+
			"first=%s second=%s", first.ModTime(), second.ModTime())
	}
}

// TestApplyDRBDStableRenderAcrossManyPasses pins the determinism
// invariant: a series of identical-input Apply passes MUST produce
// byte-identical .res content and a stable mtime. P0-5's idempotency
// depends on the content-equality check; if the renderer's output
// drifted (map iteration order, time-based fields, ...) the equality
// would trip and every reconcile would re-write. We pin the
// determinism end-to-end by running ten passes and asserting the file
// hasn't moved at all.
func TestApplyDRBDStableRenderAcrossManyPasses(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	rec, fx := mkIdempotencyReconciler(t, dir)
	expectIdempotencyStorage(fx, "")

	desired := mkIdempotencyDesired()

	if _, err := rec.Apply(t.Context(), desired); err != nil {
		t.Fatalf("first Apply: %v", err)
	}

	resPath := filepath.Join(dir, "pvc-idem.res")

	bodyReference, err := os.ReadFile(resPath)
	if err != nil {
		t.Fatalf("read after first Apply: %v", err)
	}

	first, err := os.Stat(resPath)
	if err != nil {
		t.Fatalf("stat after first Apply: %v", err)
	}

	// Sleep once before the loop so any spurious mtime drift would
	// land in a strictly-later timestamp window — making the equality
	// assertion below load-bearing rather than coincidental.
	time.Sleep(50 * time.Millisecond)

	const passes = 10
	for i := range passes {
		expectIdempotencyStorage(fx, "pvc-idem_00000")

		if _, err := rec.Apply(t.Context(), desired); err != nil {
			t.Fatalf("pass %d Apply: %v", i+2, err)
		}

		body, err := os.ReadFile(resPath)
		if err != nil {
			t.Fatalf("pass %d read: %v", i+2, err)
		}

		if string(body) != string(bodyReference) {
			t.Fatalf("pass %d body diverged from reference (non-deterministic render)", i+2)
		}

		stat, err := os.Stat(resPath)
		if err != nil {
			t.Fatalf("pass %d stat: %v", i+2, err)
		}

		if !stat.ModTime().Equal(first.ModTime()) {
			t.Errorf("pass %d: mtime moved (idempotency broken): first=%s now=%s",
				i+2, first.ModTime(), stat.ModTime())
		}
	}
}

// TestApplyDRBDReadsCurrentEvenWhenStatFresh pins the read-based
// equality contract: the idempotency skip MUST be content-driven, not
// mtime-driven. A regression that compared stat info before deciding
// whether to read+compare would mis-fire when the on-disk content was
// somehow updated out-of-band (a previous failed write leaving a
// truncated file, or an operator mid-recovery editing the .res by
// hand) — the contents wouldn't match the desired body but the test
// would skip the write because stat looked fresh.
//
// We simulate the out-of-band edit by truncating the .res after the
// first Apply, then running a second Apply with the same input. The
// re-write MUST land (content mismatch) regardless of stat shape.
func TestApplyDRBDReadsCurrentEvenWhenStatFresh(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	rec, fx := mkIdempotencyReconciler(t, dir)
	expectIdempotencyStorage(fx, "")

	desired := mkIdempotencyDesired()

	if _, err := rec.Apply(t.Context(), desired); err != nil {
		t.Fatalf("first Apply: %v", err)
	}

	resPath := filepath.Join(dir, "pvc-idem.res")

	bodyFirst, err := os.ReadFile(resPath)
	if err != nil {
		t.Fatalf("read after first Apply: %v", err)
	}

	// Simulate an out-of-band corruption: truncate the file. mtime
	// just bumped, but the content no longer matches desired.
	err = os.WriteFile(resPath, []byte("# corrupted-partial-write\n"), 0o600)
	if err != nil {
		t.Fatalf("simulate truncation: %v", err)
	}

	expectIdempotencyStorage(fx, "pvc-idem_00000")

	if _, err := rec.Apply(t.Context(), desired); err != nil {
		t.Fatalf("second Apply: %v", err)
	}

	bodyAfter, err := os.ReadFile(resPath)
	if err != nil {
		t.Fatalf("read after second Apply: %v", err)
	}

	if string(bodyAfter) == "# corrupted-partial-write\n" {
		t.Errorf("idempotency skipped the re-write despite content mismatch; "+
			"on-disk body still: %q", bodyAfter)
	}

	if string(bodyAfter) != string(bodyFirst) {
		t.Errorf("second Apply did not restore the desired body: got %q, want %q",
			bodyAfter, bodyFirst)
	}
}
