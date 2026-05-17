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

package file_test

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/file"
)

// Bug 256 (P2, space-guarantee): FILE CreateVolume and ResizeVolume's
// idempotency short-circuits did not re-fallocate in thick mode. A
// file pre-existing from a prior reconcile (or a satellite restart
// mid-allocation) survives the skip path without ever having its
// extents reserved on disk. The first write into the historically-sparse
// range can hit ENOSPC even though the file's apparent size is correct
// — same blast as Bug 247/250 but on the idempotent-skip surface.
//
// Fix: add ensureFallocated helper that:
//   - no-ops on thin,
//   - on thick stats the file and runs `fallocate -l <expected> <path>`
//     (idempotent; re-fallocating an already-allocated range is a fast
//     no-op for ext4/xfs).
//
// Call from CreateVolume's idempotent-skip path (file exists) and
// ResizeVolume's no-grow short-circuit (info.Size() >= target).

// ---------------------------------------------------------------------------
// Bug 256 — CreateVolume idempotent-skip must re-fallocate in thick.
// ---------------------------------------------------------------------------

// TestThickCreateIdempotentSkipStillFallocates: an existing backing
// file MUST be re-fallocated on the idempotent-skip path so a sparse-
// by-history file is forced to reserve its extents on disk.
func TestThickCreateIdempotentSkipStillFallocates(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()

	// Pre-seed a 1 GiB backing file so the stat succeeds and the
	// idempotent-skip path is taken.
	path := filepath.Join(dir, "pvc-1_00000.img")

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create file: %v", err)
	}

	if err := f.Truncate(1024 * 1024 * 1024); err != nil {
		t.Fatalf("seed truncate: %v", err)
	}

	_ = f.Close()

	fakeLosetup(fx, path, "/dev/loop42")

	// Thick provider — Thin: false.
	p := file.NewProvider(file.Config{Dir: dir}, fx)

	err = p.CreateVolume(t.Context(), storage.Volume{
		ResourceName: "pvc-1",
		VolumeNumber: 0,
		SizeKib:      1024 * 1024, // 1 GiB
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	wantFallocate := "fallocate -l 1073741824 " + path
	if !slices.Contains(fx.CommandLines(), wantFallocate) {
		t.Errorf("Bug 256 fix: even on the idempotent-skip path, thick "+
			"CreateVolume must re-fallocate so a sparse-by-history file "+
			"reserves its extents on disk (otherwise the first write into the "+
			"un-reserved range can ENOSPC despite the file's apparent size); "+
			"expected %q in calls; got %v",
			wantFallocate, fx.CommandLines())
	}
}

// TestThinCreateIdempotentSkipNoFallocate: FILE_THIN CreateVolume on the
// idempotent-skip path MUST NOT call fallocate — sparse / overcommit is
// the whole point of the thin contract.
func TestThinCreateIdempotentSkipNoFallocate(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()

	path := filepath.Join(dir, "pvc-1_00000.img")

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create file: %v", err)
	}

	if err := f.Truncate(1024 * 1024 * 1024); err != nil {
		t.Fatalf("seed truncate: %v", err)
	}

	_ = f.Close()

	fakeLosetup(fx, path, "/dev/loop42")

	p := file.NewProvider(file.Config{Dir: dir, Thin: true}, fx)

	err = p.CreateVolume(t.Context(), storage.Volume{
		ResourceName: "pvc-1",
		VolumeNumber: 0,
		SizeKib:      1024 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	for _, line := range fx.CommandLines() {
		if strings.HasPrefix(line, "fallocate ") {
			t.Errorf("THIN CreateVolume (idempotent-skip path) must NOT call "+
				"fallocate (would defeat sparse / overcommit semantics); got %q",
				line)
		}
	}
}

// ---------------------------------------------------------------------------
// Bug 256 — ResizeVolume no-grow short-circuit must re-fallocate in thick.
// ---------------------------------------------------------------------------

// TestThickResizeNoGrowSkipStillFallocates: ResizeVolume's
// `info.Size() >= target` no-grow short-circuit MUST still call
// fallocate in thick mode so a sparse-by-history file (e.g. a crash
// mid-CreateVolume that produced the right size but no allocation) gets
// its extents reserved on the resize-as-reconcile pass.
func TestThickResizeNoGrowSkipStillFallocates(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()

	// Pre-create a 2 GiB sparse file so the no-grow branch is taken when
	// the caller requests 2 GiB.
	path := filepath.Join(dir, "pvc-1_00000.img")

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create file: %v", err)
	}

	if err := f.Truncate(2 * 1024 * 1024 * 1024); err != nil {
		t.Fatalf("seed truncate: %v", err)
	}

	_ = f.Close()

	// Thick provider.
	p := file.NewProvider(file.Config{Dir: dir}, fx)

	err = p.ResizeVolume(t.Context(), storage.Volume{
		ResourceName: "pvc-1",
		VolumeNumber: 0,
		SizeKib:      2 * 1024 * 1024, // 2 GiB — equal to current size.
	})
	if err != nil {
		t.Fatalf("ResizeVolume: %v", err)
	}

	wantFallocate := "fallocate -l 2147483648 " + path
	if !slices.Contains(fx.CommandLines(), wantFallocate) {
		t.Errorf("Bug 256 fix: even on the no-grow short-circuit path, thick "+
			"ResizeVolume must re-fallocate so a sparse-by-history file reserves "+
			"its extents on disk (otherwise the post-resize write into the "+
			"un-reserved range can ENOSPC); expected %q in calls; got %v",
			wantFallocate, fx.CommandLines())
	}

	// `truncate` MUST NOT be issued on the no-grow path — only the
	// fallocate step is the load-bearing reconcile.
	for _, line := range fx.CommandLines() {
		if strings.HasPrefix(line, "truncate ") {
			t.Errorf("no-grow ResizeVolume must NOT re-issue `truncate`; got %q", line)
		}
	}
}

// TestThinResizeNoGrowSkipNoFallocate: FILE_THIN ResizeVolume's
// no-grow short-circuit MUST NOT call fallocate.
func TestThinResizeNoGrowSkipNoFallocate(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()

	path := filepath.Join(dir, "pvc-1_00000.img")

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create file: %v", err)
	}

	if err := f.Truncate(2 * 1024 * 1024 * 1024); err != nil {
		t.Fatalf("seed truncate: %v", err)
	}

	_ = f.Close()

	p := file.NewProvider(file.Config{Dir: dir, Thin: true}, fx)

	err = p.ResizeVolume(t.Context(), storage.Volume{
		ResourceName: "pvc-1",
		VolumeNumber: 0,
		SizeKib:      2 * 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("ResizeVolume: %v", err)
	}

	for _, line := range fx.CommandLines() {
		if strings.HasPrefix(line, "fallocate ") {
			t.Errorf("THIN ResizeVolume (no-grow path) must NOT call fallocate "+
				"(would defeat sparse / overcommit semantics); got %q", line)
		}
	}
}
