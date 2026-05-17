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

// Bug 247 (P2, space-guarantee): FILE-thick ResizeVolume always issued
// `truncate -s N`, ignoring cfg.Thin. truncate widens the file to N
// bytes but leaves the new range as sparse holes — the on-disk
// allocation is unchanged. For a thick FILE volume that's a silent
// downgrade of the space contract: the first write into the extended
// range can hit ENOSPC even though `df` on the volume reports plenty
// of free room.
//
// Mirror the CreateVolume split: thin → truncate alone (sparse), thick
// → truncate + fallocate so the extended bytes are actually reserved
// on the backing filesystem.

// TestResizeVolumeThickAllocatesNewRange pins the post-fix shape on
// the thick path: after the size bump, the provider MUST call
// `fallocate -l <target> <path>` so the extended range is reserved
// on-disk rather than left as sparse holes.
func TestResizeVolumeThickAllocatesNewRange(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()

	// Pre-create a 1 GiB backing file so ResizeVolume's stat succeeds
	// and the grow path is taken.
	path := filepath.Join(dir, "pvc-1_00000.img")

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create file: %v", err)
	}

	if err := f.Truncate(1024 * 1024 * 1024); err != nil {
		t.Fatalf("seed truncate: %v", err)
	}

	_ = f.Close()

	// Thick provider — Thin: false.
	p := file.NewProvider(file.Config{Dir: dir}, fx)

	err = p.ResizeVolume(t.Context(), storage.Volume{
		ResourceName: "pvc-1",
		VolumeNumber: 0,
		SizeKib:      2 * 1024 * 1024, // 2 GiB
	})
	if err != nil {
		t.Fatalf("ResizeVolume: %v", err)
	}

	wantFallocate := "fallocate -l 2147483648 " + path
	if !slices.Contains(fx.CommandLines(), wantFallocate) {
		t.Errorf("Bug 247: thick ResizeVolume must reserve the new range with %q "+
			"(otherwise the grown bytes are sparse holes that can ENOSPC on first write); "+
			"got calls %v",
			wantFallocate, fx.CommandLines())
	}
}

// TestResizeVolumeThinNoFallocate pins the thin half of the split:
// the thin path MUST NOT call fallocate (sparse is the whole point
// of FILE_THIN — pre-allocating defeats overcommit).
func TestResizeVolumeThinNoFallocate(t *testing.T) {
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

	// Thin provider — Thin: true.
	p := file.NewProvider(file.Config{Dir: dir, Thin: true}, fx)

	err = p.ResizeVolume(t.Context(), storage.Volume{
		ResourceName: "pvc-1",
		VolumeNumber: 0,
		SizeKib:      2 * 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("ResizeVolume: %v", err)
	}

	// Truncate is still the size-setter on thin (and on thick, before
	// the fallocate step).
	wantTruncate := "truncate -s 2147483648 " + path
	if !slices.Contains(fx.CommandLines(), wantTruncate) {
		t.Errorf("thin ResizeVolume must keep using %q to set size; got calls %v",
			wantTruncate, fx.CommandLines())
	}

	for _, line := range fx.CommandLines() {
		if strings.HasPrefix(line, "fallocate ") {
			t.Errorf("thin ResizeVolume must NOT call fallocate "+
				"(would defeat sparse / overcommit semantics); got %q",
				line)
		}
	}
}

// Bug 248 (P2, space-guarantee): FILE-thick RestoreVolumeFromSnapshot
// always used `cp --reflink=auto`, ignoring cfg.Thin. On reflink-
// capable filesystems (XFS, btrfs, cow-enabled ext4) the new "thick"
// volume CoW-shares blocks with the snapshot — writes that diverge
// from the snapshot can hit ENOSPC even though the volume's
// operator-visible size reports full allocation.
//
// Fix: thick → `cp` without --reflink (force a full byte copy so the
// new file has its own allocated blocks). Thin → keep
// `cp --reflink=auto` for the O(1) CoW path FILE_THIN advertises.

// TestRestoreVolumeFromSnapshotThickNoReflink pins the thick path:
// `cp --reflink=auto` MUST NOT appear — the new volume needs its own
// blocks to honour the thick space guarantee.
func TestRestoreVolumeFromSnapshotThickNoReflink(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()

	// Seed the snapshot .img so the source stat succeeds.
	snapPath := filepath.Join(dir, "pvc-1_snap-1_00000.img")
	if err := os.WriteFile(snapPath, []byte("snap"), 0o600); err != nil {
		t.Fatalf("seed snap: %v", err)
	}

	// Target must NOT exist so the copy path is taken (rather than
	// the idempotency short-circuit).
	dstPath := filepath.Join(dir, "pvc-2_00000.img")
	fakeLosetup(fx, dstPath, "/dev/loop42")

	// Thick provider.
	p := file.NewProvider(file.Config{Dir: dir}, fx)

	err := p.RestoreVolumeFromSnapshot(t.Context(),
		storage.Volume{
			ResourceName: "pvc-2",
			VolumeNumber: 0,
			SizeKib:      1024 * 1024,
		},
		storage.Snapshot{
			ResourceName: "pvc-1",
			SnapshotName: "snap-1",
		},
	)
	if err != nil {
		t.Fatalf("RestoreVolumeFromSnapshot: %v", err)
	}

	var cpLine string

	for _, line := range fx.CommandLines() {
		if strings.HasPrefix(line, "cp ") {
			cpLine = line

			break
		}
	}

	if cpLine == "" {
		t.Fatalf("expected a `cp` invocation copying the snapshot; got calls %v",
			fx.CommandLines())
	}

	if strings.Contains(cpLine, "--reflink=auto") {
		t.Errorf("Bug 248: thick RestoreVolumeFromSnapshot must NOT use --reflink=auto "+
			"(CoW-share with the snapshot defeats thick space guarantee → "+
			"first divergent write can ENOSPC); got %q", cpLine)
	}
}

// TestRestoreVolumeFromSnapshotThinKeepsReflink pins the thin half of
// the split: FILE_THIN keeps `cp --reflink=auto` because the whole
// point of thin is O(1) CoW snapshots. The fix must not regress the
// thin path.
func TestRestoreVolumeFromSnapshotThinKeepsReflink(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()

	snapPath := filepath.Join(dir, "pvc-1_snap-1_00000.img")
	if err := os.WriteFile(snapPath, []byte("snap"), 0o600); err != nil {
		t.Fatalf("seed snap: %v", err)
	}

	dstPath := filepath.Join(dir, "pvc-2_00000.img")
	fakeLosetup(fx, dstPath, "/dev/loop42")

	// Thin provider.
	p := file.NewProvider(file.Config{Dir: dir, Thin: true}, fx)

	err := p.RestoreVolumeFromSnapshot(t.Context(),
		storage.Volume{
			ResourceName: "pvc-2",
			VolumeNumber: 0,
			SizeKib:      1024 * 1024,
		},
		storage.Snapshot{
			ResourceName: "pvc-1",
			SnapshotName: "snap-1",
		},
	)
	if err != nil {
		t.Fatalf("RestoreVolumeFromSnapshot: %v", err)
	}

	wantCp := "cp --reflink=auto " + snapPath + " " + dstPath
	if !slices.Contains(fx.CommandLines(), wantCp) {
		t.Errorf("thin RestoreVolumeFromSnapshot must keep using %q "+
			"(FILE_THIN advertises CanSnapshots=True via reflink CoW); got calls %v",
			wantCp, fx.CommandLines())
	}
}
