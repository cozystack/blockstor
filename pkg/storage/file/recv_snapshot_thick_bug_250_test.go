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
	"bytes"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/file"
)

// Bug 250 (P2, space-guarantee, cross-node): FILE-thick RecvSnapshot used
// `os.OpenFile` + `io.Copy` regardless of cfg.Thin. The receiver path
// created the backing file from a peer's byte stream WITHOUT pre-
// allocating it via fallocate, so the recv-side "thick" copy of a
// cross-node-cloned volume had no space reservation. The first divergent
// write after attach could ENOSPC even though the volume reported full
// allocation (same blast as Bug 247 ResizeVolume, but on the wire path
// the v30/v31 audits didn't cover).
//
// Fix: when !cfg.Thin, after creating the `.partial` file pre-allocate
// it via `fallocate -l <expected_size>` before io.Copy so the bytes are
// reserved on disk. Mirrors the Bug 247 ResizeVolume fallocate pattern.

// TestRecvSnapshotThickFallocates pins the post-fix shape on the thick
// path: RecvSnapshot MUST call `fallocate -l <bytes> <partial>` BEFORE
// streaming so the backing range is reserved on-disk and the receiver
// can never out-allocate the filesystem mid-stream.
func TestRecvSnapshotThickFallocates(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()

	// Target does not exist → the recv path will be taken. Final path
	// loop-attach happens after the rename.
	finalPath := filepath.Join(dir, "pvc-2_00000.img")
	partialPath := finalPath + ".partial"

	fakeLosetup(fx, finalPath, "/dev/loop42")

	// Thick provider — Thin: false.
	p := file.NewProvider(file.Config{Dir: dir}, fx)

	// 1 GiB worth of stream — content is opaque to RecvSnapshot.
	src := bytes.NewReader(bytes.Repeat([]byte{0xAB}, 4096))

	err := p.RecvSnapshot(t.Context(),
		storage.Volume{
			ResourceName: "pvc-2",
			VolumeNumber: 0,
			SizeKib:      1024 * 1024, // 1 GiB
		},
		src,
	)
	if err != nil {
		t.Fatalf("RecvSnapshot: %v", err)
	}

	wantFallocate := "fallocate -l 1073741824 " + partialPath
	if !slices.Contains(fx.CommandLines(), wantFallocate) {
		t.Errorf("Bug 250: thick RecvSnapshot must reserve the partial file with %q "+
			"BEFORE io.Copy (otherwise a cross-node-cloned thick FILE volume has "+
			"no space reservation and writes can hit ENOSPC mid-stream); got calls %v",
			wantFallocate, fx.CommandLines())
	}
}

// TestRecvSnapshotThinNoFallocate pins the thin half of the split:
// FILE_THIN RecvSnapshot MUST NOT call fallocate (sparse is the whole
// point of the thin contract — pre-allocating defeats overcommit).
func TestRecvSnapshotThinNoFallocate(t *testing.T) {
	dir := t.TempDir()
	fx := storage.NewFakeExec()

	finalPath := filepath.Join(dir, "pvc-2_00000.img")
	fakeLosetup(fx, finalPath, "/dev/loop42")

	// Thin provider — Thin: true.
	p := file.NewProvider(file.Config{Dir: dir, Thin: true}, fx)

	src := bytes.NewReader(bytes.Repeat([]byte{0xAB}, 4096))

	err := p.RecvSnapshot(t.Context(),
		storage.Volume{
			ResourceName: "pvc-2",
			VolumeNumber: 0,
			SizeKib:      1024 * 1024,
		},
		src,
	)
	if err != nil {
		t.Fatalf("RecvSnapshot: %v", err)
	}

	for _, line := range fx.CommandLines() {
		if strings.HasPrefix(line, "fallocate ") {
			t.Errorf("thin RecvSnapshot must NOT call fallocate "+
				"(would defeat sparse / overcommit semantics on FILE_THIN); got %q",
				line)
		}
	}
}
