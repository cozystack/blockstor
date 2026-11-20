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

package file_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/file"
)

// The file provider tells whether its pool directory holds a snapshot copy.
func TestFileHasSnapshot(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	prov := file.NewProvider(file.Config{Dir: dir}, storage.NewFakeExec())
	snap := storage.Snapshot{ResourceName: "pvc-1", SnapshotName: "snap-1"}

	has, err := prov.HasSnapshot(t.Context(), snap)
	if err != nil || has {
		t.Fatalf("HasSnapshot before the copy = %v, %v; want false", has, err)
	}

	err = os.WriteFile(filepath.Join(dir, "pvc-1_snap-1_00000.img"), nil, 0o600)
	if err != nil {
		t.Fatal(err)
	}

	has, err = prov.HasSnapshot(t.Context(), snap)
	if err != nil || !has {
		t.Fatalf("HasSnapshot with the copy = %v, %v; want true", has, err)
	}
}
