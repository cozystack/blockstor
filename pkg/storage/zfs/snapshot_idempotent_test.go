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

package zfs_test

import (
	"errors"
	"testing"

	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/zfs"
)

// Issue 212: DeleteSnapshot on ZFS must be idempotent — a missing
// snapshot dataset must NOT bubble an error up. Without this fold,
// the satellite-side finalizer-strip never fires and the Snapshot
// CRD sticks in Terminating forever, also blocking the parent RD's
// cascade-delete. Mirrors the DeleteVolume idempotency already in
// place on the ZFS provider.

var errZFSSnapMissing = errors.New("dataset does not exist")

// TestDeleteSnapshotMissingIsNoop: when `zfs list` reports the
// snapshot dataset is gone, DeleteSnapshot returns nil without
// issuing a destroy.
func TestDeleteSnapshotMissingIsNoop(t *testing.T) {
	fx := storage.NewFakeExec()
	// Pre-check probe: `zfs list` on the snapshot dataset returns the
	// real-tool's "dataset does not exist" error.
	fx.Expect("zfs list -H -o name tank/pvc-1_00000@snap-1",
		storage.FakeResponse{Err: errZFSSnapMissing})

	p := zfs.NewProvider(zfs.Config{Pool: "tank"}, fx)

	err := p.DeleteSnapshot(t.Context(), storage.Snapshot{
		ResourceName: "pvc-1",
		SnapshotName: "snap-1",
	})
	if err != nil {
		t.Fatalf("DeleteSnapshot on missing dataset: got %v, want nil", err)
	}

	// destroy must NOT have run.
	for _, cmd := range fx.CommandLines() {
		if cmd == "zfs destroy tank/pvc-1_00000@snap-1" {
			t.Errorf("zfs destroy ran despite missing snapshot dataset: %v",
				fx.CommandLines())
		}
	}
}
