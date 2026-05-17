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
	"slices"
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
//
// Issue 216: CreateSnapshot on ZFS must also be idempotent — a
// pre-existing snapshot dataset must NOT trigger a fresh
// `zfs snapshot` call that the real `zfs` would reject with
// "dataset already exists" and the satellite reconciler would then
// re-queue forever. Add the same datasetExists pre-check the delete
// path already uses (Bug 212).

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

// zfsSnapDSQuery is the datasetExists probe shape on the snapshot dataset.
const zfsSnapDSQuery = "zfs list -H -o name tank/pvc-1_00000@snap-1"

// zfsSnapCreate is the `zfs snapshot` dispatch the create path
// must NOT issue when the snapshot dataset is already present.
const zfsSnapCreate = "zfs snapshot tank/pvc-1_00000@snap-1"

// TestCreateSnapshotPresentIsNoop: when `zfs list` reports the
// snapshot dataset already exists, CreateSnapshot returns nil and
// MUST NOT issue a fresh `zfs snapshot` (the real `zfs` would
// reject it with "dataset already exists", looping the reconciler).
func TestCreateSnapshotPresentIsNoop(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect(zfsSnapDSQuery,
		storage.FakeResponse{Stdout: []byte("tank/pvc-1_00000@snap-1\n")})

	p := zfs.NewProvider(zfs.Config{Pool: "tank"}, fx)

	err := p.CreateSnapshot(t.Context(), storage.Snapshot{
		ResourceName: "pvc-1",
		SnapshotName: "snap-1",
	})
	if err != nil {
		t.Fatalf("CreateSnapshot on existing snapshot dataset: got %v, want nil", err)
	}

	if slices.Contains(fx.CommandLines(), zfsSnapCreate) {
		t.Errorf("zfs snapshot ran despite pre-existing snapshot dataset: %v",
			fx.CommandLines())
	}
}

// TestCreateSnapshotAbsentDispatchesZfsSnap: when `zfs list` reports
// the dataset is missing, CreateSnapshot returns nil AND issues
// `zfs snapshot` so the materialisation actually happens. Locks the
// pre-check fold's "false → dispatch" branch.
func TestCreateSnapshotAbsentDispatchesZfsSnap(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect(zfsSnapDSQuery,
		storage.FakeResponse{Err: errZFSSnapMissing})

	p := zfs.NewProvider(zfs.Config{Pool: "tank"}, fx)

	err := p.CreateSnapshot(t.Context(), storage.Snapshot{
		ResourceName: "pvc-1",
		SnapshotName: "snap-1",
	})
	if err != nil {
		t.Fatalf("CreateSnapshot on missing snapshot dataset: got %v, want nil", err)
	}

	if !slices.Contains(fx.CommandLines(), zfsSnapCreate) {
		t.Errorf("zfs snapshot did not run despite missing snapshot dataset: %v",
			fx.CommandLines())
	}
}
