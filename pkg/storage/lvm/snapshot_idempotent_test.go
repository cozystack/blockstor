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

package lvm_test

import (
	"slices"
	"testing"

	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/lvm"
)

// Issue 212: DeleteSnapshot on LVM-thin/thick must be idempotent —
// a missing snapshot LV must NOT bubble an error up. Without this
// fold, the satellite-side finalizer-strip never fires and the
// Snapshot CRD sticks in Terminating forever, also blocking the
// parent RD's cascade-delete. Mirrors the DeleteVolume idempotency
// already in place on both providers.
//
// Issue 216: CreateSnapshot on the same providers must also be
// idempotent — a pre-existing snapshot LV must NOT trigger a fresh
// `lvcreate --snapshot` that the real `lvm` would reject with
// "already exists" and the satellite reconciler would then re-queue
// forever. The reconciler at pkg/satellite/controllers/snapshot.go
// formerly claimed `lvcreate --snapshot` short-circuits on its own;
// it does not. Add the same lvExists pre-check the delete path
// already uses (Bug 212).

// TestThinDeleteSnapshotMissingIsNoop: when lvs reports the
// snapshot LV doesn't exist, DeleteSnapshot returns nil without
// issuing lvremove.
func TestThinDeleteSnapshotMissingIsNoop(t *testing.T) {
	fx := storage.NewFakeExec()
	// lvExists path: lvs returns empty (no such LV).
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-1_snap-1_00000",
		storage.FakeResponse{Stdout: []byte("")})

	p := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "thinpool"}, fx)

	err := p.DeleteSnapshot(t.Context(), storage.Snapshot{
		ResourceName: "pvc-1",
		SnapshotName: "snap-1",
	})
	if err != nil {
		t.Fatalf("DeleteSnapshot on missing snapshot LV: got %v, want nil", err)
	}

	for _, cmd := range fx.CommandLines() {
		if cmd == "lvremove --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --force vg/pvc-1_snap-1_00000" {
			t.Errorf("lvremove ran despite missing snapshot LV: %v", fx.CommandLines())
		}
	}
}

// TestThickDeleteSnapshotMissingIsNoop: the LVM-thick counterpart.
func TestThickDeleteSnapshotMissingIsNoop(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-1_snap-1_00000",
		storage.FakeResponse{Stdout: []byte("")})

	p := lvm.NewThick(lvm.ThickConfig{VolumeGroup: "vg"}, fx)

	err := p.DeleteSnapshot(t.Context(), storage.Snapshot{
		ResourceName: "pvc-1",
		SnapshotName: "snap-1",
	})
	if err != nil {
		t.Fatalf("DeleteSnapshot on missing snapshot LV: got %v, want nil", err)
	}

	for _, cmd := range fx.CommandLines() {
		if cmd == "lvremove --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --force vg/pvc-1_snap-1_00000" {
			t.Errorf("lvremove ran despite missing snapshot LV: %v", fx.CommandLines())
		}
	}
}

// thinSnapLVQuery is the lvExists probe shape on the snapshot LV.
const thinSnapLVQuery = "lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-1_snap-1_00000"

// thinSnapLVCreate is the lvcreate --snapshot dispatch the create path
// must NOT issue when the snapshot LV is already present.
const thinSnapLVCreate = "lvcreate --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --snapshot --name pvc-1_snap-1_00000 vg/pvc-1_00000"

// thickSnapLVCreate is the LVM-thick analogue (extra `--extents 25%ORIGIN`).
const thickSnapLVCreate = "lvcreate --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --snapshot --extents 25%ORIGIN --name pvc-1_snap-1_00000 vg/pvc-1_00000"

// TestThinCreateSnapshotPresentIsNoop: when lvs reports the snapshot
// LV already exists, CreateSnapshot returns nil and MUST NOT issue
// a fresh `lvcreate --snapshot` (which the real LVM would reject
// with "already exists", looping the reconciler forever).
func TestThinCreateSnapshotPresentIsNoop(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect(thinSnapLVQuery,
		storage.FakeResponse{Stdout: []byte("pvc-1_snap-1_00000\n")})

	p := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "thinpool"}, fx)

	err := p.CreateSnapshot(t.Context(), storage.Snapshot{
		ResourceName: "pvc-1",
		SnapshotName: "snap-1",
	})
	if err != nil {
		t.Fatalf("CreateSnapshot on existing snapshot LV: got %v, want nil", err)
	}

	if slices.Contains(fx.CommandLines(), thinSnapLVCreate) {
		t.Errorf("lvcreate --snapshot ran despite pre-existing snapshot LV: %v",
			fx.CommandLines())
	}
}

// TestThinCreateSnapshotAbsentDispatchesLvcreate: when lvs reports the
// snapshot LV is missing, CreateSnapshot returns nil AND issues
// `lvcreate --snapshot` so the materialisation actually happens.
// Locks the pre-check fold's "false → dispatch" branch so a future
// refactor that short-circuits both branches gets caught.
func TestThinCreateSnapshotAbsentDispatchesLvcreate(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect(thinSnapLVQuery,
		storage.FakeResponse{Stdout: []byte("")})

	p := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "thinpool"}, fx)

	err := p.CreateSnapshot(t.Context(), storage.Snapshot{
		ResourceName: "pvc-1",
		SnapshotName: "snap-1",
	})
	if err != nil {
		t.Fatalf("CreateSnapshot on missing snapshot LV: got %v, want nil", err)
	}

	if !slices.Contains(fx.CommandLines(), thinSnapLVCreate) {
		t.Errorf("lvcreate --snapshot did not run despite missing snapshot LV: %v",
			fx.CommandLines())
	}
}

// TestThickCreateSnapshotPresentIsNoop: the LVM-thick counterpart.
func TestThickCreateSnapshotPresentIsNoop(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect(thinSnapLVQuery,
		storage.FakeResponse{Stdout: []byte("pvc-1_snap-1_00000\n")})

	p := lvm.NewThick(lvm.ThickConfig{VolumeGroup: "vg"}, fx)

	err := p.CreateSnapshot(t.Context(), storage.Snapshot{
		ResourceName: "pvc-1",
		SnapshotName: "snap-1",
	})
	if err != nil {
		t.Fatalf("CreateSnapshot on existing snapshot LV: got %v, want nil", err)
	}

	if slices.Contains(fx.CommandLines(), thickSnapLVCreate) {
		t.Errorf("lvcreate --snapshot ran despite pre-existing snapshot LV: %v",
			fx.CommandLines())
	}
}

// TestThickCreateSnapshotAbsentDispatchesLvcreate: thick "false →
// dispatch" branch guard.
func TestThickCreateSnapshotAbsentDispatchesLvcreate(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect(thinSnapLVQuery,
		storage.FakeResponse{Stdout: []byte("")})

	p := lvm.NewThick(lvm.ThickConfig{VolumeGroup: "vg"}, fx)

	err := p.CreateSnapshot(t.Context(), storage.Snapshot{
		ResourceName: "pvc-1",
		SnapshotName: "snap-1",
	})
	if err != nil {
		t.Fatalf("CreateSnapshot on missing snapshot LV: got %v, want nil", err)
	}

	if !slices.Contains(fx.CommandLines(), thickSnapLVCreate) {
		t.Errorf("lvcreate --snapshot did not run despite missing snapshot LV: %v",
			fx.CommandLines())
	}
}
