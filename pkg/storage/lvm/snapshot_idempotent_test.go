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
