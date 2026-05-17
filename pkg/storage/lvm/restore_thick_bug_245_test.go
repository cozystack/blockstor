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
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/lvm"
)

// Bug 245 (P1, data integrity): LVM-thick RestoreVolumeFromSnapshot used
// `lvcreate --snapshot --extents 25%ORIGIN`, producing a COW overlay
// capped at 25 % of origin size. Writes exceeding that silently
// invalidated the LV (lv_attr → I), corrupting the restored PV. The thin
// variant can do this because thin snapshots are uncapped CoW; thick has
// no such shortcut.
//
// Fix: produce an independent fully-allocated LV via
// `lvcreate --size <origin_size> --name <new>` followed by a `dd` copy of
// the snapshot bytes onto the new LV.

// TestThickRestoreVolumeFromSnapshotNoSnapshotFlag is the negative
// witness — the restore-into-new path must NEVER use
// `lvcreate --snapshot`. That flag is exactly what created the 25 %
// COW overlay and the silent-corruption surface; if any future
// "optimisation" reintroduces it the data-integrity guarantee is gone.
func TestThickRestoreVolumeFromSnapshotNoSnapshotFlag(t *testing.T) {
	fx := storage.NewFakeExec()

	// Target LV must not exist (proceed past idempotency short-circuit).
	fx.Expect("lvs --config "+lvm.ConfigFilter+" --noheadings -o lv_name vg/pvc-2_00000",
		storage.FakeResponse{Stdout: []byte("")})
	// Source snapshot LV exists.
	fx.Expect("lvs --config "+lvm.ConfigFilter+" --noheadings -o lv_name vg/pvc-1_snap-1_00000",
		storage.FakeResponse{Stdout: []byte("pvc-1_snap-1_00000\n")})
	// VolumeStatus probe on the snapshot LV → reports a 1 GiB origin size.
	fx.Expect("lvs --config "+lvm.ConfigFilter+" --noheadings --separator | -o lv_path,lv_size --units k --nosuffix vg/pvc-1_snap-1_00000",
		storage.FakeResponse{Stdout: []byte("/dev/vg/pvc-1_snap-1_00000|1048576.00\n")})

	p := lvm.NewThick(lvm.ThickConfig{VolumeGroup: "vg"}, fx)

	err := p.RestoreVolumeFromSnapshot(t.Context(),
		storage.Volume{
			ResourceName: "pvc-2",
			VolumeNumber: 0,
			SizeKib:      1024 * 1024, // 1 GiB (matches snapshot size)
		},
		storage.Snapshot{
			ResourceName: "pvc-1",
			SnapshotName: "snap-1",
		},
	)
	if err != nil {
		t.Fatalf("RestoreVolumeFromSnapshot: %v", err)
	}

	for _, line := range fx.CommandLines() {
		if strings.HasPrefix(line, "lvcreate ") && strings.Contains(line, "--snapshot") {
			t.Errorf("Bug 245: restore-into-new path must NOT use `lvcreate --snapshot` "+
				"(produces capped COW overlay → silent corruption); got %q",
				line)
		}

		if strings.Contains(line, "25%ORIGIN") {
			t.Errorf("Bug 245: restore-into-new path must NOT use 25%%ORIGIN extents "+
				"(thick-snapshot cap → silent corruption); got %q", line)
		}
	}
}

// TestThickRestoreVolumeFromSnapshotIssuesFullSizeLvcreateAndDD pins the
// canonical thick-restore shape: `lvcreate --size <origin_size>` for an
// independent fully-allocated LV, followed by `dd` to copy the snapshot
// bytes onto it. This is heavy I/O but it's the correct semantic for
// "restore snapshot to a new independent volume on thick LVM" — thin
// uses CoW, thick has no shortcut.
func TestThickRestoreVolumeFromSnapshotIssuesFullSizeLvcreateAndDD(t *testing.T) {
	fx := storage.NewFakeExec()

	fx.Expect("lvs --config "+lvm.ConfigFilter+" --noheadings -o lv_name vg/pvc-2_00000",
		storage.FakeResponse{Stdout: []byte("")})
	fx.Expect("lvs --config "+lvm.ConfigFilter+" --noheadings -o lv_name vg/pvc-1_snap-1_00000",
		storage.FakeResponse{Stdout: []byte("pvc-1_snap-1_00000\n")})
	fx.Expect("lvs --config "+lvm.ConfigFilter+" --noheadings --separator | -o lv_path,lv_size --units k --nosuffix vg/pvc-1_snap-1_00000",
		storage.FakeResponse{Stdout: []byte("/dev/vg/pvc-1_snap-1_00000|1048576.00\n")})

	p := lvm.NewThick(lvm.ThickConfig{VolumeGroup: "vg"}, fx)

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

	var (
		lvcreateLine string
		ddLine       string
	)

	for _, line := range fx.CommandLines() {
		switch {
		case strings.HasPrefix(line, "lvcreate ") && strings.Contains(line, "--size ") &&
			strings.Contains(line, "--name pvc-2_00000") && !strings.Contains(line, "--snapshot"):
			lvcreateLine = line
		case strings.HasPrefix(line, "dd "):
			ddLine = line
		}
	}

	if lvcreateLine == "" {
		t.Errorf("Bug 245 fix: expected `lvcreate --size <origin_size> --name pvc-2_00000 vg` "+
			"(no --snapshot); got calls %v", fx.CommandLines())
	}

	// lvcreate must be sized at the origin's full size (1 GiB == 1024 MiB).
	if lvcreateLine != "" && !strings.Contains(lvcreateLine, "--size 1024MiB") {
		t.Errorf("Bug 245 fix: lvcreate must size the new LV at the origin's full size "+
			"(1024MiB); got %q", lvcreateLine)
	}

	if ddLine == "" {
		t.Errorf("Bug 245 fix: expected a `dd` invocation to copy snapshot bytes onto "+
			"the new LV; got calls %v", fx.CommandLines())
	}

	// dd must read FROM the snapshot LV and write TO the new target LV.
	if ddLine != "" {
		if !strings.Contains(ddLine, "if=/dev/vg/pvc-1_snap-1_00000") {
			t.Errorf("Bug 245 fix: dd must read from /dev/vg/pvc-1_snap-1_00000; got %q", ddLine)
		}

		if !strings.Contains(ddLine, "of=/dev/vg/pvc-2_00000") {
			t.Errorf("Bug 245 fix: dd must write to /dev/vg/pvc-2_00000; got %q", ddLine)
		}
	}
}
