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
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/lvm"
)

// TestThinKind: the provider declares the upstream LINSTOR provider kind
// name verbatim so it round-trips through the StoragePool CRD.
func TestThinKind(t *testing.T) {
	p := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "thinpool"}, storage.NewFakeExec())
	if got := p.Kind(); got != "LVM_THIN" {
		t.Errorf("Kind: got %q, want LVM_THIN", got)
	}
}

// TestThinCreateVolumeIssuesLvcreate pins the create command shape.
// Anyone refactoring lvcreate args will see this test fail and need to
// update it deliberately.
func TestThinCreateVolumeIssuesLvcreate(t *testing.T) {
	fx := storage.NewFakeExec()
	// Idempotency check: pretend the volume does not exist yet.
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("")})

	p := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "thinpool"}, fx)

	err := p.CreateVolume(t.Context(), storage.Volume{
		ResourceName: "pvc-1",
		VolumeNumber: 0,
		SizeKib:      1024 * 1024, // 1 GiB
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	want := "lvcreate --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --thin --virtualsize 1024MiB --name pvc-1_00000 vg/thinpool"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("expected %q in calls; got %v", want, fx.CommandLines())
	}
}

// TestThinCreateVolumeIdempotent: if the LV already exists, no lvcreate
// is issued. Reconcile loops re-call CreateVolume; this is what makes
// them safe.
func TestThinCreateVolumeIdempotent(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("pvc-1_00000\n")})

	p := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "thinpool"}, fx)

	err := p.CreateVolume(t.Context(), storage.Volume{
		ResourceName: "pvc-1",
		VolumeNumber: 0,
		SizeKib:      1024 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	for _, line := range fx.CommandLines() {
		if line == "lvcreate" || (len(line) > 9 && line[:9] == "lvcreate --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } ") {
			t.Errorf("idempotent CreateVolume issued lvcreate: %s", line)
		}
	}
}

// TestThinDeleteVolume: lvremove gets called with -f.
func TestThinDeleteVolume(t *testing.T) {
	fx := storage.NewFakeExec()
	// Pretend the LV exists so Delete actually fires.
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("pvc-1_00000\n")})

	p := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "thinpool"}, fx)

	err := p.DeleteVolume(t.Context(), storage.Volume{
		ResourceName: "pvc-1",
		VolumeNumber: 0,
	})
	if err != nil {
		t.Fatalf("DeleteVolume: %v", err)
	}

	want := "lvremove --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --force vg/pvc-1_00000"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("expected %q in calls; got %v", want, fx.CommandLines())
	}
}

// TestThinDeleteVolumeIdempotent: missing LV → swallow, no lvremove.
func TestThinDeleteVolumeIdempotent(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("")})

	p := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "thinpool"}, fx)

	err := p.DeleteVolume(t.Context(), storage.Volume{
		ResourceName: "pvc-1",
		VolumeNumber: 0,
	})
	if err != nil {
		t.Fatalf("DeleteVolume on missing: got %v, want nil", err)
	}

	for _, line := range fx.CommandLines() {
		if len(line) > 9 && line[:9] == "lvremove --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } " {
			t.Errorf("idempotent DeleteVolume issued lvremove: %s", line)
		}
	}
}

// TestThinVolumeStatusParsesLvs: the provider parses lvs output and
// returns DevicePath + sizes.
func TestThinVolumeStatusParsesLvs(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings --separator | -o lv_path,lv_size --units k --nosuffix vg/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("/dev/vg/pvc-1_00000|1048576.00\n")})

	p := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "thinpool"}, fx)

	got, err := p.VolumeStatus(t.Context(), storage.Volume{
		ResourceName: "pvc-1",
		VolumeNumber: 0,
	})
	if err != nil {
		t.Fatalf("VolumeStatus: %v", err)
	}

	if got.DevicePath != "/dev/vg/pvc-1_00000" {
		t.Errorf("DevicePath: got %q", got.DevicePath)
	}

	if got.AllocatedKib != 1048576 {
		t.Errorf("AllocatedKib: got %d, want 1048576", got.AllocatedKib)
	}

	if got.State != "PROVISIONED" {
		t.Errorf("State: got %q, want PROVISIONED", got.State)
	}
}

// TestThinVolumeStatusMissing: empty lvs output → NOT_PROVISIONED.
func TestThinVolumeStatusMissing(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings --separator | -o lv_path,lv_size --units k --nosuffix vg/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("")})

	p := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "thinpool"}, fx)

	got, err := p.VolumeStatus(t.Context(), storage.Volume{
		ResourceName: "pvc-1",
		VolumeNumber: 0,
	})
	if err != nil {
		t.Fatalf("VolumeStatus: %v", err)
	}

	if got.State != "NOT_PROVISIONED" {
		t.Errorf("State: got %q, want NOT_PROVISIONED", got.State)
	}

	if got.DevicePath != "" {
		t.Errorf("DevicePath: got %q, want empty", got.DevicePath)
	}
}

// TestThinPoolStatusParsesVgsLvs: PoolStatus reports free + total + can-snap.
func TestThinPoolStatusParsesVgsLvs(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings --separator | -o lv_size,data_percent --units k --nosuffix vg/thinpool",
		storage.FakeResponse{Stdout: []byte("104857600.00|25.00\n")})

	p := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "thinpool"}, fx)

	got, err := p.PoolStatus(t.Context())
	if err != nil {
		t.Fatalf("PoolStatus: %v", err)
	}

	if got.TotalCapacityKib != 104857600 {
		t.Errorf("TotalCapacityKib: got %d", got.TotalCapacityKib)
	}

	wantFree := int64(104857600 - (104857600 * 25 / 100))
	if got.FreeCapacityKib != wantFree {
		t.Errorf("FreeCapacityKib: got %d, want %d", got.FreeCapacityKib, wantFree)
	}

	if !got.SupportsSnapshots {
		t.Errorf("SupportsSnapshots: got false, want true")
	}
}

// TestThinCreateSnapshot uses lvcreate -s.
func TestThinCreateSnapshot(t *testing.T) {
	fx := storage.NewFakeExec()
	p := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "thinpool"}, fx)

	err := p.CreateSnapshot(t.Context(), storage.Snapshot{
		ResourceName: "pvc-1",
		SnapshotName: "snap-1",
	})
	if err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}

	// LV layout: snapshot taken of the resource's volume 0.
	want := "lvcreate --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --snapshot --name pvc-1_snap-1_00000 vg/pvc-1_00000"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("expected %q in calls; got %v", want, fx.CommandLines())
	}
}

// TestThinExecError surfaces the wrapped exec error verbatim.
func TestThinExecError(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("")}) // not exists
	fx.Expect("lvcreate --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --thin --virtualsize 1024MiB --name pvc-1_00000 vg/thinpool",
		storage.FakeResponse{Err: errLVCreateFailed})

	p := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "thinpool"}, fx)

	err := p.CreateVolume(t.Context(), storage.Volume{
		ResourceName: "pvc-1",
		VolumeNumber: 0,
		SizeKib:      1024 * 1024,
	})
	if !errors.Is(err, errLVCreateFailed) {
		t.Errorf("CreateVolume err: got %v, want wraps errLVCreateFailed", err)
	}
}

var errLVCreateFailed = errors.New("lvcreate: insufficient free space")

// TestThinResizeVolumeIssuesLvextend pins the lvextend command shape.
// CSI ControllerExpandVolume → REST PUT → reconciler ResizeVolume,
// so the wire-visible behaviour is "lvextend --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --size <newMiB>MiB
// vg/lv". Refactors that change the args will fail loudly here.
func TestThinResizeVolumeIssuesLvextend(t *testing.T) {
	fx := storage.NewFakeExec()
	p := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "thinpool"}, fx)

	err := p.ResizeVolume(t.Context(), storage.Volume{
		ResourceName: "pvc-1",
		VolumeNumber: 0,
		SizeKib:      2048 * 1024, // 2 GiB
	})
	if err != nil {
		t.Fatalf("ResizeVolume: %v", err)
	}

	want := "lvextend --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --size 2048MiB vg/pvc-1_00000"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("expected %q; got %v", want, fx.CommandLines())
	}
}

// TestThinDeleteSnapshot pins the snapshot teardown command shape.
// `lvremove --force <vg>/<rd>_<snap>_00000` matches what upstream
// LINSTOR's LVM_THIN snapshot delete drives.
func TestThinDeleteSnapshot(t *testing.T) {
	fx := storage.NewFakeExec()
	p := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)

	err := p.DeleteSnapshot(t.Context(), storage.Snapshot{
		ResourceName: "pvc-1",
		SnapshotName: "snap-1",
	})
	if err != nil {
		t.Fatalf("DeleteSnapshot: %v", err)
	}

	want := "lvremove --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --force vg/pvc-1_snap-1_00000"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("expected %q; got %v", want, fx.CommandLines())
	}
}

// TestThinPoolStatusEmptyOutput: empty `lvs` output → "thin pool not
// found" error rather than panic. Matches the satellite's expected
// fail-loud path on a misconfigured pool name.
func TestThinPoolStatusEmptyOutput(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings --separator | -o lv_size,data_percent --units k --nosuffix vg/missing",
		storage.FakeResponse{Stdout: []byte("")})

	p := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "missing"}, fx)

	_, err := p.PoolStatus(t.Context())
	if err == nil {
		t.Errorf("expected error on empty lvs output")
	}
}

// TestThinPoolStatusBadColumns: malformed `lvs` output → parse
// error, not a slice-out-of-bounds panic.
func TestThinPoolStatusBadColumns(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings --separator | -o lv_size,data_percent --units k --nosuffix vg/tp",
		storage.FakeResponse{Stdout: []byte("only-one-col\n")})

	p := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)

	_, err := p.PoolStatus(t.Context())
	if err == nil {
		t.Errorf("expected error on malformed lvs output")
	}
}

// TestThinPoolStatusBadDataPercent: well-shaped output but the
// data_percent column is non-numeric (lvs does this for inactive
// thin pools). Surfaces a wrapped ParseFloat error rather than a
// panic on float coercion.
func TestThinPoolStatusBadDataPercent(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings --separator | -o lv_size,data_percent --units k --nosuffix vg/tp",
		storage.FakeResponse{Stdout: []byte("104857600.00|inactive\n")})

	p := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)

	_, err := p.PoolStatus(t.Context())
	if err == nil {
		t.Errorf("expected ParseFloat error on non-numeric data_percent")
	}
}

// TestThinPoolStatusGarbageFromLvs pins the parse-error wrap on the
// thin pool's capacity calculation: when lvs emits a non-numeric
// fragment (LVM-side bug, locale issue, garbled pipe), the
// PoolStatus call must surface it as a wrapped error rather than
// crash or return zero. Reaches the parseFloatToInt64 error branch
// (which previously sat at 75%).
func TestThinPoolStatusGarbageFromLvs(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings --separator | -o lv_size,data_percent --units k --nosuffix vg/thinpool",
		storage.FakeResponse{Stdout: []byte("not-a-number|25.00\n")})

	p := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "thinpool"}, fx)

	_, err := p.PoolStatus(t.Context())
	if err == nil {
		t.Fatalf("PoolStatus on garbage lvs output: got nil, want parse error")
	}
}

// TestThinCreateSnapshotErrorWraps: lvcreate -s failure must
// surface with the "lvcreate --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } -s" wrap keyword for operator grep.
func TestThinCreateSnapshotErrorWraps(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("lvcreate --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --snapshot --name pvc-1_snap-1_00000 vg/pvc-1_00000",
		storage.FakeResponse{Err: errLVMCmdFailed})

	p := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "thinpool"}, fx)

	err := p.CreateSnapshot(t.Context(), storage.Snapshot{
		ResourceName: "pvc-1",
		SnapshotName: "snap-1",
	})
	if err == nil {
		t.Fatalf("CreateSnapshot: got nil, want error")
	}

	if !strings.Contains(err.Error(), "lvcreate -s") {
		t.Errorf("wrap: %q must contain \"lvcreate -s\"", err.Error())
	}
}

// TestThinDeleteSnapshotErrorWraps: lvremove on a snapshot LV must
// surface with the "lvremove --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } -f" wrap keyword.
func TestThinDeleteSnapshotErrorWraps(t *testing.T) {
	fx := storage.NewFakeExec()
	fx.Expect("lvremove --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --force vg/pvc-1_snap-1_00000",
		storage.FakeResponse{Err: errLVMCmdFailed})

	p := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "thinpool"}, fx)

	err := p.DeleteSnapshot(t.Context(), storage.Snapshot{
		ResourceName: "pvc-1",
		SnapshotName: "snap-1",
	})
	if err == nil {
		t.Fatalf("DeleteSnapshot: got nil, want error")
	}

	if !strings.Contains(err.Error(), "lvremove -f") {
		t.Errorf("wrap: %q must contain \"lvremove -f\"", err.Error())
	}
}
