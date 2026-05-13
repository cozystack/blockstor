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

package controllers_test

import (
	"errors"
	"testing"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/satellite/controllers"
	"github.com/cozystack/blockstor/pkg/storage"
)

// errNoDRBDMeta stands in for drbdmeta's "no metadata block" error.
// HasDRBDSignature swallows any drbdmeta error as "no signature";
// pinning a sentinel here makes the FakeExec wiring intent-clear
// to readers.
var errNoDRBDMeta = errors.New("drbdmeta: no metadata")

// lsblkCmdLine is the exact command line FakeExec keys responses on
// for `satellite.Lsblk`. Centralised here so a change to Lsblk's
// arg list (e.g. adding a column) trips one match-string to update
// instead of one per test.
const lsblkCmdLine = "lsblk -Pb -o NAME,KNAME,SIZE,FSTYPE,TYPE,MOUNTPOINT,WWN,MODEL,SERIAL,ROTA,TRAN"

// pvsCmdLine matches the LVM signature probe key. The flag-string
// is built by `lvm.Args(...)` — pinning the literal here keeps the
// test independent of accidental refactors that would otherwise
// only fail at runtime.
const pvsCmdLine = "pvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o pv_name"

// lsblkRow returns the canonical `KEY="value" KEY="value" ...` line
// the lsblk parser expects, for one fake device. Keeps the test
// readable — three rows of inline `KEY="..."` would dominate the
// happy-path test.
func lsblkRow(name, kname, size, fstype, typ, mount, wwn, model, serial, rota, tran string) string {
	return `NAME="` + name + `" KNAME="` + kname + `" SIZE="` + size + `" FSTYPE="` + fstype + `" TYPE="` + typ + `" MOUNTPOINT="` + mount + `" WWN="` + wwn + `" MODEL="` + model + `" SERIAL="` + serial + `" ROTA="` + rota + `" TRAN="` + tran + `"`
}

// TestReconcilerPublishesFreeDisks pins the happy path: a fresh
// lsblk scan returns three rows (sda partitioned, sdb free, sdc
// mounted). The discovery loop must publish a PhysicalDevice CRD
// for each TYPE=disk row, with `Free=True` only on sdb (the one
// where IsDeviceFree returns true).
//
// Why three rows: sda exercises the "filesystem signature → not
// free" branch, sdb the "clean disk → free" branch, sdc the
// "mounted → not free" branch. Combined they pin every input the
// scanner must produce a CRD for.
func TestReconcilerPublishesFreeDisks(t *testing.T) {
	t.Parallel()

	scheme := newStoragePoolScheme(t)

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.PhysicalDevice{}).
		Build()

	fx := storage.NewFakeExec()
	fx.Expect(lsblkCmdLine, storage.FakeResponse{
		Stdout: []byte(
			lsblkRow("sda", "sda", "1000204886016", "lvm2_member", "disk", "", "0x5000c500a3b1c2d1", "DISK_A", "SN-A", "0", "sata") + "\n" +
				lsblkRow("sdb", "sdb", "2000398934016", "", "disk", "", "0x5000c500a3b1c2d2", "DISK_B", "SN-B", "0", "sata") + "\n" +
				lsblkRow("sdc", "sdc", "500107862016", "ext4", "disk", "/data", "0x5000c500a3b1c2d3", "DISK_C", "SN-C", "1", "sata") + "\n"),
	})
	// sda's IsFreeBlockDevice fails on FSType="lvm2_member" → no
	// signature probes run. sdb passes lsblk filter; pvs/zpool/
	// drbdmeta/wipefs all must say "no signature".
	fx.Expect(pvsCmdLine, storage.FakeResponse{Stdout: []byte("")})
	fx.Expect("zpool list -PHv", storage.FakeResponse{Stdout: []byte("")})
	fx.Expect("drbdmeta 0 v09 /dev/sdb internal dump-md", storage.FakeResponse{Err: errNoDRBDMeta})
	fx.Expect("wipefs -n /dev/sdb", storage.FakeResponse{Stdout: []byte("")})

	runnable := &controllers.PhysicalDeviceDiscoveryRunnable{
		Client:   cli,
		Exec:     fx,
		NodeName: "n1",
	}

	err := controllers.ScanOnceForTest(t.Context(), runnable, logr.Discard())
	if err != nil {
		t.Fatalf("scanOnce: %v", err)
	}

	var list blockstoriov1alpha1.PhysicalDeviceList

	err = cli.List(t.Context(), &list,
		client.MatchingLabels{blockstoriov1alpha1.PhysicalDeviceLabelNode: "n1"})
	if err != nil {
		t.Fatalf("list PhysicalDevices: %v", err)
	}

	// All three TYPE=disk rows must produce a CRD (the scanner
	// publishes free + non-free so operators see the full picture
	// in `kubectl get physicaldevice`; only `free=true` ones
	// surface in `linstor ps l`).
	if len(list.Items) != 3 {
		t.Fatalf("PhysicalDevice count: got %d, want 3 (one per TYPE=disk lsblk row)", len(list.Items))
	}

	got := indexByKName(list.Items)
	if got["sda"] == nil || got["sdb"] == nil || got["sdc"] == nil {
		t.Fatalf("missing CRD for one of sda/sdb/sdc: %+v", got)
	}

	if got["sda"].Status.DevicePath != "/dev/disk/by-id/wwn-0x5000c500a3b1c2d1" {
		t.Errorf("sda DevicePath: got %q, want /dev/disk/by-id/wwn-0x5000c500a3b1c2d1",
			got["sda"].Status.DevicePath)
	}

	if cond := findCondition(got["sdb"].Status.Conditions, "Free"); cond == nil || cond.Status != metav1.ConditionTrue {
		t.Errorf("sdb Free condition: got %+v, want Status=True (clean disk)", cond)
	}

	if cond := findCondition(got["sda"].Status.Conditions, "Free"); cond == nil || cond.Status != metav1.ConditionFalse {
		t.Errorf("sda Free condition: got %+v, want Status=False (FSType=lvm2_member)", cond)
	}

	if cond := findCondition(got["sdc"].Status.Conditions, "Free"); cond == nil || cond.Status != metav1.ConditionFalse {
		t.Errorf("sdc Free condition: got %+v, want Status=False (mounted on /data)", cond)
	}

	if got["sdb"].Status.SizeBytes != 2000398934016 {
		t.Errorf("sdb SizeBytes: got %d, want 2000398934016", got["sdb"].Status.SizeBytes)
	}
}

// TestReconcilerRefreshesExistingCRDOnRescan pins the "running
// scanner picks up state change" path: sdb was free on tick 1
// (CRD created with Free=True); operator `pvcreate`s it; tick 2
// must flip the existing CRD's Free condition to False rather
// than leaving the stale True in place.
func TestReconcilerRefreshesExistingCRDOnRescan(t *testing.T) {
	t.Parallel()

	scheme := newStoragePoolScheme(t)

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.PhysicalDevice{}).
		Build()

	fx := storage.NewFakeExec()

	// Tick 1: sdb is clean.
	fx.Expect(lsblkCmdLine, storage.FakeResponse{
		Stdout: []byte(lsblkRow("sdb", "sdb", "2000398934016", "", "disk", "", "0xWWN-B", "DISK_B", "SN-B", "0", "sata") + "\n"),
	})
	fx.Expect(pvsCmdLine, storage.FakeResponse{Stdout: []byte("")})
	fx.Expect("zpool list -PHv", storage.FakeResponse{Stdout: []byte("")})
	fx.Expect("drbdmeta 0 v09 /dev/sdb internal dump-md", storage.FakeResponse{Err: errNoDRBDMeta})
	fx.Expect("wipefs -n /dev/sdb", storage.FakeResponse{Stdout: []byte("")})

	runnable := &controllers.PhysicalDeviceDiscoveryRunnable{
		Client:   cli,
		Exec:     fx,
		NodeName: "n1",
	}

	err := controllers.ScanOnceForTest(t.Context(), runnable, logr.Discard())
	if err != nil {
		t.Fatalf("scanOnce tick 1: %v", err)
	}

	// Tick 2: operator just ran `pvcreate /dev/sdb` — pvs now
	// reports it AND lsblk's FSTYPE shows lvm2_member. Either
	// alone would flip Free=False; pinning both covers the
	// realistic "signature appeared between scans" path.
	fx.Reset()
	fx.Expect(lsblkCmdLine, storage.FakeResponse{
		Stdout: []byte(lsblkRow("sdb", "sdb", "2000398934016", "lvm2_member", "disk", "", "0xWWN-B", "DISK_B", "SN-B", "0", "sata") + "\n"),
	})
	fx.Expect(pvsCmdLine, storage.FakeResponse{Stdout: []byte("/dev/sdb\n")})

	err = controllers.ScanOnceForTest(t.Context(), runnable, logr.Discard())
	if err != nil {
		t.Fatalf("scanOnce tick 2: %v", err)
	}

	var list blockstoriov1alpha1.PhysicalDeviceList

	err = cli.List(t.Context(), &list)
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	if len(list.Items) != 1 {
		t.Fatalf("PhysicalDevice count: got %d, want 1 (same device, same stable id)", len(list.Items))
	}

	cond := findCondition(list.Items[0].Status.Conditions, "Free")
	if cond == nil {
		t.Fatalf("Free condition missing after tick 2: %+v", list.Items[0].Status.Conditions)
	}

	if cond.Status != metav1.ConditionFalse {
		t.Errorf("Free condition: got %v, want False (sdb now has lvm2_member signature)", cond.Status)
	}
}

// TestReconcilerRemovesCRDForDisappearedDisk pins the prune path:
// sdc was published on tick 1 (CRD exists); the drive is yanked
// (or its kname slot disappears) before tick 2; tick 2 must delete
// the CRD so `linstor ps l` no longer shows the ghost.
//
// Documents the choice: we DELETE rather than mark-stale because
// the CRD's metadata.name is derived from a stable ID — when the
// drive returns, the same name re-materialises with fresh Status.
// Stale-marking would require another field on Status and add
// "is this stale enough to delete" logic for no operator-visible
// benefit.
func TestReconcilerRemovesCRDForDisappearedDisk(t *testing.T) {
	t.Parallel()

	scheme := newStoragePoolScheme(t)

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.PhysicalDevice{}).
		Build()

	fx := storage.NewFakeExec()

	// Tick 1: sdc is present + free.
	fx.Expect(lsblkCmdLine, storage.FakeResponse{
		Stdout: []byte(lsblkRow("sdc", "sdc", "500107862016", "", "disk", "", "0xWWN-C", "DISK_C", "SN-C", "0", "sata") + "\n"),
	})
	fx.Expect(pvsCmdLine, storage.FakeResponse{Stdout: []byte("")})
	fx.Expect("zpool list -PHv", storage.FakeResponse{Stdout: []byte("")})
	fx.Expect("drbdmeta 0 v09 /dev/sdc internal dump-md", storage.FakeResponse{Err: errNoDRBDMeta})
	fx.Expect("wipefs -n /dev/sdc", storage.FakeResponse{Stdout: []byte("")})

	runnable := &controllers.PhysicalDeviceDiscoveryRunnable{
		Client:   cli,
		Exec:     fx,
		NodeName: "n1",
	}

	err := controllers.ScanOnceForTest(t.Context(), runnable, logr.Discard())
	if err != nil {
		t.Fatalf("scanOnce tick 1: %v", err)
	}

	var preList blockstoriov1alpha1.PhysicalDeviceList
	if err := cli.List(t.Context(), &preList); err != nil {
		t.Fatalf("list pre-tick-2: %v", err)
	}

	if len(preList.Items) != 1 {
		t.Fatalf("pre-tick-2: got %d CRDs, want 1", len(preList.Items))
	}

	// Tick 2: sdc gone from lsblk entirely. No other devices.
	fx.Reset()
	fx.Expect(lsblkCmdLine, storage.FakeResponse{Stdout: []byte("")})

	err = controllers.ScanOnceForTest(t.Context(), runnable, logr.Discard())
	if err != nil {
		t.Fatalf("scanOnce tick 2: %v", err)
	}

	var postList blockstoriov1alpha1.PhysicalDeviceList
	if err := cli.List(t.Context(), &postList); err != nil {
		t.Fatalf("list post-tick-2: %v", err)
	}

	if len(postList.Items) != 0 {
		t.Errorf("post-tick-2: got %d CRDs, want 0 (sdc gone from lsblk)", len(postList.Items))
	}

	// Belt-and-braces: a follow-up Get on the sdc CRD must surface
	// NotFound (would catch a regression that "kept" the CRD by
	// some other code path).
	stale := &blockstoriov1alpha1.PhysicalDevice{}
	err = cli.Get(t.Context(), client.ObjectKey{Name: preList.Items[0].Name}, stale)
	if err == nil {
		t.Errorf("expected NotFound on disappeared CRD, got nil error (object still there: %+v)", stale)
	} else if !apierrors.IsNotFound(err) {
		t.Errorf("expected NotFound on disappeared CRD, got: %v", err)
	}
}

// TestReconcilerSkipsAttachInFlight pins the prune-side safety
// rule: a CRD whose Spec.AttachTo is set is owned by the
// PhysicalDeviceReconciler (attach is in flight) — the discovery
// loop MUST NOT delete it even if lsblk no longer surfaces the
// device. Otherwise a discovery race would whisk the CRD out
// from under an in-progress pvcreate / zpool create.
func TestReconcilerSkipsAttachInFlight(t *testing.T) {
	t.Parallel()

	scheme := newStoragePoolScheme(t)

	existing := &blockstoriov1alpha1.PhysicalDevice{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "n1.wwn-0xstuck",
			Labels: map[string]string{blockstoriov1alpha1.PhysicalDeviceLabelNode: "n1"},
		},
		Spec: blockstoriov1alpha1.PhysicalDeviceSpec{
			AttachTo: &blockstoriov1alpha1.AttachToPool{
				StoragePoolName: "lvm-thin",
				ProviderKind:    "LVM_THIN",
			},
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(existing).
		WithStatusSubresource(&blockstoriov1alpha1.PhysicalDevice{}).
		Build()

	fx := storage.NewFakeExec()
	fx.Expect(lsblkCmdLine, storage.FakeResponse{Stdout: []byte("")})

	runnable := &controllers.PhysicalDeviceDiscoveryRunnable{
		Client:   cli,
		Exec:     fx,
		NodeName: "n1",
	}

	err := controllers.ScanOnceForTest(t.Context(), runnable, logr.Discard())
	if err != nil {
		t.Fatalf("scanOnce: %v", err)
	}

	var got blockstoriov1alpha1.PhysicalDevice
	err = cli.Get(t.Context(), client.ObjectKey{Name: "n1.wwn-0xstuck"}, &got)
	if err != nil {
		t.Fatalf("CRD with AttachTo set should survive prune: %v", err)
	}
}

// indexByKName indexes a PhysicalDeviceList by the CurrentDevPath
// suffix (kname) so tests can write `got["sdb"]` rather than
// hand-walking the slice. Returns a fresh pointer per entry; tests
// must not mutate.
func indexByKName(items []blockstoriov1alpha1.PhysicalDevice) map[string]*blockstoriov1alpha1.PhysicalDevice {
	out := map[string]*blockstoriov1alpha1.PhysicalDevice{}

	for i := range items {
		path := items[i].Status.CurrentDevPath
		// CurrentDevPath is "/dev/<kname>".
		const prefix = "/dev/"

		if len(path) > len(prefix) && path[:len(prefix)] == prefix {
			out[path[len(prefix):]] = &items[i]
		}
	}

	return out
}

// findCondition locates a Status Condition by Type. Returns nil
// when absent — caller asserts on the absence.
func findCondition(conds []metav1.Condition, condType string) *metav1.Condition {
	for i := range conds {
		if conds[i].Type == condType {
			return &conds[i]
		}
	}

	return nil
}
