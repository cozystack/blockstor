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
const lsblkCmdLine = "lsblk -Pb -o NAME,KNAME,PKNAME,MAJ:MIN,SIZE,FSTYPE,TYPE,MOUNTPOINT,WWN,MODEL,SERIAL,ROTA,TRAN"

// pvsCmdLine matches the LVM signature probe key. The flag-string
// is built by `lvm.Args(...)` — pinning the literal here keeps the
// test independent of accidental refactors that would otherwise
// only fail at runtime.
const pvsCmdLine = "pvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o pv_name"

// lsblkRow returns the canonical `KEY="value" KEY="value" ...` line
// the lsblk parser expects, for one fake device. Keeps the test
// readable — three rows of inline `KEY="..."` would dominate the
// happy-path test. PKNAME is empty for top-level disks; partitions
// emit it as their parent disk's kname (Bug 89 parent-child
// detection).
func lsblkRow(name, kname, size, fstype, typ, mount, wwn, model, serial, rota, tran string) string {
	return lsblkRowWithParent(name, kname, "", size, fstype, typ, mount, wwn, model, serial, rota, tran)
}

// lsblkRowWithParent is the Bug 89 variant that takes an explicit
// PKNAME. Use this for partition rows so the discovery loop's
// parent-child busy-disk detection has the data it needs.
func lsblkRowWithParent(name, kname, pkname, size, fstype, typ, mount, wwn, model, serial, rota, tran string) string {
	return `NAME="` + name + `" KNAME="` + kname + `" PKNAME="` + pkname + `" SIZE="` + size + `" FSTYPE="` + fstype + `" TYPE="` + typ + `" MOUNTPOINT="` + mount + `" WWN="` + wwn + `" MODEL="` + model + `" SERIAL="` + serial + `" ROTA="` + rota + `" TRAN="` + tran + `"`
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

	if got["sda"].Status.DevicePath != "/dev/sda" {
		t.Errorf("sda DevicePath: got %q, want /dev/sda (kernel name; Bug 69)",
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

// TestPublishDeviceUsesKernelPath pins Bug 69. Operator's
// `linstor ps l` is expected to render kernel-name paths
// (`/dev/vda`, `/dev/sdb`, `/dev/nvme0n1`) so `linstor ps cdp`
// and downstream tooling can be fed those values verbatim.
// Current publishDevice hardcodes `/dev/disk/by-id/<stableID>`,
// which for virtio-no-serial devices renders as
// `/dev/disk/by-id/by-path-vda` — useless to operators and a
// frequent dev-stand surprise.
//
// Pinned contract: Status.DevicePath = "/dev/<kname>" for every
// published PhysicalDevice, regardless of stableID source (WWN /
// model+serial / by-path fallback). The stableID is still recorded
// in Status.StableID for CRD-name determinism and CRD-reuse on
// device renumbering, but the user-facing path is the kernel name.
func TestPublishDeviceUsesKernelPath(t *testing.T) {
	t.Parallel()

	scheme := newStoragePoolScheme(t)

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.PhysicalDevice{}).
		Build()

	fx := storage.NewFakeExec()
	// Three rows exercise the three stableID paths:
	//   - sdb with WWN → stableID is "wwn-0x…" → was rendering
	//     `/dev/disk/by-id/wwn-0x…`; should be `/dev/sdb`.
	//   - nvme0n1 with model+serial → was rendering
	//     `/dev/disk/by-id/<model>-<serial>`; should be `/dev/nvme0n1`.
	//   - vda (virtio, no WWN/serial) → stableID is the
	//     "by-path-vda" fallback → was rendering
	//     `/dev/disk/by-id/by-path-vda`; should be `/dev/vda`.
	fx.Expect(lsblkCmdLine, storage.FakeResponse{
		Stdout: []byte(
			lsblkRow("sdb", "sdb", "2000398934016", "", "disk", "", "0x5000c500a3b1c2d2", "DISK_B", "SN-B", "0", "sata") + "\n" +
				lsblkRow("nvme0n1", "nvme0n1", "1000204886016", "", "disk", "", "", "Samsung_SSD_980", "S5GXNF0NB12345", "0", "nvme") + "\n" +
				lsblkRow("vda", "vda", "10737418240", "", "disk", "", "", "", "", "1", "virtio") + "\n"),
	})

	// All three rows pass IsFreeBlockDevice (no FSType, TYPE=disk,
	// no mountpoint). Each gets the standard signature-probe
	// fan-out, all of which say "no signature".
	for _, dev := range []string{"sdb", "nvme0n1", "vda"} {
		fx.Expect("drbdmeta 0 v09 /dev/"+dev+" internal dump-md", storage.FakeResponse{Err: errNoDRBDMeta})
		fx.Expect("wipefs -n /dev/"+dev, storage.FakeResponse{Stdout: []byte("")})
	}

	fx.Expect(pvsCmdLine, storage.FakeResponse{Stdout: []byte("")})
	fx.Expect("zpool list -PHv", storage.FakeResponse{Stdout: []byte("")})

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
	if err := cli.List(t.Context(), &list,
		client.MatchingLabels{blockstoriov1alpha1.PhysicalDeviceLabelNode: "n1"}); err != nil {
		t.Fatalf("list PhysicalDevices: %v", err)
	}

	got := indexByKName(list.Items)

	for _, tc := range []struct {
		kname    string
		wantPath string
	}{
		{"sdb", "/dev/sdb"},
		{"nvme0n1", "/dev/nvme0n1"},
		{"vda", "/dev/vda"},
	} {
		dev, ok := got[tc.kname]
		if !ok {
			t.Errorf("no CRD for %s", tc.kname)

			continue
		}

		if dev.Status.DevicePath != tc.wantPath {
			t.Errorf("Bug 69: %s DevicePath: got %q, want %q (kernel name, not /dev/disk/by-id/...)",
				tc.kname, dev.Status.DevicePath, tc.wantPath)
		}
	}
}

// TestPublishDeviceFiltersZFSZvols pins Bug 70. ZFS volume devices
// (`/dev/zd0`, `/dev/zd16`, …) are kernel block devices with
// MAJ:MIN starting at 230. They show up in `lsblk` as TYPE=disk
// with no parent, and on a ZFS-host they outnumber the real disks
// — so a non-filtered `linstor ps l` is dominated by entries for
// volumes already in use by an existing zpool. Operators have no
// way to tell apart "/dev/zd0 (this is YOUR zvol)" from
// "/dev/sdb (free disk you can pool)".
//
// Pinned contract: lsblk rows whose KName matches `zd[0-9]+` MUST
// NOT produce a PhysicalDevice CRD — the discovery runnable skips
// them before signature probing. A small allow-list for the
// kernel-name prefixes operators actually want to pool
// (sd*, nvme*, vd*, hd*, xvd*, mmcblk*) is the cleanest gate.
func TestPublishDeviceFiltersZFSZvols(t *testing.T) {
	t.Parallel()

	scheme := newStoragePoolScheme(t)

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.PhysicalDevice{}).
		Build()

	fx := storage.NewFakeExec()
	// Mix of real disks + zvols. The discovery loop must publish
	// CRDs for sdb / vdb, and SKIP zd0 / zd16 entirely.
	fx.Expect(lsblkCmdLine, storage.FakeResponse{
		Stdout: []byte(
			lsblkRow("sdb", "sdb", "2000398934016", "", "disk", "", "0x5000c500a3b1c2d2", "DISK_B", "SN-B", "0", "sata") + "\n" +
				lsblkRow("vdb", "vdb", "10737418240", "", "disk", "", "", "", "", "1", "virtio") + "\n" +
				lsblkRow("zd0", "zd0", "5368709120", "", "disk", "", "", "", "", "0", "") + "\n" +
				lsblkRow("zd16", "zd16", "1073741824", "", "disk", "", "", "", "", "0", "") + "\n"),
	})

	// Signature probes are only invoked for the non-zvol rows. If
	// the filter regresses and zvols slip through, FakeExec returns
	// "unexpected command" for the missing zd0/zd16 probes — that
	// surfaces as the test failing in a clear way separate from the
	// "extra CRD created" assertion below.
	for _, dev := range []string{"sdb", "vdb"} {
		fx.Expect("drbdmeta 0 v09 /dev/"+dev+" internal dump-md", storage.FakeResponse{Err: errNoDRBDMeta})
		fx.Expect("wipefs -n /dev/"+dev, storage.FakeResponse{Stdout: []byte("")})
	}

	fx.Expect(pvsCmdLine, storage.FakeResponse{Stdout: []byte("")})
	fx.Expect("zpool list -PHv", storage.FakeResponse{Stdout: []byte("")})

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
	if err := cli.List(t.Context(), &list,
		client.MatchingLabels{blockstoriov1alpha1.PhysicalDeviceLabelNode: "n1"}); err != nil {
		t.Fatalf("list PhysicalDevices: %v", err)
	}

	got := indexByKName(list.Items)

	if got["zd0"] != nil || got["zd16"] != nil {
		t.Errorf("Bug 70: zvols leaked into PhysicalDevice list (zd0=%v zd16=%v); they must be filtered out before signature probing",
			got["zd0"] != nil, got["zd16"] != nil)
	}

	if got["sdb"] == nil || got["vdb"] == nil {
		t.Errorf("Bug 70 regression: real disks dropped along with zvols (sdb=%v vdb=%v)",
			got["sdb"] != nil, got["vdb"] != nil)
	}
}

// TestScanOnceFiltersDRBDDevices pins Bug 72. DRBD volumes
// surface as TYPE=disk with no FSType (the FS lives inside the
// replicated volume), so they pass the Type=disk + empty-FSType
// + signature-probe gates and would be published as "free
// for wipe" — operator running `linstor ps cdp` against them
// would destroy an active replica. Filter by kernel major
// number 147 in scanOnce, mirroring upstream LINSTOR's
// LsBlkUtils.filterDeviceCandidates.
func TestScanOnceFiltersDRBDDevices(t *testing.T) {
	t.Parallel()

	scheme := newStoragePoolScheme(t)

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.PhysicalDevice{}).
		Build()

	// lsblk row with explicit MAJ:MIN. sdb is a real disk (major 8);
	// drbd1000 is a DRBD volume (major 147) that must be filtered
	// out before signature probing. If the filter regresses and DRBD
	// rows slip through, FakeExec returns "unexpected command" for
	// the missing drbdmeta probe — that surfaces clearly.
	row := func(name, kname, majmin, size, typ string) string {
		return `NAME="` + name + `" KNAME="` + kname + `" MAJ:MIN="` + majmin +
			`" SIZE="` + size + `" FSTYPE="" TYPE="` + typ +
			`" MOUNTPOINT="" WWN="" MODEL="" SERIAL="" ROTA="0" TRAN=""`
	}

	fx := storage.NewFakeExec()
	fx.Expect(lsblkCmdLine, storage.FakeResponse{
		Stdout: []byte(
			row("sdb", "sdb", "8:16", "2000398934016", "disk") + "\n" +
				row("drbd1000", "drbd1000", "147:1000", "10737418240", "disk") + "\n"),
	})

	// Signature probes only fire for sdb; drbd1000 should be filtered
	// out at the Major==147 gate before they're consulted.
	fx.Expect("drbdmeta 0 v09 /dev/sdb internal dump-md", storage.FakeResponse{Err: errNoDRBDMeta})
	fx.Expect("wipefs -n /dev/sdb", storage.FakeResponse{Stdout: []byte("")})
	fx.Expect(pvsCmdLine, storage.FakeResponse{Stdout: []byte("")})
	fx.Expect("zpool list -PHv", storage.FakeResponse{Stdout: []byte("")})

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
	if err := cli.List(t.Context(), &list,
		client.MatchingLabels{blockstoriov1alpha1.PhysicalDeviceLabelNode: "n1"}); err != nil {
		t.Fatalf("list PhysicalDevices: %v", err)
	}

	got := indexByKName(list.Items)

	if got["drbd1000"] != nil {
		t.Errorf("Bug 72: DRBD device (major=147) leaked into PhysicalDevice list; must be filtered out before signature probing")
	}

	if got["sdb"] == nil {
		t.Errorf("Bug 72 regression: real disk dropped along with DRBD")
	}
}
