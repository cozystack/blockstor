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
	"context"
	"path/filepath"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/satellite/controllers"
	"github.com/cozystack/blockstor/pkg/storage"
)

// Issue 74 regression pack: a backing pool destroyed out-of-band
// (`zpool destroy`, `vgremove`, deleting the FILE_THIN dir) MUST
// flip `Status.PoolMissing=true` on the next capacity probe so the
// wire view in `linstor sp l` lands `state=Faulty` instead of
// silently staying `Ok` with zeroed capacity.
//
// On the live stand the ZFS_THIN provider's `PoolStatus` probe
// returned success-with-empty-output, the reconciler took the
// success branch, and `PoolMissing` got cleared. These tests pin
// the corrected behaviour for each backend kind: an empty probe
// response surfaces as an error, the reconciler flips
// `PoolMissing=true`, and a healthy probe in the same shape
// keeps `PoolMissing=false`.

// reconcileStoragePool drives one Reconcile pass on a StoragePool
// CRD named `<pool>-<node>` with the given FakeExec. Mirrors the
// pattern used by storagepool_replacement_test.go's reconcile
// helper but keeps the (node, pool) names parametric so the three
// backend kinds can share it.
func reconcileStoragePool(t *testing.T, cli client.Client, fx *storage.FakeExec, node, pool string) {
	t.Helper()

	reconciler := &controllers.StoragePoolReconciler{
		Client: cli,
		Config: controllers.Config{
			NodeName: node,
			Apply:    newSatelliteReconcilerForTests(),
			Exec:     fx,
		},
	}

	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: pool + "-" + node},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
}

// newPoolCRD builds a StoragePool CRD pre-stamped with the
// satellite finalizer so `Reconcile` skips the finalizer-stamp
// requeue and goes straight to the writeCapacity path.
func newPoolCRD(node, pool, kind string, props map[string]string) *blockstoriov1alpha1.StoragePool {
	return &blockstoriov1alpha1.StoragePool{
		ObjectMeta: metav1.ObjectMeta{
			Name:       pool + "-" + node,
			Finalizers: []string{controllers.StoragePoolFinalizer},
		},
		Spec: blockstoriov1alpha1.StoragePoolSpec{
			NodeName:     node,
			PoolName:     pool,
			ProviderKind: kind,
			Props:        props,
		},
	}
}

// TestStoragePoolReconciler_MissingZPool_FlipsPoolMissing is the
// primary Bug 74 pin: a ZFS_THIN provider whose backing pool was
// destroyed via `zpool destroy` MUST flip `Status.PoolMissing=true`
// on the next probe.
//
// FakeExec has no canned response for the `zpool list` command, so
// it returns empty stdout + nil error — exactly the shape the live
// stand produced. The provider MUST treat that as "pool absent"
// rather than "success with no data".
func TestStoragePoolReconciler_MissingZPool_FlipsPoolMissing(t *testing.T) {
	t.Parallel()

	const (
		node = "n1"
		pool = "zfs-thin"
	)

	scheme := newStoragePoolScheme(t)

	crd := newPoolCRD(node, pool, "ZFS_THIN", map[string]string{
		"StorDriver/ZPoolThin": "blockstor-zfs",
	})

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(crd).
		WithStatusSubresource(&blockstoriov1alpha1.StoragePool{}).
		Build()

	// Empty FakeExec: `zpool list` returns empty stdout, nil err.
	fx := storage.NewFakeExec()
	reconcileStoragePool(t, cli, fx, node, pool)

	var got blockstoriov1alpha1.StoragePool

	err := cli.Get(context.Background(), client.ObjectKey{Name: pool + "-" + node}, &got)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if !got.Status.PoolMissing {
		t.Errorf("Status.PoolMissing = false after destroyed zpool probe, want true")
	}

	if got.Status.FreeCapacity != 0 {
		t.Errorf("Status.FreeCapacity = %d after destroyed zpool probe, want 0",
			got.Status.FreeCapacity)
	}

	if got.Status.TotalCapacity != 0 {
		t.Errorf("Status.TotalCapacity = %d after destroyed zpool probe, want 0",
			got.Status.TotalCapacity)
	}
}

// TestStoragePoolReconciler_MissingLVMVG_FlipsPoolMissing is the
// LVM_THIN twin of the ZFS pin: `vgs <vg>` against a missing VG
// returns empty stdout with exit 5 in production. The provider
// MUST surface that as an error so the reconciler flips
// `PoolMissing=true`.
func TestStoragePoolReconciler_MissingLVMVG_FlipsPoolMissing(t *testing.T) {
	t.Parallel()

	const (
		node = "n1"
		pool = "lvm-thin"
		vg   = "blockstor-vg"
		tp   = "thin"
	)

	scheme := newStoragePoolScheme(t)

	crd := newPoolCRD(node, pool, "LVM_THIN", map[string]string{
		"StorDriver/LvmVg":    vg,
		"StorDriver/ThinPool": tp,
	})

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(crd).
		WithStatusSubresource(&blockstoriov1alpha1.StoragePool{}).
		Build()

	fx := storage.NewFakeExec()
	reconcileStoragePool(t, cli, fx, node, pool)

	var got blockstoriov1alpha1.StoragePool

	err := cli.Get(context.Background(), client.ObjectKey{Name: pool + "-" + node}, &got)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if !got.Status.PoolMissing {
		t.Errorf("Status.PoolMissing = false after missing VG probe, want true")
	}

	if got.Status.FreeCapacity != 0 {
		t.Errorf("Status.FreeCapacity = %d after missing VG probe, want 0",
			got.Status.FreeCapacity)
	}
}

// TestStoragePoolReconciler_MissingFileDir_FlipsPoolMissing is the
// FILE_THIN twin: a non-existent backing directory MUST surface
// from `PoolStatus` as an error so the reconciler flips
// `PoolMissing=true`. statfs(2) on ENOENT already returns an
// error, so this is the "already-correct" backend — the test is
// a regression pin to keep it that way.
func TestStoragePoolReconciler_MissingFileDir_FlipsPoolMissing(t *testing.T) {
	t.Parallel()

	const (
		node = "n1"
		pool = "file-thin"
	)

	// A path under t.TempDir that we never actually create. The
	// parent dir exists (TempDir() creates it) but the named
	// subdir does not, so statfs returns ENOENT.
	missingDir := filepath.Join(t.TempDir(), "does-not-exist")

	scheme := newStoragePoolScheme(t)

	crd := newPoolCRD(node, pool, "FILE_THIN", map[string]string{
		"StorDriver/FileDir": missingDir,
	})

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(crd).
		WithStatusSubresource(&blockstoriov1alpha1.StoragePool{}).
		Build()

	fx := storage.NewFakeExec()
	reconcileStoragePool(t, cli, fx, node, pool)

	var got blockstoriov1alpha1.StoragePool

	err := cli.Get(context.Background(), client.ObjectKey{Name: pool + "-" + node}, &got)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if !got.Status.PoolMissing {
		t.Errorf("Status.PoolMissing = false after ENOENT FILE_THIN probe, want true")
	}
}

// TestStoragePoolReconciler_HealthyPoolPreservesOk is the sanity
// twin: when the LVM_THIN probe returns well-shaped output the
// reconciler MUST keep `PoolMissing=false` and stamp the parsed
// capacity. Pinned so the empty-output-is-missing tightening
// in this commit doesn't accidentally regress the success path.
func TestStoragePoolReconciler_HealthyPoolPreservesOk(t *testing.T) {
	t.Parallel()

	const (
		node           = "n1"
		pool           = drvReplacementVG
		initialFreeKib = int64(50 * 1024 * 1024) // 50 GiB
	)

	scheme := newStoragePoolScheme(t)

	crd := newPoolCRD(node, pool, "LVM_THIN", map[string]string{
		"StorDriver/LvmVg":    drvReplacementVG,
		"StorDriver/ThinPool": drvReplacementThin,
	})

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(crd).
		WithStatusSubresource(&blockstoriov1alpha1.StoragePool{}).
		Build()

	fx := storage.NewFakeExec()
	expectLvsOutput(fx, initialFreeKib)
	reconcileStoragePool(t, cli, fx, node, pool)

	var got blockstoriov1alpha1.StoragePool

	err := cli.Get(context.Background(), client.ObjectKey{Name: pool + "-" + node}, &got)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.Status.PoolMissing {
		t.Errorf("Status.PoolMissing = true on healthy probe, want false")
	}

	if got.Status.FreeCapacity != initialFreeKib {
		t.Errorf("Status.FreeCapacity = %d on healthy probe, want %d",
			got.Status.FreeCapacity, initialFreeKib)
	}

	if got.Status.TotalCapacity != initialFreeKib {
		t.Errorf("Status.TotalCapacity = %d on healthy probe, want %d",
			got.Status.TotalCapacity, initialFreeKib)
	}
}
