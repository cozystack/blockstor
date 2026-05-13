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
	"slices"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/satellite"
	"github.com/cozystack/blockstor/pkg/satellite/controllers"
	"github.com/cozystack/blockstor/pkg/storage"
)

// newStoragePoolScheme + newSatelliteReconciler are sugar for
// the StoragePool tests: a runtime.Scheme that knows the
// project's CRDs plus a satellite.Reconciler wired with a
// FakeExec so RegisterProvider's map is real but every shell-
// out is a unit-test-controlled stub.
func newStoragePoolScheme(t *testing.T) *runtime.Scheme {
	t.Helper()

	s := runtime.NewScheme()
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("corev1: %v", err)
	}

	if err := blockstoriov1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("blockstor: %v", err)
	}

	return s
}

func newSatelliteReconcilerForTests() *satellite.Reconciler {
	return satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{},
	})
}

// TestStoragePoolReconcileStampsFinalizer pins the first half
// of the Phase 10.8 lifecycle: on first observation of a
// StoragePool scoped to this node, the reconciler stamps
// `StoragePoolFinalizer` and requeues so the next pass runs
// `NewProviderFromKind` against an already-finalized CRD.
func TestStoragePoolReconcileStampsFinalizer(t *testing.T) {
	t.Parallel()

	scheme := newStoragePoolScheme(t)

	pool := &blockstoriov1alpha1.StoragePool{
		ObjectMeta: metav1.ObjectMeta{Name: "lvm-thin-n1"},
		Spec: blockstoriov1alpha1.StoragePoolSpec{
			NodeName:     "n1",
			PoolName:     "lvm-thin",
			ProviderKind: "LVM_THIN",
			Props: map[string]string{
				"StorDriver/LvmVg":    "vg",
				"StorDriver/ThinPool": "tp",
			},
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool).
		Build()

	rec := newSatelliteReconcilerForTests()

	reconciler := &controllers.StoragePoolReconciler{
		Client: cli,
		Config: controllers.Config{
			NodeName: "n1",
			Apply:    rec,
			Exec:     storage.NewFakeExec(),
		},
	}

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "lvm-thin-n1"},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if !requeueRequested(result) {
		t.Errorf("expected requeue after stamping finalizer, got %+v", result)
	}

	var got blockstoriov1alpha1.StoragePool
	if err := cli.Get(context.Background(), client.ObjectKey{Name: "lvm-thin-n1"}, &got); err != nil {
		t.Fatalf("Get: %v", err)
	}

	if !slices.Contains(got.Finalizers, controllers.StoragePoolFinalizer) {
		t.Errorf("StoragePoolFinalizer missing after Reconcile: %v", got.Finalizers)
	}
}

// TestStoragePoolReconcilePoolMissingLifecycle pins the Bug 50
// semantic: when the satellite's PoolStatus probe fails (operator
// did `zpool destroy` / `vgremove` out-of-band) the reconciler
// MUST stamp `Status.PoolMissing=true` + zero the capacity, and
// the next successful probe MUST clear PoolMissing back to false.
//
// Three reconcile passes, each driving one transition:
//
//  1. healthy probe → PoolMissing stays false, capacity stamped.
//  2. PoolStatus fails (`lvs` returns empty stdout, lvm-thin treats
//     it as "thin pool not found") → PoolMissing=true, capacity=0.
//  3. healthy probe again → PoolMissing flips back to false, fresh
//     capacity stamped.
//
// Implementation note: the test reuses the LVM-thin `lvs` command
// shape from `storagepool_replacement_test.go::drvReplacementLvsCmd`
// so a single FakeExec stub drives both the healthy and the missing
// passes.
func TestStoragePoolReconcilePoolMissingLifecycle(t *testing.T) {
	t.Parallel()

	const initialFreeKib int64 = 50 * 1024 * 1024 // 50 GiB

	scheme := newStoragePoolScheme(t)

	pool := &blockstoriov1alpha1.StoragePool{
		ObjectMeta: metav1.ObjectMeta{
			Name:       drvReplacementVG + "-n1",
			Finalizers: []string{controllers.StoragePoolFinalizer},
		},
		Spec: blockstoriov1alpha1.StoragePoolSpec{
			NodeName:     "n1",
			PoolName:     drvReplacementVG,
			ProviderKind: "LVM_THIN",
			Props: map[string]string{
				"StorDriver/LvmVg":    drvReplacementVG,
				"StorDriver/ThinPool": drvReplacementThin,
			},
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool).
		WithStatusSubresource(&blockstoriov1alpha1.StoragePool{}).
		Build()

	// --- Pass 1: healthy probe — PoolMissing stays false. ---

	fx := storage.NewFakeExec()
	expectLvsOutput(fx, initialFreeKib)
	_ = reconcileDrvReplacement(t, cli, fx)

	var afterHealthy blockstoriov1alpha1.StoragePool

	err := cli.Get(context.Background(), client.ObjectKey{Name: drvReplacementVG + "-n1"}, &afterHealthy)
	if err != nil {
		t.Fatalf("Get after healthy reconcile: %v", err)
	}

	if afterHealthy.Status.PoolMissing {
		t.Errorf("Pass 1: PoolMissing=true on healthy probe, want false")
	}

	if afterHealthy.Status.FreeCapacity != initialFreeKib {
		t.Errorf("Pass 1: Status.FreeCapacity = %d, want %d (healthy probe)",
			afterHealthy.Status.FreeCapacity, initialFreeKib)
	}

	// --- Pass 2: PoolStatus fails — empty `lvs` stdout makes
	// lvm.Thin.PoolStatus surface a "thin pool not found" error. ---

	fx = storage.NewFakeExec() // empty Responses => empty stdout
	_ = reconcileDrvReplacement(t, cli, fx)

	var afterMissing blockstoriov1alpha1.StoragePool

	err = cli.Get(context.Background(), client.ObjectKey{Name: drvReplacementVG + "-n1"}, &afterMissing)
	if err != nil {
		t.Fatalf("Get after pool-missing reconcile: %v", err)
	}

	if !afterMissing.Status.PoolMissing {
		t.Errorf("Pass 2: PoolMissing=false after PoolStatus error, want true")
	}

	if afterMissing.Status.FreeCapacity != 0 {
		t.Errorf("Pass 2: Status.FreeCapacity = %d, want 0 (pool missing)",
			afterMissing.Status.FreeCapacity)
	}

	if afterMissing.Status.TotalCapacity != 0 {
		t.Errorf("Pass 2: Status.TotalCapacity = %d, want 0 (pool missing)",
			afterMissing.Status.TotalCapacity)
	}

	// --- Pass 3: healthy probe again — PoolMissing clears. ---

	fx = storage.NewFakeExec()
	expectLvsOutput(fx, initialFreeKib)
	_ = reconcileDrvReplacement(t, cli, fx)

	var afterRecovery blockstoriov1alpha1.StoragePool

	err = cli.Get(context.Background(), client.ObjectKey{Name: drvReplacementVG + "-n1"}, &afterRecovery)
	if err != nil {
		t.Fatalf("Get after recovery reconcile: %v", err)
	}

	if afterRecovery.Status.PoolMissing {
		t.Errorf("Pass 3: PoolMissing=true after recovery, want false")
	}

	if afterRecovery.Status.FreeCapacity != initialFreeKib {
		t.Errorf("Pass 3: Status.FreeCapacity = %d, want %d (recovered)",
			afterRecovery.Status.FreeCapacity, initialFreeKib)
	}
}

// TestStoragePoolReconcileDeleteIsDeregisterOnly pins the
// safety contract: deleting a StoragePool CRD MUST NOT touch
// the backend (no `vgremove`, no `zpool destroy`). The
// satellite-side delete handler only deregisters the
// in-memory provider + strips the finalizer; on-disk pool
// lifecycle is the operator's concern via
// `linstor physical-storage create-device-pool` (creation
// path) and out-of-band cleanup tooling (teardown path).
func TestStoragePoolReconcileDeleteIsDeregisterOnly(t *testing.T) {
	t.Parallel()

	scheme := newStoragePoolScheme(t)
	now := metav1.Now()

	pool := &blockstoriov1alpha1.StoragePool{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "lvm-thin-n1",
			Finalizers:        []string{controllers.StoragePoolFinalizer},
			DeletionTimestamp: &now,
		},
		Spec: blockstoriov1alpha1.StoragePoolSpec{
			NodeName:     "n1",
			PoolName:     "lvm-thin",
			ProviderKind: "LVM_THIN",
			Props: map[string]string{
				"StorDriver/LvmVg":    "vg",
				"StorDriver/ThinPool": "tp",
			},
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool).
		Build()

	rec := newSatelliteReconcilerForTests()
	fx := storage.NewFakeExec()

	reconciler := &controllers.StoragePoolReconciler{
		Client: cli,
		Config: controllers.Config{
			NodeName: "n1",
			Apply:    rec,
			Exec:     fx,
		},
	}

	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "lvm-thin-n1"},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	// Backend MUST be untouched — no vgremove / zpool destroy /
	// rm calls of any flavour.
	for _, call := range fx.Calls {
		switch call.Name {
		case "vgremove", "zpool", "rm":
			t.Errorf("destructive backend command issued on StoragePool delete: %s %v", call.Name, call.Args)
		}
	}

	// Finalizer stripped — apiserver can finalise.
	var got blockstoriov1alpha1.StoragePool

	err = cli.Get(context.Background(), client.ObjectKey{Name: "lvm-thin-n1"}, &got)
	if err == nil && slices.Contains(got.Finalizers, controllers.StoragePoolFinalizer) {
		t.Errorf("finalizer still present after deregister: %v", got.Finalizers)
	}
}
