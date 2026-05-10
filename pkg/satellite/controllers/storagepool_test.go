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

	if !result.Requeue {
		t.Errorf("expected Requeue=true after stamping finalizer, got %+v", result)
	}

	var got blockstoriov1alpha1.StoragePool
	if err := cli.Get(context.Background(), client.ObjectKey{Name: "lvm-thin-n1"}, &got); err != nil {
		t.Fatalf("Get: %v", err)
	}

	if !slices.Contains(got.Finalizers, controllers.StoragePoolFinalizer) {
		t.Errorf("StoragePoolFinalizer missing after Reconcile: %v", got.Finalizers)
	}
}

// TestStoragePoolReconcileRunsDestroyWhenConsentGiven pins the
// Phase 10.8 destructive teardown path: a StoragePool with
// `Spec.DestroyOnDelete=true` + a DeletionTimestamp + our
// finalizer triggers `provider.Destroy` (verified through the
// FakeExec call log), deregisters the in-memory provider, and
// strips the finalizer so the apiserver finalises.
func TestStoragePoolReconcileRunsDestroyWhenConsentGiven(t *testing.T) {
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
			NodeName:        "n1",
			PoolName:        "lvm-thin",
			ProviderKind:    "LVM_THIN",
			DestroyOnDelete: true,
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

	// Satellite.NewProviderFromKind for LVM_THIN constructs a
	// real provider; we don't need to register it on `rec`
	// because handlePoolDelete builds a fresh one + calls
	// Destroy. The FakeExec response for the `vgs` idempotency
	// probe must return a non-empty stdout so Destroy proceeds
	// to vgremove.
	fx.Expect("vgs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o vg_name vg",
		storage.FakeResponse{Stdout: []byte("vg\n")})
	fx.Expect("vgremove --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --force vg",
		storage.FakeResponse{Stdout: []byte("")})

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

	// FakeExec saw both the probe and vgremove.
	if len(fx.Calls) != 2 {
		t.Errorf("expected 2 exec calls (vgs probe + vgremove), got %d: %+v", len(fx.Calls), fx.Calls)
	}

	// Finalizer stripped — apiserver can now finalise.
	var got blockstoriov1alpha1.StoragePool

	err = cli.Get(context.Background(), client.ObjectKey{Name: "lvm-thin-n1"}, &got)
	if err == nil && slices.Contains(got.Finalizers, controllers.StoragePoolFinalizer) {
		t.Errorf("finalizer still present after destructive delete: %v", got.Finalizers)
	}
}

// TestStoragePoolReconcileSkipsDestroyByDefault pins the safe
// default: `Spec.DestroyOnDelete=false` (the default) MUST NOT
// run vgremove on delete. The CRD's finalizer is stripped and
// the in-memory provider is deregistered, but the on-disk VG
// stays intact so operators can manually re-import the data.
func TestStoragePoolReconcileSkipsDestroyByDefault(t *testing.T) {
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
			// DestroyOnDelete defaults to false.
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

	for _, call := range fx.Calls {
		if call.Name == "vgremove" || call.Name == "zpool" {
			t.Errorf("destructive command issued under DestroyOnDelete=false: %s %v", call.Name, call.Args)
		}
	}
}
