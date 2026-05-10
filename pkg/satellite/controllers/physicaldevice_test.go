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

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/satellite/controllers"
	"github.com/cozystack/blockstor/pkg/storage"
)

// TestPhysicalDeviceReconcileStampsFinalizer pins the first-
// observation path of the attach reconciler: a PhysicalDevice
// with a fresh `Spec.AttachTo` gets the
// `PhysicalDeviceAttachFinalizer` stamped + Requeue.
func TestPhysicalDeviceReconcileStampsFinalizer(t *testing.T) {
	t.Parallel()

	scheme := newStoragePoolScheme(t)

	dev := &blockstoriov1alpha1.PhysicalDevice{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "n1.wwn-0xDEADBEEF",
			Labels: map[string]string{blockstoriov1alpha1.PhysicalDeviceLabelNode: "n1"},
		},
		Spec: blockstoriov1alpha1.PhysicalDeviceSpec{
			AttachTo: &blockstoriov1alpha1.AttachToPool{
				StoragePoolName: "lvm-thin",
				ProviderKind:    "LVM_THIN",
				VGName:          "vg",
				ThinPoolName:    "tp",
			},
		},
		Status: blockstoriov1alpha1.PhysicalDeviceStatus{
			DevicePath: "/dev/disk/by-id/wwn-0xDEADBEEF",
			Phase:      blockstoriov1alpha1.PhysicalDevicePhaseAvailable,
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(dev).
		WithStatusSubresource(&blockstoriov1alpha1.PhysicalDevice{}).
		Build()

	reconciler := &controllers.PhysicalDeviceReconciler{
		Client: cli,
		Config: controllers.Config{
			NodeName: "n1",
			Apply:    newSatelliteReconcilerForTests(),
			Exec:     storage.NewFakeExec(),
		},
	}

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "n1.wwn-0xDEADBEEF"},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if !result.Requeue {
		t.Errorf("expected Requeue=true after stamping finalizer, got %+v", result)
	}

	var got blockstoriov1alpha1.PhysicalDevice
	if err := cli.Get(context.Background(), client.ObjectKey{Name: "n1.wwn-0xDEADBEEF"}, &got); err != nil {
		t.Fatalf("Get: %v", err)
	}

	if !slices.Contains(got.Finalizers, controllers.PhysicalDeviceAttachFinalizer) {
		t.Errorf("PhysicalDeviceAttachFinalizer missing: %v", got.Finalizers)
	}
}

// TestPhysicalDeviceReconcileDeviceMissing pins the Step-1
// pre-flight: a non-FILE attach with no DevicePath/CurrentDevPath
// stamps `Phase=Failed` + a `DeviceMissing` Condition rather
// than blindly issuing pvcreate against an empty device path.
func TestPhysicalDeviceReconcileDeviceMissing(t *testing.T) {
	t.Parallel()

	scheme := newStoragePoolScheme(t)

	dev := &blockstoriov1alpha1.PhysicalDevice{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "n1.gone",
			Labels:     map[string]string{blockstoriov1alpha1.PhysicalDeviceLabelNode: "n1"},
			Finalizers: []string{controllers.PhysicalDeviceAttachFinalizer},
		},
		Spec: blockstoriov1alpha1.PhysicalDeviceSpec{
			AttachTo: &blockstoriov1alpha1.AttachToPool{
				StoragePoolName: "lvm-thin",
				ProviderKind:    "LVM_THIN",
			},
		},
		// DevicePath + CurrentDevPath intentionally empty.
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(dev).
		WithStatusSubresource(&blockstoriov1alpha1.PhysicalDevice{}).
		Build()

	reconciler := &controllers.PhysicalDeviceReconciler{
		Client: cli,
		Config: controllers.Config{
			NodeName: "n1",
			Apply:    newSatelliteReconcilerForTests(),
			Exec:     storage.NewFakeExec(),
		},
	}

	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "n1.gone"},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	var got blockstoriov1alpha1.PhysicalDevice
	if err := cli.Get(context.Background(), client.ObjectKey{Name: "n1.gone"}, &got); err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.Status.Phase != blockstoriov1alpha1.PhysicalDevicePhaseFailed {
		t.Errorf("Phase: got %q, want Failed", got.Status.Phase)
	}

	if meta.FindStatusCondition(got.Status.Conditions, "DeviceMissing") == nil {
		t.Errorf("DeviceMissing condition missing: %+v", got.Status.Conditions)
	}
}

// TestPhysicalDeviceReconcilePoolMissingRequeues pins the
// Step-4 race-matrix path: an attach request that lands before
// the target StoragePool reconciles must RequeueAfter (not
// Fail) so the CDP-creates-pool-and-device GitOps race
// resolves naturally.
func TestPhysicalDeviceReconcilePoolMissingRequeues(t *testing.T) {
	t.Parallel()

	scheme := newStoragePoolScheme(t)

	dev := &blockstoriov1alpha1.PhysicalDevice{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "n1.wwn-0xDEADBEEF",
			Labels:     map[string]string{blockstoriov1alpha1.PhysicalDeviceLabelNode: "n1"},
			Finalizers: []string{controllers.PhysicalDeviceAttachFinalizer},
		},
		Spec: blockstoriov1alpha1.PhysicalDeviceSpec{
			AttachTo: &blockstoriov1alpha1.AttachToPool{
				StoragePoolName: "lvm-thin",
				ProviderKind:    "LVM_THIN",
			},
		},
		Status: blockstoriov1alpha1.PhysicalDeviceStatus{
			DevicePath: "/dev/disk/by-id/wwn-0xDEADBEEF",
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(dev).
		WithStatusSubresource(&blockstoriov1alpha1.PhysicalDevice{}).
		Build()

	reconciler := &controllers.PhysicalDeviceReconciler{
		Client: cli,
		Config: controllers.Config{
			NodeName: "n1",
			Apply:    newSatelliteReconcilerForTests(),
			Exec:     storage.NewFakeExec(),
		},
	}

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "n1.wwn-0xDEADBEEF"},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if result.RequeueAfter == 0 {
		t.Errorf("expected RequeueAfter on missing pool, got %+v", result)
	}

	var got blockstoriov1alpha1.PhysicalDevice
	if err := cli.Get(context.Background(), client.ObjectKey{Name: "n1.wwn-0xDEADBEEF"}, &got); err != nil {
		t.Fatalf("Get: %v", err)
	}

	if meta.FindStatusCondition(got.Status.Conditions, "PoolMissing") == nil {
		t.Errorf("PoolMissing condition missing: %+v", got.Status.Conditions)
	}

	if got.Status.Phase == blockstoriov1alpha1.PhysicalDevicePhaseFailed {
		t.Errorf("Phase prematurely flipped to Failed; want still-in-progress")
	}
}

// TestPhysicalDeviceReconcileStripsFinalizerOnDelete pins the
// `kubectl delete physicaldevice X mid-attach` honour-the-
// DeletionTimestamp path: the reconciler MUST strip our
// finalizer so the apiserver finalises. The provider commands
// `Attach` ran are idempotent + safe to leave on disk; pool
// teardown is Phase 10.8's StoragePool concern.
func TestPhysicalDeviceReconcileStripsFinalizerOnDelete(t *testing.T) {
	t.Parallel()

	scheme := newStoragePoolScheme(t)
	now := metav1.Now()

	dev := &blockstoriov1alpha1.PhysicalDevice{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "n1.wwn-0xDEADBEEF",
			Labels:            map[string]string{blockstoriov1alpha1.PhysicalDeviceLabelNode: "n1"},
			Finalizers:        []string{controllers.PhysicalDeviceAttachFinalizer},
			DeletionTimestamp: &now,
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
		WithObjects(dev).
		WithStatusSubresource(&blockstoriov1alpha1.PhysicalDevice{}).
		Build()

	reconciler := &controllers.PhysicalDeviceReconciler{
		Client: cli,
		Config: controllers.Config{
			NodeName: "n1",
			Apply:    newSatelliteReconcilerForTests(),
			Exec:     storage.NewFakeExec(),
		},
	}

	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "n1.wwn-0xDEADBEEF"},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	var got blockstoriov1alpha1.PhysicalDevice

	err = cli.Get(context.Background(), client.ObjectKey{Name: "n1.wwn-0xDEADBEEF"}, &got)
	if err == nil && slices.Contains(got.Finalizers, controllers.PhysicalDeviceAttachFinalizer) {
		t.Errorf("finalizer still present after delete-on-mid-attach: %v", got.Finalizers)
	}
}
