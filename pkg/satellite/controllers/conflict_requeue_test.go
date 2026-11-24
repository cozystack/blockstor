// SPDX-License-Identifier: Apache-2.0

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
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/satellite"
	"github.com/cozystack/blockstor/pkg/satellite/controllers"
	"github.com/cozystack/blockstor/pkg/storage"
)

var (
	errUpdateBroken   = errors.New("apiserver refused the write")
	errObjectModified = errors.New("the object has been modified; please apply your changes to the latest version and try again")
)

// failUpdates makes every Update return err, the way a finalizer write
// fails when another writer bumped the object since our read.
func failUpdates(err error) interceptor.Funcs {
	return interceptor.Funcs{
		Update: func(context.Context, client.WithWatch, client.Object, ...client.UpdateOption) error {
			return err
		},
	}
}

func objectModified(resource, name string) error {
	return apierrors.NewConflict(schema.GroupResource{Group: "blockstor.org", Resource: resource}, name, errObjectModified)
}

func reconcileWithFailingFinalizerWrite(t *testing.T, kind string, err error) (ctrl.Result, error) {
	t.Helper()

	res := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-c.n1"},
		Spec:       blockstoriov1alpha1.ResourceSpec{ResourceDefinitionName: "pvc-c", NodeName: "n1"},
	}
	snap := &blockstoriov1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-c.snap-c"},
		Spec: blockstoriov1alpha1.SnapshotSpec{
			ResourceDefinitionName: "pvc-c",
			SnapshotName:           "snap-c",
			Nodes:                  []string{"n1"},
		},
	}
	pool := &blockstoriov1alpha1.StoragePool{
		ObjectMeta: metav1.ObjectMeta{Name: "thin.n1"},
		Spec:       blockstoriov1alpha1.StoragePoolSpec{NodeName: "n1", PoolName: "thin", ProviderKind: "LVM_THIN"},
	}
	dev := &blockstoriov1alpha1.PhysicalDevice{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "n1.wwn-0xc",
			Labels: map[string]string{blockstoriov1alpha1.PhysicalDeviceLabelNode: "n1"},
		},
		Spec: blockstoriov1alpha1.PhysicalDeviceSpec{
			AttachTo: &blockstoriov1alpha1.AttachToPool{StoragePoolName: "thin", ProviderKind: "LVM_THIN", VGName: "vg", ThinPoolName: "tp"},
		},
	}

	cli := interceptor.NewClient(
		fake.NewClientBuilder().WithScheme(newSnapshotScheme(t)).WithObjects(res, snap, pool, dev).Build(),
		failUpdates(err),
	)
	cfg := controllers.Config{NodeName: "n1", Exec: storage.NewFakeExec()}

	var reconciler interface {
		Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error)
	}

	switch kind {
	case "resource":
		reconciler = &controllers.ResourceReconciler{Client: cli, Config: cfg}
	case "storagepool":
		reconciler = &controllers.StoragePoolReconciler{Client: cli, Config: cfg}
	case "physicaldevice":
		reconciler = &controllers.PhysicalDeviceReconciler{Client: cli, Config: cfg}
	default:
		reconciler = &controllers.SnapshotReconciler{Client: cli, Config: cfg}
	}

	name := map[string]string{
		"resource":       "pvc-c.n1",
		"snapshot":       "pvc-c.snap-c",
		"storagepool":    "thin.n1",
		"physicaldevice": "n1.wwn-0xc",
	}[kind]

	return reconciler.Reconcile(context.Background(), ctrl.Request{ //nolint:wrapcheck // the result under test
		NamespacedName: types.NamespacedName{Name: name},
	})
}

// A finalizer write that lost an optimistic-concurrency race is retried,
// not reported: reported, controller-runtime logs it as "Reconciler
// error" at ERROR on every volume create and delete.
func TestSatelliteRetriesAFinalizerWriteThatLostARace(t *testing.T) {
	t.Parallel()

	for kind, resource := range map[string]string{
		"resource":       "resources",
		"snapshot":       "snapshots",
		"storagepool":    "storagepools",
		"physicaldevice": "physicaldevices",
	} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()

			result, err := reconcileWithFailingFinalizerWrite(t, kind, objectModified(resource, "pvc-c"))
			if err != nil {
				t.Fatalf("reconcile returned %v, want the conflict retried without an error", err)
			}

			if !result.Requeue { //nolint:staticcheck // the rate-limited retry an error would get
				t.Errorf("result %+v, want a requeue", result)
			}
		})
	}
}

// Any other failed write is still a failure.
func TestSatelliteReportsAFinalizerWriteThatFailed(t *testing.T) {
	t.Parallel()

	for _, kind := range []string{"resource", "snapshot", "storagepool", "physicaldevice"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()

			_, err := reconcileWithFailingFinalizerWrite(t, kind, errUpdateBroken)
			if !errors.Is(err, errUpdateBroken) {
				t.Fatalf("reconcile returned %v, want the write error", err)
			}
		})
	}
}

// A snapshot status stamp retries its write a bounded number of times.
// Every satellite of the snapshot stamps its own entry in the same
// status at about the same moment, so on a busy take the budget can
// run out; that is still a lost race, retried like any other, and not
// a "Reconciler error" at ERROR.
func TestSatelliteRetriesASnapshotStatusStampThatRanOutOfRetries(t *testing.T) {
	t.Parallel()

	snap := &blockstoriov1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "pvc-c.snap-c",
			Finalizers: []string{controllers.SatelliteSnapshotFinalizer},
		},
		Spec: blockstoriov1alpha1.SnapshotSpec{
			ResourceDefinitionName: "pvc-c",
			SnapshotName:           "snap-c",
			Nodes:                  []string{"n1"},
			SuspendIO:              true,
		},
	}

	cli := interceptor.NewClient(
		fake.NewClientBuilder().
			WithScheme(newSnapshotScheme(t)).
			WithStatusSubresource(&blockstoriov1alpha1.Snapshot{}).
			WithObjects(snap).
			Build(),
		interceptor.Funcs{
			SubResourceUpdate: func(context.Context, client.Client, string, client.Object, ...client.SubResourceUpdateOption) error {
				return objectModified("snapshots", "pvc-c.snap-c")
			},
		},
	)

	fx := storage.NewFakeExec()
	fx.Expect("drbdadm suspend-io pvc-c", storage.FakeResponse{})

	reconciler := &controllers.SnapshotReconciler{
		Client: cli,
		Config: controllers.Config{
			NodeName: "n1",
			Apply: satellite.NewReconciler(satellite.ReconcilerConfig{
				Providers: map[string]storage.Provider{},
				Adm:       drbd.NewAdm(fx),
			}),
			Exec: fx,
		},
	}

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pvc-c.snap-c"},
	})
	if err != nil {
		t.Fatalf("reconcile returned %v, want the exhausted stamp retried without an error", err)
	}

	if !result.Requeue { //nolint:staticcheck // the rate-limited retry an error would get
		t.Errorf("result %+v, want a requeue", result)
	}
}

// errorCountSink counts the ERROR lines a reconcile writes.
type errorCountSink struct{ count *int }

func (errorCountSink) Init(logr.RuntimeInfo)            {}
func (errorCountSink) Enabled(int) bool                 { return true }
func (errorCountSink) Info(int, string, ...any)         {}
func (s errorCountSink) Error(error, string, ...any)    { *s.count++ }
func (s errorCountSink) WithValues(...any) logr.LogSink { return s }
func (s errorCountSink) WithName(string) logr.LogSink   { return s }

// A snapshot delete that failed on this node is recorded in the status
// before the pass retries it, and that stamp races the other nodes'
// stamps of the same status. Losing that race is not an error anyone
// can act on, so it stays out of the ERROR log like any other conflict.
func TestSatelliteLogsALostDeleteFailureStampBelowError(t *testing.T) {
	t.Parallel()

	now := metav1.Now()
	snap := &blockstoriov1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "pvc-c.snap-c",
			Finalizers:        []string{controllers.SatelliteSnapshotFinalizer},
			DeletionTimestamp: &now,
		},
		Spec: blockstoriov1alpha1.SnapshotSpec{
			ResourceDefinitionName: "pvc-c",
			SnapshotName:           "snap-c",
			Nodes:                  []string{"n1"},
		},
	}

	cli := interceptor.NewClient(
		fake.NewClientBuilder().
			WithScheme(newSnapshotScheme(t)).
			WithStatusSubresource(&blockstoriov1alpha1.Snapshot{}).
			WithObjects(snap).
			Build(),
		interceptor.Funcs{
			SubResourceUpdate: func(context.Context, client.Client, string, client.Object, ...client.SubResourceUpdateOption) error {
				return objectModified("snapshots", "pvc-c.snap-c")
			},
		},
	)

	fx := storage.NewFakeExec()
	reconciler := &controllers.SnapshotReconciler{
		Client: cli,
		Config: controllers.Config{
			NodeName: "n1",
			Apply: satellite.NewReconciler(satellite.ReconcilerConfig{
				Providers: map[string]storage.Provider{},
				Adm:       drbd.NewAdm(fx),
			}),
			Exec: fx,
		},
	}

	var errorLines int

	ctx := logf.IntoContext(context.Background(), logr.New(errorCountSink{count: &errorLines}))

	result, err := reconciler.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pvc-c.snap-c"},
	})
	if err != nil {
		t.Fatalf("reconcile returned %v, want the failed delete retried", err)
	}

	if !result.Requeue { //nolint:staticcheck // the retry of the failed delete
		t.Errorf("result %+v, want a requeue", result)
	}

	if errorLines != 0 {
		t.Errorf("a lost delete-failure stamp wrote %d ERROR lines, want none", errorLines)
	}
}
