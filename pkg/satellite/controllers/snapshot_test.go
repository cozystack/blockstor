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

	"github.com/cockroachdb/errors"
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
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/lvm"
)

// newSnapshotScheme is the runtime.Scheme used by Snapshot
// reconciler tests — corev1 + the blockstor v1alpha1 group.
func newSnapshotScheme(t *testing.T) *runtime.Scheme {
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

// seedThinResource registers an LVM-thin provider on the
// satellite reconciler and primes its resource→pool map by
// running one Apply pass. The follow-up SnapshotReconciler
// `handleDelete` then routes `DeleteSnapshot` to the right
// provider via the recorded mapping.
func seedThinResource(t *testing.T, fx *storage.FakeExec, resourceName, pool string) *satellite.Reconciler {
	t.Helper()

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{pool: thin},
	})

	_, err := rec.Apply(context.Background(), []*intent.DesiredResource{
		{
			Name: resourceName, NodeName: "n1",
			Volumes: []*intent.DesiredVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: pool},
			},
		},
	})
	if err != nil {
		t.Fatalf("Apply (seed): %v", err)
	}

	return rec
}

// TestSnapshotReconcileAddsFinalizer pins the Bug 64 fix: the
// first observation of a Snapshot scoped to this satellite MUST
// stamp `SatelliteSnapshotFinalizer` before `CreateSnapshot`
// runs. Without the finalizer, kube-apiserver removes the CRD
// on `kubectl delete` before the satellite sees the
// DeletionTimestamp event, and the on-disk ZFS / LVM-thin
// snapshot survives as an orphan.
func TestSnapshotReconcileAddsFinalizer(t *testing.T) {
	t.Parallel()

	scheme := newSnapshotScheme(t)

	snap := &blockstoriov1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.snap-1"},
		Spec: blockstoriov1alpha1.SnapshotSpec{
			ResourceDefinitionName: "pvc-1",
			SnapshotName:           "snap-1",
			Nodes:                  []string{"n1"},
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(snap).
		Build()

	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{},
	})

	reconciler := &controllers.SnapshotReconciler{
		Client: cli,
		Config: controllers.Config{
			NodeName: "n1",
			Apply:    rec,
			Exec:     storage.NewFakeExec(),
		},
	}

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pvc-1.snap-1"},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if result.RequeueAfter <= 0 {
		t.Errorf("expected RequeueAfter > 0 after stamping finalizer, got %+v", result)
	}

	var got blockstoriov1alpha1.Snapshot
	if err := cli.Get(context.Background(), client.ObjectKey{Name: "pvc-1.snap-1"}, &got); err != nil {
		t.Fatalf("Get: %v", err)
	}

	if !slices.Contains(got.Finalizers, controllers.SatelliteSnapshotFinalizer) {
		t.Errorf("SatelliteSnapshotFinalizer missing after Reconcile: %v", got.Finalizers)
	}
}

// TestSnapshotReconcileDrainsOnDelete pins the second half of
// the Bug 64 lifecycle: a Snapshot with a DeletionTimestamp +
// our finalizer must drive `lvremove` on the provider before
// the finalizer is stripped. A regression would either skip
// `DeleteSnapshot` (orphan LV) or strip the finalizer before
// the on-disk teardown succeeded (apiserver finalises the CRD
// while the LV lingers).
func TestSnapshotReconcileDrainsOnDelete(t *testing.T) {
	t.Parallel()

	scheme := newSnapshotScheme(t)
	now := metav1.Now()

	snap := &blockstoriov1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "pvc-1.snap-1",
			Finalizers:        []string{controllers.SatelliteSnapshotFinalizer},
			DeletionTimestamp: &now,
		},
		Spec: blockstoriov1alpha1.SnapshotSpec{
			ResourceDefinitionName: "pvc-1",
			SnapshotName:           "snap-1",
			Nodes:                  []string{"n1"},
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(snap).
		Build()

	fx := storage.NewFakeExec()
	// Apply seed: `lvs` for pvc-1_00000 returns empty so
	// CreateVolume runs (lvcreate --thin …).
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("")})

	rec := seedThinResource(t, fx, "pvc-1", "thin1")

	reconciler := &controllers.SnapshotReconciler{
		Client: cli,
		Config: controllers.Config{
			NodeName: "n1",
			Apply:    rec,
			Exec:     fx,
		},
	}

	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pvc-1.snap-1"},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	// lvremove against the snapshot LV MUST have fired.
	wantCmd := "lvremove --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --force vg/pvc-1_snap-1_00000"
	if !slices.Contains(fx.CommandLines(), wantCmd) {
		t.Errorf("DeleteSnapshot did not invoke %q on the provider; got %v",
			wantCmd, fx.CommandLines())
	}

	// Finalizer MUST be stripped so the apiserver can finalise.
	var got blockstoriov1alpha1.Snapshot
	err = cli.Get(context.Background(), client.ObjectKey{Name: "pvc-1.snap-1"}, &got)
	if err == nil && slices.Contains(got.Finalizers, controllers.SatelliteSnapshotFinalizer) {
		t.Errorf("SatelliteSnapshotFinalizer still present after successful drain: %v",
			got.Finalizers)
	}
}

// TestSnapshotReconcileMarksFailedOnTerminalError pins the F18
// cli-parity fix: when Apply.CreateSnapshot returns a terminal
// error (Terminal=true — e.g. parent volume not found, unknown
// resource, provider returned ErrTerminal), the SnapshotReconciler
// MUST stamp Status.Flags=["FAILED"] on the CRD before returning
// and MUST NOT requeue. The wire shape's crdToWireSnapshot
// surfaces this as `flags: ["FAILED"]`, which the Python CLI
// maps to State="Failed" in `linstor s l`.
//
// A regression that left Status.Flags empty would leave the CLI
// in State="Incomplete" forever, hiding the failure from the
// operator and from CSI's CreateSnapshot success-polling loop.
//
// Setup: no providers registered for the snapshot's RD ⇒
// providerForResource returns "unknown resource", which the
// reconciler classifies as Terminal=true.
func TestSnapshotReconcileMarksFailedOnTerminalError(t *testing.T) {
	t.Parallel()

	scheme := newSnapshotScheme(t)

	snap := &blockstoriov1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "pvc-1.snap-1",
			Finalizers: []string{controllers.SatelliteSnapshotFinalizer},
		},
		Spec: blockstoriov1alpha1.SnapshotSpec{
			ResourceDefinitionName: "pvc-missing",
			SnapshotName:           "snap-1",
			Nodes:                  []string{"n1"},
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Snapshot{}).
		WithObjects(snap).
		Build()

	// Empty providers map ⇒ Apply.CreateSnapshot's
	// providerForResource lookup fails ⇒ Terminal=true.
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{},
	})

	reconciler := &controllers.SnapshotReconciler{
		Client: cli,
		Config: controllers.Config{
			NodeName: "n1",
			Apply:    rec,
			Exec:     storage.NewFakeExec(),
		},
	}

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pvc-1.snap-1"},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	// Terminal failures MUST NOT requeue — they're dead-letter.
	if result.Requeue || result.RequeueAfter > 0 {
		t.Errorf("terminal failure should NOT requeue; got %+v", result)
	}

	var got blockstoriov1alpha1.Snapshot

	err = cli.Get(context.Background(), client.ObjectKey{Name: "pvc-1.snap-1"}, &got)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if !slices.Contains(got.Status.Flags, blockstoriov1alpha1.SnapshotStatusFlagFailed) {
		t.Errorf("Status.Flags missing FAILED after terminal error: %+v",
			got.Status.Flags)
	}
}

// TestSnapshotReconcileKeepsIncompleteOnTransientError pins the
// flip side: when Apply.CreateSnapshot returns a transient
// failure (Terminal=false — lvm temporary lock, exec wrapper
// noise, busy dataset), the reconciler MUST requeue without
// stamping Status.Flags. The CRD stays in the "Incomplete" wire
// state and the controller-runtime rate limiter handles
// back-off.
//
// A regression that prematurely stamped FAILED on a transient
// failure would dead-letter a snapshot that the next pass
// would have completed successfully, and force the operator
// to delete + recreate it for a recoverable hiccup.
//
// Setup: provider's lvcreate exits non-zero (the exec wrapper
// returns a plain error that does NOT wrap ErrTerminal /
// ErrNotFound) ⇒ Terminal=false.
func TestSnapshotReconcileKeepsIncompleteOnTransientError(t *testing.T) {
	t.Parallel()

	scheme := newSnapshotScheme(t)

	snap := &blockstoriov1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "pvc-1.snap-1",
			Finalizers: []string{controllers.SatelliteSnapshotFinalizer},
		},
		Spec: blockstoriov1alpha1.SnapshotSpec{
			ResourceDefinitionName: "pvc-1",
			SnapshotName:           "snap-1",
			Nodes:                  []string{"n1"},
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Snapshot{}).
		WithObjects(snap).
		Build()

	fx := storage.NewFakeExec()
	// Seed: lvs returns empty so CreateVolume runs.
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("")})
	// Snapshot path: lvcreate -s fails with a transient-looking
	// error. The error is NOT wrapped in ErrTerminal/ErrNotFound,
	// so the reconciler classifies it as transient.
	fx.Expect("lvcreate --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --snapshot --name pvc-1_snap-1_00000 vg/pvc-1_00000",
		storage.FakeResponse{Err: errLvmTemporaryLock})

	rec := seedThinResource(t, fx, "pvc-1", "thin1")

	reconciler := &controllers.SnapshotReconciler{
		Client: cli,
		Config: controllers.Config{
			NodeName: "n1",
			Apply:    rec,
			Exec:     fx,
		},
	}

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pvc-1.snap-1"},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	// Transient failures MUST requeue so the next pass retries.
	if !result.Requeue && result.RequeueAfter == 0 {
		t.Errorf("transient failure should requeue; got %+v", result)
	}

	var got blockstoriov1alpha1.Snapshot

	err = cli.Get(context.Background(), client.ObjectKey{Name: "pvc-1.snap-1"}, &got)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if slices.Contains(got.Status.Flags, blockstoriov1alpha1.SnapshotStatusFlagFailed) {
		t.Errorf("Status.Flags should not contain FAILED on transient error; got %+v",
			got.Status.Flags)
	}
}

// errLvmTemporaryLock simulates a transient lvm condition the
// kernel resolves on its own (e.g. a busy vg lock that lvcreate
// retries on its next invocation). NOT wrapped in
// storage.ErrTerminal / storage.ErrNotFound on purpose — the
// reconciler reads that absence as "transient, retry".
var errLvmTemporaryLock = errors.New("lvm: Locking type 1 initialisation failed")

// TestSnapshotReconcileNoOpOnUnrelatedNode pins the
// node-membership filter: a Snapshot whose `Spec.Nodes` does
// NOT contain our NodeName must short-circuit Reconcile with
// no provider calls, no finalizer stamping, and no Update
// against the apiserver. The predicate at SetupWithManager is
// the watch-layer filter; this defensive check covers the
// case where the watch cache is mid-resync and a stray event
// for someone else's Snapshot reaches us.
func TestSnapshotReconcileNoOpOnUnrelatedNode(t *testing.T) {
	t.Parallel()

	scheme := newSnapshotScheme(t)

	snap := &blockstoriov1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.snap-1"},
		Spec: blockstoriov1alpha1.SnapshotSpec{
			ResourceDefinitionName: "pvc-1",
			SnapshotName:           "snap-1",
			Nodes:                  []string{"n2", "n3"},
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(snap).
		Build()

	fx := storage.NewFakeExec()
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{},
	})

	reconciler := &controllers.SnapshotReconciler{
		Client: cli,
		Config: controllers.Config{
			NodeName: "n1",
			Apply:    rec,
			Exec:     fx,
		},
	}

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pvc-1.snap-1"},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if result.RequeueAfter != 0 {
		t.Errorf("unrelated-node Snapshot should NOT requeue; got %+v", result)
	}

	if len(fx.CommandLines()) != 0 {
		t.Errorf("unrelated-node Snapshot triggered provider calls: %v", fx.CommandLines())
	}

	var got blockstoriov1alpha1.Snapshot
	if err := cli.Get(context.Background(), client.ObjectKey{Name: "pvc-1.snap-1"}, &got); err != nil {
		t.Fatalf("Get: %v", err)
	}

	if slices.Contains(got.Finalizers, controllers.SatelliteSnapshotFinalizer) {
		t.Errorf("finalizer stamped on a Snapshot for another node: %v", got.Finalizers)
	}
}
