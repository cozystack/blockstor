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

package controllers

import (
	"context"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/satellite"
	"github.com/cozystack/blockstor/pkg/storage"
)

// flakyCreateProvider is a minimal storage.Provider whose CreateVolume
// fails the first N invocations and succeeds afterwards. Used to
// simulate the upstream-LINSTOR toggle-disk retry scenario: storage
// pool briefly unavailable, satellite retries, eventually converges.
type flakyCreateProvider struct {
	failsRemaining int32
	created        int32
}

func (f *flakyCreateProvider) Kind() string { return "FAKE" }

func (f *flakyCreateProvider) PoolStatus(_ context.Context) (storage.PoolStatus, error) {
	return storage.PoolStatus{TotalCapacityKib: 1024 * 1024, FreeCapacityKib: 1024 * 1024}, nil
}

func (f *flakyCreateProvider) CreateVolume(_ context.Context, _ storage.Volume) error {
	atomic.AddInt32(&f.created, 1)

	if atomic.LoadInt32(&f.failsRemaining) > 0 {
		atomic.AddInt32(&f.failsRemaining, -1)

		return errors.New("create-md: storage pool transiently unavailable")
	}

	return nil
}

func (f *flakyCreateProvider) DeleteVolume(_ context.Context, _ storage.Volume) error {
	return nil
}

func (f *flakyCreateProvider) ResizeVolume(_ context.Context, _ storage.Volume) error { return nil }

func (f *flakyCreateProvider) VolumeStatus(_ context.Context, vol storage.Volume) (storage.VolumeStatus, error) {
	return storage.VolumeStatus{
		DevicePath: "/dev/fake/" + vol.ResourceName,
		UsableKib:  vol.SizeKib,
	}, nil
}

func (f *flakyCreateProvider) CreateSnapshot(_ context.Context, _ storage.Snapshot) error {
	return nil
}

func (f *flakyCreateProvider) DeleteSnapshot(_ context.Context, _ storage.Snapshot) error {
	return nil
}

func (f *flakyCreateProvider) RestoreVolumeFromSnapshot(_ context.Context, _ storage.Volume, _ storage.Snapshot) error {
	return nil
}

// newToggleDiskTestScheme returns a scheme wired with the CRDs the
// reconciler walks during a Resource reconcile. Mirrors the
// StoragePool tests' shape.
func newToggleDiskTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()

	s := runtime.NewScheme()
	if err := blockstoriov1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("blockstor scheme: %v", err)
	}

	return s
}

// makeStorageOnlyRD seeds a ResourceDefinition that bypasses the DRBD
// allocation gate (LayerStack=["STORAGE"]) so the test can isolate the
// storage-carve failure path. The toggle-disk retry counter is
// orthogonal to the DRBD half — the upstream LINSTOR contract counts
// every per-resource Apply failure, regardless of which sub-layer
// surfaced the error.
func makeStorageOnlyRD(name string) *blockstoriov1alpha1.ResourceDefinition {
	return &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			LayerStack: []string{"STORAGE"},
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 65536},
			},
		},
	}
}

// makeToggledResource returns a Resource that the satellite reconciler
// will treat as "mid-conversion to diskful" — DISKLESS flag absent,
// finalizer already stamped (so the test skips the finalizer-only
// short-circuit), spec.storagePool pointing at the test pool, and
// matching node-name.
func makeToggledResource(rdName, node, pool string) *blockstoriov1alpha1.Resource {
	return &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{
			Name:       rdName + "." + node,
			Finalizers: []string{SatelliteResourceFinalizer},
		},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: rdName,
			NodeName:               node,
			StoragePool:            pool,
			// no DISKLESS flag — mid-conversion to diskful
		},
	}
}

// TestToggleDiskIncrementsRetriesOnFailure pins Bug 39: on a transient
// storage-carve failure during diskless→diskful conversion, the
// satellite reconciler MUST increment Status.ToggleDiskRetries by 1
// per failing Apply pass and reset to 0 once the conversion converges.
//
// Three reconcile passes drive the state machine:
//
//  1. Provider's CreateVolume fails → retries 0 → 1.
//  2. Provider's CreateVolume fails again → retries 1 → 2.
//  3. Provider's CreateVolume succeeds → retries 2 → 0.
//
// Without the fix, the counter stayed at 0 forever and operators had
// no signal that the conversion was looping — the exact symptom
// scenario 07-toggle-disk §7.6 reproduces.
func TestToggleDiskIncrementsRetriesOnFailure(t *testing.T) {
	t.Parallel()

	const (
		rdName = "pvc-toggle"
		node   = "n-toggle"
		pool   = "lvm-thin"
	)

	scheme := newToggleDiskTestScheme(t)

	rd := makeStorageOnlyRD(rdName)
	res := makeToggledResource(rdName, node, pool)

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(rd, res).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}).
		Build()

	provider := &flakyCreateProvider{failsRemaining: 2}

	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{pool: provider},
		NodeName:  node,
	})

	reconciler := &ResourceReconciler{
		Client: cli,
		Config: Config{
			NodeName: node,
			Apply:    rec,
			Exec:     storage.NewFakeExec(),
		},
	}

	// --- Pass 1: first CreateVolume fails → retries 1. ---

	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: res.Name},
	})
	if err != nil {
		t.Fatalf("Reconcile pass 1: %v", err)
	}

	var afterPass1 blockstoriov1alpha1.Resource

	err = cli.Get(context.Background(), client.ObjectKey{Name: res.Name}, &afterPass1)
	if err != nil {
		t.Fatalf("Get after pass 1: %v", err)
	}

	if afterPass1.Status.ToggleDiskRetries != 1 {
		t.Errorf("Pass 1: ToggleDiskRetries = %d, want 1",
			afterPass1.Status.ToggleDiskRetries)
	}

	// --- Pass 2: second CreateVolume fails → retries 2. ---

	_, err = reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: res.Name},
	})
	if err != nil {
		t.Fatalf("Reconcile pass 2: %v", err)
	}

	var afterPass2 blockstoriov1alpha1.Resource

	err = cli.Get(context.Background(), client.ObjectKey{Name: res.Name}, &afterPass2)
	if err != nil {
		t.Fatalf("Get after pass 2: %v", err)
	}

	if afterPass2.Status.ToggleDiskRetries != 2 {
		t.Errorf("Pass 2: ToggleDiskRetries = %d, want 2",
			afterPass2.Status.ToggleDiskRetries)
	}

	// --- Pass 3: CreateVolume succeeds → retries reset to 0. ---

	_, err = reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: res.Name},
	})
	if err != nil {
		t.Fatalf("Reconcile pass 3: %v", err)
	}

	var afterPass3 blockstoriov1alpha1.Resource

	err = cli.Get(context.Background(), client.ObjectKey{Name: res.Name}, &afterPass3)
	if err != nil {
		t.Fatalf("Get after pass 3: %v", err)
	}

	if afterPass3.Status.ToggleDiskRetries != 0 {
		t.Errorf("Pass 3: ToggleDiskRetries = %d, want 0 (reset on success)",
			afterPass3.Status.ToggleDiskRetries)
	}

	// Sanity: the third pass actually called CreateVolume — without
	// this assertion a regression that mistakenly short-circuits the
	// Apply chain could still hit retries=0 by accident.
	if got := atomic.LoadInt32(&provider.created); got != 3 {
		t.Errorf("CreateVolume call count: got %d, want 3", got)
	}
}

// recordingDeleteProvider counts DeleteVolume calls so the cancel
// rollback test can pin that the satellite actually invoked the
// storage tear-down (rather than just flipping flags and walking
// away with a half-carved LV on disk).
type recordingDeleteProvider struct {
	flakyCreateProvider
	deleted int32
}

func (r *recordingDeleteProvider) DeleteVolume(_ context.Context, _ storage.Volume) error {
	atomic.AddInt32(&r.deleted, 1)

	return nil
}

// TestToggleDiskCancelUnwindsPartialState pins Bug 40: when the REST
// shim writes Spec.ToggleDiskCancel=true mid-conversion, the satellite
// reconciler MUST tear down the partially-carved storage (DeleteVolume
// + the drbdadm-down path inside DeleteResource), re-stamp the
// DISKLESS flag on Spec, clear ToggleDiskCancel, and reset
// Status.ToggleDiskRetries to 0.
//
// Setup: Resource has Status.Volumes populated (storage was carved on
// an earlier reconcile) and Status.ToggleDiskRetries=3 (the prior
// conversion attempt failed three times before the operator gave up
// and asked for a cancel). The DISKLESS flag is absent (Spec asks
// for diskful) and Spec.ToggleDiskCancel=true is the cancel intent
// the REST handler stamped.
//
// Expected end-state: Spec.Flags ∋ DISKLESS, Spec.ToggleDiskCancel=
// false, Status.ToggleDiskRetries=0, DeleteVolume called at least
// once (the rollback path).
func TestToggleDiskCancelUnwindsPartialState(t *testing.T) {
	t.Parallel()

	const (
		rdName = "pvc-cancel"
		node   = "n-cancel"
		pool   = "lvm-thin"
	)

	scheme := newToggleDiskTestScheme(t)

	rd := makeStorageOnlyRD(rdName)
	res := makeToggledResource(rdName, node, pool)
	res.Spec.ToggleDiskCancel = true
	// Simulate a Resource that was already mid-conversion: storage
	// was carved on a prior reconcile (DevicePath populated) and the
	// retry counter has been bumped to 3 by prior failures.
	res.Status.Volumes = []blockstoriov1alpha1.ResourceVolumeStatus{
		{VolumeNumber: 0, DevicePath: "/dev/fake/" + rdName, AllocatedKib: 65536, DiskState: "Inconsistent"},
	}
	res.Status.ToggleDiskRetries = 3

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(rd, res).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}).
		Build()

	provider := &recordingDeleteProvider{}

	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{pool: provider},
		NodeName:  node,
	})

	reconciler := &ResourceReconciler{
		Client: cli,
		Config: Config{
			NodeName: node,
			Apply:    rec,
			Exec:     storage.NewFakeExec(),
		},
	}

	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: res.Name},
	})
	if err != nil {
		t.Fatalf("Reconcile cancel: %v", err)
	}

	var after blockstoriov1alpha1.Resource

	err = cli.Get(context.Background(), client.ObjectKey{Name: res.Name}, &after)
	if err != nil {
		t.Fatalf("Get after cancel: %v", err)
	}

	if !slices.Contains(after.Spec.Flags, apiv1.ResourceFlagDiskless) {
		t.Errorf("DISKLESS flag NOT re-stamped on cancel: %v", after.Spec.Flags)
	}

	if after.Spec.ToggleDiskCancel {
		t.Errorf("ToggleDiskCancel still true after rollback: %+v", after.Spec)
	}

	if after.Status.ToggleDiskRetries != 0 {
		t.Errorf("ToggleDiskRetries NOT cleared on cancel: got %d, want 0",
			after.Status.ToggleDiskRetries)
	}

	if got := atomic.LoadInt32(&provider.deleted); got == 0 {
		t.Errorf("DeleteVolume not invoked during cancel rollback")
	}
}
