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
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
)

// TestStampVolumeStatusCopiesStoragePool pins Bug 75: the satellite's
// per-volume Status stamp MUST carry `Spec.StoragePool` onto
// `Status.Volumes[i].StoragePool`. The REST view layer reads this
// field through `volumesFromStatus()`; without the stamp `linstor v l`
// shows `StoragePool: None` for every replica even though the Spec
// has the correct pool name.
//
// Source of truth is `Resource.Spec.StoragePool` — the field the
// controller-side dispatcher already authored before the satellite
// took over the reconcile.
func TestStampVolumeStatusCopiesStoragePool(t *testing.T) {
	t.Parallel()

	const (
		node   = "n-stamp"
		pool   = "zfs-thin"
		rdName = "pvc-stamp"
	)

	scheme := newToggleDiskTestScheme(t)

	resName := rdName + "." + node

	res := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: resName},
		Spec: blockstoriov1alpha1.ResourceSpec{
			NodeName:               node,
			ResourceDefinitionName: rdName,
			StoragePool:            pool,
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(res).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}).
		Build()

	reconciler := &ResourceReconciler{
		Client: cli,
		Config: Config{NodeName: node},
	}

	results := []*intent.ResourceApplyResult{
		{
			Name:     rdName,
			NodeName: node,
			Ok:       true,
			Volumes: []*intent.ResourceApplyVolumeResult{
				{VolumeNumber: 0, DevicePath: "/dev/drbd1000"},
			},
		},
	}

	err := reconciler.stampVolumeStatus(context.Background(), res, results)
	if err != nil {
		t.Fatalf("stampVolumeStatus: %v", err)
	}

	var after blockstoriov1alpha1.Resource

	err = cli.Get(context.Background(), client.ObjectKey{Name: resName}, &after)
	if err != nil {
		t.Fatalf("post-stamp Get: %v", err)
	}

	if len(after.Status.Volumes) != 1 {
		t.Fatalf("Status.Volumes length: got %d, want 1 (full: %+v)",
			len(after.Status.Volumes), after.Status.Volumes)
	}

	vol := after.Status.Volumes[0]

	if vol.VolumeNumber != 0 {
		t.Errorf("VolumeNumber: got %d, want 0", vol.VolumeNumber)
	}

	if vol.DevicePath != "/dev/drbd1000" {
		t.Errorf("DevicePath: got %q, want %q", vol.DevicePath, "/dev/drbd1000")
	}

	if vol.StoragePool != pool {
		t.Errorf("StoragePool: got %q, want %q (Bug 75)", vol.StoragePool, pool)
	}
}

// TestBuildObserverVolumeStatusPreservesStoragePool pins the observer
// half of Bug 75: when the observer rebuilds `Status.Volumes` from a
// drbdsetup-events2 frame, it MUST carry through the StoragePool name
// (sourced from `Resource.Spec.StoragePool`) so a subsequent SSA apply
// with `ForceOwnership=false` does not race the satellite-stamp owner
// and end up with an empty StoragePool field on the listMap entry.
//
// The observer never authored StoragePool before, so the field was
// implicitly empty on every event-driven Status write. Together with
// the satellite-stamp gap above this is why `linstor v l` showed
// `StoragePool: None` in production.
func TestBuildObserverVolumeStatusPreservesStoragePool(t *testing.T) {
	t.Parallel()

	const pool = "zfs-thin"

	ev := &observation{
		ResourceName: "pvc-obs",
		Volumes: []volumeObservation{
			{VolumeNumber: 0, DiskState: "UpToDate", CurrentUUID: "BEEF"},
			{VolumeNumber: 1, DiskState: "Inconsistent"},
		},
	}

	got := buildObserverVolumeStatus(ev, pool)
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2: %+v", len(got), got)
	}

	for _, v := range got {
		if v.StoragePool != pool {
			t.Errorf("VolumeNumber=%d StoragePool: got %q, want %q (Bug 75)",
				v.VolumeNumber, v.StoragePool, pool)
		}
	}
}

// TestBuildObserverVolumeStatusEmptyStoragePoolOmitted documents the
// nil-input contract: when the caller hasn't been able to resolve the
// pool name (e.g. the parent Resource is mid-creation), the observer
// MUST NOT claim ownership of the StoragePool field — that would let
// an empty value win on SSA merge against the satellite-stamp owner.
func TestBuildObserverVolumeStatusEmptyStoragePoolOmitted(t *testing.T) {
	t.Parallel()

	ev := &observation{
		ResourceName: "pvc-obs",
		Volumes: []volumeObservation{
			{VolumeNumber: 0, DiskState: "UpToDate"},
		},
	}

	got := buildObserverVolumeStatus(ev, "")
	if len(got) != 1 {
		t.Fatalf("len = %d, want 1", len(got))
	}

	if got[0].StoragePool != "" {
		t.Errorf("StoragePool: got %q, want empty (no pool known, no claim)",
			got[0].StoragePool)
	}
}

// TestPurgeStaleVolumeStatusDropsRemovedVolumeNumber pins the Bug 355
// followup contract: when the parent RD's VolumeDefinitions no longer
// contain a VolumeNumber that's still present in the Resource's
// Status.Volumes (typical after a `vd d <rd> N` cascade), the
// satellite's post-apply stamp MUST drop the stale entry within one
// reconcile pass.
//
// The apiserver-side cascade in commit 4a22ff489 correctly removes
// vol N from Spec.Volumes + the volume-numbers annotation, but
// Status.Volumes[*] entries authored by the observer's listMap
// field-owner persist because SSA only collapses a listMap entry
// when NO field-owner claims any subfield on it. `purgeStaleVolumeStatus`
// closes that gap via a JSON merge-patch that REPLACES `status.volumes`
// with only the desired survivors.
func TestPurgeStaleVolumeStatusDropsRemovedVolumeNumber(t *testing.T) {
	t.Parallel()

	const (
		node   = "n-purge"
		rdName = "pvc-purge"
	)

	scheme := newToggleDiskTestScheme(t)

	resName := rdName + "." + node

	// Pre-state: RD has just one VolumeDefinition (vol 1) — the
	// `vd d <rd> 0` cascade already ran on the apiserver side. But
	// Status.Volumes still carries both vol 0 (stale, observer-
	// owned) and vol 1 (active).
	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: rdName},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			LayerStack: []string{"STORAGE"},
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 1, SizeKib: 65536},
			},
		},
	}

	res := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: resName},
		Spec: blockstoriov1alpha1.ResourceSpec{
			NodeName:               node,
			ResourceDefinitionName: rdName,
		},
		Status: blockstoriov1alpha1.ResourceStatus{
			Volumes: []blockstoriov1alpha1.ResourceVolumeStatus{
				{VolumeNumber: 0, DevicePath: "/dev/drbd1000", DiskState: "UpToDate"},
				{VolumeNumber: 1, DevicePath: "/dev/drbd1001", DiskState: "UpToDate"},
			},
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(res, rd).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}).
		Build()

	reconciler := &ResourceReconciler{
		Client: cli,
		Config: Config{NodeName: node},
	}

	err := reconciler.purgeStaleVolumeStatus(context.Background(), res, rd)
	if err != nil {
		t.Fatalf("purgeStaleVolumeStatus: %v", err)
	}

	var after blockstoriov1alpha1.Resource

	err = cli.Get(context.Background(), client.ObjectKey{Name: resName}, &after)
	if err != nil {
		t.Fatalf("post-purge Get: %v", err)
	}

	if len(after.Status.Volumes) != 1 {
		t.Fatalf("Status.Volumes length: got %d, want 1 (stale vol 0 should have been pruned; full: %+v)",
			len(after.Status.Volumes), after.Status.Volumes)
	}

	if after.Status.Volumes[0].VolumeNumber != 1 {
		t.Errorf("survivor VolumeNumber: got %d, want 1 (vol 0 should have been dropped)",
			after.Status.Volumes[0].VolumeNumber)
	}

	// Caller-copy reflection: subsequent reads inside the same
	// reconcile must see the post-purge slice.
	if len(res.Status.Volumes) != 1 || res.Status.Volumes[0].VolumeNumber != 1 {
		t.Errorf("caller-copy reflection: got %+v, want [{VolumeNumber:1, ...}]",
			res.Status.Volumes)
	}
}

// TestPurgeStaleVolumeStatusNoopWhenAllDesired pins the steady-state
// contract: when every Status.Volumes entry's VolumeNumber is still
// present in the RD's VolumeDefinitions, the purge MUST be a no-op
// (no patch issued, no churn on Status). Validated by inspecting that
// the Status survives untouched after a purge call on a balanced
// pre-state.
func TestPurgeStaleVolumeStatusNoopWhenAllDesired(t *testing.T) {
	t.Parallel()

	const (
		node   = "n-purge-noop"
		rdName = "pvc-purge-noop"
	)

	scheme := newToggleDiskTestScheme(t)

	resName := rdName + "." + node

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: rdName},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			LayerStack: []string{"STORAGE"},
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 65536},
				{VolumeNumber: 1, SizeKib: 65536},
			},
		},
	}

	res := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: resName},
		Spec: blockstoriov1alpha1.ResourceSpec{
			NodeName:               node,
			ResourceDefinitionName: rdName,
		},
		Status: blockstoriov1alpha1.ResourceStatus{
			Volumes: []blockstoriov1alpha1.ResourceVolumeStatus{
				{VolumeNumber: 0, DevicePath: "/dev/drbd1000", DiskState: "UpToDate"},
				{VolumeNumber: 1, DevicePath: "/dev/drbd1001", DiskState: "UpToDate"},
			},
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(res, rd).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}).
		Build()

	reconciler := &ResourceReconciler{
		Client: cli,
		Config: Config{NodeName: node},
	}

	err := reconciler.purgeStaleVolumeStatus(context.Background(), res, rd)
	if err != nil {
		t.Fatalf("purgeStaleVolumeStatus: %v", err)
	}

	var after blockstoriov1alpha1.Resource

	err = cli.Get(context.Background(), client.ObjectKey{Name: resName}, &after)
	if err != nil {
		t.Fatalf("post-purge Get: %v", err)
	}

	if len(after.Status.Volumes) != 2 {
		t.Fatalf("Status.Volumes length: got %d, want 2 (no-op purge should leave both vol 0 and vol 1 intact; full: %+v)",
			len(after.Status.Volumes), after.Status.Volumes)
	}
}

// TestPurgeStaleVolumeStatusEmptyVolumeDefinitionsIsNoop documents the
// defensive contract: if the parent RD has zero VolumeDefinitions
// (freshly created RD, mid-cascade RD teardown, or transient cache
// trail), the purge MUST be a no-op — the per-Resource delete path
// owns full Status teardown, and blanking Status.Volumes here would
// race that path.
func TestPurgeStaleVolumeStatusEmptyVolumeDefinitionsIsNoop(t *testing.T) {
	t.Parallel()

	const (
		node   = "n-purge-empty"
		rdName = "pvc-purge-empty"
	)

	scheme := newToggleDiskTestScheme(t)

	resName := rdName + "." + node

	rd := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: rdName},
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			LayerStack: []string{"STORAGE"},
		},
	}

	res := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: resName},
		Spec: blockstoriov1alpha1.ResourceSpec{
			NodeName:               node,
			ResourceDefinitionName: rdName,
		},
		Status: blockstoriov1alpha1.ResourceStatus{
			Volumes: []blockstoriov1alpha1.ResourceVolumeStatus{
				{VolumeNumber: 0, DevicePath: "/dev/drbd1000", DiskState: "UpToDate"},
			},
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(res, rd).
		WithStatusSubresource(&blockstoriov1alpha1.Resource{}).
		Build()

	reconciler := &ResourceReconciler{
		Client: cli,
		Config: Config{NodeName: node},
	}

	err := reconciler.purgeStaleVolumeStatus(context.Background(), res, rd)
	if err != nil {
		t.Fatalf("purgeStaleVolumeStatus: %v", err)
	}

	var after blockstoriov1alpha1.Resource

	err = cli.Get(context.Background(), client.ObjectKey{Name: resName}, &after)
	if err != nil {
		t.Fatalf("post-purge Get: %v", err)
	}

	if len(after.Status.Volumes) != 1 {
		t.Errorf("Status.Volumes length: got %d, want 1 (empty VolumeDefinitions must NOT trigger purge; full: %+v)",
			len(after.Status.Volumes), after.Status.Volumes)
	}
}
