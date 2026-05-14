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
