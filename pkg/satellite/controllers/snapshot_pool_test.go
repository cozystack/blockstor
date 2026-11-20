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
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/satellite/controllers"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// takenSnapshot is a snapshot whose copy on n1 is already taken.
func takenSnapshot(pool string) *blockstoriov1alpha1.Snapshot {
	return &blockstoriov1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "pvc-1.snap-1",
			Finalizers: []string{controllers.SatelliteSnapshotFinalizer},
		},
		Spec: blockstoriov1alpha1.SnapshotSpec{
			ResourceDefinitionName: "pvc-1",
			SnapshotName:           "snap-1",
			Nodes:                  []string{"n1"},
			TakeSnapshot:           true,
		},
		Status: blockstoriov1alpha1.SnapshotStatus{
			NodeStatus: []blockstoriov1alpha1.SnapshotPerNodeStatus{
				{NodeName: "n1", Ready: true, CreateTimestamp: 1, StoragePool: pool},
			},
		},
	}
}

// replicaIn is the replica of pvc-1 on n1, in pool.
func replicaIn(pool string) *blockstoriov1alpha1.Resource {
	return &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: k8s.ResourceCRName("pvc-1", "n1")},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "pvc-1",
			NodeName:               "n1",
			StoragePool:            pool,
		},
	}
}

// reconcileTaken runs one satellite pass over the taken snapshot and returns
// the pool its n1 entry records afterwards.
func reconcileTaken(t *testing.T, objs ...client.Object) string {
	t.Helper()

	cli := fake.NewClientBuilder().
		WithScheme(newSnapshotScheme(t)).
		WithStatusSubresource(&blockstoriov1alpha1.Snapshot{}).
		WithObjects(objs...).
		Build()

	reconciler := &controllers.SnapshotReconciler{
		Client: cli,
		Config: controllers.Config{NodeName: "n1"},
	}

	// A pass that only adds this node's finalizer ends there; the ones
	// after it reach the taken copy.
	for range 3 {
		_, err := reconciler.Reconcile(t.Context(), ctrl.Request{
			NamespacedName: types.NamespacedName{Name: "pvc-1.snap-1"},
		})
		if err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
	}

	var got blockstoriov1alpha1.Snapshot

	err := cli.Get(t.Context(), client.ObjectKey{Name: "pvc-1.snap-1"}, &got)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if len(got.Status.NodeStatus) != 1 {
		t.Fatalf("Status.NodeStatus = %+v, want one entry for n1", got.Status.NodeStatus)
	}

	return got.Status.NodeStatus[0].StoragePool
}

// The copy on a node is recorded with the pool of the replica it was taken
// from, so a restore still finds it once that replica is gone.
func TestSnapshotRecordsThePoolOfItsCopy(t *testing.T) {
	t.Parallel()

	if pool := reconcileTaken(t, takenSnapshot(""), replicaIn("zfs-data")); pool != "zfs-data" {
		t.Errorf("recorded pool = %q, want zfs-data", pool)
	}
}

// A finished take has both flags cleared, and a snapshot from before the
// record is one of those: its copy is recorded all the same, while its
// replica is still there to read the pool from.
func TestSnapshotRecordsThePoolOfAFinishedTake(t *testing.T) {
	t.Parallel()

	snap := takenSnapshot("")
	snap.Spec.TakeSnapshot = false
	snap.Status.Flags = []string{"SUCCESSFUL"}

	if pool := reconcileTaken(t, snap, replicaIn("zfs-data")); pool != "zfs-data" {
		t.Errorf("recorded pool = %q, want zfs-data", pool)
	}
}

// A replica moved to another pool later leaves its earlier copies where they
// were, so the record is not rewritten.
func TestSnapshotKeepsThePoolItRecorded(t *testing.T) {
	t.Parallel()

	if pool := reconcileTaken(t, takenSnapshot("zfs-data"), replicaIn("zfs-other")); pool != "zfs-data" {
		t.Errorf("recorded pool = %q, want the one recorded first, zfs-data", pool)
	}
}

// With the replica gone there is nothing to read the pool from, and nothing
// is recorded rather than a guess.
func TestSnapshotRecordsNoPoolWithoutItsReplica(t *testing.T) {
	t.Parallel()

	if pool := reconcileTaken(t, takenSnapshot("")); pool != "" {
		t.Errorf("recorded pool = %q with no replica on the node, want none", pool)
	}
}

// A diskless replica holds no copy, and its historical pool is not the one
// the snapshot lives in.
func TestSnapshotRecordsNoPoolFromADisklessReplica(t *testing.T) {
	t.Parallel()

	res := replicaIn("zfs-data")
	res.Spec.Flags = []string{"DISKLESS"}

	if pool := reconcileTaken(t, takenSnapshot(""), res); pool != "" {
		t.Errorf("recorded pool = %q from a diskless replica, want none", pool)
	}
}
