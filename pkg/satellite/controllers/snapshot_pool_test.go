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
	"slices"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/satellite"
	"github.com/cozystack/blockstor/pkg/satellite/controllers"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/zfs"
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

// zfsPools is a satellite on n1 with one ZFS pool per name, where the
// pools named in holding hold the copy pvc-1@snap-1.
func zfsPools(names []string, holding ...string) (*satellite.Reconciler, *storage.FakeExec) {
	fx := storage.NewFakeExec()
	providers := map[string]storage.Provider{}

	for _, name := range names {
		providers[name] = zfs.NewProvider(zfs.Config{Pool: name, Thin: true}, fx)

		if slices.Contains(holding, name) {
			ds := name + "/pvc-1_00000@snap-1"
			fx.Expect("zfs list -H -o name "+ds, storage.FakeResponse{Stdout: []byte(ds + "\n")})
		}
	}

	return satellite.NewReconciler(satellite.ReconcilerConfig{Providers: providers, Exec: fx}), fx
}

// reconcileTaken runs the satellite passes over the taken snapshot and
// returns the pool its n1 entry records afterwards.
func reconcileTaken(t *testing.T, apply *satellite.Reconciler, objs ...client.Object) string {
	t.Helper()

	cli := fake.NewClientBuilder().
		WithScheme(newSnapshotScheme(t)).
		WithStatusSubresource(&blockstoriov1alpha1.Snapshot{}).
		WithObjects(objs...).
		Build()

	reconciler := &controllers.SnapshotReconciler{
		Client: cli,
		Config: controllers.Config{NodeName: "n1", Apply: apply},
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

// A copy taken before the record existed is recorded with the pool that
// holds it, which the satellite asks its pools for. The replica is no
// guide: re-created in another pool since, it would name the wrong one.
func TestSnapshotRecordsThePoolHoldingItsCopy(t *testing.T) {
	t.Parallel()

	apply, _ := zfsPools([]string{"zfs-data", "zfs-other"}, "zfs-data")

	if pool := reconcileTaken(t, apply, takenSnapshot(""), replicaIn("zfs-other")); pool != "zfs-data" {
		t.Errorf("recorded pool = %q, want zfs-data, the pool that holds the copy", pool)
	}
}

// The probe needs no replica, so a copy whose replica is gone is recorded
// all the same.
func TestSnapshotRecordsThePoolOfACopyWithoutItsReplica(t *testing.T) {
	t.Parallel()

	apply, _ := zfsPools([]string{"zfs-data"}, "zfs-data")

	if pool := reconcileTaken(t, apply, takenSnapshot("")); pool != "zfs-data" {
		t.Errorf("recorded pool = %q, want zfs-data", pool)
	}
}

// A finished take has both flags cleared, and a snapshot from before the
// record is one of those: its copy is recorded all the same.
func TestSnapshotRecordsThePoolOfAFinishedTake(t *testing.T) {
	t.Parallel()

	snap := takenSnapshot("")
	snap.Spec.TakeSnapshot = false
	snap.Status.Flags = []string{"SUCCESSFUL"}

	apply, _ := zfsPools([]string{"zfs-data"}, "zfs-data")

	if pool := reconcileTaken(t, apply, snap); pool != "zfs-data" {
		t.Errorf("recorded pool = %q, want zfs-data", pool)
	}
}

// A record is not rewritten, and the pools are not probed again once it is
// there.
func TestSnapshotKeepsThePoolItRecorded(t *testing.T) {
	t.Parallel()

	apply, fx := zfsPools([]string{"zfs-data", "zfs-other"}, "zfs-other")

	if pool := reconcileTaken(t, apply, takenSnapshot("zfs-data")); pool != "zfs-data" {
		t.Errorf("recorded pool = %q, want the one recorded first, zfs-data", pool)
	}

	for _, line := range fx.CommandLines() {
		if strings.HasPrefix(line, "zfs list") {
			t.Errorf("probed the pools for a copy whose pool is recorded: %q", line)
		}
	}
}

// With no pool holding the copy nothing is recorded rather than a guess.
func TestSnapshotRecordsNoPoolNoneHolds(t *testing.T) {
	t.Parallel()

	apply, _ := zfsPools([]string{"zfs-data"})

	if pool := reconcileTaken(t, apply, takenSnapshot(""), replicaIn("zfs-data")); pool != "" {
		t.Errorf("recorded pool = %q with no pool holding the copy, want none", pool)
	}
}

// Two pools that both report the copy are told apart by the replica's pool,
// and left unrecorded when it does not settle it.
func TestSnapshotPoolTieGoesToTheReplica(t *testing.T) {
	t.Parallel()

	both := []string{"pool-a", "pool-b"}

	apply, _ := zfsPools(both, both...)
	if pool := reconcileTaken(t, apply, takenSnapshot(""), replicaIn("pool-b")); pool != "pool-b" {
		t.Errorf("recorded pool = %q, want pool-b, the replica's", pool)
	}

	apply, _ = zfsPools(both, both...)
	if pool := reconcileTaken(t, apply, takenSnapshot("")); pool != "" {
		t.Errorf("recorded pool = %q for a tie with no replica, want none", pool)
	}
}

// The take records the pool in the same write that marks the copy Ready.
// Recorded on a later pass, it would be lost whenever the source replica
// goes in between, which is what linstor-csi does as soon as the
// VolumeSnapshot is ready; the probe here finds nothing, so only the take
// can record it.
func TestSnapshotTakeRecordsItsPoolWithReady(t *testing.T) {
	t.Parallel()

	snap := &blockstoriov1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "pvc-1.snap-1",
			Finalizers: []string{controllers.SatelliteSnapshotFinalizer},
		},
		Spec: blockstoriov1alpha1.SnapshotSpec{
			ResourceDefinitionName: "pvc-1",
			SnapshotName:           "snap-1",
			Nodes:                  []string{"n1"},
			SuspendIO:              true,
			TakeSnapshot:           true,
			VolumeDefinitions:      []blockstoriov1alpha1.SnapshotVolumeRef{{VolumeNumber: 0}},
		},
		Status: blockstoriov1alpha1.SnapshotStatus{
			NodeStatus: []blockstoriov1alpha1.SnapshotPerNodeStatus{{NodeName: "n1", SuspendIOAcked: true}},
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(newSnapshotScheme(t)).
		WithStatusSubresource(&blockstoriov1alpha1.Snapshot{}).
		WithObjects(snap).
		Build()

	fx := storage.NewFakeExec()
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("")})

	reconciler := &controllers.SnapshotReconciler{
		Client: cli,
		Config: controllers.Config{
			NodeName: "n1",
			Apply:    seedThinResourceWithExec(t, fx, "pvc-1", "thin1"),
			Exec:     fx,
		},
	}

	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pvc-1.snap-1"},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	var got blockstoriov1alpha1.Snapshot
	if err := cli.Get(t.Context(), client.ObjectKey{Name: "pvc-1.snap-1"}, &got); err != nil {
		t.Fatalf("Get: %v", err)
	}

	if len(got.Status.NodeStatus) != 1 || !got.Status.NodeStatus[0].Ready {
		t.Fatalf("Status.NodeStatus = %+v, want n1 Ready after the take", got.Status.NodeStatus)
	}

	if pool := got.Status.NodeStatus[0].StoragePool; pool != "thin1" {
		t.Errorf("pool recorded with Ready = %q, want thin1, the pool the copy was taken in", pool)
	}
}
