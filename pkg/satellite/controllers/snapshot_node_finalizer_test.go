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
	"slices"
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/satellite"
	"github.com/cozystack/blockstor/pkg/satellite/controllers"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/lvm"
)

const snapLVLookup = "lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-1_snap-1_00000"

func thinPool(node string) *blockstoriov1alpha1.StoragePool {
	return &blockstoriov1alpha1.StoragePool{
		ObjectMeta: metav1.ObjectMeta{Name: node + ".thin1"},
		Spec:       blockstoriov1alpha1.StoragePoolSpec{NodeName: node, PoolName: "thin1", ProviderKind: "LVM_THIN"},
	}
}

// snapshotSatellite is one node's SnapshotReconciler with its pool
// registered, but without any applied replica: the satellite does not know
// the resource's pool, as after the replica was deleted.
func snapshotSatellite(t *testing.T, cli client.Client, node string) (*controllers.SnapshotReconciler, *storage.FakeExec) {
	t.Helper()

	fx := storage.NewFakeExec()
	fx.Expect(snapLVLookup, storage.FakeResponse{Stdout: []byte("pvc-1_snap-1_00000\n")})

	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{"thin1": lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)},
		NodeName:  node,
	})

	return &controllers.SnapshotReconciler{Client: cli, Config: controllers.Config{NodeName: node, Apply: rec, Exec: fx}}, fx
}

func reconcileSnapshot(t *testing.T, r *controllers.SnapshotReconciler) {
	t.Helper()

	_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "pvc-1.snap-1"}})
	if err != nil {
		t.Fatalf("Reconcile on %s: %v", r.Config.NodeName, err)
	}
}

func lvremoved(fx *storage.FakeExec) bool {
	for _, line := range fx.CommandLines() {
		if strings.HasPrefix(line, "lvremove ") && strings.HasSuffix(line, "vg/pvc-1_snap-1_00000") {
			return true
		}
	}

	return false
}

// A Snapshot on two nodes stays until both have deleted their copy. With the
// old shared finalizer the first node's delete released the CRD and the
// second node's copy was never deleted.
func TestSnapshotStaysUntilEveryNodeDeletedItsCopy(t *testing.T) {
	t.Parallel()

	now := metav1.Now()
	snap := &blockstoriov1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "pvc-1.snap-1",
			DeletionTimestamp: &now,
			Finalizers:        []string{controllers.SnapshotNodeFinalizer("n1"), controllers.SnapshotNodeFinalizer("n2")},
		},
		Spec: blockstoriov1alpha1.SnapshotSpec{ResourceDefinitionName: "pvc-1", SnapshotName: "snap-1", Nodes: []string{"n1", "n2"}},
	}

	cli := fake.NewClientBuilder().WithScheme(newSnapshotScheme(t)).WithObjects(snap, thinPool("n1"), thinPool("n2")).Build()

	first, fx1 := snapshotSatellite(t, cli, "n1")
	reconcileSnapshot(t, first)

	if !lvremoved(fx1) {
		t.Fatalf("n1 did not delete its copy: %v", fx1.CommandLines())
	}

	var got blockstoriov1alpha1.Snapshot

	err := cli.Get(t.Context(), client.ObjectKey{Name: "pvc-1.snap-1"}, &got)
	if err != nil {
		t.Fatalf("the Snapshot went after n1 alone deleted its copy: %v", err)
	}

	if want := []string{controllers.SnapshotNodeFinalizer("n2")}; !slices.Equal(got.Finalizers, want) {
		t.Errorf("finalizers after n1: %v, want %v", got.Finalizers, want)
	}

	second, fx2 := snapshotSatellite(t, cli, "n2")
	reconcileSnapshot(t, second)

	if !lvremoved(fx2) {
		t.Fatalf("n2 did not delete its copy: %v", fx2.CommandLines())
	}

	err = cli.Get(t.Context(), client.ObjectKey{Name: "pvc-1.snap-1"}, &got)
	if !apierrors.IsNotFound(err) {
		t.Errorf("the Snapshot outlived both deletes: err=%v finalizers=%v", err, got.Finalizers)
	}
}

// A satellite whose pool is not registered yet keeps its hold: it has not
// looked for its copy.
func TestSnapshotDeleteWaitsForTheNodesPools(t *testing.T) {
	t.Parallel()

	now := metav1.Now()
	snap := &blockstoriov1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "pvc-1.snap-1",
			DeletionTimestamp: &now,
			Finalizers:        []string{controllers.SnapshotNodeFinalizer("n1")},
		},
		Spec: blockstoriov1alpha1.SnapshotSpec{ResourceDefinitionName: "pvc-1", SnapshotName: "snap-1", Nodes: []string{"n1"}},
	}

	cli := fake.NewClientBuilder().WithScheme(newSnapshotScheme(t)).WithObjects(snap, thinPool("n1")).Build()

	r := &controllers.SnapshotReconciler{Client: cli, Config: controllers.Config{
		NodeName: "n1",
		Apply:    satellite.NewReconciler(satellite.ReconcilerConfig{NodeName: "n1"}),
		Exec:     storage.NewFakeExec(),
	}}
	reconcileSnapshot(t, r)

	var got blockstoriov1alpha1.Snapshot

	err := cli.Get(t.Context(), client.ObjectKey{Name: "pvc-1.snap-1"}, &got)
	if err != nil || !slices.Contains(got.Finalizers, controllers.SnapshotNodeFinalizer("n1")) {
		t.Errorf("n1 released its hold before its pool was registered: err=%v finalizers=%v", err, got.Finalizers)
	}
}

// A live Snapshot that still carries the old shared finalizer loses it once
// every node in its list holds its own, and not before.
func TestSharedSnapshotFinalizerIsRetiredOnceEveryNodeHoldsItsOwn(t *testing.T) {
	t.Parallel()

	snap := &blockstoriov1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "pvc-1.snap-1",
			Finalizers: []string{controllers.SatelliteSnapshotFinalizer},
		},
		Spec: blockstoriov1alpha1.SnapshotSpec{ResourceDefinitionName: "pvc-1", SnapshotName: "snap-1", Nodes: []string{"n1", "n2"}},
	}

	cli := fake.NewClientBuilder().WithScheme(newSnapshotScheme(t)).WithObjects(snap).Build()

	finalizers := func() []string {
		var got blockstoriov1alpha1.Snapshot

		err := cli.Get(t.Context(), client.ObjectKey{Name: "pvc-1.snap-1"}, &got)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}

		return got.Finalizers
	}

	first, _ := snapshotSatellite(t, cli, "n1")
	reconcileSnapshot(t, first)
	reconcileSnapshot(t, first)

	if got := finalizers(); !slices.Contains(got, controllers.SatelliteSnapshotFinalizer) {
		t.Fatalf("shared finalizer dropped while n2 holds nothing of its own: %v", got)
	}

	second, _ := snapshotSatellite(t, cli, "n2")
	reconcileSnapshot(t, second)
	reconcileSnapshot(t, second)

	got := finalizers()
	if slices.Contains(got, controllers.SatelliteSnapshotFinalizer) {
		t.Errorf("shared finalizer kept after both nodes hold their own: %v", got)
	}

	for _, node := range []string{"n1", "n2"} {
		if !slices.Contains(got, controllers.SnapshotNodeFinalizer(node)) {
			t.Errorf("%s's finalizer missing: %v", node, got)
		}
	}
}

// Every node name maps to a valid finalizer, a long one included, and the
// mapping follows naming.Key so a node's spellings share one key.
func TestSnapshotNodeFinalizerIsAlwaysAQualifiedName(t *testing.T) {
	t.Parallel()

	long := strings.Repeat("worker-", 20) + "1.example.org"

	for _, node := range []string{"n1", "Node-1", "worker-1.example.org", long} {
		key := controllers.SnapshotNodeFinalizer(node)
		if errs := validation.IsQualifiedName(key); len(errs) != 0 {
			t.Errorf("%q -> %q: %v", node, key, errs)
		}
	}

	if controllers.SnapshotNodeFinalizer("Node-1") != controllers.SnapshotNodeFinalizer("node-1") {
		t.Errorf("two spellings of one node got different finalizers")
	}

	if controllers.SnapshotNodeFinalizer(long) == controllers.SnapshotNodeFinalizer(long+"x") {
		t.Errorf("two long node names collided")
	}
}
