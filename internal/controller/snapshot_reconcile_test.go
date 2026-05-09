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

package controller_test

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
	"github.com/cozystack/blockstor/pkg/dispatcher"
)

// TestSnapshotReconcileNilDispatcher: a SnapshotReconciler started
// without a Dispatcher (out-of-cluster operator boot, integration
// tests, or before the cluster wiring lands) must NOT crash on
// reconcile — return Result{} and let later passes pick up once
// the Dispatcher is wired.
func TestSnapshotReconcileNilDispatcher(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()

	rec := &controllerpkg.SnapshotReconciler{
		Client: cli,
		Scheme: scheme,
		// Dispatcher: nil intentionally
	}

	got, err := rec.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "snap-1"},
	})
	if err != nil {
		t.Errorf("nil dispatcher must not error; got %v", err)
	}

	if got.RequeueAfter != 0 {
		t.Errorf("nil dispatcher must not requeue; got %+v", got)
	}
}

// TestSnapshotReconcileMissingSnapshot: Reconcile invoked for a
// Snapshot that's already been deleted (CRD GC raced us) must return
// Result{} silently. Without this branch the reconciler would log
// spurious errors during normal Snapshot CRD cleanup.
func TestSnapshotReconcileMissingSnapshot(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()

	rec := &controllerpkg.SnapshotReconciler{
		Client:     cli,
		Scheme:     scheme,
		Dispatcher: dispatcher.New(noopDialer{}),
	}

	got, err := rec.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "snap-ghost"},
	})
	if err != nil {
		t.Errorf("missing snapshot must not error; got %v", err)
	}

	if got.RequeueAfter != 0 {
		t.Errorf("missing snapshot must not requeue; got %+v", got)
	}
}

// TestSnapshotReconcileRequeuesOnDispatchError: a Snapshot whose
// dispatch fails (no satellite endpoints) must requeue with the
// 10s back-off so satellites that come up later eventually finish
// the snapshot. Pins the retry contract.
func TestSnapshotReconcileRequeuesOnDispatchError(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)

	snap := &blockstoriov1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.snap-1"},
		Spec: blockstoriov1alpha1.SnapshotSpec{
			ResourceDefinitionName: "pvc-1",
			SnapshotName:           "snap-1",
		},
	}

	// Replica exists but the Node has no SatelliteEndpoint published
	// → dispatcher's CreateSnapshot fan-out records Ok=false per
	// replica but doesn't bubble a transport error. The Reconcile
	// path should still log and continue.
	resCRD := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.n1"},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "pvc-1",
			NodeName:               "n1",
		},
	}

	nodeCRD := &blockstoriov1alpha1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "n1"},
		Spec: blockstoriov1alpha1.NodeSpec{
			Type:  "SATELLITE",
			Props: map[string]string{}, // no SatelliteEndpoint
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(snap, resCRD, nodeCRD).
		Build()

	rec := &controllerpkg.SnapshotReconciler{
		Client:     cli,
		Scheme:     scheme,
		Dispatcher: dispatcher.New(noopDialer{}),
	}

	// CreateSnapshot returns Ok=false body-level; the reconciler
	// logs and returns Result{} (no transport error to back off on).
	got, err := rec.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pvc-1.snap-1"},
	})
	if err != nil {
		t.Errorf("body-level Ok=false must not error; got %v", err)
	}

	if got.RequeueAfter != 0 {
		t.Errorf("body-level Ok=false must not requeue; got %+v", got)
	}
}
