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
	"github.com/cozystack/blockstor/pkg/store"
)

// TestNodeReconcileNilStore: a Reconciler started without a Store
// (envtest scaffolding before the cluster wiring lands) must NOT
// crash on reconcile — return Result{} silently.
func TestNodeReconcileNilStore(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()

	rec := &controllerpkg.NodeReconciler{
		Client: cli,
		Scheme: scheme,
		// Store: nil intentionally.
	}

	got, err := rec.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "n1"},
	})
	if err != nil {
		t.Errorf("nil store must not error; got %v", err)
	}

	if got.RequeueAfter != 0 {
		t.Errorf("nil store must not requeue; got %+v", got)
	}
}

// TestNodeReconcileMissingNode: Reconcile invoked for a Node that's
// already been deleted (CRD GC raced us) must return Result{} silently.
// Without this branch the Node reconciler logs spurious errors during
// normal cluster scale-down.
func TestNodeReconcileMissingNode(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()

	rec := &controllerpkg.NodeReconciler{
		Client: cli,
		Scheme: scheme,
		Store:  store.NewInMemory(),
	}

	got, err := rec.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "ghost-node"},
	})
	if err != nil {
		t.Errorf("missing node must not error; got %v", err)
	}

	if got.RequeueAfter != 0 {
		t.Errorf("missing node must not requeue; got %+v", got)
	}
}

// TestNodeReconcileHealthyNodeIsNoOp: a Node with neither EVICTED
// nor LOST flag must short-circuit immediately — no Resource list,
// no migration. Pins the cheap-path that runs on every Node watch
// event during normal cluster operation.
func TestNodeReconcileHealthyNodeIsNoOp(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)

	nodeCRD := &blockstoriov1alpha1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "n1"},
		Spec: blockstoriov1alpha1.NodeSpec{
			Type: "SATELLITE",
			// No flags — healthy node.
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(nodeCRD).
		Build()

	rec := &controllerpkg.NodeReconciler{
		Client: cli,
		Scheme: scheme,
		Store:  store.NewInMemory(),
	}

	got, err := rec.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "n1"},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if got.RequeueAfter != 0 {
		t.Errorf("healthy node must not requeue (cheap-path no-op); got %+v", got)
	}
}
