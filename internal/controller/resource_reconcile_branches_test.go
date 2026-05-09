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
	"slices"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
	"github.com/cozystack/blockstor/pkg/dispatcher"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestResourceReconcileNilDispatcher: bootstrap-time path. envtest
// suite_test.go constructs the reconciler without a Dispatcher
// before the gRPC wiring lands — keep the no-op so the boilerplate
// test stays green.
func TestResourceReconcileNilDispatcher(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()

	rec := &controllerpkg.ResourceReconciler{
		Client: cli,
		Scheme: scheme,
		// Dispatcher: nil intentionally.
	}

	got, err := rec.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pvc-1.n1"},
	})
	if err != nil {
		t.Errorf("nil dispatcher must not error; got %v", err)
	}

	if got.RequeueAfter != 0 {
		t.Errorf("nil dispatcher must not requeue; got %+v", got)
	}
}

// TestResourceReconcileMissingResource: Reconcile invoked for a
// Resource that's already been deleted (CRD GC raced us) → silent
// Result{}. Pins idempotent finalizer cleanup.
func TestResourceReconcileMissingResource(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()

	rec := &controllerpkg.ResourceReconciler{
		Client:     cli,
		Scheme:     scheme,
		Dispatcher: dispatcher.New(noopDialer{}),
		Store:      store.NewInMemory(),
	}

	got, err := rec.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "ghost.n1"},
	})
	if err != nil {
		t.Errorf("missing resource must not error; got %v", err)
	}

	if got.RequeueAfter != 0 {
		t.Errorf("missing resource must not requeue; got %+v", got)
	}
}

// TestResourceReconcileAddsFinalizer: a fresh Resource without our
// finalizer must get one stamped on the first reconcile pass and
// then requeue so the next pass actually runs runApply. Pins the
// "always have a finalizer when alive" invariant — without it the
// delete path can't engage when DeletionTimestamp lands.
func TestResourceReconcileAddsFinalizer(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)

	resCRD := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-fin.n1"},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "pvc-fin",
			NodeName:               "n1",
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(resCRD).
		Build()

	rec := &controllerpkg.ResourceReconciler{
		Client:     cli,
		Scheme:     scheme,
		Dispatcher: dispatcher.New(noopDialer{}),
		Store:      store.NewInMemory(),
	}

	got, err := rec.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pvc-fin.n1"},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	// First pass requests a requeue so the next pass picks up the
	// finalizer-stamped object.
	if !got.Requeue {
		t.Errorf("first-pass must requeue after stamping finalizer; got %+v", got)
	}

	post := &blockstoriov1alpha1.Resource{}
	if err := cli.Get(context.Background(),
		types.NamespacedName{Name: "pvc-fin.n1"}, post); err != nil {
		t.Fatalf("Get: %v", err)
	}

	if !slices.Contains(post.Finalizers, "blockstor.io.blockstor.io/resource") {
		t.Errorf("finalizer not stamped: %v", post.Finalizers)
	}
}
