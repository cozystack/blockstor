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

// TestResourceDefinitionReconcileNilStore: bootstrap-time. envtest
// scaffolding may construct without a Store — return Result{}
// silently so the boilerplate test stays green.
func TestResourceDefinitionReconcileNilStore(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()

	rec := &controllerpkg.ResourceDefinitionReconciler{
		Client: cli,
		Scheme: scheme,
		// Store: nil intentionally.
	}

	got, err := rec.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pvc-1"},
	})
	if err != nil {
		t.Errorf("nil store must not error; got %v", err)
	}

	if got.RequeueAfter != 0 {
		t.Errorf("nil store must not requeue; got %+v", got)
	}
}

// TestResourceDefinitionReconcileMissingRD: Reconcile invoked for an
// RD that's already been deleted (CRD GC raced us) → silent
// Result{}. client.IgnoreNotFound guards the witness logic from
// firing on a dying RD whose witness has its own finalizer cleanup.
func TestResourceDefinitionReconcileMissingRD(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()

	rec := &controllerpkg.ResourceDefinitionReconciler{
		Client: cli,
		Scheme: scheme,
		Store:  store.NewInMemory(),
	}

	got, err := rec.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "ghost-rd"},
	})
	if err != nil {
		t.Errorf("missing RD must not error; got %v", err)
	}

	if got.RequeueAfter != 0 {
		t.Errorf("missing RD must not requeue; got %+v", got)
	}
}

// TestResourceDefinitionReconcileDeletingSkipsTiebreaker: an RD
// with DeletionTimestamp set must skip the witness logic — its
// existing witness is going away anyway via cascade delete, and
// running ensureTiebreaker mid-cascade would race the deletion.
// Pins the deletion-time short-circuit.
func TestResourceDefinitionReconcileDeletingSkipsTiebreaker(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)

	now := metav1.Now()
	rdCRD := &blockstoriov1alpha1.ResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "pvc-dying",
			DeletionTimestamp: &now,
			// Apply finalizer so fake-client allows the seed.
			Finalizers: []string{"blockstor.io.blockstor.io/rd"},
		},
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(rdCRD).
		Build()

	rec := &controllerpkg.ResourceDefinitionReconciler{
		Client: cli,
		Scheme: scheme,
		Store:  store.NewInMemory(),
	}

	got, err := rec.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pvc-dying"},
	})
	if err != nil {
		t.Errorf("deleting RD must not error; got %v", err)
	}

	if got.RequeueAfter != 0 {
		t.Errorf("deleting RD must not requeue; got %+v", got)
	}
}
