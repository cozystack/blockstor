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

	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	controllerpkg "github.com/cozystack/blockstor/internal/controller"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestResourceGroupReconcileNilStore: bootstrap-time. envtest
// scaffolding may construct the reconciler without a Store —
// keep the no-op so the boilerplate suite stays green.
func TestResourceGroupReconcileNilStore(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()

	rec := &controllerpkg.ResourceGroupReconciler{
		Client: cli,
		Scheme: scheme,
		// Store: nil intentionally.
	}

	got, err := rec.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "rg-1"},
	})
	if err != nil {
		t.Errorf("nil store must not error; got %v", err)
	}

	if got.RequeueAfter != 0 {
		t.Errorf("nil store must not requeue; got %+v", got)
	}
}

// TestResourceGroupReconcileMissingRG: Reconcile invoked for an RG
// that's already been deleted (CRD GC raced us) → silent Result{}.
// Child RDs handle their own lifecycle; missing RG isn't an error
// worth requeueing.
func TestResourceGroupReconcileMissingRG(t *testing.T) {
	t.Parallel()

	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()

	rec := &controllerpkg.ResourceGroupReconciler{
		Client: cli,
		Scheme: scheme,
		Store:  store.NewInMemory(),
	}

	got, err := rec.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "ghost-rg"},
	})
	if err != nil {
		t.Errorf("missing RG must not error; got %v", err)
	}

	if got.RequeueAfter != 0 {
		t.Errorf("missing RG must not requeue; got %+v", got)
	}
}

// TestResourceGroupReconcileEmptyChildList: an RG with no spawned
// RDs (e.g. just-created template) reconciles cleanly — the placer
// is never called, no requeue. Pins the cheap-path that runs on
// every RG watch event before any RDs are spawned.
func TestResourceGroupReconcileEmptyChildList(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := newScheme(t)
	st := store.NewInMemory()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "rg-fresh",
		SelectFilter: apiv1.AutoSelectFilter{
			PlaceCount: 2,
		},
	}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	cli := fake.NewClientBuilder().WithScheme(scheme).Build()

	rec := &controllerpkg.ResourceGroupReconciler{
		Client: cli,
		Scheme: scheme,
		Store:  st,
	}

	got, err := rec.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "rg-fresh"},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if got.RequeueAfter != 0 {
		t.Errorf("empty RG must not requeue; got %+v", got)
	}
}
