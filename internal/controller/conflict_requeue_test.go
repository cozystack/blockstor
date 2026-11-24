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

package controller_test

import (
	"context"
	"errors"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
	"github.com/cozystack/blockstor/pkg/store"
)

var errObjectModified = errors.New("the object has been modified; please apply your changes to the latest version and try again")

func rdGroupResource() schema.GroupResource {
	return schema.GroupResource{Group: "blockstor.org", Resource: "resourcedefinitions"}
}

// patchRDWith fails the first RD patch of the pass (the layer-stack
// pin) with what race returns.
func patchRDWith(race func(ctx context.Context, c client.WithWatch, obj client.Object) error) interceptor.Funcs {
	raced := false

	return interceptor.Funcs{
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if _, ok := obj.(*blockstoriov1alpha1.ResourceDefinition); ok && !raced {
				raced = true

				return race(ctx, c, obj)
			}

			return c.Patch(ctx, obj, patch, opts...)
		},
	}
}

func reconcileRD(t *testing.T, cli client.Client) (ctrl.Result, error) {
	t.Helper()

	rec := &controllerpkg.ResourceDefinitionReconciler{
		Client:   cli,
		Scheme:   newScheme(t),
		Store:    store.NewInMemory(),
		Recorder: events.NewFakeRecorder(10),
	}

	return rec.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "pvc-pin"}})
}

// A pass that started before its RD was deleted finds the RD gone on its
// first write. There is nothing left to do for a deleted definition, so
// the pass ends without an error; returned, it was logged at ERROR right
// after every volume delete.
func TestRDPassThatRacedItsDeletionEndsQuietly(t *testing.T) {
	t.Parallel()

	cli := interceptor.NewClient(pinClient(t, pinGroup(luksStack...), pinRD("rg-pin")),
		patchRDWith(func(ctx context.Context, c client.WithWatch, obj client.Object) error {
			if err := c.Delete(ctx, obj); err != nil {
				return err
			}

			return apierrors.NewNotFound(rdGroupResource(), obj.GetName())
		}))

	result, err := reconcileRD(t, cli)
	if err != nil {
		t.Fatalf("reconcile: %v, want none for a deleted RD", err)
	}

	if result != (ctrl.Result{}) {
		t.Errorf("result %+v, want the pass over", result)
	}
}

// A NotFound while the RD still exists is a real failure.
func TestRDPassReportsANotFoundOfALiveRD(t *testing.T) {
	t.Parallel()

	cli := interceptor.NewClient(pinClient(t, pinGroup(luksStack...), pinRD("rg-pin")),
		patchRDWith(func(_ context.Context, _ client.WithWatch, obj client.Object) error {
			return apierrors.NewNotFound(rdGroupResource(), obj.GetName())
		}))

	_, err := reconcileRD(t, cli)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("reconcile: %v, want the NotFound", err)
	}
}

// A controller write that lost an optimistic-concurrency race is
// retried, not reported, for a Resource as for an RD.
func TestControllerRetriesAWriteThatLostARace(t *testing.T) {
	t.Parallel()

	res := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-c.n1", Finalizers: []string{"blockstor.org/resource"}},
		Spec:       blockstoriov1alpha1.ResourceSpec{ResourceDefinitionName: "pvc-c", NodeName: "n1"},
	}
	scheme := newScheme(t)
	cli := interceptor.NewClient(fake.NewClientBuilder().WithScheme(scheme).WithObjects(res).Build(), interceptor.Funcs{
		Update: func(_ context.Context, _ client.WithWatch, obj client.Object, _ ...client.UpdateOption) error {
			return apierrors.NewConflict(schema.GroupResource{Group: "blockstor.org", Resource: "resources"}, obj.GetName(), errObjectModified)
		},
	})

	rec := &controllerpkg.ResourceReconciler{Client: cli, Scheme: scheme}

	result, err := rec.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "pvc-c.n1"}})
	if err != nil {
		t.Fatalf("reconcile: %v, want the conflict retried without an error", err)
	}

	if !result.Requeue { //nolint:staticcheck // the rate-limited retry an error would get
		t.Errorf("result %+v, want a requeue", result)
	}
}
