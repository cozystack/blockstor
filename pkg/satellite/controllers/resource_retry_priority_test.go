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

package controllers

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

var errTransientRead = errors.New("transient read failure")

// controller-runtime requeues an item at the priority it was dequeued
// with unless the Result names one. A low-priority re-check that finds
// something to repair must not keep that priority for the retries the
// repair needs, or each retry waits behind every other replica's idle
// re-check, which is the backlog the low priority was meant to remove.
func TestRetriesRunAtNormalPriority(t *testing.T) {
	t.Parallel()

	replica := func() *blockstoriov1alpha1.Resource {
		return &blockstoriov1alpha1.Resource{
			ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.node-1"},
			Spec:       blockstoriov1alpha1.ResourceSpec{NodeName: "node-1", ResourceDefinitionName: "pvc-1"},
		}
	}

	cases := map[string]interceptor.Funcs{
		// The finalizer write asks for another pass after a delay.
		"requeue after": {},
		// A failed read retries through the rate limiter.
		"error": {
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				return errTransientRead
			},
		},
		// A pass that lost a write race is retried by the conflict
		// Retrier, whose Result names no priority of its own.
		"conflict": {
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				return apierrors.NewConflict(schema.GroupResource{Resource: "resources"}, key.Name, errTransientRead)
			},
		},
	}

	for name, funcs := range cases {
		cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(replica()).WithInterceptorFuncs(funcs).Build()
		r := &ResourceReconciler{Client: cli, Config: Config{NodeName: "node-1"}}

		result, _ := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "pvc-1.node-1"}})
		if result.Priority == nil || *result.Priority != 0 {
			t.Errorf("%s: retry priority = %v, want normal (0)", name, result.Priority)
		}
	}
}
