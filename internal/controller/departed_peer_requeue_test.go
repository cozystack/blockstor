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
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	controllerpkg "github.com/cozystack/blockstor/internal/controller"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// The witness waits while a survivor's kernel still holds the node that
// just lost its replica. The survivor dropping that connection is a
// status write, which does not wake the reconciler, so the pass that
// held the witness back comes back by itself.
func TestReconcileComesBackForAWitnessHeldByADepartedPeer(t *testing.T) {
	t.Parallel()

	const rdName = "pvc-departed-requeue"

	ctx := t.Context()
	scheme := newScheme(t)
	st := store.NewInMemory()

	seedNodes(t, ctx, st, "n1", "n2", "n3")

	for _, n := range []string{"n1", "n2"} {
		res := apiv1.Resource{Name: rdName, NodeName: n}
		withKernelPeers(&res, "n1", "n2", "n3")

		if err := st.Resources().Create(ctx, &res); err != nil {
			t.Fatalf("seed diskful %s: %v", n, err)
		}
	}

	rd := &blockstoriov1alpha1.ResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: rdName}}
	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rd).Build()
	rec := &controllerpkg.ResourceDefinitionReconciler{Client: cli, Scheme: scheme, Store: st}

	result, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: rdName}})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if _, err := st.Resources().Get(ctx, rdName, "n3"); err == nil {
		t.Fatalf("witness placed on n3 while survivors still hold its old connection")
	}

	if result.RequeueAfter <= 0 {
		t.Errorf("RequeueAfter = %v, want a requeue while the departed peer holds the witness back", result.RequeueAfter)
	}
}
