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
	"errors"
	"slices"
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

// A source deleted while its clone ran is kept by the clone's internal
// snapshot, marked to be deleted once empty (see the truncate handler). The
// clone's delete reaps that snapshot, and the satellite drains it after the
// clone's own volumes are gone; nobody asks to delete the source again.
// Before the definition controller finished the request, the source stayed
// forever as an empty definition. One that is still kept waits, and one in
// use again drops the request.
func TestDefinitionMarkedDeleteWhenEmptyGoesOnceEmpty(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		marked      bool
		seed        func(t *testing.T, st store.Store)
		wantDeleted bool
		wantMarked  bool
	}{
		"marked and empty": {marked: true, wantDeleted: true},
		"not marked": {
			marked: false,
		},
		"clone's snapshot still there": {
			marked:     true,
			wantMarked: true,
			seed: func(t *testing.T, st store.Store) {
				t.Helper()

				seedSnapshot(t, st, map[string]string{store.CloneSnapshotOwnerProp: "pvc-clone"})
			},
		},
		"a replica of its own again": {
			marked: true,
			seed: func(t *testing.T, st store.Store) {
				t.Helper()

				if err := st.Resources().Create(t.Context(), &apiv1.Resource{Name: "pvc-src", NodeName: "n1"}); err != nil {
					t.Fatalf("seed replica: %v", err)
				}
			},
		},
		"a snapshot of its own": {
			marked: true,
			seed: func(t *testing.T, st store.Store) {
				t.Helper()

				seedSnapshot(t, st, nil)
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			scheme := newScheme(t)
			st := store.NewInMemory()

			props := map[string]string{}
			if tc.marked {
				props[store.DeleteWhenEmptyProp] = "true"
			}

			if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-src", Props: props}); err != nil {
				t.Fatalf("seed definition: %v", err)
			}

			if tc.seed != nil {
				tc.seed(t, st)
			}

			rd := &blockstoriov1alpha1.ResourceDefinition{
				ObjectMeta: metav1.ObjectMeta{Name: "pvc-src"},
				Spec:       blockstoriov1alpha1.ResourceDefinitionSpec{Props: props},
			}
			cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rd).Build()
			rec := &controllerpkg.ResourceDefinitionReconciler{Client: cli, Scheme: scheme, Store: st}

			if _, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "pvc-src"}}); err != nil {
				t.Fatalf("Reconcile: %v", err)
			}

			got, err := st.ResourceDefinitions().Get(ctx, "pvc-src")
			if deleted := errors.Is(err, store.ErrNotFound); deleted != tc.wantDeleted {
				t.Fatalf("deleted = %v (err %v), want %v", deleted, err, tc.wantDeleted)
			}

			if tc.wantDeleted {
				return
			}

			if marked := got.Props[store.DeleteWhenEmptyProp] != ""; marked != tc.wantMarked {
				t.Errorf("marked = %v, want %v", marked, tc.wantMarked)
			}

			if slices.Contains(got.Flags, apiv1.ResourceFlagDelete) {
				t.Errorf("a definition that stays is left marked DELETE: %v", got.Flags)
			}
		})
	}
}

func seedSnapshot(t *testing.T, st store.Store, props map[string]string) {
	t.Helper()

	if err := st.Snapshots().Create(t.Context(), &apiv1.Snapshot{
		Name: "snap-1", ResourceName: "pvc-src", Nodes: []string{"n1"}, Props: props,
	}); err != nil {
		t.Fatalf("seed snapshot: %v", err)
	}
}
