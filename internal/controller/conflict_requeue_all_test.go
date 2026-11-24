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
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	controllerpkg "github.com/cozystack/blockstor/internal/controller"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

func objectModifiedErr() error {
	return apierrors.NewConflict(schema.GroupResource{Group: "blockstor.org", Resource: "objects"}, "obj", errObjectModified)
}

// conflictingClient fails every read with a Conflict. A read does not
// conflict on a real apiserver; it is the first call of every pass
// below, and what the pass does with the error is all these tests
// look at.
func conflictingClient(t *testing.T) client.Client {
	t.Helper()

	return interceptor.NewClient(fake.NewClientBuilder().WithScheme(newScheme(t)).Build(), interceptor.Funcs{
		Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
			return objectModifiedErr()
		},
	})
}

// conflictingStore fails the controller props read and the definition
// list with a Conflict, the first store calls that return an error in
// the store-driven passes.
type conflictingStore struct {
	store.Store
}

func (s conflictingStore) ControllerProps() store.ControllerPropsStore {
	return conflictingProps{s.Store.ControllerProps()}
}

func (s conflictingStore) ResourceDefinitions() store.ResourceDefinitionStore {
	return conflictingRDs{s.Store.ResourceDefinitions()}
}

type conflictingProps struct {
	store.ControllerPropsStore
}

func (conflictingProps) Get(context.Context) (map[string]string, error) {
	return nil, objectModifiedErr()
}

type conflictingRDs struct {
	store.ResourceDefinitionStore
}

func (conflictingRDs) List(context.Context) ([]apiv1.ResourceDefinition, error) {
	return nil, objectModifiedErr()
}

func conflictingStoreWith(t *testing.T) store.Store {
	t.Helper()

	ctx := context.Background()
	st := store.NewInMemory()

	err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{Name: "rg-c"})
	if err != nil {
		t.Fatalf("create RG: %v", err)
	}

	err = st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-c", ResourceGroupName: "rg-c"})
	if err != nil {
		t.Fatalf("create RD: %v", err)
	}

	return conflictingStore{st}
}

// Every controller reconciler retries a pass that lost an
// optimistic-concurrency race rather than reporting it, so that
// controller-runtime does not log it as "Reconciler error" at ERROR.
func TestEveryControllerReconcilerRetriesAConflict(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		name string
		rec  func(t *testing.T) reconcile.Reconciler
	}{
		"migration": {"pvc-c.n1", func(t *testing.T) reconcile.Reconciler {
			t.Helper()

			return &controllerpkg.ResourceMigrationReconciler{Client: conflictingClient(t)}
		}},
		"snapshot": {"pvc-c.snap-c", func(t *testing.T) reconcile.Reconciler {
			t.Helper()

			return &controllerpkg.SnapshotReconciler{Client: conflictingClient(t)}
		}},
		"node heartbeat": {"n1", func(t *testing.T) reconcile.Reconciler {
			t.Helper()

			return &controllerpkg.NodeHeartbeatReconciler{Client: conflictingClient(t), Now: time.Now}
		}},
		"node": {"n1", func(t *testing.T) reconcile.Reconciler {
			t.Helper()

			return &controllerpkg.NodeReconciler{Client: conflictingClient(t), Store: store.NewInMemory()}
		}},
		"node label sync": {"n1", func(t *testing.T) reconcile.Reconciler {
			t.Helper()

			return &controllerpkg.NodeLabelSyncReconciler{Client: conflictingClient(t)}
		}},
		"resource state projection": {"pvc-c.n1", func(t *testing.T) reconcile.Reconciler {
			t.Helper()

			return &controllerpkg.ResourceStateProjectionReconciler{Client: conflictingClient(t)}
		}},
		"auto-diskful": {"pvc-c", func(t *testing.T) reconcile.Reconciler {
			t.Helper()

			return &controllerpkg.AutoDiskfulReconciler{Client: conflictingClient(t), Store: conflictingStoreWith(t)}
		}},
		"resource group": {"rg-c", func(t *testing.T) reconcile.Reconciler {
			t.Helper()

			return &controllerpkg.ResourceGroupReconciler{Client: conflictingClient(t), Store: conflictingStoreWith(t)}
		}},
		"rebalance": {"rg-c", func(t *testing.T) reconcile.Reconciler {
			t.Helper()

			return &controllerpkg.RGRebalanceReconciler{Client: conflictingClient(t), Store: conflictingStoreWith(t)}
		}},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			result, err := tc.rec(t).Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: tc.name}})
			if err != nil {
				t.Fatalf("reconcile: %v, want the conflict retried without an error", err)
			}

			if !result.Requeue { //nolint:staticcheck // the rate-limited retry an error would get
				t.Errorf("result %+v, want a requeue", result)
			}
		})
	}
}
