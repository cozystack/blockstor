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
	"maps"
	"slices"
	"sync"
	"testing"

	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	controllerpkg "github.com/cozystack/blockstor/internal/controller"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// midPassStore stands in for a client that changes a definition while
// a placement pass runs over it, the way linstor-csi switches balancing
// off on a volume right before it deletes the volume's replicas. The
// change lands once, either right after the pass has listed the
// definitions (so the pass holds a copy without it) or right after the
// placer created its first replica.
type midPassStore struct {
	store.Store

	t        *testing.T
	afterRD  bool
	change   func(rd *apiv1.ResourceDefinition)
	changeMu sync.Once

	mu      sync.Mutex
	created int
}

func (s *midPassStore) apply(ctx context.Context) {
	s.changeMu.Do(func() {
		rd, err := s.Store.ResourceDefinitions().Get(ctx, "pvc-interval")
		if err != nil {
			s.t.Errorf("get rd: %v", err)

			return
		}

		// A copy, so the pass's own listed copy keeps the old map.
		rd.Props = maps.Clone(rd.Props)
		if rd.Props == nil {
			rd.Props = map[string]string{}
		}

		rd.Flags = append([]string(nil), rd.Flags...)
		s.change(&rd)

		if err := s.Store.ResourceDefinitions().Update(ctx, &rd); err != nil {
			s.t.Errorf("change rd: %v", err)
		}
	})
}

func (s *midPassStore) ResourceDefinitions() store.ResourceDefinitionStore {
	return &midPassRDs{ResourceDefinitionStore: s.Store.ResourceDefinitions(), s: s}
}

func (s *midPassStore) Resources() store.ResourceStore {
	return &midPassResources{ResourceStore: s.Store.Resources(), s: s}
}

type midPassRDs struct {
	store.ResourceDefinitionStore

	s *midPassStore
}

func (r *midPassRDs) List(ctx context.Context) ([]apiv1.ResourceDefinition, error) {
	listed, err := r.ResourceDefinitionStore.List(ctx)
	if err == nil && r.s.afterRD {
		r.s.apply(ctx)
	}

	return listed, err
}

type midPassResources struct {
	store.ResourceStore

	s *midPassStore
}

func (r *midPassResources) Create(ctx context.Context, res *apiv1.Resource) error {
	err := r.ResourceStore.Create(ctx, res)
	if err == nil {
		r.s.mu.Lock()
		r.s.created++
		r.s.mu.Unlock()
	}

	if err == nil && !r.s.afterRD {
		r.s.apply(ctx)
	}

	return err
}

func switchBalancingOff(rd *apiv1.ResourceDefinition) {
	rd.Props[apiv1.PropBalanceResourcesEnabled] = "False"
}

func flagForDeletion(rd *apiv1.ResourceDefinition) {
	rd.Flags = append(rd.Flags, apiv1.ResourceFlagDelete)
}

// TestPlacementPassesHonourADefinitionChangedMidPass is the reported
// failure: linstor-csi deleted a volume while the scheduled pass of its
// group was running. The pass had listed the definitions before
// linstor-csi switched balancing off, checked its old copy, and placed
// a replica on the node linstor-csi had just deleted one from. That
// replica came after linstor-csi listed what to delete, so it was never
// deleted and kept the definition, a kernel resource and its ports.
// LINSTOR's balance task reads the definition under the lock the switch
// is written under, so it never places there.
//
// Each case changes the definition once, at a different point of the
// pass, and the definition must end with only the replica it had. A
// change that lands before the pass reaches the definition must keep
// the pass from creating anything at all, not just from keeping it:
// a replica created and deleted again still makes its satellite set up
// and tear down storage and a kernel resource.
func TestPlacementPassesHonourADefinitionChangedMidPass(t *testing.T) {
	t.Parallel()

	passes := map[string]func(t *testing.T, ctx context.Context, st store.Store){
		"scheduled rebalance": reconcileRebalance,
		"group event": func(t *testing.T, ctx context.Context, st store.Store) {
			t.Helper()

			scheme := newScheme(t)
			cli := fake.NewClientBuilder().WithScheme(scheme).Build()
			rec := &controllerpkg.ResourceGroupReconciler{Client: cli, Scheme: scheme, Store: st}

			if _, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "rg"}}); err != nil {
				t.Fatalf("Reconcile: %v", err)
			}
		},
	}

	changes := map[string]func(rd *apiv1.ResourceDefinition){
		"balancing switched off": switchBalancingOff,
		"definition deleted":     flagForDeletion,
	}

	for passName, pass := range passes {
		for changeName, change := range changes {
			for _, afterRD := range []bool{true, false} {
				when := "after the definitions are listed"
				if !afterRD {
					when = "after the first replica is created"
				}

				t.Run(passName+"/"+changeName+"/"+when, func(t *testing.T) {
					t.Parallel()

					ctx := context.Background()
					inner := store.NewInMemory()

					seedIntervalFixture(t, ctx, inner, 3, []string{"n1"})

					st := &midPassStore{Store: inner, t: t, afterRD: afterRD, change: change}

					pass(t, ctx, st)

					if n := replicaCount(t, ctx, inner, "pvc-interval"); n != 1 {
						t.Fatalf("got %d replicas; want only the one the definition had", n)
					}

					if afterRD && st.created != 0 {
						t.Fatalf("the pass created %d replicas on a definition it should have skipped", st.created)
					}
				})
			}
		}
	}
}

// TestPlacementPassStillTopsUpAnUnchangedDefinition: the extra reads do
// not stop a pass from filling a definition nobody touched.
func TestPlacementPassStillTopsUpAnUnchangedDefinition(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	inner := store.NewInMemory()

	seedIntervalFixture(t, ctx, inner, 3, []string{"n1"})

	st := &midPassStore{Store: inner, t: t, afterRD: true, change: func(*apiv1.ResourceDefinition) {}}

	reconcileRebalance(t, ctx, st)

	if n := replicaCount(t, ctx, inner, "pvc-interval"); n != 3 {
		t.Fatalf("got %d replicas; want 3", n)
	}
}

// TestPlacementPassTakesBackOnlyItsOwnReplicas: when balancing stops on
// a definition while a pass places on it, the pass deletes the replicas
// it created and nothing else. Here an operator switches balancing off
// and creates a replica by hand while the pass runs; LINSTOR keeps
// that replica, so the pass must too.
func TestPlacementPassTakesBackOnlyItsOwnReplicas(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	inner := store.NewInMemory()

	seedIntervalFixture(t, ctx, inner, 3, []string{"n1"})

	if err := inner.Nodes().Create(ctx, &apiv1.Node{
		Name: "n4", Type: apiv1.NodeTypeSatellite, ConnectionStatus: apiv1.NodeTypeOnline,
	}); err != nil {
		t.Fatalf("seed node n4: %v", err)
	}

	byHand := func(rd *apiv1.ResourceDefinition) {
		switchBalancingOff(rd)

		err := inner.Resources().Create(ctx, &apiv1.Resource{
			Name: "pvc-interval", NodeName: "n4", Flags: []string{apiv1.ResourceFlagDiskless},
		})
		if err != nil {
			t.Errorf("create the replica by hand: %v", err)
		}
	}

	st := &midPassStore{Store: inner, t: t, change: byHand}

	reconcileRebalance(t, ctx, st)

	got, err := inner.Resources().ListByDefinition(ctx, "pvc-interval")
	if err != nil {
		t.Fatalf("list replicas: %v", err)
	}

	nodes := make([]string, 0, len(got))
	for i := range got {
		nodes = append(nodes, got[i].NodeName)
	}

	slices.Sort(nodes)

	if !slices.Equal(nodes, []string{"n1", "n4"}) {
		t.Fatalf("replicas on %v; want the one the definition had and the one made by hand, [n1 n4]", nodes)
	}
}
