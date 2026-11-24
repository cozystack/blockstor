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
	"sync/atomic"
	"testing"

	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	controllerpkg "github.com/cozystack/blockstor/internal/controller"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// reconcileRGEvent runs the RG reconciler once for "rg", as any RG
// watch event does, the initial list on a controller start included.
func reconcileRGEvent(t *testing.T, ctx context.Context, st store.Store) {
	t.Helper()

	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()
	rec := &controllerpkg.ResourceGroupReconciler{Client: cli, Scheme: scheme, Store: st}

	if _, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "rg"}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
}

// TestRGEventHonoursBalanceResourcesEnabled: linstor-csi deleting a
// volume with a snapshot writes BalanceResourcesEnabled=False on the
// definition and removes the replicas one by one. A controller restart
// in that window replays every RG; the event must not give the
// definition a new replica, which would resync the volume being deleted
// and outlive csi's own replica list. The switch is read on the
// definition, the group and the controller, as the balance task reads
// it.
func TestRGEventHonoursBalanceResourcesEnabled(t *testing.T) {
	t.Parallel()

	off := map[string]string{apiv1.PropBalanceResourcesEnabled: "False"}

	cases := []struct {
		name                string
		rdProps, rgProps    map[string]string
		ctrlProps           map[string]string
		wantReplicasOnEvent int
	}{
		{name: "definition", rdProps: off, wantReplicasOnEvent: 2},
		{name: "group", rgProps: off, wantReplicasOnEvent: 2},
		{name: "controller", ctrlProps: off, wantReplicasOnEvent: 2},
		{name: "unset", wantReplicasOnEvent: 3},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			st := store.NewInMemory()

			seedIntervalFixture(t, ctx, st, 3, []string{"n1", "n2"})

			rd, err := st.ResourceDefinitions().Get(ctx, "pvc-interval")
			if err != nil {
				t.Fatalf("get rd: %v", err)
			}

			rd.Props = tc.rdProps
			if err := st.ResourceDefinitions().Update(ctx, &rd); err != nil {
				t.Fatalf("set rd props: %v", err)
			}

			rg, err := st.ResourceGroups().Get(ctx, "rg")
			if err != nil {
				t.Fatalf("get rg: %v", err)
			}

			rg.Props = tc.rgProps
			if err := st.ResourceGroups().Update(ctx, &rg); err != nil {
				t.Fatalf("set rg props: %v", err)
			}

			if tc.ctrlProps != nil {
				if err := st.ControllerProps().Set(ctx, tc.ctrlProps); err != nil {
					t.Fatalf("set controller props: %v", err)
				}
			}

			reconcileRGEvent(t, ctx, st)

			if n := replicaCount(t, ctx, st, "pvc-interval"); n != tc.wantReplicasOnEvent {
				t.Fatalf("got %d replicas after an RG event; want %d", n, tc.wantReplicasOnEvent)
			}
		})
	}
}

// TestRGEventIgnoresUpToDateOnOfflineNode: a replica on an offline node
// keeps the DiskState its satellite last sent. LINSTOR holds no state
// for an offline peer, so that replica does not show the definition
// still has its data, and no replica is placed that would have nothing
// to sync from.
func TestRGEventIgnoresUpToDateOnOfflineNode(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewInMemory()

	seedIntervalFixture(t, ctx, st, 3, []string{"n1"})

	if err := st.Nodes().SetConnectionStatus(ctx, "n1", apiv1.NodeTypeOffline); err != nil {
		t.Fatalf("take n1 offline: %v", err)
	}

	reconcileRGEvent(t, ctx, st)

	if n := replicaCount(t, ctx, st, "pvc-interval"); n != 1 {
		t.Fatalf("got %d replicas; want only the one on the offline node", n)
	}
}

// TestRGEventReadsDiskStateWithoutCase: LINSTOR compares the disk state
// to UpToDate without regard to case.
func TestRGEventReadsDiskStateWithoutCase(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewInMemory()

	seedIntervalFixture(t, ctx, st, 3, nil)

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name:     "pvc-interval",
		NodeName: "n1",
		Volumes:  []apiv1.Volume{{VolumeNumber: 0, State: apiv1.VolumeState{DiskState: "uptodate"}}},
	}); err != nil {
		t.Fatalf("seed replica: %v", err)
	}

	reconcileRGEvent(t, ctx, st)

	if n := replicaCount(t, ctx, st, "pvc-interval"); n != 3 {
		t.Fatalf("got %d replicas; want 3", n)
	}
}

// TestRGModifyConstraintOnlyLeavesSnapshotOnlyDefinitionEmpty: an `rg
// modify` that changes a placement constraint and not the place count
// still schedules a rebalance, but LINSTOR places nothing for it. A
// definition kept only by its snapshots must stay at zero replicas.
func TestRGModifyConstraintOnlyLeavesSnapshotOnlyDefinitionEmpty(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewInMemory()

	seedRebalanceFixture(t, ctx, st, 3, nil)

	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name: "snap", ResourceName: "pvc-rebalance", Nodes: []string{"n1", "n2", "n3"},
	}); err != nil {
		t.Fatalf("seed snapshot: %v", err)
	}

	rg, err := st.ResourceGroups().Get(ctx, "rg")
	if err != nil {
		t.Fatalf("get rg: %v", err)
	}

	delete(rg.Annotations, apiv1.AnnotationRGRebalancePlaceCount)

	if err := st.ResourceGroups().Update(ctx, &rg); err != nil {
		t.Fatalf("drop the place-count marker: %v", err)
	}

	reconcileRebalance(t, ctx, st)

	if n := replicaCount(t, ctx, st, "pvc-rebalance"); n != 0 {
		t.Fatalf("a constraint-only rg modify gave a snapshot-only definition %d replicas; want 0", n)
	}

	got, err := st.ResourceGroups().Get(ctx, "rg")
	if err != nil {
		t.Fatalf("get rg: %v", err)
	}

	if _, ok := got.Annotations[apiv1.AnnotationRGRebalancePending]; ok {
		t.Fatalf("rebalance-pending not stripped: %v", got.Annotations)
	}
}

// TestRGModifyPlaceCountRaiseSkipsDeletingDefinition: a raised place
// count tops every child definition up, as LINSTOR's `rg modify` does,
// except one being deleted, which no placement may grow.
func TestRGModifyPlaceCountRaiseSkipsDeletingDefinition(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewInMemory()

	seedRebalanceFixture(t, ctx, st, 3, []string{"n1"})

	rd, err := st.ResourceDefinitions().Get(ctx, "pvc-rebalance")
	if err != nil {
		t.Fatalf("get rd: %v", err)
	}

	rd.Flags = []string{apiv1.ResourceFlagDelete}
	if err := st.ResourceDefinitions().Update(ctx, &rd); err != nil {
		t.Fatalf("flag rd for deletion: %v", err)
	}

	reconcileRebalance(t, ctx, st)

	if n := replicaCount(t, ctx, st, "pvc-rebalance"); n != 1 {
		t.Fatalf("a definition being deleted got %d replicas from rg modify; want 1", n)
	}
}

// countingStore counts the replica reads that go past the cache.
type countingStore struct {
	store.Store

	uncached *atomic.Int32
}

func (s countingStore) Resources() store.ResourceStore {
	return countingResources{ResourceStore: s.Store.Resources(), uncached: s.uncached}
}

type countingResources struct {
	store.ResourceStore

	uncached *atomic.Int32
}

func (r countingResources) ListByDefinitionUncached(ctx context.Context, rdName string) ([]apiv1.Resource, error) {
	r.uncached.Add(1)

	return r.ListByDefinition(ctx, rdName)
}

// TestRGEventReadsPastTheCacheOnlyToConfirm: an RG event reaches every
// child definition, on every controller start. A definition the cache
// already shows without an UpToDate replica is skipped on the cached
// read alone; only one the cache shows with data is re-read past the
// cache, since that is the answer a stale cache can get wrong.
func TestRGEventReadsPastTheCacheOnlyToConfirm(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	inner := store.NewInMemory()

	seedIntervalFixture(t, ctx, inner, 3, nil)

	uncached := &atomic.Int32{}
	st := countingStore{Store: inner, uncached: uncached}

	reconcileRGEvent(t, ctx, st)

	if n := uncached.Load(); n != 0 {
		t.Fatalf("%d uncached replica reads for a definition the cache shows empty; want 0", n)
	}

	if err := inner.Resources().Create(ctx, &apiv1.Resource{
		Name: "pvc-interval", NodeName: "n1", Volumes: upToDateVolumes(),
	}); err != nil {
		t.Fatalf("seed replica: %v", err)
	}

	reconcileRGEvent(t, ctx, st)

	if n := uncached.Load(); n == 0 {
		t.Fatal("a definition the cache shows with data was placed on without a read past the cache")
	}
}
