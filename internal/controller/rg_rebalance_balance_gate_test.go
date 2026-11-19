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

	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	controllerpkg "github.com/cozystack/blockstor/internal/controller"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// reconcileRebalance runs one rebalance pass for "rg". Without the
// rebalance-pending annotation that is the scheduled tick the
// five-minute requeue drives.
func reconcileRebalance(t *testing.T, ctx context.Context, st store.Store) {
	t.Helper()

	scheme := newScheme(t)
	cli := fake.NewClientBuilder().WithScheme(scheme).Build()
	rec := &controllerpkg.RGRebalanceReconciler{Client: cli, Scheme: scheme, Store: st}

	if _, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "rg"}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
}

func replicaCount(t *testing.T, ctx context.Context, st store.Store, rd string) int {
	t.Helper()

	got, err := st.Resources().ListByDefinition(ctx, rd)
	if err != nil {
		t.Fatalf("list replicas of %q: %v", rd, err)
	}

	return len(got)
}

// TestScheduledRebalanceLeavesSnapshotOnlyDefinitionEmpty is the
// reported failure: a volume with a snapshot is deleted, every replica
// goes, and the definition stays because the snapshot needs it. The
// next tick read it as three replicas short and placed three fresh,
// empty ones. LINSTOR's balance task skips a definition with no
// UpToDate diskful replica, so it stays at zero until the last snapshot
// is deleted and the definition with it.
func TestScheduledRebalanceLeavesSnapshotOnlyDefinitionEmpty(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewInMemory()

	seedIntervalFixture(t, ctx, st, 3, nil)

	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name:         "snap",
		ResourceName: "pvc-interval",
		Nodes:        []string{"n1", "n2", "n3"},
	}); err != nil {
		t.Fatalf("seed snapshot: %v", err)
	}

	reconcileRebalance(t, ctx, st)

	if n := replicaCount(t, ctx, st, "pvc-interval"); n != 0 {
		t.Fatalf("a definition kept only by its snapshot got %d replicas; want 0", n)
	}
}

// TestScheduledRebalanceNeedsAnUpToDateDiskfulReplica: a replica counts
// as holding the data only when it has a disk, is not being deleted and
// reports every volume UpToDate. Replicas that fail any of those leave
// the definition out of the tick, however short of its place count.
func TestScheduledRebalanceNeedsAnUpToDateDiskfulReplica(t *testing.T) {
	t.Parallel()

	cases := map[string]apiv1.Resource{
		"inconsistent": {
			NodeName: "n1",
			Volumes:  []apiv1.Volume{{VolumeNumber: 0, State: apiv1.VolumeState{DiskState: "Inconsistent"}}},
		},
		"no volume state yet": {NodeName: "n1"},
		"being deleted": {
			NodeName: "n1", Flags: []string{apiv1.ResourceFlagDelete}, Volumes: upToDateVolumes(),
		},
		"delete handshake running": {
			NodeName: "n1", Flags: []string{apiv1.ResourceFlagDeleting}, Volumes: upToDateVolumes(),
		},
		"tiebreaker": {
			NodeName: "n1",
			Flags:    []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker},
			Volumes:  upToDateVolumes(),
		},
		"one volume behind": {
			NodeName: "n1",
			Volumes: []apiv1.Volume{
				{VolumeNumber: 0, State: apiv1.VolumeState{DiskState: "UpToDate"}},
				{VolumeNumber: 1, State: apiv1.VolumeState{DiskState: "Outdated"}},
			},
		},
	}

	for name, replica := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			st := store.NewInMemory()

			seedIntervalFixture(t, ctx, st, 3, nil)

			replica.Name = "pvc-interval"
			if err := st.Resources().Create(ctx, &replica); err != nil {
				t.Fatalf("seed replica: %v", err)
			}

			reconcileRebalance(t, ctx, st)

			if n := replicaCount(t, ctx, st, "pvc-interval"); n != 1 {
				t.Fatalf("got %d replicas; want the seeded one only", n)
			}
		})
	}
}

// TestScheduledRebalanceSkipsDeletingDefinition: LINSTOR's balance task
// skips a definition flagged for deletion even when it still has an
// UpToDate replica.
func TestScheduledRebalanceSkipsDeletingDefinition(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewInMemory()

	seedIntervalFixture(t, ctx, st, 3, []string{"n1"})

	rd, err := st.ResourceDefinitions().Get(ctx, "pvc-interval")
	if err != nil {
		t.Fatalf("get rd: %v", err)
	}

	rd.Flags = []string{apiv1.ResourceFlagDelete}
	if err := st.ResourceDefinitions().Update(ctx, &rd); err != nil {
		t.Fatalf("flag rd for deletion: %v", err)
	}

	reconcileRebalance(t, ctx, st)

	if n := replicaCount(t, ctx, st, "pvc-interval"); n != 1 {
		t.Fatalf("a definition being deleted got %d replicas; want 1", n)
	}
}

// TestScheduledRebalanceTopsUpFromAnUpToDateReplica pins the other side:
// one UpToDate diskful replica is enough for the tick to fill the gap.
func TestScheduledRebalanceTopsUpFromAnUpToDateReplica(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewInMemory()

	seedIntervalFixture(t, ctx, st, 3, []string{"n1"})

	reconcileRebalance(t, ctx, st)

	if n := replicaCount(t, ctx, st, "pvc-interval"); n != 3 {
		t.Fatalf("got %d replicas; want 3", n)
	}
}

// TestRGModifyStillPlacesOnEmptyDefinition: the rebalance-pending
// annotation stands for `rg modify`, which LINSTOR runs the autoplacer
// for on every child definition with fewer replicas than the new count,
// empty ones included. The balance-task checks do not apply to it.
func TestRGModifyStillPlacesOnEmptyDefinition(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewInMemory()

	seedRebalanceFixture(t, ctx, st, 3, nil)

	reconcileRebalance(t, ctx, st)

	if n := replicaCount(t, ctx, st, "pvc-rebalance"); n != 3 {
		t.Fatalf("got %d replicas after rg modify; want 3", n)
	}
}
