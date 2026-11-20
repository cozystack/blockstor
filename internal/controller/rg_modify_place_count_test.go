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
	"slices"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestRGModifyPlaceCountCountsEveryReplicaButTiebreakers pins which
// children an `rg modify` that names a place count places on. LINSTOR
// counts a child's replicas other than tiebreakers, diskless ones and
// ones being deleted included, and runs the autoplacer only while that
// count is below the place count. The autoplacer itself then counts
// only the diskful replicas, so a child let through is topped up to
// the place count in diskful replicas.
func TestRGModifyPlaceCountCountsEveryReplicaButTiebreakers(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		placeCount  int32
		existing    []string
		third       *apiv1.Resource
		wantRows    int
		wantDiskful int
	}{
		{
			name:       "a diskless replica counts",
			placeCount: 3,
			third:      &apiv1.Resource{NodeName: "n3", Flags: []string{apiv1.ResourceFlagDiskless}},
			wantRows:   3, wantDiskful: 2,
		},
		{
			name:       "a replica being deleted counts",
			placeCount: 3,
			third:      &apiv1.Resource{NodeName: "n3", Flags: []string{apiv1.ResourceFlagDelete}},
			wantRows:   3, wantDiskful: 3,
		},
		{
			name:       "a tiebreaker does not count",
			placeCount: 3,
			third: &apiv1.Resource{
				NodeName: "n3", Flags: []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker},
			},
			wantDiskful: 3,
		},
		{
			name:       "a lowered place count still tops up a child below it",
			placeCount: 2,
			existing:   []string{"n1"},
			wantRows:   2, wantDiskful: 2,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			st := store.NewInMemory()

			replicas := tc.existing
			if replicas == nil {
				replicas = []string{"n1", "n2"}
			}

			seedBug040RebalanceFixture(t, ctx, st, tc.placeCount, nil, nil, replicas)

			if tc.third != nil {
				tc.third.Name = "pvc-bug040-rebalance"
				if err := st.Resources().Create(ctx, tc.third); err != nil {
					t.Fatalf("seed third replica: %v", err)
				}
			}

			byNode := runBug040Rebalance(t, ctx, st)

			if tc.wantRows != 0 && len(byNode) != tc.wantRows {
				t.Errorf("got %d replicas (%v); want %d", len(byNode), byNode, tc.wantRows)
			}

			if n := diskfulReplicas(byNode); n != tc.wantDiskful {
				t.Errorf("got %d diskful replicas (%v); want %d", n, byNode, tc.wantDiskful)
			}
		})
	}
}

// TestRGModifyPlaceCountPlacesOnSnapshotOnlyDefinition: the case the
// balance checks exist for, seen from `rg modify`. A definition whose
// volume was deleted while a snapshot keeps it has no replica at all.
// LINSTOR's balance task leaves it alone for want of an UpToDate
// replica, but its `rg modify` with a place count counts zero replicas,
// which is below any place count, and autoplaces a full set. blockstor
// does the same, with balancing on and off.
func TestRGModifyPlaceCountPlacesOnSnapshotOnlyDefinition(t *testing.T) {
	t.Parallel()

	for _, disabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "balancing on", true: "balancing off"}[disabled], func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			st := store.NewInMemory()

			seedRebalanceFixture(t, ctx, st, 3, nil)

			if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
				Name: "snap", ResourceName: "pvc-rebalance", Nodes: []string{"n1", "n2", "n3"},
			}); err != nil {
				t.Fatalf("seed snapshot: %v", err)
			}

			if disabled {
				if err := st.ControllerProps().Set(ctx, map[string]string{apiv1.PropBalanceResourcesEnabled: "false"}); err != nil {
					t.Fatalf("set controller props: %v", err)
				}
			}

			reconcileRebalance(t, ctx, st)

			if n := replicaCount(t, ctx, st, "pvc-rebalance"); n != 3 {
				t.Fatalf("got %d replicas after rg modify --place-count 3; want 3", n)
			}
		})
	}
}

func diskfulReplicas(byNode map[string]apiv1.Resource) int {
	n := 0

	for _, res := range byNode {
		if !slices.Contains(res.Flags, apiv1.ResourceFlagDiskless) {
			n++
		}
	}

	return n
}
