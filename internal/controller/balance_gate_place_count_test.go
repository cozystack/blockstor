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
	"github.com/cozystack/blockstor/pkg/naming"
	"github.com/cozystack/blockstor/pkg/store"
)

// gatedPaths are the placements nobody asked for that the balance gate
// guards: the scheduled tick and an RG watch event.
var gatedPaths = map[string]func(*testing.T, context.Context, store.Store){
	"scheduled tick": reconcileRebalance,
	"RG event":       reconcileRGEvent,
}

// seedFourthNode adds an online node n4 with a pool to the interval
// fixture, so a pass has somewhere to put a replica none of n1..n3 can
// take.
func seedFourthNode(t *testing.T, ctx context.Context, st store.Store) {
	t.Helper()

	if err := st.Nodes().Create(ctx, &apiv1.Node{
		Name:             "n4",
		Type:             apiv1.NodeTypeSatellite,
		ConnectionStatus: apiv1.NodeTypeOnline,
	}); err != nil {
		t.Fatalf("seed node n4: %v", err)
	}

	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		StoragePoolName: "pool",
		NodeName:        "n4",
		ProviderKind:    apiv1.StoragePoolKindLVMThin,
	}); err != nil {
		t.Fatalf("seed pool n4: %v", err)
	}
}

// activeDiskful counts the replicas of rd that hold an active disk on a
// node other than the ones listed as disabled.
func activeDiskful(t *testing.T, ctx context.Context, st store.Store, rd string, disabled ...string) int {
	t.Helper()

	got, err := st.Resources().ListByDefinition(ctx, rd)
	if err != nil {
		t.Fatalf("list replicas of %q: %v", rd, err)
	}

	n := 0

	for i := range got {
		if slices.ContainsFunc(disabled, func(d string) bool { return naming.Same(d, got[i].NodeName) }) {
			continue
		}

		if slices.Contains(got[i].Flags, apiv1.ResourceFlagDiskless) ||
			slices.Contains(got[i].Flags, apiv1.ResourceFlagInactive) {
			continue
		}

		n++
	}

	return n
}

// TestBalanceGatePlaceCountCountsDiskfulLikeThePlacer: the gate ends
// the check from the cache once a definition's diskful replicas reach
// the place count, and must count them as the placer does, or it skips
// a definition the placer would top up. Two UpToDate diskful replicas
// and a third that does not count towards place count 3 (a tiebreaker,
// an INACTIVE replica, one on an EVICTED or a LOST node) leave the
// definition one short, and the pass gives it a diskful replica.
func TestBalanceGatePlaceCountCountsDiskfulLikeThePlacer(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		third     apiv1.Resource
		nodeFlags []string
	}{
		"tiebreaker": {third: apiv1.Resource{
			NodeName: "n3",
			Flags:    []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker},
			Volumes:  upToDateVolumes(),
		}},
		"diskless": {third: apiv1.Resource{
			NodeName: "n3",
			Flags:    []string{apiv1.ResourceFlagDiskless},
			Volumes:  upToDateVolumes(),
		}},
		"inactive": {third: apiv1.Resource{
			NodeName: "n3",
			Flags:    []string{apiv1.ResourceFlagInactive},
			Volumes:  upToDateVolumes(),
		}},
		"on an evicted node": {
			third:     apiv1.Resource{NodeName: "n3", Volumes: upToDateVolumes()},
			nodeFlags: []string{apiv1.NodeFlagEvicted},
		},
		"on a lost node": {
			third:     apiv1.Resource{NodeName: "n3", Volumes: upToDateVolumes()},
			nodeFlags: []string{apiv1.NodeFlagLost},
		},
	}

	for path, reconcile := range gatedPaths {
		for name, tc := range cases {
			t.Run(path+"/"+name, func(t *testing.T) {
				t.Parallel()

				ctx := context.Background()
				st := store.NewInMemory()

				seedIntervalFixture(t, ctx, st, 3, []string{"n1", "n2"})
				seedFourthNode(t, ctx, st)

				var disabled []string

				if tc.nodeFlags != nil {
					node, err := st.Nodes().Get(ctx, "n3")
					if err != nil {
						t.Fatalf("get n3: %v", err)
					}

					node.Flags = tc.nodeFlags
					if err := st.Nodes().Update(ctx, &node); err != nil {
						t.Fatalf("flag n3: %v", err)
					}

					disabled = []string{"n3"}
				}

				third := tc.third
				third.Name = "pvc-interval"

				if err := st.Resources().Create(ctx, &third); err != nil {
					t.Fatalf("seed third replica: %v", err)
				}

				reconcile(t, ctx, st)

				if n := activeDiskful(t, ctx, st, "pvc-interval", disabled...); n != 3 {
					t.Fatalf("got %d diskful replicas that count towards place count 3; want 3", n)
				}
			})
		}
	}
}

// TestBalanceGateLeavesDisklessOnRemainingAlone: with DisklessOnRemaining
// on the group, the placer adds a diskless replica on every node left
// over, even to a definition that already has its diskful replicas.
// LINSTOR's balance task adjusts only a definition with fewer diskful
// replicas than the place count, so a gated pass over a full definition
// adds nothing, and the node left over stays without a replica.
func TestBalanceGateLeavesDisklessOnRemainingAlone(t *testing.T) {
	t.Parallel()

	for path, reconcile := range gatedPaths {
		t.Run(path, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			st := store.NewInMemory()

			rg := seedIntervalFixture(t, ctx, st, 3, []string{"n1", "n2", "n3"})
			seedFourthNode(t, ctx, st)

			rg.SelectFilter.DisklessOnRemaining = true
			if err := st.ResourceGroups().Update(ctx, rg); err != nil {
				t.Fatalf("set diskless-on-remaining: %v", err)
			}

			reconcile(t, ctx, st)

			if n := replicaCount(t, ctx, st, "pvc-interval"); n != 3 {
				t.Fatalf("a definition at its place count got %d replicas from the pass; want its 3", n)
			}
		})
	}
}
