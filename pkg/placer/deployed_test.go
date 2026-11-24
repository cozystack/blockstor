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

package placer_test

import (
	"slices"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/placer"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestPlaceDeployedReportsOnlyTheReplicasItPlaced pins the set the
// REST create waits on, LINSTOR's deployedResources: the replicas the
// call created or promoted, and none of those already there.
func TestPlaceDeployedReportsOnlyTheReplicasItPlaced(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		seed   func(t *testing.T, st store.Store)
		filter apiv1.AutoSelectFilter
		want   []string
	}{
		{
			name: "target already met deploys nothing",
			seed: func(t *testing.T, st store.Store) {
				t.Helper()
				seedStore(t, st, []string{"n1", "n2", "n3"})
				seedDiskful(t, st, "n1", "n2")
			},
			filter: apiv1.AutoSelectFilter{PlaceCount: 2},
		},
		{
			name: "added replica only",
			seed: func(t *testing.T, st store.Store) {
				t.Helper()
				seedStore(t, st, []string{"n1", "n2", "n3"})
				seedDiskful(t, st, "n1", "n2")
			},
			filter: apiv1.AutoSelectFilter{PlaceCount: 3},
			want:   []string{"n3"},
		},
		{
			name:   "promoted witness counts as deployed",
			seed:   seedTwoDiskfulPlusWitness,
			filter: apiv1.AutoSelectFilter{PlaceCount: 3},
			want:   []string{"n3"},
		},
		{
			name: "diskless on remaining is deployed too",
			seed: func(t *testing.T, st store.Store) {
				t.Helper()
				seedStore(t, st, []string{"n1", "n2", "n3"})
				seedDiskful(t, st, "n1", "n2")
			},
			filter: apiv1.AutoSelectFilter{PlaceCount: 2, DisklessOnRemaining: true},
			want:   []string{"n3"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			st := store.NewInMemory()
			tc.seed(t, st)

			got, err := placer.New(st).PlaceDeployed(t.Context(), "pvc-1", &tc.filter)
			if err != nil {
				t.Fatalf("PlaceDeployed: %v", err)
			}

			deployed := slices.Clone(got.Deployed)
			slices.Sort(deployed)

			if !slices.Equal(deployed, tc.want) {
				t.Errorf("deployed: got %v, want %v", deployed, tc.want)
			}
		})
	}
}

func seedDiskful(t *testing.T, st store.Store, nodes ...string) {
	t.Helper()

	for _, n := range nodes {
		if err := st.Resources().Create(t.Context(), &apiv1.Resource{
			Name: "pvc-1", NodeName: n,
			Props: map[string]string{"StorPoolName": "pool"},
		}); err != nil {
			t.Fatalf("seed diskful %s: %v", n, err)
		}
	}
}

// A create that finds a witness the tiebreaker reconciler wrote during
// the placement takes it over, and the autoplace has to wait for it as
// for any replica it placed: LINSTOR's autoplace takes a TieBreaker over
// and keeps it in deployedResources. A witness already being deleted is
// not taken over, so it is not waited for either.
func TestPlaceDeployedCountsWitnessTakenOverMidPlacement(t *testing.T) {
	t.Parallel()

	nodes := []string{"n1", "n2", "n3"}

	for _, tc := range []struct {
		name  string
		build func(inner store.Store) store.Store
		want  []string
	}{
		{
			name:  "witness taken over after already exists",
			build: func(inner store.Store) store.Store { return witnessOnSecondReplica{Store: inner, nodes: nodes} },
			want:  nodes,
		},
		{
			name: "witness being deleted is not deployed to",
			build: func(inner store.Store) store.Store {
				return witnessDuringPlacement{Store: inner, node: "n1", flags: terminatingWitness()}
			},
			want: []string{"n2", "n3"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			inner := store.NewInMemory()
			seedStore(t, inner, nodes)

			got, err := placer.New(tc.build(inner)).PlaceDeployed(t.Context(), "pvc-1", &apiv1.AutoSelectFilter{PlaceCount: 3})
			if err != nil {
				t.Fatalf("PlaceDeployed: %v", err)
			}

			deployed := slices.Clone(got.Deployed)
			slices.Sort(deployed)

			if !slices.Equal(deployed, tc.want) {
				t.Errorf("deployed: got %v, want %v", deployed, tc.want)
			}
		})
	}
}
