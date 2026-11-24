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
	"context"
	"slices"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/placer"
	"github.com/cozystack/blockstor/pkg/store"
)

// beatenToNode stands in for another writer that creates a replica on
// one node right before the placer does, so the placer's Create finds
// it there.
type beatenToNode struct {
	store.Store

	node string
}

func (b beatenToNode) Resources() store.ResourceStore {
	return beatenResources{ResourceStore: b.Store.Resources(), node: b.node}
}

type beatenResources struct {
	store.ResourceStore

	node string
}

func (b beatenResources) Create(ctx context.Context, res *apiv1.Resource) error {
	if res.NodeName == b.node {
		other := apiv1.Resource{Name: res.Name, NodeName: res.NodeName, Flags: []string{apiv1.ResourceFlagDiskless}}
		if err := b.ResourceStore.Create(ctx, &other); err != nil {
			return err //nolint:wrapcheck // test double
		}
	}

	return b.ResourceStore.Create(ctx, res) //nolint:wrapcheck // test double
}

// PlaceCreating names the nodes it created a replica on, diskful or
// diskless, and no other: a caller that takes back what one call did
// must not delete a replica another writer created, or a witness the
// call only promoted.
func TestPlaceCreatingNamesOnlyTheReplicasItCreated(t *testing.T) {
	t.Parallel()

	t.Run("another writer was first on a node", func(t *testing.T) {
		t.Parallel()

		inner := store.NewInMemory()
		seedStore(t, inner, []string{"n1", "n2", "n3"})

		created, placed, _, err := placer.New(beatenToNode{Store: inner, node: "n3"}).PlaceCreating(
			t.Context(), "pvc-1", &apiv1.AutoSelectFilter{PlaceCount: 2, DisklessOnRemaining: true})
		if err != nil {
			t.Fatalf("PlaceCreating: %v", err)
		}

		slices.Sort(created)

		if placed != 2 || !slices.Equal(created, []string{"n1", "n2"}) {
			t.Fatalf("placed %d, created on %v; want 2 placed, created on [n1 n2] (n3 was the other writer's)", placed, created)
		}
	})

	t.Run("a witness promoted in place", func(t *testing.T) {
		t.Parallel()

		st := store.NewInMemory()
		seedTwoDiskfulPlusWitness(t, st)

		created, _, _, err := placer.New(st).PlaceCreating(t.Context(), "pvc-1", &apiv1.AutoSelectFilter{PlaceCount: 3})
		if err != nil {
			t.Fatalf("PlaceCreating: %v", err)
		}

		assertWitnessUpgraded(t, st)

		if len(created) != 0 {
			t.Fatalf("created on %v; want none, the witness on n3 was promoted", created)
		}
	})
}
