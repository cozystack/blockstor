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
	"sync"
	"testing"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/placer"
	"github.com/cozystack/blockstor/pkg/store"
)

// terminatingWitness is the flags of a TieBreaker witness the
// controller is deleting: the store shows a replica with a
// DeletionTimestamp with the DELETE flag.
func terminatingWitness() []string {
	return []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker, apiv1.ResourceFlagDelete}
}

// witnessDuringPlacement writes a witness with the given flags on node
// right after the placement's second diskful replica, the moment the
// controller's tiebreaker reconciler reacts to a three-replica spawn.
type witnessDuringPlacement struct {
	store.Store

	node  string
	flags []string
}

func (s witnessDuringPlacement) Resources() store.ResourceStore {
	return &witnessDuringPlacementResources{ResourceStore: s.Store.Resources(), node: s.node, flags: s.flags}
}

type witnessDuringPlacementResources struct {
	store.ResourceStore

	node  string
	flags []string
	once  sync.Once
}

func (r *witnessDuringPlacementResources) Create(ctx context.Context, res *apiv1.Resource) error {
	err := r.ResourceStore.Create(ctx, res)
	if err != nil {
		return err //nolint:wrapcheck // test double
	}

	replicas, err := r.ListByDefinition(ctx, res.Name)
	if err != nil {
		return err //nolint:wrapcheck // test double
	}

	if len(replicas) == 2 {
		r.once.Do(func() {
			_ = r.ResourceStore.Create(ctx, &apiv1.Resource{Name: res.Name, NodeName: r.node, Flags: r.flags})
		})
	}

	return nil
}

// A witness the controller is already deleting is not taken over: the
// promotion would land on an object about to vanish, and the placement
// would report a replica that is gone a moment later. The node is not
// placed on, so the placement reports one replica short instead of
// counting it, both for a witness written during the placement and for
// one the placement saw before it started.
func TestPlaceDoesNotTakeOverWitnessBeingDeleted(t *testing.T) {
	t.Parallel()

	nodes := []string{"n1", "n2", "n3"}

	for _, tc := range []struct {
		name  string
		build func(t *testing.T, inner store.Store) store.Store
	}{
		{"written during the placement", func(_ *testing.T, inner store.Store) store.Store {
			return witnessDuringPlacement{Store: inner, node: "n1", flags: terminatingWitness()}
		}},
		{"seen before the placement", func(t *testing.T, inner store.Store) store.Store {
			t.Helper()

			err := inner.Resources().Create(t.Context(), &apiv1.Resource{Name: "pvc-1", NodeName: "n1", Flags: terminatingWitness()})
			if err != nil {
				t.Fatalf("seed witness: %v", err)
			}

			return inner
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			inner := store.NewInMemory()
			seedStore(t, inner, nodes)

			placed, want, err := placer.New(tc.build(t, inner)).Place(t.Context(), "pvc-1", &apiv1.AutoSelectFilter{PlaceCount: 3})
			if err != nil {
				t.Fatalf("Place: %v", err)
			}

			if placed != 2 || want != 3 {
				t.Errorf("placed %d of %d, want 2 of 3: the witness being deleted counted as placed", placed, want)
			}

			got, err := inner.Resources().Get(t.Context(), "pvc-1", "n1")
			if err != nil {
				t.Fatalf("get n1: %v", err)
			}

			if !slices.Equal(got.Flags, terminatingWitness()) || got.Props["StorPoolName"] != "" {
				t.Errorf("n1: flags %v, StorPoolName %q; want the witness being deleted left as it was",
					got.Flags, got.Props["StorPoolName"])
			}
		})
	}
}

// witnessRewrittenOnPromote makes the witness on node vanish as the
// placement promotes it, and a new one appear as the placement then
// creates the replica: the tiebreaker reconciler collapsing the witness
// and growing it again within one placement.
type witnessRewrittenOnPromote struct {
	store.Store

	resources *witnessRewrittenOnPromoteResources
}

func newWitnessRewrittenOnPromote(inner store.Store, node string) witnessRewrittenOnPromote {
	return witnessRewrittenOnPromote{
		Store:     inner,
		resources: &witnessRewrittenOnPromoteResources{ResourceStore: inner.Resources(), node: node},
	}
}

func (s witnessRewrittenOnPromote) Resources() store.ResourceStore {
	return s.resources
}

type witnessRewrittenOnPromoteResources struct {
	store.ResourceStore

	node            string
	vanish, rewrite sync.Once
}

func (r *witnessRewrittenOnPromoteResources) PatchResourceSpec(
	ctx context.Context, rdName, node string, mutate func(*apiv1.Resource) error,
) error {
	if node != r.node {
		return r.ResourceStore.PatchResourceSpec(ctx, rdName, node, mutate) //nolint:wrapcheck // test double
	}

	var vanished bool

	r.vanish.Do(func() {
		_ = r.Delete(ctx, rdName, node)
		vanished = true
	})

	if vanished {
		return errors.Wrapf(store.ErrNotFound, "resource %q on node %q", rdName, node)
	}

	return r.ResourceStore.PatchResourceSpec(ctx, rdName, node, mutate) //nolint:wrapcheck // test double
}

func (r *witnessRewrittenOnPromoteResources) Create(ctx context.Context, res *apiv1.Resource) error {
	if res.NodeName == r.node {
		r.rewrite.Do(func() {
			_ = r.ResourceStore.Create(ctx, &apiv1.Resource{
				Name: res.Name, NodeName: r.node,
				Flags: []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker},
			})
		})
	}

	return r.ResourceStore.Create(ctx, res) //nolint:wrapcheck // test double
}

// The create a vanished witness falls back to can find a new witness
// written in between. Taking its "already exists" for the diskful
// replica would count that witness as placed, the bug the take-over
// fixes, one step further on; the new witness is taken over too.
func TestPlaceTakesOverWitnessWrittenAgainAfterItVanished(t *testing.T) {
	t.Parallel()

	inner := store.NewInMemory()
	seedStore(t, inner, []string{"n1", "n2", "n3"})

	err := inner.Resources().Create(t.Context(), &apiv1.Resource{
		Name: "pvc-1", NodeName: "n3",
		Flags: []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker},
	})
	if err != nil {
		t.Fatalf("seed witness: %v", err)
	}

	st := newWitnessRewrittenOnPromote(inner, "n3")

	placed, _, err := placer.New(st).Place(t.Context(), "pvc-1", &apiv1.AutoSelectFilter{PlaceCount: 3})
	if err != nil {
		t.Fatalf("Place: %v", err)
	}

	if placed != 3 {
		t.Errorf("placed: got %d, want 3", placed)
	}

	got, err := inner.Resources().Get(t.Context(), "pvc-1", "n3")
	if err != nil {
		t.Fatalf("get n3: %v", err)
	}

	if isWitness(got.Flags) || got.Props["StorPoolName"] != "pool" {
		t.Errorf("n3: flags %v, StorPoolName %q; want a diskful replica in \"pool\"", got.Flags, got.Props["StorPoolName"])
	}
}

func isWitness(flags []string) bool {
	return slices.Contains(flags, apiv1.ResourceFlagDiskless) || slices.Contains(flags, apiv1.ResourceFlagTieBreaker)
}
