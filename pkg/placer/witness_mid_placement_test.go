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
	"time"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/placer"
	"github.com/cozystack/blockstor/pkg/store"
)

// witnessOnSecondReplica is a store in which the controller's
// tiebreaker reconciler reacts to a placement: as soon as the
// definition has two diskful replicas, a TIE_BREAKER witness appears on
// the one node of nodes that has none yet, the way the controller adds
// one to a definition it sees with an even number of diskful replicas.
type witnessOnSecondReplica struct {
	store.Store

	nodes []string
}

func (s witnessOnSecondReplica) Resources() store.ResourceStore {
	return &witnessOnSecondReplicaResources{ResourceStore: s.Store.Resources(), nodes: s.nodes}
}

type witnessOnSecondReplicaResources struct {
	store.ResourceStore

	nodes []string
	once  sync.Once
}

func (r *witnessOnSecondReplicaResources) Create(ctx context.Context, res *apiv1.Resource) error {
	err := r.ResourceStore.Create(ctx, res)
	if err != nil || slices.Contains(res.Flags, apiv1.ResourceFlagDiskless) {
		return err //nolint:wrapcheck // test double
	}

	replicas, err := r.ListByDefinition(ctx, res.Name)
	if err != nil {
		return err //nolint:wrapcheck // test double
	}

	if len(replicas) != 2 {
		return nil
	}

	r.once.Do(func() {
		for _, node := range r.nodes {
			if !slices.ContainsFunc(replicas, func(existing apiv1.Resource) bool { return existing.NodeName == node }) {
				_ = r.ResourceStore.Create(ctx, &apiv1.Resource{
					Name: res.Name, NodeName: node,
					Flags: []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker},
				})
			}
		}
	})

	return nil
}

// TestPlacePromotesWitnessGrownMidPlacement is the three-replica spawn
// that came up with a TieBreaker instead of its third diskful replica.
// The placer writes the replicas one by one; between the second and
// the third the controller saw two diskful replicas and put a witness
// on the third node. The placer's create of the third replica then
// found the witness, took the "already exists" for a replica in place,
// and reported the placement done with two diskful replicas. The
// definition stayed one replica short, its quorum on the witness, until
// a later placement promoted it minutes on. LINSTOR places a diskful
// replica over a TieBreaker by taking the witness over, which is what
// the placer does for a witness it saw before it started.
func TestPlacePromotesWitnessGrownMidPlacement(t *testing.T) {
	t.Parallel()

	nodes := []string{"n1", "n2", "n3"}

	inner := store.NewInMemory()
	seedStore(t, inner, nodes)

	st := witnessOnSecondReplica{Store: inner, nodes: nodes}

	placed, _, err := placer.New(st).Place(t.Context(), "pvc-1", &apiv1.AutoSelectFilter{PlaceCount: 3})
	if err != nil {
		t.Fatalf("Place: %v", err)
	}

	if placed != len(nodes) {
		t.Errorf("placed: got %d, want %d", placed, len(nodes))
	}

	got, err := inner.Resources().ListByDefinition(t.Context(), "pvc-1")
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	if len(got) != len(nodes) {
		t.Fatalf("replicas: got %d, want %d; %+v", len(got), len(nodes), got)
	}

	for i := range got {
		if slices.Contains(got[i].Flags, apiv1.ResourceFlagDiskless) || slices.Contains(got[i].Flags, apiv1.ResourceFlagTieBreaker) {
			t.Errorf("%s: flags %v, want a diskful replica on every node", got[i].NodeName, got[i].Flags)
		}

		if got[i].Props["StorPoolName"] != "pool" {
			t.Errorf("%s: StorPoolName %q, want \"pool\"", got[i].NodeName, got[i].Props["StorPoolName"])
		}
	}
}

// TestPlaceLeavesDiskfulReplicaWrittenMeanwhile pins the other "already
// exists": a diskful replica another writer created on the node while
// the placement ran is left as it is, its pool included.
func TestPlaceLeavesDiskfulReplicaWrittenMeanwhile(t *testing.T) {
	t.Parallel()

	inner := store.NewInMemory()
	seedStore(t, inner, []string{"n1", "n2", "n3"})

	st := diskfulOnFirstCreate{Store: inner, node: "n1", pool: "other"}

	_, _, err := placer.New(st).Place(t.Context(), "pvc-1", &apiv1.AutoSelectFilter{PlaceCount: 3})
	if err != nil {
		t.Fatalf("Place: %v", err)
	}

	got, err := inner.Resources().Get(t.Context(), "pvc-1", "n1")
	if err != nil {
		t.Fatalf("get n1: %v", err)
	}

	if got.Props["StorPoolName"] != "other" {
		t.Errorf("n1 StorPoolName: got %q, want \"other\" (the replica written meanwhile is left alone)", got.Props["StorPoolName"])
	}

	if slices.Contains(got.Flags, apiv1.ResourceFlagDiskless) {
		t.Errorf("n1 flags: got %v, want it diskful", got.Flags)
	}
}

// diskfulOnFirstCreate creates a diskful replica on node, backed by
// pool, right before the placer's first create of the definition.
type diskfulOnFirstCreate struct {
	store.Store

	node, pool string
}

func (s diskfulOnFirstCreate) Resources() store.ResourceStore {
	return &diskfulOnFirstCreateResources{ResourceStore: s.Store.Resources(), node: s.node, pool: s.pool}
}

type diskfulOnFirstCreateResources struct {
	store.ResourceStore

	node, pool string
	once       sync.Once
}

func (r *diskfulOnFirstCreateResources) Create(ctx context.Context, res *apiv1.Resource) error {
	r.once.Do(func() {
		_ = r.ResourceStore.Create(ctx, &apiv1.Resource{
			Name: res.Name, NodeName: r.node,
			Props: map[string]string{"StorPoolName": r.pool},
		})
	})

	return r.ResourceStore.Create(ctx, res) //nolint:wrapcheck // test double
}

// holdObserver records, at every replica create, whether the
// definition carries a placement in progress that is still due.
type holdObserver struct {
	store.Store

	mu   *sync.Mutex
	held *[]bool
}

func (s holdObserver) Resources() store.ResourceStore {
	return holdObserverResources{ResourceStore: s.Store.Resources(), st: s}
}

type holdObserverResources struct {
	store.ResourceStore

	st holdObserver
}

func (r holdObserverResources) Create(ctx context.Context, res *apiv1.Resource) error {
	rd, err := r.st.ResourceDefinitions().Get(ctx, res.Name)
	if err != nil {
		return err //nolint:wrapcheck // test double
	}

	r.st.mu.Lock()
	*r.st.held = append(*r.st.held, placementHeld(&rd))
	r.st.mu.Unlock()

	return r.ResourceStore.Create(ctx, res) //nolint:wrapcheck // test double
}

func placementHeld(rd *apiv1.ResourceDefinition) bool {
	deadline, err := time.Parse(time.RFC3339, rd.Annotations[apiv1.PlacementInProgressUntilAnnotation])

	return err == nil && time.Now().Before(deadline)
}

// A placement that writes several diskful replicas tells the tiebreaker
// reconciler so for as long as it writes them, and says it is done when
// it is: the reconciler, seeing two of three replicas, would otherwise
// put a witness on the third node, which the peers set up as diskless
// and the placement then turns diskful under them. A placement of one
// replica passes through no intermediate shape and leaves the
// definition alone.
func TestPlaceHoldsTheWitnessWhileItWritesReplicas(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		count int32
		held  bool
	}{
		{name: "three replicas", count: 3, held: true},
		{name: "one replica", count: 1, held: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			inner := store.NewInMemory()
			seedStore(t, inner, []string{"n1", "n2", "n3"})

			err := inner.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-1"})
			if err != nil {
				t.Fatalf("seed definition: %v", err)
			}

			var held []bool

			st := holdObserver{Store: inner, mu: &sync.Mutex{}, held: &held}

			placed, _, err := placer.New(st).Place(t.Context(), "pvc-1", &apiv1.AutoSelectFilter{PlaceCount: apiv1.LaxInt32(tc.count)})
			if err != nil || placed != int(tc.count) {
				t.Fatalf("Place: placed %d, err %v; want %d", placed, err, tc.count)
			}

			if len(held) != int(tc.count) {
				t.Fatalf("observed %d creates, want %d", len(held), tc.count)
			}

			for i, h := range held {
				if h != tc.held {
					t.Errorf("create %d: placement in progress %v, want %v", i+1, h, tc.held)
				}
			}

			rd, err := inner.ResourceDefinitions().Get(t.Context(), "pvc-1")
			if err != nil {
				t.Fatalf("get definition: %v", err)
			}

			if v, ok := rd.Annotations[apiv1.PlacementInProgressUntilAnnotation]; ok {
				t.Errorf("placement done, the definition still carries %s=%q", apiv1.PlacementInProgressUntilAnnotation, v)
			}
		})
	}
}
