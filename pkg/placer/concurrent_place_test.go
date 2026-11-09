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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/placer"
	"github.com/cozystack/blockstor/pkg/store"
)

// laggingStore stands for the Kubernetes store behind an informer cache
// that has not seen any replica yet: ListByDefinition answers from the
// cache, ListByDefinitionUncached from the API server.
type laggingStore struct {
	store.Store

	replicas *laggingReplicas
}

func (s laggingStore) Resources() store.ResourceStore { return s.replicas }

type laggingReplicas struct {
	store.ResourceStore

	// readers and together hold a reader of the API server, after it has
	// read, until a second one has read too, or a moment passes: two
	// placements that may run at once both read before either writes.
	readers  atomic.Int32
	together chan struct{}
}

func (r *laggingReplicas) ListByDefinition(context.Context, string) ([]apiv1.Resource, error) {
	return nil, nil
}

func (r *laggingReplicas) ListByDefinitionUncached(ctx context.Context, rdName string) ([]apiv1.Resource, error) {
	seen, err := r.ResourceStore.ListByDefinition(ctx, rdName)

	if r.readers.Add(1) == 2 {
		close(r.together)
	}

	select {
	case <-r.together:
	case <-time.After(300 * time.Millisecond):
	}

	return seen, err //nolint:wrapcheck // a test double passing the store's answer through
}

func newLaggingStore(t *testing.T) (store.Store, store.Store) {
	t.Helper()

	inner := store.NewInMemory()
	seedStore(t, inner, []string{"n1", "n2", "n3"})

	return inner, laggingStore{Store: inner, replicas: &laggingReplicas{
		ResourceStore: inner.Resources(), together: make(chan struct{}),
	}}
}

func replicaCount(t *testing.T, st store.Store) int {
	t.Helper()

	got, err := st.Resources().ListByDefinition(t.Context(), "pvc-1")
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	return len(got)
}

// The REST autoplace and a resource-group pass place one definition at
// once, with different filters. Each must see the other's replicas, or
// the definition ends up with more than it asked for.
func TestConcurrentPlacementsOfOneDefinitionDoNotOverplace(t *testing.T) {
	t.Parallel()

	for round := range 3 {
		inner, lagging := newLaggingStore(t)

		filters := []*apiv1.AutoSelectFilter{
			{PlaceCount: 2, NodeNameList: []string{"n1", "n2"}},
			{PlaceCount: 2, NodeNameList: []string{"n3"}},
		}

		var wg sync.WaitGroup

		for _, filter := range filters {
			wg.Go(func() {
				// One of the two comes up short on its node list by design.
				if _, _, err := placer.New(lagging).Place(t.Context(), "pvc-1", filter); err != nil {
					t.Logf("placement: %v", err)
				}
			})
		}

		wg.Wait()

		if got := replicaCount(t, inner); got != 2 {
			t.Fatalf("round %d: replicas after two placements of place_count=2: %d, want 2", round, got)
		}
	}
}

// A placement right after another counts the replicas the first made,
// though the cache has not caught up with them yet.
func TestAPlacementCountsReplicasTheCacheHasNotSeen(t *testing.T) {
	t.Parallel()

	inner, lagging := newLaggingStore(t)

	if _, _, err := placer.New(lagging).Place(t.Context(), "pvc-1",
		&apiv1.AutoSelectFilter{PlaceCount: 2, NodeNameList: []string{"n1", "n2"}}); err != nil {
		t.Fatalf("first placement: %v", err)
	}

	// Nothing is left to place; a shortfall error from the full node list
	// is no concern here, only the count.
	if _, _, err := placer.New(lagging).Place(t.Context(), "pvc-1", &apiv1.AutoSelectFilter{PlaceCount: 2}); err != nil {
		t.Logf("second placement: %v", err)
	}

	if got := replicaCount(t, inner); got != 2 {
		t.Errorf("replicas after a second placement of place_count=2: %d, want 2", got)
	}
}
