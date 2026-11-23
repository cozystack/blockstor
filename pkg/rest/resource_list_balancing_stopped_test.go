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

package rest

import (
	"context"
	"encoding/json"
	"maps"
	"net/http"
	"slices"
	"sync/atomic"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// placedLate is the node a placement pass put a replica on right before
// linstor-csi switched balancing off; the cache has not seen it yet.
const placedLate = "n-late"

// trailingReplicaCache answers a definition's replica list from a cache
// that has not seen the replica on placedLate, while the API server has.
type trailingReplicaCache struct {
	store.ResourceStore

	uncached *atomic.Int64
}

func (c trailingReplicaCache) ListByDefinition(ctx context.Context, rdName string) ([]apiv1.Resource, error) {
	all, err := c.ResourceStore.ListByDefinition(ctx, rdName)

	return slices.DeleteFunc(all, func(r apiv1.Resource) bool { return r.NodeName == placedLate }), err
}

func (c trailingReplicaCache) ListByDefinitionUncached(ctx context.Context, rdName string) ([]apiv1.Resource, error) {
	c.uncached.Add(1)

	return c.ResourceStore.ListByDefinition(ctx, rdName) //nolint:wrapcheck // test double
}

// trailingDefinitionCache answers Get with the definition as it was
// before balancing was switched off and before it started going: the
// switch went through another replica of the server.
type trailingDefinitionCache struct {
	store.ResourceDefinitionStore
}

func (c trailingDefinitionCache) Get(ctx context.Context, name string) (apiv1.ResourceDefinition, error) {
	rd, err := c.ResourceDefinitionStore.Get(ctx, name)
	rd.Props = maps.Clone(rd.Props)
	delete(rd.Props, apiv1.PropBalanceResourcesEnabled)
	rd.Flags = slices.DeleteFunc(slices.Clone(rd.Flags), func(f string) bool { return f == apiv1.ResourceFlagDelete })

	return rd, err //nolint:wrapcheck // test double
}

type trailingCacheStore struct {
	store.Store

	definitionTrails bool
	uncached         *atomic.Int64
}

func (s trailingCacheStore) Resources() store.ResourceStore {
	return trailingReplicaCache{ResourceStore: s.Store.Resources(), uncached: s.uncached}
}

func (s trailingCacheStore) ResourceDefinitions() store.ResourceDefinitionStore {
	if s.definitionTrails {
		return trailingDefinitionCache{s.Store.ResourceDefinitions()}
	}

	return s.Store.ResourceDefinitions()
}

// linstor-csi deletes a volume by switching balancing off on its
// definition, listing the replicas with this endpoint and deleting each
// one listed. A replica a placement pass created right before the
// switch must be on that list, as it is on LINSTOR's, or it is never
// deleted and keeps the definition, a kernel resource and its ports for
// good. A cache that has not seen that replica yet must not answer.
func TestResourceListShowsEveryReplicaOnceBalancingStops(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		props            map[string]string
		flags            []string
		definitionTrails bool
		wantLate         bool
	}{
		"balancing switched off": {
			props:    map[string]string{apiv1.PropBalanceResourcesEnabled: "False"},
			wantLate: true,
		},
		"balancing switched off through another server replica": {
			props:            map[string]string{apiv1.PropBalanceResourcesEnabled: "False"},
			definitionTrails: true,
			wantLate:         true,
		},
		"definition being deleted": {
			flags:    []string{apiv1.ResourceFlagDelete},
			wantLate: true,
		},
		"definition being deleted through another server replica": {
			flags:            []string{apiv1.ResourceFlagDelete},
			definitionTrails: true,
			wantLate:         true,
		},
		// The list linstor-csi reads while it creates and attaches a
		// volume stays on the cache.
		"balancing on": {},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			backend := store.NewInMemory()

			err := backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
				Name: "pvc-going", Props: tc.props, Flags: tc.flags,
			})
			if err != nil {
				t.Fatalf("seed rd: %v", err)
			}

			for _, node := range []string{"n1", placedLate} {
				if err := backend.Resources().Create(ctx, &apiv1.Resource{Name: "pvc-going", NodeName: node}); err != nil {
					t.Fatalf("seed replica on %s: %v", node, err)
				}
			}

			var uncached atomic.Int64

			base, stop := startServerWithStore(t, trailingCacheStore{
				Store: backend, definitionTrails: tc.definitionTrails, uncached: &uncached,
			})
			defer stop()

			resp := httpGet(t, base+"/v1/resource-definitions/pvc-going/resources")
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d", resp.StatusCode)
			}

			var got []apiv1.ResourceWithVolumes
			if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
				t.Fatalf("decode: %v", err)
			}

			late := slices.ContainsFunc(got, func(r apiv1.ResourceWithVolumes) bool { return r.NodeName == placedLate })
			if late != tc.wantLate {
				t.Errorf("replica on %s listed = %v, want %v (got %d replicas)", placedLate, late, tc.wantLate, len(got))
			}

			if !tc.wantLate && uncached.Load() != 0 {
				t.Errorf("listed past the cache %d times for a definition with balancing on", uncached.Load())
			}
		})
	}
}
