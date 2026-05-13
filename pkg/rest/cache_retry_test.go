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
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// flakyRGStore wraps an underlying ResourceGroupStore and returns
// store.ErrNotFound for the first `notFoundUntil` Get() calls on the
// configured name, then delegates to the real store for the rest.
// Mirrors a controller-runtime informer cache that hasn't seen a
// write done on a sibling apiserver replica yet.
type flakyRGStore struct {
	store.ResourceGroupStore

	target        string
	notFoundUntil int
	calls         atomic.Int32
}

func (f *flakyRGStore) Get(ctx context.Context, name string) (apiv1.ResourceGroup, error) {
	if name == f.target {
		n := f.calls.Add(1)
		if int(n) <= f.notFoundUntil {
			return apiv1.ResourceGroup{}, errors.Wrapf(store.ErrNotFound, "resource group %q", name)
		}
	}

	return f.ResourceGroupStore.Get(ctx, name) //nolint:wrapcheck // pass-through to underlying store
}

// flakyRDStore is the RD-side equivalent of flakyRGStore.
type flakyRDStore struct {
	store.ResourceDefinitionStore

	target        string
	notFoundUntil int
	calls         atomic.Int32
}

func (f *flakyRDStore) Get(ctx context.Context, name string) (apiv1.ResourceDefinition, error) {
	if name == f.target {
		n := f.calls.Add(1)
		if int(n) <= f.notFoundUntil {
			return apiv1.ResourceDefinition{}, errors.Wrapf(store.ErrNotFound, "resource definition %q", name)
		}
	}

	return f.ResourceDefinitionStore.Get(ctx, name) //nolint:wrapcheck // pass-through
}

// flakyStore lets us substitute the RG / RD views with flaky ones
// while everything else keeps using the wrapped InMemory.
type flakyStore struct {
	store.Store

	rgs *flakyRGStore
	rds *flakyRDStore
}

func (f *flakyStore) ResourceGroups() store.ResourceGroupStore {
	if f.rgs == nil {
		return f.Store.ResourceGroups()
	}

	return f.rgs
}

func (f *flakyStore) ResourceDefinitions() store.ResourceDefinitionStore {
	if f.rds == nil {
		return f.Store.ResourceDefinitions()
	}

	return f.rds
}

func TestGetRGWithCacheRetry_SucceedsAfterCacheMiss(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()

	err := st.ResourceGroups().Create(t.Context(), &apiv1.ResourceGroup{Name: "rg-1"})
	if err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	flaky := &flakyStore{
		Store: st,
		rgs: &flakyRGStore{
			ResourceGroupStore: st.ResourceGroups(),
			target:             "rg-1",
			notFoundUntil:      1, // first call → NotFound, second → real
		},
	}

	start := time.Now()

	rg, err := getRGWithCacheRetry(t.Context(), flaky, "rg-1")
	if err != nil {
		t.Fatalf("getRGWithCacheRetry: %v", err)
	}

	if rg.Name != "rg-1" {
		t.Fatalf("got name %q, want rg-1", rg.Name)
	}

	if elapsed := time.Since(start); elapsed < cacheRetryDelay {
		t.Fatalf("retry returned in %s, expected at least one cacheRetryDelay (%s)", elapsed, cacheRetryDelay)
	}

	if got := flaky.rgs.calls.Load(); got != 2 {
		t.Fatalf("expected 2 Get attempts (NotFound, then hit), got %d", got)
	}
}

func TestGetRDWithCacheRetry_SucceedsAfterCacheMiss(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()

	err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "rd-1"})
	if err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	flaky := &flakyStore{
		Store: st,
		rds: &flakyRDStore{
			ResourceDefinitionStore: st.ResourceDefinitions(),
			target:                  "rd-1",
			notFoundUntil:           2, // two cache misses, then real
		},
	}

	rd, err := getRDWithCacheRetry(t.Context(), flaky, "rd-1")
	if err != nil {
		t.Fatalf("getRDWithCacheRetry: %v", err)
	}

	if rd.Name != "rd-1" {
		t.Fatalf("got name %q, want rd-1", rd.Name)
	}

	if got := flaky.rds.calls.Load(); got != 3 {
		t.Fatalf("expected 3 Get attempts (2 NotFound + 1 hit), got %d", got)
	}
}

func TestGetRGWithCacheRetry_RealNotFoundStillSurfaces(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()

	// Object is never created, so every retry returns NotFound.
	start := time.Now()

	_, err := getRGWithCacheRetry(t.Context(), st, "does-not-exist")
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}

	// Should have waited (cacheRetryAttempts - 1) * cacheRetryDelay
	// before giving up — give a wide margin to avoid CI flake.
	minWait := time.Duration(cacheRetryAttempts-1) * cacheRetryDelay
	if elapsed := time.Since(start); elapsed < minWait {
		t.Fatalf("retry loop returned in %s, expected at least %s", elapsed, minWait)
	}
}

// TestSpawn_SurvivesCacheMissOnRGGet covers the integration: a
// `POST /v1/resource-groups/{rg}/spawn` request whose RG read hits a
// trailing informer cache must still succeed once the cache catches
// up within the retry budget.
func TestSpawn_SurvivesCacheMissOnRGGet(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()

	// Seed RG (so the underlying store has it) but wrap the
	// ResourceGroups view so the first call returns NotFound.
	err := st.ResourceGroups().Create(t.Context(), &apiv1.ResourceGroup{
		Name: "sc-cache-race",
		SelectFilter: apiv1.AutoSelectFilter{
			PlaceCount: 0, // skip the autoplace step (no satellites in test)
		},
	})
	if err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	flaky := &flakyStore{
		Store: st,
		rgs: &flakyRGStore{
			ResourceGroupStore: st.ResourceGroups(),
			target:             "sc-cache-race",
			notFoundUntil:      1, // first call → NotFound, second → real
		},
	}

	srv := &Server{Store: flaky}

	body, err := json.Marshal(apiv1.ResourceGroupSpawn{
		ResourceDefinitionName: "pvc-cache-race",
	})
	if err != nil {
		t.Fatalf("marshal spawn body: %v", err)
	}

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost,
		"/v1/resource-groups/sc-cache-race/spawn", bytes.NewReader(body))
	req.SetPathValue("rg", "sc-cache-race")

	rr := httptest.NewRecorder()

	srv.handleSpawn(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("spawn under cache-miss returned %d, want 201; body: %s",
			rr.Code, rr.Body.String())
	}

	if flaky.rgs.calls.Load() < 2 {
		t.Fatalf("expected at least 2 RG Get attempts, got %d", flaky.rgs.calls.Load())
	}
}
