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
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
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

// lagSnapshotStore wraps an underlying SnapshotStore and models the
// production read-after-write informer-cache lag: a successful
// Create() arms `missBudget` — the next `missBudget` Get() calls on
// the just-created snapshot return store.ErrNotFound (the cache has
// not observed the apiserver write yet), then reads delegate to the
// real store (the watch event arrived). This is the exact shape
// linstor-csi's CreateVolume-from-snapshot trips over: POST snapshot
// create, then an immediate GET on a replica whose cache trails.
type lagSnapshotStore struct {
	store.SnapshotStore

	missBudget int

	mu      sync.Mutex
	pending map[string]int
}

func (f *lagSnapshotStore) Create(ctx context.Context, snap *apiv1.Snapshot) error {
	err := f.SnapshotStore.Create(ctx, snap)
	if err != nil {
		return err //nolint:wrapcheck // pass-through
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.pending == nil {
		f.pending = map[string]int{}
	}

	f.pending[snap.ResourceName+"/"+snap.Name] = f.missBudget

	return nil
}

func (f *lagSnapshotStore) Get(ctx context.Context, rdName, snapName string) (apiv1.Snapshot, error) {
	f.mu.Lock()

	key := rdName + "/" + snapName
	if left := f.pending[key]; left > 0 {
		f.pending[key] = left - 1
		f.mu.Unlock()

		return apiv1.Snapshot{}, errors.Wrapf(store.ErrNotFound, "snapshot %q on RD %q", snapName, rdName)
	}
	f.mu.Unlock()

	return f.SnapshotStore.Get(ctx, rdName, snapName) //nolint:wrapcheck // pass-through
}

// flakyStore lets us substitute the RG / RD / Snapshot views with
// flaky ones while everything else keeps using the wrapped InMemory.
type flakyStore struct {
	store.Store

	rgs   *flakyRGStore
	rds   *flakyRDStore
	snaps *lagSnapshotStore
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

func (f *flakyStore) Snapshots() store.SnapshotStore {
	if f.snaps == nil {
		return f.Store.Snapshots()
	}

	return f.snaps
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

func TestGetSnapshotWithCacheRetry_SucceedsAfterCacheMiss(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()

	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	flaky := &flakyStore{
		Store: st,
		snaps: &lagSnapshotStore{
			SnapshotStore: st.Snapshots(),
			missBudget:    2, // two cache misses after the create, then real
		},
	}

	err := flaky.Snapshots().Create(t.Context(),
		&apiv1.Snapshot{Name: "snap-1", ResourceName: "pvc-1"})
	if err != nil {
		t.Fatalf("seed snapshot: %v", err)
	}

	snap, err := getSnapshotWithCacheRetry(t.Context(), flaky, "pvc-1", "snap-1")
	if err != nil {
		t.Fatalf("getSnapshotWithCacheRetry: %v", err)
	}

	if snap.Name != "snap-1" || snap.ResourceName != "pvc-1" {
		t.Fatalf("got %q on %q, want snap-1 on pvc-1", snap.Name, snap.ResourceName)
	}
}

func TestGetSnapshotWithCacheRetry_RealNotFoundStillSurfaces(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()

	// Snapshot is never created, so every retry returns NotFound —
	// the caller's 404 contract (unknown snapshot → 404 envelope)
	// must survive the retry wrapper.
	start := time.Now()

	_, err := getSnapshotWithCacheRetry(t.Context(), st, "pvc-1", "does-not-exist")
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}

	minWait := time.Duration(cacheRetryAttempts-1) * cacheRetryDelay
	if elapsed := time.Since(start); elapsed < minWait {
		t.Fatalf("retry loop returned in %s, expected at least %s", elapsed, minWait)
	}
}

// TestSnapshotGet_SurvivesCacheMissAfterCreate pins the F1 hot path
// end-to-end on the wire: linstor-csi's CreateVolume-from-snapshot
// GETs the snapshot IMMEDIATELY after the create POST (size guard
// before snapshot-restore-resource). With the informer-cached store
// the create writes straight to the apiserver while the follow-up
// read is served from a cache that may not have observed the write
// yet — the GET must absorb that lag instead of 404-ing the whole
// CreateVolume (TestGroupJ/CSICreateVolumeFromSnapshot regression).
func TestSnapshotGet_SurvivesCacheMissAfterCreate(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()

	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-clone-src"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	flaky := &flakyStore{
		Store: st,
		snaps: &lagSnapshotStore{
			SnapshotStore: st.Snapshots(),
			missBudget:    1, // first read after create → NotFound, then real
		},
	}

	base, stop := startServerWithStore(t, flaky)
	defer stop()

	body, err := json.Marshal(apiv1.Snapshot{Name: "snap-1"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	createResp := httpPost(t, base+"/v1/resource-definitions/pvc-clone-src/snapshots", body)
	defer func() { _ = createResp.Body.Close() }()

	if createResp.StatusCode != http.StatusCreated {
		t.Fatalf("create status: got %d, want 201", createResp.StatusCode)
	}

	// Immediate follow-up GET — the exact linstor-csi sequence.
	getResp := httpGet(t, base+"/v1/resource-definitions/pvc-clone-src/snapshots/snap-1")
	defer func() { _ = getResp.Body.Close() }()

	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("get-after-create status: got %d, want 200 (cache-lag 404 must be absorbed)",
			getResp.StatusCode)
	}

	var got apiv1.Snapshot
	if err := json.NewDecoder(getResp.Body).Decode(&got); err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}

	if got.Name != "snap-1" || got.ResourceName != "pvc-clone-src" {
		t.Fatalf("got %q on %q, want snap-1 on pvc-clone-src", got.Name, got.ResourceName)
	}
}

// TestSnapshotRestore_SurvivesCacheMissAfterCreate pins the second
// half of the same hot path: `POST .../snapshot-restore-resource/
// {snap}` right after the snapshot create. In production with N
// apiserver replicas the restore POST can land on a replica whose
// cache has not observed the snapshot yet — the source-snapshot read
// must absorb the lag instead of failing the restore with 404.
func TestSnapshotRestore_SurvivesCacheMissAfterCreate(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-clone-src"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	// The restore path refuses vol-less snapshots (Bug 151), so the
	// seeded source carries one VD.
	if err := st.VolumeDefinitions().Create(ctx, "pvc-clone-src", &apiv1.VolumeDefinition{
		VolumeNumber: 0,
		SizeKib:      1024,
	}); err != nil {
		t.Fatalf("seed VD: %v", err)
	}

	flaky := &flakyStore{
		Store: st,
		snaps: &lagSnapshotStore{
			SnapshotStore: st.Snapshots(),
			missBudget:    1, // first read after create → NotFound, then real
		},
	}

	base, stop := startServerWithStore(t, flaky)
	defer stop()

	body, err := json.Marshal(apiv1.Snapshot{Name: "snap-1"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	createResp := httpPost(t, base+"/v1/resource-definitions/pvc-clone-src/snapshots", body)
	defer func() { _ = createResp.Body.Close() }()

	if createResp.StatusCode != http.StatusCreated {
		t.Fatalf("create status: got %d, want 201", createResp.StatusCode)
	}

	restoreBody := []byte(`{"to_resource":"pvc-clone-dest"}`)

	restoreResp := httpPost(t,
		base+"/v1/resource-definitions/pvc-clone-src/snapshot-restore-resource/snap-1", restoreBody)
	defer func() { _ = restoreResp.Body.Close() }()

	if restoreResp.StatusCode != http.StatusCreated {
		t.Fatalf("restore-after-create status: got %d, want 201 (cache-lag 404 must be absorbed)",
			restoreResp.StatusCode)
	}
}
