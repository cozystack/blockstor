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
	"io"
	"net/http"
	"testing"
	"time"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Bug 124 — phantom DRBD resources persist on `r l` 5-30 s after
// `rd d` returns SUCCESS.
//
// Root cause: the REST `Store` is a thin shim over a controller-
// runtime cached client. `Resources().Delete()` and
// `ResourceDefinitions().Delete()` write to the apiserver and ack
// synchronously, but subsequent `Resources().List()` reads come back
// through the informer cache, which only updates when its watch
// stream observes the event. Between SUCCESS and the next watch
// frame `r l` returns the pre-delete picture.
//
// These tests pin the cache-invalidation-hook fix: after every write
// that the user expects on the very next read, handleRDDelete /
// handleResourceDelete waits for the local store to confirm the
// deletion is observable before returning 200. See
// pkg/rest/cache_invalidation.go.
//
// We can't simulate informer-cache lag with the vanilla
// store.NewInMemory() (Delete returns and the very next List
// reflects it). The `laggingStore` wrapper below buffers deletes
// against the underlying inmemory store with a configurable
// `lagDuration`; reads continue to see "pending" deletes until the
// lag window elapses. That mirrors production behaviour closely
// enough to exercise the post-delete wait.

// laggingResources wraps an inMemory ResourceStore so Delete is
// applied to the underlying store only after `lag` elapses, while
// reads (List / ListByDefinition / Get) keep reflecting the pre-
// delete state until the underlying delete commits. That mirrors
// the production informer-cache trail: the apiserver acks the
// delete synchronously but the local watch-driven cache only
// observes it after the watch event arrives.
//
// Delete returns nil immediately (the caller thinks the operation
// succeeded), schedules a goroutine to call inner.Delete after
// `lag`, and the convergence wait in the REST handler then polls
// until the read path agrees.
type laggingResources struct {
	inner store.ResourceStore

	lag time.Duration
}

func (l *laggingResources) List(ctx context.Context) ([]apiv1.Resource, error) {
	return l.inner.List(ctx) //nolint:wrapcheck // test helper
}

func (l *laggingResources) ListByDefinition(ctx context.Context, rdName string) ([]apiv1.Resource, error) {
	return l.inner.ListByDefinition(ctx, rdName) //nolint:wrapcheck // test helper
}

func (l *laggingResources) Get(ctx context.Context, rdName, node string) (apiv1.Resource, error) {
	return l.inner.Get(ctx, rdName, node) //nolint:wrapcheck // test helper
}

func (l *laggingResources) Create(ctx context.Context, r *apiv1.Resource) error {
	return l.inner.Create(ctx, r) //nolint:wrapcheck // test helper
}

func (l *laggingResources) Update(ctx context.Context, r *apiv1.Resource) error {
	return l.inner.Update(ctx, r) //nolint:wrapcheck // test helper
}

func (l *laggingResources) Delete(ctx context.Context, rdName, node string) error {
	// Confirm the row exists right now so the caller still sees
	// ErrNotFound for a row that genuinely never existed. The
	// real delete is deferred to a goroutine; the caller's
	// follow-up reads will observe the row for `lag` before it
	// vanishes — exactly the cache-trail Bug 124 reports.
	_, err := l.inner.Get(ctx, rdName, node)
	if err != nil {
		return err //nolint:wrapcheck // test helper
	}

	lag := l.lag
	inner := l.inner

	go func() { //nolint:contextcheck // commit goroutine outlives caller ctx by design
		time.Sleep(lag)

		_ = inner.Delete(context.Background(), rdName, node)
	}()

	return nil
}

func (l *laggingResources) SetState(ctx context.Context, rdName, node string,
	state apiv1.ResourceState, volumes []apiv1.VolumeObservation,
) error {
	return l.inner.SetState(ctx, rdName, node, state, volumes) //nolint:wrapcheck // test helper
}

func (l *laggingResources) ClearDRBDPort(ctx context.Context, rdName, node string) error {
	return l.inner.ClearDRBDPort(ctx, rdName, node) //nolint:wrapcheck // test helper
}

func (l *laggingResources) PatchResourceSpec(ctx context.Context, rdName, node string, mutate func(*apiv1.Resource) error) error {
	return l.inner.PatchResourceSpec(ctx, rdName, node, mutate) //nolint:wrapcheck // test helper
}

// laggingRDs wraps the inMemory ResourceDefinitionStore: Delete
// returns immediately but the underlying inner.Delete fires after
// `lag`, so reads continue to surface the RD for the lag window.
type laggingRDs struct {
	inner store.ResourceDefinitionStore

	lag time.Duration
}

func (l *laggingRDs) List(ctx context.Context) ([]apiv1.ResourceDefinition, error) {
	return l.inner.List(ctx) //nolint:wrapcheck // test helper
}

func (l *laggingRDs) Get(ctx context.Context, name string) (apiv1.ResourceDefinition, error) {
	return l.inner.Get(ctx, name) //nolint:wrapcheck // test helper
}

func (l *laggingRDs) Create(ctx context.Context, rd *apiv1.ResourceDefinition) error {
	return l.inner.Create(ctx, rd) //nolint:wrapcheck // test helper
}

func (l *laggingRDs) Update(ctx context.Context, rd *apiv1.ResourceDefinition) error {
	return l.inner.Update(ctx, rd) //nolint:wrapcheck // test helper
}

func (l *laggingRDs) PatchResourceDefinitionSpec(ctx context.Context, name string, mutate func(*apiv1.ResourceDefinition) error) error {
	return l.inner.PatchResourceDefinitionSpec(ctx, name, mutate) //nolint:wrapcheck // test helper
}

func (l *laggingRDs) Delete(ctx context.Context, name string) error {
	_, err := l.inner.Get(ctx, name)
	if err != nil {
		return err //nolint:wrapcheck // test helper
	}

	lag := l.lag
	inner := l.inner

	go func() {
		time.Sleep(lag)

		_ = inner.Delete(context.Background(), name)
	}()

	return nil
}

// laggingStore is a store.Store that wires the lagging Resource +
// RD stores in front of an InMemory backbone. Everything else
// passes through unchanged. Lag is identical for both surfaces
// (RD deletes cascade to Resource deletes upstream via
// handleRDDelete.cascadeDeleteResources, so the timing has to
// line up).
type laggingStore struct {
	inner *store.InMemory

	resources           *laggingResources
	resourceDefinitions *laggingRDs
}

func newLaggingStore(lag time.Duration) *laggingStore {
	inner := store.NewInMemory()

	return &laggingStore{
		inner: inner,
		resources: &laggingResources{
			inner: inner.Resources(),
			lag:   lag,
		},
		resourceDefinitions: &laggingRDs{
			inner: inner.ResourceDefinitions(),
			lag:   lag,
		},
	}
}

func (s *laggingStore) Nodes() store.NodeStore               { return s.inner.Nodes() }
func (s *laggingStore) StoragePools() store.StoragePoolStore { return s.inner.StoragePools() }
func (s *laggingStore) ResourceGroups() store.ResourceGroupStore {
	return s.inner.ResourceGroups()
}

func (s *laggingStore) ResourceDefinitions() store.ResourceDefinitionStore {
	return s.resourceDefinitions
}

func (s *laggingStore) Resources() store.ResourceStore { return s.resources }

func (s *laggingStore) VolumeDefinitions() store.VolumeDefinitionStore {
	return s.inner.VolumeDefinitions()
}

func (s *laggingStore) Snapshots() store.SnapshotStore { return s.inner.Snapshots() }

func (s *laggingStore) PhysicalDevices() store.PhysicalDeviceStore {
	return s.inner.PhysicalDevices()
}

func (s *laggingStore) ControllerProps() store.ControllerPropsStore {
	return s.inner.ControllerProps()
}

// seedRDWithResources stamps an RD and `replicas`-many child
// Resources directly on the inner inmemory store so the bug-fixture
// is ready before the laggingStore semantics kick in. The RD-create
// path through REST writes via the lagging surface too — the
// pending bookkeeping isn't relevant for creates (it only buffers
// deletes), but using the inner store keeps the fixture explicit.
func seedRDWithResources(t *testing.T, st *laggingStore, rdName string, nodes []string) {
	t.Helper()

	ctx := t.Context()

	err := st.inner.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: rdName,
	})
	if err != nil {
		t.Fatalf("seed RD %q: %v", rdName, err)
	}

	for _, n := range nodes {
		err := st.inner.Resources().Create(ctx, &apiv1.Resource{
			Name:     rdName,
			NodeName: n,
		})
		if err != nil {
			t.Fatalf("seed Resource %q on %q: %v", rdName, n, err)
		}
	}
}

// laggingDuration is the simulated informer-cache trail used by the
// Bug 124 tests. Long enough that on current HEAD (no convergence
// wait) the GET-r-list immediately after rd-delete observes the
// phantom rows; short enough that the convergence wait completes
// inside the per-test timeout.
const laggingDuration = 300 * time.Millisecond

// TestBug124RDDeleteInvalidatesResourceListCache pins the core
// contract: after DELETE /v1/resource-definitions/<rd> returns 200,
// the very next GET /v1/view/resources sees zero rows for that RD.
// On current main HEAD (no convergence wait) the GET observes
// phantom rows for `laggingDuration` after the delete.
func TestBug124RDDeleteInvalidatesResourceListCache(t *testing.T) {
	st := newLaggingStore(laggingDuration)
	seedRDWithResources(t, st, "spawned108", []string{"dev-kvaps-worker-1"})

	base, stop := startServerWithStore(t, st)
	defer stop()

	// Sanity: the seeded row is observable before the delete.
	if rows := getViewResources(t, base); len(rows) != 1 {
		t.Fatalf("pre-delete view rows: got %d, want 1 (seed broken)", len(rows))
	}

	delResp := httpDelete(t, base+"/v1/resource-definitions/spawned108")
	_ = delResp.Body.Close()

	if delResp.StatusCode != http.StatusOK {
		t.Fatalf("DELETE status: got %d, want 200", delResp.StatusCode)
	}

	// The wire-level invariant: the GET that runs *immediately*
	// after DELETE returns must see zero phantom rows. With the
	// fix, the handler blocks until the lagging store's pending-
	// delete window expires. Without the fix, the row is still
	// pending and shows up.
	rows := getViewResources(t, base)
	if len(rows) != 0 {
		t.Errorf("post-delete view rows: got %d (phantoms: %+v), want 0",
			len(rows), rowNames(rows))
	}
}

// TestBug124ViewResourcesEndpointAlsoInvalidated is the same
// contract but exercises GET /v1/view/resources after the RD
// delete from a fresh request. The fix covers both `r l` (rendered
// from /v1/view/resources) and `rd l --resources` shapes — they
// share the underlying Store.Resources().List() and so the wait
// covers both with one hook.
func TestBug124ViewResourcesEndpointAlsoInvalidated(t *testing.T) {
	st := newLaggingStore(laggingDuration)
	seedRDWithResources(t, st, "spawned108",
		[]string{"dev-kvaps-worker-1", "dev-kvaps-worker-2"})

	base, stop := startServerWithStore(t, st)
	defer stop()

	// Populate the view first so any future response-cache layer
	// (none today, but the test pins the contract regardless) has
	// something to invalidate.
	if rows := getViewResources(t, base); len(rows) != 2 {
		t.Fatalf("pre-delete view rows: got %d, want 2 (seed broken)", len(rows))
	}

	delResp := httpDelete(t, base+"/v1/resource-definitions/spawned108")
	_ = delResp.Body.Close()

	if delResp.StatusCode != http.StatusOK {
		t.Fatalf("DELETE status: got %d, want 200", delResp.StatusCode)
	}

	// Tight loop of 10 GETs immediately after the DELETE returns.
	// The cluster-side reproducer ran 10 `r l` calls in a tight
	// loop and counted phantoms; pin the same shape here so a
	// future regression is caught on the very first GET, not via
	// flake-after-N.
	for i := range 10 {
		rows := getViewResources(t, base)
		if len(rows) != 0 {
			t.Errorf("iter %d: view rows: got %d (%+v), want 0",
				i, len(rows), rowNames(rows))

			return
		}
	}
}

// TestBug124ResourceDeleteIndividualInvalidates exercises the per-
// replica drop path: DELETE /v1/resource-definitions/<rd>/resources/
// <node> must take the dropped row out of the next `r l` view
// immediately, not after the cache lag. The same convergence wait
// covers this surface via waitForResourceDeletionVisible.
func TestBug124ResourceDeleteIndividualInvalidates(t *testing.T) {
	st := newLaggingStore(laggingDuration)
	seedRDWithResources(t, st, "spawned108",
		[]string{"dev-kvaps-worker-1", "dev-kvaps-worker-2"})

	base, stop := startServerWithStore(t, st)
	defer stop()

	if rows := getViewResources(t, base); len(rows) != 2 {
		t.Fatalf("pre-delete view rows: got %d, want 2 (seed broken)", len(rows))
	}

	delResp := httpDelete(t,
		base+"/v1/resource-definitions/spawned108/resources/dev-kvaps-worker-1")
	_ = delResp.Body.Close()

	if delResp.StatusCode != http.StatusOK {
		t.Fatalf("DELETE status: got %d, want 200", delResp.StatusCode)
	}

	rows := getViewResources(t, base)
	if len(rows) != 1 {
		t.Errorf("post-delete view rows: got %d (%+v), want 1 (only worker-2 left)",
			len(rows), rowNames(rows))

		return
	}

	if rows[0].NodeName != "dev-kvaps-worker-2" {
		t.Errorf("surviving row: NodeName=%q, want %q",
			rows[0].NodeName, "dev-kvaps-worker-2")
	}
}

// TestBug124RDDeleteWaitsForConvergence asserts the "no waiting"
// semantic from the caller's point of view: DELETE returns AFTER
// the lag window has elapsed (because handler waited for cache),
// not before. The exact lower bound is the configured `lag` minus
// jitter; we assert ≥ 80% of the lag so a CPU-pegged CI doesn't
// false-fail on timing noise.
//
// Use a fake clock if necessary to assert "no waiting" semantics
// from the *user's* perspective: from where the user sits, `rd d`
// MUST appear synchronous with respect to the next `r l`. We
// measure that by clocking the DELETE round-trip and checking it
// covered the lag window.
func TestBug124RDDeleteWaitsForConvergence(t *testing.T) {
	st := newLaggingStore(laggingDuration)
	seedRDWithResources(t, st, "spawned108", []string{"dev-kvaps-worker-1"})

	base, stop := startServerWithStore(t, st)
	defer stop()

	start := time.Now()

	delResp := httpDelete(t, base+"/v1/resource-definitions/spawned108")
	_ = delResp.Body.Close()

	elapsed := time.Since(start)

	if delResp.StatusCode != http.StatusOK {
		t.Fatalf("DELETE status: got %d, want 200", delResp.StatusCode)
	}

	// 80%-of-lag floor: the convergence wait must actually wait.
	floor := laggingDuration * 8 / 10
	if elapsed < floor {
		t.Errorf("DELETE returned in %v; want at least %v "+
			"(convergence wait should block until cache catches up)",
			elapsed, floor)
	}

	// And the budget cap (cacheConvergeBudget) must not be exceeded
	// for the well-behaved cache that converges quickly — we don't
	// want the fix to globally inflate every delete by 5 s.
	ceiling := laggingDuration + cacheConvergeBudget
	if elapsed > ceiling {
		t.Errorf("DELETE took %v; want at most %v (budget exceeded)",
			elapsed, ceiling)
	}
}

// (httpDelete is defined in nodes_test.go and reused here.)

// getViewResources GETs /v1/view/resources and decodes the JSON
// envelope into the typed wire shape. Centralised because every
// Bug 124 test does the same shape of read.
func getViewResources(t *testing.T, base string) []apiv1.ResourceWithVolumes {
	t.Helper()

	resp := httpGet(t, base+"/v1/view/resources")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("GET /v1/view/resources status: %d body=%s", resp.StatusCode, body)
	}

	var out []apiv1.ResourceWithVolumes

	err := json.NewDecoder(resp.Body).Decode(&out)
	if err != nil {
		t.Fatalf("decode view: %v", err)
	}

	return out
}

// rowNames is a debug helper for failure messages: render the
// (rd, node) pairs of a result set in a single string so flaking
// tests print actionable phantom-row info.
func rowNames(rows []apiv1.ResourceWithVolumes) []string {
	out := make([]string, 0, len(rows))
	for i := range rows {
		out = append(out, rows[i].Name+"@"+rows[i].NodeName)
	}

	return out
}
