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
	"strings"
	"testing"
	"time"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Bug 193 — `linstor s d X mysnap` returns 200 immediately even when
// the Snapshot CRD's satellite-side finalizer never runs (satellite
// paused, disconnected, reconciler crashed). Result: orphan Snapshot
// CRDs pile up under `kubectl get snapshot` because the apiserver's
// success reply made operators (and CSI replays) move on.
//
// These tests pin three contracts on `handleSnapshotDelete`:
//
//  1. Wait-for-convergence: the handler blocks until the local store
//     reports the snapshot is gone, OR a bounded timeout fires.
//  2. Stuck-finalizer surface: when the snapshot stays observable past
//     the wait budget, the response flips to 504 + an actionable
//     envelope citing the satellite issue. The naked 200 success path
//     from Bug 193 must not happen on a stuck satellite.
//  3. Fast paths: snapshots without the satellite finalizer (or already
//     reaped) return immediately — no extra latency.

// stuckSnapshots wraps an InMemory SnapshotStore so that, once Delete
// has been called against `(rdName, snapName)`, subsequent Get calls
// keep returning the snapshot indefinitely (or for `clearAfter` time)
// — exactly the wire-shape a Snapshot CRD with a never-running
// satellite finalizer presents. Other surfaces (Create / List /
// Update) pass through unchanged.
//
// When `clearAfter == 0` the snapshot is stuck forever (simulates a
// dead satellite). When `clearAfter > 0` the inner Delete is deferred
// to a goroutine that fires after the lag — simulating a slow-but-
// healthy satellite that does eventually strip the finalizer.
type stuckSnapshots struct {
	inner store.SnapshotStore

	clearAfter time.Duration
}

func (s *stuckSnapshots) List(ctx context.Context) ([]apiv1.Snapshot, error) {
	return s.inner.List(ctx) //nolint:wrapcheck // test helper
}

func (s *stuckSnapshots) ListByDefinition(ctx context.Context, rdName string) ([]apiv1.Snapshot, error) {
	return s.inner.ListByDefinition(ctx, rdName) //nolint:wrapcheck // test helper
}

func (s *stuckSnapshots) Get(ctx context.Context, rdName, snapName string) (apiv1.Snapshot, error) {
	return s.inner.Get(ctx, rdName, snapName) //nolint:wrapcheck // test helper
}

func (s *stuckSnapshots) Create(ctx context.Context, snap *apiv1.Snapshot) error {
	return s.inner.Create(ctx, snap) //nolint:wrapcheck // test helper
}

func (s *stuckSnapshots) Update(ctx context.Context, snap *apiv1.Snapshot) error {
	return s.inner.Update(ctx, snap) //nolint:wrapcheck // test helper
}

// Delete confirms the row exists right now (so genuinely-absent
// snapshots still surface NotFound to the caller) but does NOT remove
// it from the inner store synchronously. When `clearAfter` is zero
// the row stays forever — the caller's follow-up Get will keep
// returning the snap, mimicking a Snapshot CRD whose satellite
// finalizer never gets stripped. When `clearAfter` is > 0 the inner
// Delete is scheduled to fire after the lag, modelling a healthy-but-
// slow satellite.
func (s *stuckSnapshots) Delete(ctx context.Context, rdName, snapName string) error {
	_, err := s.inner.Get(ctx, rdName, snapName)
	if err != nil {
		return err //nolint:wrapcheck // test helper
	}

	if s.clearAfter == 0 {
		// Stuck forever — the apiserver acked the Delete (we return
		// nil) but the row stays visible to follow-up Gets.
		return nil
	}

	lag := s.clearAfter
	inner := s.inner

	go func() { //nolint:contextcheck // commit goroutine outlives caller ctx by design
		time.Sleep(lag)

		_ = inner.Delete(context.Background(), rdName, snapName)
	}()

	return nil
}

// stuckSnapshotStore is a Store wrapping InMemory where only the
// SnapshotStore surface uses the stuck wrapper. The rest of the
// store passes through unchanged.
type stuckSnapshotStore struct {
	inner *store.InMemory

	snapshots *stuckSnapshots
}

func newStuckSnapshotStore(clearAfter time.Duration) *stuckSnapshotStore {
	inner := store.NewInMemory()

	return &stuckSnapshotStore{
		inner: inner,
		snapshots: &stuckSnapshots{
			inner:      inner.Snapshots(),
			clearAfter: clearAfter,
		},
	}
}

func (s *stuckSnapshotStore) Nodes() store.NodeStore               { return s.inner.Nodes() }
func (s *stuckSnapshotStore) StoragePools() store.StoragePoolStore { return s.inner.StoragePools() }
func (s *stuckSnapshotStore) ResourceGroups() store.ResourceGroupStore {
	return s.inner.ResourceGroups()
}

func (s *stuckSnapshotStore) ResourceDefinitions() store.ResourceDefinitionStore {
	return s.inner.ResourceDefinitions()
}

func (s *stuckSnapshotStore) Resources() store.ResourceStore { return s.inner.Resources() }

func (s *stuckSnapshotStore) VolumeDefinitions() store.VolumeDefinitionStore {
	return s.inner.VolumeDefinitions()
}

func (s *stuckSnapshotStore) Snapshots() store.SnapshotStore { return s.snapshots }

func (s *stuckSnapshotStore) PhysicalDevices() store.PhysicalDeviceStore {
	return s.inner.PhysicalDevices()
}

func (s *stuckSnapshotStore) ControllerProps() store.ControllerPropsStore {
	return s.inner.ControllerProps()
}

// seedSnapshot stamps a parent RD + Snapshot directly on the inner
// inmemory store so the bug-fixture is in place before the stuck
// semantics kick in on the Delete path.
func seedSnapshot(t *testing.T, st *stuckSnapshotStore, rd, snap string) {
	t.Helper()

	ctx := t.Context()

	err := st.inner.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rd})
	if err != nil {
		t.Fatalf("seed RD %q: %v", rd, err)
	}

	err = st.inner.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name:         snap,
		ResourceName: rd,
	})
	if err != nil {
		t.Fatalf("seed Snapshot %s/%s: %v", rd, snap, err)
	}
}

// readEnvelope decodes the LINSTOR-shaped []ApiCallRc body. Returns
// the slice plus the raw body for failure messages.
func readEnvelope(t *testing.T, resp *http.Response) ([]apiv1.APICallRc, []byte) {
	t.Helper()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	var rcs []apiv1.APICallRc

	err = json.Unmarshal(body, &rcs)
	if err != nil {
		t.Fatalf("decode envelope: %v (raw=%s)", err, body)
	}

	return rcs, body
}

// TestBug193SnapshotDeleteWaitsForFinalizer is the primary contract:
// when the satellite finalizer never runs (the stuckSnapshots wrapper
// holds the row forever), DELETE must NOT return 200 immediately.
// Instead the handler waits up to the bounded budget, then surfaces a
// 504 envelope citing the stuck satellite. The pre-fix wire shape was
// "200 + snapshot deleted: snap1" within milliseconds — a regression
// here means we've gone back to lying to the caller about the
// snapshot being reaped.
func TestBug193SnapshotDeleteWaitsForFinalizer(t *testing.T) {
	st := newStuckSnapshotStore(0) // 0 = stuck forever
	seedSnapshot(t, st, "rd1", "snap1")

	base, stop := startServerWithStore(t, st)
	defer stop()

	start := time.Now()

	resp := httpDelete(t, base+"/v1/resource-definitions/rd1/snapshots/snap1")
	defer func() { _ = resp.Body.Close() }()

	elapsed := time.Since(start)

	// 1) Status: 504 Gateway Timeout, not 200.
	if resp.StatusCode != http.StatusGatewayTimeout {
		body, _ := io.ReadAll(resp.Body)
		t.Errorf("status: got %d, want 504 (body=%s)", resp.StatusCode, body)

		return
	}

	// 2) Envelope: ApiCallRc array with the MASK_ERROR bit set, a
	//    cause line that names the satellite finalizer stuck-state,
	//    and a correction pointing operators at the satellite.
	rcs, raw := readEnvelope(t, resp)
	if len(rcs) == 0 {
		t.Fatalf("empty envelope: %s", raw)
	}

	if rcs[0].RetCode&apiCallRcError == 0 {
		t.Errorf("envelope ret_code missing MASK_ERROR bit: 0x%x (raw=%s)",
			rcs[0].RetCode, raw)
	}

	// Cause must cite the satellite finalizer / timeout — operators
	// reading the message learn what to look at.
	mustContainAny(t, "cause", rcs[0].Cause,
		"satellite", "finalizer", "timeout")

	// Correction must point at the satellite — the operator's next
	// action is to inspect the satellite, not retry the REST call.
	mustContainAny(t, "correction", rcs[0].Correc, "satellite")

	// 3) The handler must actually have waited — not given up in
	//    milliseconds and not parked past the bounded budget. We
	//    allow a wide jitter window (cache-converge poll cadence
	//    is 500 ms today).
	floor := 5 * time.Second
	ceiling := 12 * time.Second

	if elapsed < floor {
		t.Errorf("DELETE returned in %v; want at least %v (handler should "+
			"have waited for the finalizer)", elapsed, floor)
	}

	if elapsed > ceiling {
		t.Errorf("DELETE returned in %v; want at most %v (wait budget "+
			"should be bounded)", elapsed, ceiling)
	}
}

// TestBug193SnapshotDeleteFastPathWhenNoFinalizer pins the regression
// floor: a healthy satellite that runs the finalizer on the very next
// reconcile (clearAfter = 50ms, well under the 500ms poll interval)
// must surface 200 within a tight bound. The fix must not punish the
// healthy path with a multi-second timeout — it only kicks in when
// the snapshot stays observable.
func TestBug193SnapshotDeleteFastPathWhenNoFinalizer(t *testing.T) {
	// 50ms clearAfter: the goroutine fires fast, but the first
	// 500ms poll tick from the convergence loop has not yet hit.
	// The handler should observe the deletion on its second poll
	// — well inside the 10s budget, and the elapsed time will be
	// dominated by the polling cadence (one tick ≈ 500ms).
	st := newStuckSnapshotStore(50 * time.Millisecond)
	seedSnapshot(t, st, "rd1", "snap1")

	base, stop := startServerWithStore(t, st)
	defer stop()

	start := time.Now()

	resp := httpDelete(t, base+"/v1/resource-definitions/rd1/snapshots/snap1")
	defer func() { _ = resp.Body.Close() }()

	elapsed := time.Since(start)

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Errorf("status: got %d, want 200 (body=%s)", resp.StatusCode, body)

		return
	}

	rcs, raw := readEnvelope(t, resp)
	if len(rcs) == 0 {
		t.Fatalf("empty envelope: %s", raw)
	}

	// Success envelope: RetCode is non-negative (no MASK_ERROR bit).
	if rcs[0].RetCode&apiCallRcError != 0 {
		t.Errorf("envelope unexpectedly carries MASK_ERROR: 0x%x (raw=%s)",
			rcs[0].RetCode, raw)
	}

	// Within one polling tick + jitter. The poll cadence is 500ms;
	// 2s leaves comfortable headroom for CI noise.
	if elapsed > 2*time.Second {
		t.Errorf("DELETE returned in %v; want under 2s (fast path "+
			"must not pay the stuck-finalizer wait budget)", elapsed)
	}
}

// TestBug193SnapshotDeleteAlreadyDeleted pins the idempotent
// no-op path: a snapshot that was never there must return 200 + warn
// envelope immediately, with no wait. CSI DeleteSnapshot retries
// after a successful drop need this to stay sub-second.
func TestBug193SnapshotDeleteAlreadyDeleted(t *testing.T) {
	st := newStuckSnapshotStore(0)
	// Seed only the parent RD — no snapshot row. The first DELETE
	// hits the absent-snapshot fast path before the stuck wrapper
	// gets engaged.
	err := st.inner.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "rd1"})
	if err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	start := time.Now()

	resp := httpDelete(t, base+"/v1/resource-definitions/rd1/snapshots/snap-ghost")
	defer func() { _ = resp.Body.Close() }()

	elapsed := time.Since(start)

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Errorf("status: got %d, want 200 (body=%s)", resp.StatusCode, body)

		return
	}

	rcs, raw := readEnvelope(t, resp)
	if len(rcs) == 0 {
		t.Fatalf("empty envelope: %s", raw)
	}

	// The warn-not-found mask is the idempotency contract from the
	// pre-existing handleSnapshotDelete code (cli-parity-audit #33).
	if rcs[0].RetCode != warnSnapshotNotFound {
		t.Errorf("idempotent ret_code: got 0x%x, want warnSnapshotNotFound (0x%x)",
			rcs[0].RetCode, warnSnapshotNotFound)
	}

	// Must NOT pay the stuck-finalizer wait budget on the no-op path.
	if elapsed > 2*time.Second {
		t.Errorf("DELETE returned in %v; want sub-second (idempotent "+
			"no-op must not wait)", elapsed)
	}
}

// mustContainAny fails the test if `haystack` contains none of the
// needles (case-insensitive). Used to assert envelope strings without
// over-constraining the exact wording.
func mustContainAny(t *testing.T, field, haystack string, needles ...string) {
	t.Helper()

	lower := strings.ToLower(haystack)
	for _, n := range needles {
		if strings.Contains(lower, strings.ToLower(n)) {
			return
		}
	}

	t.Errorf("envelope %s does not mention any of %v: %q",
		field, needles, haystack)
}
