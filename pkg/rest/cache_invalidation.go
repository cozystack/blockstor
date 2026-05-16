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
	"time"

	"github.com/cockroachdb/errors"

	"github.com/cozystack/blockstor/pkg/store"
)

// Bug 124: phantom DRBD resources persist on `linstor r l` for tens of
// seconds after `linstor rd d` returns SUCCESS.
//
// Root cause is informer-cache lag. The REST `Store` is a thin shim
// over a controller-runtime cached client. Writes (Delete here) are
// sent straight to the apiserver and acknowledged synchronously, but
// subsequent reads (List / Get) on the same client come back through
// the informer cache, which only updates when its watch stream
// observes the change. In a 3-replica apiserver Deployment the cache
// also has to roundtrip the etcd-replicated event onto the local
// informer; between SUCCESS and the next watch frame, `Resources().
// List()` returns the pre-delete picture.
//
// The fix is a post-delete cache-convergence wait. After every write
// that the user expects to be reflected on the very next read, we
// poll the store's read path until the deletion is observable, then
// return. This is the "cache invalidation hook" — we don't actively
// invalidate the informer cache (controller-runtime doesn't expose
// that surface), but we do block the caller's response until the
// invalidation has happened on the local replica.
//
// Trade-off: `rd d` and `r d` now pay a few-ms-to-low-hundreds-of-ms
// extra on the response, in exchange for monotonic read-your-writes
// on `r l` / `view/resources`. The latency budget on `rd d` is
// dominated by the satellite finalizer drain (seconds), so the
// convergence wait is in the noise. `r l` itself stays cache-hot
// (no extra apiserver round-trip on the hot read path), so the
// list latency cost is zero — only the writer pays.
//
// Alternative considered: route resource-list reads through
// `mgr.GetAPIReader()` to bypass the informer cache entirely. That
// fixes the symptom but pays an apiserver round-trip on every `r l`,
// which is hit on a tight loop by linstor-csi's ListVolumes and the
// recovery copilot. Cache-invalidation-after-write is strictly
// cheaper at steady state (writers are rare, readers are not).

// cacheConvergeBudget is the upper bound on how long handleRDDelete /
// handleResourceDelete will block waiting for the local informer
// cache to observe the deletion. Picked so a healthy cache (single-
// digit-millisecond watch latency) is well within budget, but a
// degenerate cluster (apiserver overloaded, watch stalled) doesn't
// hang a synchronous CSI call past its own gRPC deadline.
//
// On timeout we surface SUCCESS anyway: the apiserver write did
// commit (the user-visible delete completed), so the caller seeing
// a phantom row on the next `r l` is the same UX we had before this
// fix — a strict regression to pre-Bug-124 behaviour, not a new bug.
const cacheConvergeBudget = 5 * time.Second

// cacheConvergePollInterval is the gap between successive read
// attempts during the convergence wait. 50 ms keeps the worst-case
// extra latency under 100 ms in the steady-state-fast case (one or
// two polls before the cache catches up), and at 100 polls/sec the
// load on the store interface is negligible.
const cacheConvergePollInterval = 50 * time.Millisecond

// waitForRDDeletionVisible blocks until the local Store reports that
// the ResourceDefinition `name` and every child Resource under it
// are gone from the cache view, or `cacheConvergeBudget` elapses.
// Returns nil whether the deletion converged or the budget ran out
// — both are acceptable outcomes for the caller (the write already
// committed). Context cancellation aborts the wait early.
//
// This is invoked by handleRDDelete AFTER the apiserver-side Delete
// returns success, so the lifetimes line up: every reader on this
// replica sees a converged cache by the time the handler responds
// 200.
func (s *Server) waitForRDDeletionVisible(ctx context.Context, name string) {
	if s == nil || s.Store == nil {
		return
	}

	deadline := time.Now().Add(cacheConvergeBudget)

	for {
		if rdDeletionVisible(ctx, s.Store, name) {
			return
		}

		if time.Now().After(deadline) {
			return
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(cacheConvergePollInterval):
		}
	}
}

// rdDeletionVisible is the single-shot predicate used by
// waitForRDDeletionVisible: "does the local store agree that this RD
// and its children are gone?". RD-level visibility is tested via Get
// (NotFound = converged); child-resource visibility via
// ListByDefinition (empty = converged). Both must hold to return
// true; either Lagging surface is enough to keep waiting.
//
// Any non-NotFound error (transport, decode, …) on either probe is
// treated as "converged enough" — we don't want to loop forever on a
// permanent failure, and the caller has already committed the write
// upstream of this check.
func rdDeletionVisible(ctx context.Context, st store.Store, name string) bool {
	_, err := st.ResourceDefinitions().Get(ctx, name)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return true
	}

	if err == nil {
		// RD still observable in cache.
		return false
	}

	children, listErr := st.Resources().ListByDefinition(ctx, name)
	if listErr != nil && !errors.Is(listErr, store.ErrNotFound) {
		return true
	}

	return len(children) == 0
}

// waitForResourceDeletionVisible blocks until the local Store
// reports that the single replica (rdName, node) is gone from the
// cache view, or `cacheConvergeBudget` elapses. Mirror of
// waitForRDDeletionVisible for the per-replica DELETE endpoint
// (`DELETE /v1/resource-definitions/{rd}/resources/{node}`).
//
// Invoked by handleResourceDelete after the apiserver-side Delete
// commits, so a follow-up `r l` on the same replica reflects the
// drop immediately rather than after a 5–30 s watch lag.
func (s *Server) waitForResourceDeletionVisible(ctx context.Context, rdName, node string) {
	if s == nil || s.Store == nil {
		return
	}

	deadline := time.Now().Add(cacheConvergeBudget)

	for {
		if resourceDeletionVisible(ctx, s.Store, rdName, node) {
			return
		}

		if time.Now().After(deadline) {
			return
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(cacheConvergePollInterval):
		}
	}
}

// resourceDeletionVisible is the single-shot predicate behind
// waitForResourceDeletionVisible: "does the local store agree that
// this single replica is gone?". A NotFound from Get means
// converged; any non-NotFound error is also treated as "stop
// waiting" (same rationale as rdDeletionVisible).
func resourceDeletionVisible(ctx context.Context, st store.Store, rdName, node string) bool {
	_, err := st.Resources().Get(ctx, rdName, node)
	if err == nil {
		return false
	}

	if errors.Is(err, store.ErrNotFound) {
		return true
	}

	// Non-NotFound error: don't loop on a permanent failure.
	return true
}

// waitForVDDeletionVisible blocks until the local Store reports that
// VolumeDefinition (rdName, volumeNumber) is gone from the cache
// view, or `cacheConvergeBudget` elapses. Mirror of
// waitForRDDeletionVisible for the per-VD DELETE endpoint
// (`DELETE /v1/resource-definitions/{rd}/volume-definitions/{vn}`).
// Bug 139: without this hook the very next `GET /v1/view/resources`
// after VD delete catches the pre-delete picture in the informer
// cache and surfaces the dropped volume on the per-resource Volumes
// slice for tens of seconds.
func (s *Server) waitForVDDeletionVisible(ctx context.Context, rdName string, volumeNumber int32) {
	if s == nil || s.Store == nil {
		return
	}

	deadline := time.Now().Add(cacheConvergeBudget)

	for {
		if vdDeletionVisible(ctx, s.Store, rdName, volumeNumber) {
			return
		}

		if time.Now().After(deadline) {
			return
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(cacheConvergePollInterval):
		}
	}
}

// vdDeletionVisible is the single-shot predicate behind
// waitForVDDeletionVisible: "does the local store agree that this
// VD is gone?". A NotFound from Get means converged; any non-
// NotFound error is also treated as "stop waiting" (same rationale
// as rdDeletionVisible). The parent RD missing also counts as
// converged — the VD can't exist under an absent parent.
func vdDeletionVisible(ctx context.Context, st store.Store, rdName string, volumeNumber int32) bool {
	_, err := st.VolumeDefinitions().Get(ctx, rdName, volumeNumber)
	if err == nil {
		return false
	}

	// Any error path — NotFound on either VD or parent RD,
	// transport, decode — counts as "stop waiting".
	return true
}

// Bug 193: `linstor s d X mysnap` returns 200 immediately even when
// the Snapshot CRD's satellite-side finalizer never runs (paused /
// disconnected / crashed satellite). The DELETE call lands K8s-side
// metadata.deletionTimestamp but the CRD survives until the
// satellite reconciler drops `blockstor.io.blockstor.io/satellite-
// snapshot` from the finalizer list. Operators saw orphan Snapshot
// CRDs piling up under `kubectl get snapshot` because the apiserver's
// success reply made them (and CSI replays) move on, none the wiser.
//
// The fix is a Bug-124/139-style bounded wait, but with a different
// failure mode: if the snapshot stays observable past the budget we
// surface 504 + an actionable envelope instead of swallowing the
// timeout, because the underlying assumption (satellite finalizer
// will run any moment now) has demonstrably broken. Operators get a
// concrete next action ("check satellite status") instead of a
// silently-half-completed delete.

// snapshotDeleteWaitBudget is the upper bound on how long
// handleSnapshotDelete will block waiting for the Snapshot CRD to
// disappear after issuing Delete. 10s leaves comfortable headroom
// for a healthy satellite's reconcile loop (the satellite's own
// snapshot finalizer requeue is 1s — pkg/satellite/controllers/
// snapshot.go: `snapshotFinalizerRequeue = time.Second`) while
// still capping the per-call worst-case latency. Past the budget
// we conclude the satellite is stuck and surface 504.
const snapshotDeleteWaitBudget = 10 * time.Second

// snapshotDeleteWaitPoll is the cadence between successive Get
// probes during the wait. 500ms matches the Bug 124 / 139 spec
// — fast enough that a one-tick reconcile lands inside the second
// poll, slow enough that the K8s read load is negligible on a
// 3-replica apiserver.
const snapshotDeleteWaitPoll = 500 * time.Millisecond

// waitForSnapshotDeletionVisible blocks until the local Store
// reports the Snapshot (rdName, snapName) is gone, OR the wait
// budget elapses. Returns true if convergence was observed (the
// snapshot is gone — finalizer drained, CRD reaped); returns false
// on budget exhaustion or ctx cancellation (the snapshot is still
// observable, satellite likely stuck).
//
// Unlike waitForRDDeletionVisible / waitForVDDeletionVisible (which
// always return success because the apiserver write committed even
// if the cache hasn't caught up), this helper's caller MUST
// distinguish converged-vs-timeout: a stuck satellite is a real
// failure mode the operator needs to know about, not a silently-
// swallowed "still pending" cache lag.
//
// Use the request context's deadline if shorter than the budget so
// CSI-side gRPC deadlines cap our wait, not the other way around.
func (s *Server) waitForSnapshotDeletionVisible(ctx context.Context, rdName, snapName string) bool {
	if s == nil || s.Store == nil {
		return true
	}

	deadline := time.Now().Add(snapshotDeleteWaitBudget)

	// Honour the caller's deadline if it sits inside ours — a CSI
	// driver with a tight gRPC timeout shouldn't get parked for a
	// full 10s.
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}

	for {
		if snapshotDeletionVisible(ctx, s.Store, rdName, snapName) {
			return true
		}

		if !time.Now().Before(deadline) {
			return false
		}

		select {
		case <-ctx.Done():
			return false
		case <-time.After(snapshotDeleteWaitPoll):
		}
	}
}

// snapshotDeletionVisible is the single-shot predicate behind
// waitForSnapshotDeletionVisible: "does the local store agree that
// this Snapshot is gone?". A NotFound from Get means converged
// (finalizer drained, CRD reaped); a successful Get means the CRD
// is still observable, almost certainly because the satellite
// finalizer hasn't run yet. Any non-NotFound transport error counts
// as "stop waiting" — we don't loop forever on a permanent
// store-layer failure.
func snapshotDeletionVisible(ctx context.Context, st store.Store, rdName, snapName string) bool {
	_, err := st.Snapshots().Get(ctx, rdName, snapName)
	if err == nil {
		// CRD still observable — finalizer likely still set.
		return false
	}

	// NotFound OR any other error: stop waiting. NotFound is the
	// converged case; a transport error means we can't probe and
	// looping won't help.
	return true
}
