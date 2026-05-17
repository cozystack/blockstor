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

package main

import (
	"net/http"
	"sync/atomic"

	"github.com/cockroachdb/errors"
)

// readyState is the controller's readiness gate. Construction yields a
// not-ready instance; MarkCacheSynced and MarkBound each flip an
// independent latch and Check returns nil once all required signals
// have fired. Check is the controller-runtime healthz shape
// (`func(*http.Request) error`) so it plugs straight into
// `manager.AddReadyzCheck`.
//
// Motivation (issue 217): cmd/controller previously exposed /readyz
// via `healthz.Ping`, which is true immediately on registration —
// the same defect Bug 207 fixed for cmd/satellite and Bug 213 fixed
// for cmd/apiserver. The legacy single-binary controller path
// re-opened the issue: kubelet would mark the pod Ready before
// controller-runtime's cache had completed its initial sync, so
// reconcilers running against an empty informer would issue spurious
// writes / racy decisions for the first ~1-5 s after pod start.
//
// The type is intentionally a duplicate of cmd/apiserver's readyState
// rather than a shared `pkg/health` package — extracting it would
// drag cmd/satellite (which uses a simpler single-signal gate) into a
// refactor outside the scope of this fix. Future consolidation should
// land in its own change.
//
// The gate has two independent signals:
//   - MarkCacheSynced: always required, flipped off
//     `mgr.GetCache().WaitForCacheSync`. Reconcilers must not start
//     making decisions on a stale-empty informer.
//   - MarkBound: only required when ArmREST has been called (the
//     --enable-rest-api=true single-binary deployment). When REST is
//     OFF (the default since the Phase 11.x apiserver split),
//     MarkBound is never called and the gate becomes ready as soon
//     as the cache syncs.
type readyState struct {
	cacheSynced atomic.Bool
	bound       atomic.Bool
	restArmed   atomic.Bool
}

// newReadyState returns a fresh not-ready gate.
func newReadyState() *readyState {
	return &readyState{}
}

// ArmREST tells the gate that a REST listener is part of the readiness
// contract. Called when --enable-rest-api=true so the gate also waits
// on MarkBound; not called when REST is OFF (the default).
func (r *readyState) ArmREST() {
	r.restArmed.Store(true)
}

// MarkCacheSynced transitions the cache-sync latch to true.
// Idempotent — wired off `mgr.GetCache().WaitForCacheSync` in main.go.
func (r *readyState) MarkCacheSynced() {
	r.cacheSynced.Store(true)
}

// MarkBound transitions the REST-bind latch to true. Idempotent —
// wired off the rest.Server.OnReady callback which fires once
// net.Listen has returned a usable listener (before Serve enters its
// accept loop).
func (r *readyState) MarkBound() {
	r.bound.Store(true)
}

// Check satisfies the controller-runtime healthz.Checker contract.
// Returns nil once all required signals have fired, otherwise a
// non-nil error which CheckHandler renders as HTTP 500 + the error
// string.
//
// The *http.Request argument is unused but required by the signature.
func (r *readyState) Check(_ *http.Request) error {
	if !r.cacheSynced.Load() {
		return errors.New("controller cache has not completed initial sync")
	}

	if r.restArmed.Load() && !r.bound.Load() {
		return errors.New("controller REST listener has not bound")
	}

	return nil
}
