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

// readyState is the apiserver's readiness gate. Construction yields a
// not-ready instance; MarkCacheSynced and MarkBound each flip an
// independent latch and Check returns nil only once BOTH have fired.
// Check is the controller-runtime healthz shape
// (`func(*http.Request) error`) so it plugs straight into
// `manager.AddReadyzCheck`.
//
// Motivation (issue 213): cmd/apiserver previously exposed /readyz via
// `healthz.Ping`, which is true immediately on registration. The
// kubelet would mark the pod Ready before controller-runtime's cache
// had completed its initial sync AND before the REST listener had
// bound, so kube-proxy added the pod to EndpointSlice in a window
// where clients hitting it got `Connection refused` (no socket) or
// stale-empty cached reads (no informer data). This surfaced as
// spurious 5xx during rolling restarts of the apiserver Deployment.
//
// The gate mirrors cmd/satellite's post-issue-207 readyState but with
// two independent signals — cache-sync alone is not enough for the
// apiserver because the REST listener lives on a separate port and
// its bind happens asynchronously off `mgr.Add(&rest.Server{...})`.
type readyState struct {
	cacheSynced atomic.Bool
	bound       atomic.Bool
}

// newReadyState returns a fresh not-ready gate.
func newReadyState() *readyState {
	return &readyState{}
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
// Returns nil once BOTH MarkCacheSynced and MarkBound have fired,
// otherwise a non-nil error which CheckHandler renders as HTTP 500
// + the error string.
//
// The *http.Request argument is unused but required by the signature.
func (r *readyState) Check(_ *http.Request) error {
	if !r.cacheSynced.Load() {
		return errors.New("apiserver cache has not completed initial sync")
	}

	if !r.bound.Load() {
		return errors.New("apiserver REST listener has not bound")
	}

	return nil
}
