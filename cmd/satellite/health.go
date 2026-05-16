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

// readyState is the satellite's readiness gate. Construction yields a
// not-ready instance; MarkReady flips it to ready exactly once and is
// safe to call repeatedly. Check is the controller-runtime healthz
// shape (`func(*http.Request) error`) so it plugs straight into
// `manager.AddReadyzCheck`.
//
// Motivation (issue 207): cmd/satellite previously exposed no
// /readyz at all, so kubelet had no way to keep a satellite pod out
// of any backing Service until its CRD caches had synced. This gate
// ties /readyz to the manager-cache-sync handoff in main.go — the
// readiness probe trips 503 until the first sync completes, then 200.
type readyState struct {
	ready atomic.Bool
}

// newReadyState returns a fresh not-ready gate.
func newReadyState() *readyState {
	return &readyState{}
}

// MarkReady transitions the gate to ready. Idempotent — wired off
// `mgr.GetCache().WaitForCacheSync` in main.go, but the satellite
// startup may have other one-shot fire-and-forget triggers later;
// calling twice must not regress the state.
func (r *readyState) MarkReady() {
	r.ready.Store(true)
}

// Check satisfies the controller-runtime healthz.Checker contract.
// Returns nil once MarkReady has fired, otherwise a non-nil error
// which CheckHandler renders as HTTP 500 + the error string.
//
// The *http.Request argument is unused but required by the signature.
func (r *readyState) Check(_ *http.Request) error {
	if !r.ready.Load() {
		return errors.New("satellite cache has not completed initial sync")
	}

	return nil
}
