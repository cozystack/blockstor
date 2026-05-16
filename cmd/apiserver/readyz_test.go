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
	"net/http/httptest"
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/healthz"
)

// TestReadyState_StartsNotReady pins the initial state of the
// apiserver's readiness gate: freshly constructed it reports
// not-ready, so kube-proxy keeps the apiserver pod out of the
// backing Service until BOTH (a) controller-runtime's cache has
// completed its initial sync AND (b) the REST listener has bound.
//
// Regression guard for issue 213: cmd/apiserver previously wired
// readyz to healthz.Ping, which is true immediately, so clients
// hitting EndpointSlice in the first ~1-5 s after pod start got
// "Connection refused" and spurious 5xx during rolling restarts.
func TestReadyState_StartsNotReady(t *testing.T) {
	t.Parallel()

	rs := newReadyState()

	err := rs.Check(nil)
	if err == nil {
		t.Fatal("expected readyState.Check to fail before any signal, got nil")
	}
}

// TestReadyState_CacheSyncAloneNotReady verifies that the cache-sync
// signal on its own does NOT satisfy the gate. The REST listener
// must also have bound — otherwise kube-proxy would route traffic
// to a pod that has no socket open and clients would see
// "Connection refused".
func TestReadyState_CacheSyncAloneNotReady(t *testing.T) {
	t.Parallel()

	rs := newReadyState()
	rs.MarkCacheSynced()

	err := rs.Check(nil)
	if err == nil {
		t.Fatal("expected readyState.Check to fail with only cache-sync signal, got nil")
	}
}

// TestReadyState_BindAloneNotReady verifies that the REST-bind
// signal on its own does NOT satisfy the gate. The
// controller-runtime cache must also have completed its initial
// sync — otherwise cached-client reads from REST handlers would
// return stale-empty results (no objects → 404 envelope for
// freshly-created CRDs) until the informer catches up.
func TestReadyState_BindAloneNotReady(t *testing.T) {
	t.Parallel()

	rs := newReadyState()
	rs.MarkBound()

	err := rs.Check(nil)
	if err == nil {
		t.Fatal("expected readyState.Check to fail with only REST-bind signal, got nil")
	}
}

// TestReadyState_BothSignalsReady verifies that once BOTH signals
// have fired, Check returns nil and the kubelet/kube-proxy is free
// to route traffic. Order of the two MarkX calls is irrelevant —
// cache-sync may complete before or after the REST listener binds
// depending on cluster cold/warm state.
func TestReadyState_BothSignalsReady(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		mark func(*readyState)
	}{
		{
			name: "cache-sync then bind",
			mark: func(rs *readyState) {
				rs.MarkCacheSynced()
				rs.MarkBound()
			},
		},
		{
			name: "bind then cache-sync",
			mark: func(rs *readyState) {
				rs.MarkBound()
				rs.MarkCacheSynced()
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rs := newReadyState()
			tc.mark(rs)

			err := rs.Check(nil)
			if err != nil {
				t.Fatalf("expected nil after both signals, got %v", err)
			}
		})
	}
}

// TestReadyState_IdempotentMarks verifies that calling MarkCacheSynced
// / MarkBound more than once is safe and does not regress the gate.
// The startup path may have multiple fire-and-forget triggers later;
// neither signal must ever flip back to not-ready.
func TestReadyState_IdempotentMarks(t *testing.T) {
	t.Parallel()

	rs := newReadyState()
	rs.MarkCacheSynced()
	rs.MarkCacheSynced()
	rs.MarkBound()
	rs.MarkBound()

	err := rs.Check(nil)
	if err != nil {
		t.Fatalf("expected nil after idempotent marks, got %v", err)
	}
}

// TestReadyState_AsHTTPHandler wires readyState.Check through
// healthz.CheckHandler — the same wrapper controller-runtime uses
// to mount the check on /readyz — and asserts the HTTP status flips
// from non-2xx to 200 only once BOTH signals have fired. This is
// the contract kube-proxy sees over the wire.
func TestReadyState_AsHTTPHandler(t *testing.T) {
	t.Parallel()

	rs := newReadyState()
	handler := healthz.CheckHandler{Checker: rs.Check}

	// Pre-any-signal: non-2xx so kube-proxy keeps the pod out
	// of EndpointSlice.
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/readyz", nil)
	handler.ServeHTTP(rec, req)

	if rec.Code == http.StatusOK {
		t.Fatalf("expected non-200 before any signal, got %d", rec.Code)
	}

	// Cache-sync only: still non-2xx.
	rs.MarkCacheSynced()

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code == http.StatusOK {
		t.Fatalf("expected non-200 with only cache-sync, got %d", rec.Code)
	}

	// REST-bind also fires → 200.
	rs.MarkBound()

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 after both signals, got %d body=%q", rec.Code, rec.Body.String())
	}
}
