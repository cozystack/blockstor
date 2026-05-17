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
// controller's readiness gate: freshly constructed it reports
// not-ready, so kube-proxy keeps the controller pod out of the
// backing Service until BOTH (a) controller-runtime's cache has
// completed its initial sync AND (b) — when --enable-rest-api=true —
// the REST listener has bound.
//
// Regression guard for issue 217: cmd/controller previously wired
// /readyz to healthz.Ping, mirroring the same defect Bug 207 fixed
// for cmd/satellite and Bug 213 fixed for cmd/apiserver. The legacy
// single-binary controller path re-opened the issue — even though
// REST is OFF by default, the manifest's readyz query would return
// 200 before reconciler caches were synced and surface as racing
// writes on cold-start.
func TestReadyState_StartsNotReady(t *testing.T) {
	t.Parallel()

	rs := newReadyState()

	err := rs.Check(nil)
	if err == nil {
		t.Fatal("expected readyState.Check to fail before any signal, got nil")
	}
}

// TestReadyState_CacheSyncAloneNotReadyWithREST verifies that when
// the REST gate is armed (--enable-rest-api=true), cache-sync alone
// is not enough — the REST listener must also have bound. Otherwise
// kube-proxy would route LINSTOR-CLI / linstor-csi traffic to a pod
// with no socket open and clients would see "Connection refused".
func TestReadyState_CacheSyncAloneNotReadyWithREST(t *testing.T) {
	t.Parallel()

	rs := newReadyState()
	rs.ArmREST()
	rs.MarkCacheSynced()

	err := rs.Check(nil)
	if err == nil {
		t.Fatal("expected readyState.Check to fail with only cache-sync (REST armed), got nil")
	}
}

// TestReadyState_BindAloneNotReady verifies that the REST-bind
// signal on its own does NOT satisfy the gate — cache-sync is
// still required so cached-client reads from REST handlers do not
// return stale-empty results before the informer has caught up.
func TestReadyState_BindAloneNotReady(t *testing.T) {
	t.Parallel()

	rs := newReadyState()
	rs.ArmREST()
	rs.MarkBound()

	err := rs.Check(nil)
	if err == nil {
		t.Fatal("expected readyState.Check to fail with only REST-bind signal, got nil")
	}
}

// TestReadyState_CacheSyncReadyWhenRESTOff verifies that with REST
// disabled (the default for the controller binary), cache-sync alone
// is sufficient — there is no REST listener to wait for, only the
// reconciler caches.
func TestReadyState_CacheSyncReadyWhenRESTOff(t *testing.T) {
	t.Parallel()

	rs := newReadyState()
	rs.MarkCacheSynced()

	err := rs.Check(nil)
	if err != nil {
		t.Fatalf("expected nil with cache-sync (REST not armed), got %v", err)
	}
}

// TestReadyState_BothSignalsReady verifies that once BOTH signals
// have fired (REST armed), Check returns nil regardless of the
// order in which MarkCacheSynced and MarkBound were called.
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
			rs.ArmREST()
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
func TestReadyState_IdempotentMarks(t *testing.T) {
	t.Parallel()

	rs := newReadyState()
	rs.ArmREST()
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
	rs.ArmREST()

	handler := healthz.CheckHandler{Checker: rs.Check}

	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/readyz", nil)
	handler.ServeHTTP(rec, req)

	if rec.Code == http.StatusOK {
		t.Fatalf("expected non-200 before any signal, got %d", rec.Code)
	}

	rs.MarkCacheSynced()

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code == http.StatusOK {
		t.Fatalf("expected non-200 with only cache-sync, got %d", rec.Code)
	}

	rs.MarkBound()

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 after both signals, got %d body=%q", rec.Code, rec.Body.String())
	}
}
