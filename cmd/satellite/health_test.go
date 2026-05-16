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

// TestReadyState_StartsNotReady pins the initial state: a freshly
// constructed readyState reports not-ready so the kubelet's
// readinessProbe keeps the satellite pod out of any backing Service
// until the first cache sync completes. Bug 207: without this,
// kubelet would route traffic to a pod whose manager hasn't observed
// any CRDs yet and the satellite would silently return stale answers.
func TestReadyState_StartsNotReady(t *testing.T) {
	t.Parallel()

	rs := newReadyState()

	err := rs.Check(nil)
	if err == nil {
		t.Fatal("expected readyState.Check to fail before MarkReady, got nil")
	}
}

// TestReadyState_MarkReadyFlips checks that MarkReady transitions
// the gate exactly once and Check then reports nil. Mirrors the
// "first sync done" handoff that wraps mgr.GetCache().WaitForCacheSync.
func TestReadyState_MarkReadyFlips(t *testing.T) {
	t.Parallel()

	rs := newReadyState()
	rs.MarkReady()

	err := rs.Check(nil)
	if err != nil {
		t.Fatalf("expected nil after MarkReady, got %v", err)
	}

	// Idempotent: a second MarkReady must not panic / regress.
	rs.MarkReady()

	err = rs.Check(nil)
	if err != nil {
		t.Fatalf("expected nil after second MarkReady, got %v", err)
	}
}

// TestReadyState_AsHTTPHandler wires readyState.Check through
// healthz.CheckHandler — the same wrapper controller-runtime uses
// to mount the check on /readyz — and asserts the HTTP status flips
// from 500 to 200 once MarkReady fires. This is the contract the
// kubelet sees over the wire.
func TestReadyState_AsHTTPHandler(t *testing.T) {
	t.Parallel()

	rs := newReadyState()
	handler := healthz.CheckHandler{Checker: rs.Check}

	// Pre-MarkReady: handler must respond non-2xx so kubelet's
	// readinessProbe trips the pod out of any backing Service.
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/readyz", nil)
	handler.ServeHTTP(rec, req)

	if rec.Code == http.StatusOK {
		t.Fatalf("expected non-200 before MarkReady, got %d", rec.Code)
	}

	// Post-MarkReady: handler returns 200, kubelet now routes
	// traffic to this pod.
	rs.MarkReady()

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 after MarkReady, got %d body=%q", rec.Code, rec.Body.String())
	}
}
