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
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/store"
)

// TestErrorReportsListEmpty: linstor CLI calls /v1/error-reports on
// every `error-reports list`; an empty list is the right answer when
// no errors have been recorded.
func TestErrorReportsListEmpty(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpGet(t, base+"/v1/error-reports")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got []any

	err := json.NewDecoder(resp.Body).Decode(&got)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(got) != 0 {
		t.Errorf("expected empty list; got %d items", len(got))
	}
}

// TestErrorReportGetMissing: an unknown error report id → 404. Upstream
// returns 404 here too; clients depend on it for the "report expired"
// branch.
func TestErrorReportGetMissing(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpGet(t, base+"/v1/error-reports/abcdef-1234")
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", resp.StatusCode)
	}
}

// TestErrorReportsListReturnsRecentEvents: cli-parity-audit row #17 —
// `linstor err l` returns empty against blockstor today because the
// REST handler unconditionally answers `[]`. We now back the endpoint
// with an in-memory ring buffer that reconcilers push to on hitting a
// non-retryable failure. This test exercises the wire round-trip:
// RecordErrorReport pushes a structured entry → the LIST endpoint
// surfaces it under the same Filename id, with the same NodeName, and
// the same human-readable Text — exactly what `linstor err l` then
// prints. The Filename is the id the operator passes to `linstor err
// show <id>`, so we also verify it round-trips through the GET handler.
func TestErrorReportsListReturnsRecentEvents(t *testing.T) {
	srv := &Server{
		Addr:      pickFreeAddr(t),
		Store:     store.NewInMemory(),
		Client:    newFakeRESTClient(t),
		Namespace: testRESTNamespace,
	}

	// Pre-seed the ring before Start runs so the data is visible on
	// the very first GET. RecordErrorReport is the same API the
	// reconcilers will call once they're wired through; doing it
	// from a test keeps the surface honest.
	srv.RecordErrorReport(ErrorReportEntry{
		NodeName:         "satellite-a",
		Filename:         "ErrorReport-blockstor-test-1.log",
		ErrorTime:        1_700_000_000_000,
		Module:           "Controller",
		ExceptionMessage: "zfs pool 'tank' missing on satellite",
		Text:             "satellite-a reported zfs pool 'tank' as missing during reconcile pass; volume placement is blocked.",
	})

	base, stop := startServerCustom(t, srv)
	defer stop()

	resp := httpGet(t, base+"/v1/error-reports")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got []ErrorReportEntry
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("len: got %d, want 1; entries=%+v", len(got), got)
	}

	if got[0].Filename != "ErrorReport-blockstor-test-1.log" {
		t.Errorf("Filename: got %q, want preserved id", got[0].Filename)
	}

	if got[0].NodeName != "satellite-a" {
		t.Errorf("NodeName: got %q, want 'satellite-a'", got[0].NodeName)
	}

	if got[0].ErrorTime != 1_700_000_000_000 {
		t.Errorf("ErrorTime: got %d, want 1700000000000", got[0].ErrorTime)
	}

	if !strings.Contains(got[0].ExceptionMessage, "zfs pool 'tank' missing") {
		t.Errorf("ExceptionMessage: got %q, want substring 'zfs pool tank missing'", got[0].ExceptionMessage)
	}

	// The id the operator copies out of `linstor err l` must round-
	// trip through `linstor err show <id>` (= GET .../{id}). Upstream
	// returns an array even on the single-id GET; golinstor unmarshals
	// into []ErrorReport and pulls [0], so we keep the shape.
	resp2 := httpGet(t, base+"/v1/error-reports/ErrorReport-blockstor-test-1.log")
	defer func() { _ = resp2.Body.Close() }()

	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("show status: got %d, want 200", resp2.StatusCode)
	}

	var shown []ErrorReportEntry
	if err := json.NewDecoder(resp2.Body).Decode(&shown); err != nil {
		t.Fatalf("decode show: %v", err)
	}

	if len(shown) != 1 || shown[0].Filename != "ErrorReport-blockstor-test-1.log" {
		t.Errorf("show round-trip: got %+v, want singleton with same Filename", shown)
	}
}

// TestErrorReportsListNewestFirst pins the order contract operators rely
// on: `linstor err l` prints the most recent failures at the top so
// post-incident greps land on the right window. Without a stable sort,
// two pushes a millisecond apart could land in either order — which
// breaks the operator's "scroll to first non-repeat" workflow.
func TestErrorReportsListNewestFirst(t *testing.T) {
	srv := &Server{
		Addr:      pickFreeAddr(t),
		Store:     store.NewInMemory(),
		Client:    newFakeRESTClient(t),
		Namespace: testRESTNamespace,
	}

	srv.RecordErrorReport(ErrorReportEntry{
		Filename:  "older.log",
		ErrorTime: 100,
	})
	srv.RecordErrorReport(ErrorReportEntry{
		Filename:  "newer.log",
		ErrorTime: 200,
	})

	base, stop := startServerCustom(t, srv)
	defer stop()

	resp := httpGet(t, base+"/v1/error-reports")
	defer func() { _ = resp.Body.Close() }()

	var got []ErrorReportEntry
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("len: got %d, want 2", len(got))
	}

	if got[0].Filename != "newer.log" {
		t.Errorf("order: got[0]=%q, want 'newer.log' (newest first)", got[0].Filename)
	}
}
