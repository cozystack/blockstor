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
	"time"

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

// TestErrorReportsListFilterNodeSinceLimit pins scenario 7.W04 from
// wave2 §"Quorum, observability, capacity": GET /v1/error-reports
// honours `node`, `since`, and `limit` query parameters so a paging
// CLI (`linstor err l -n nodeA --since=2026-05-14T00:00Z --limit=10`)
// doesn't drag the full in-memory ring across the wire. Cross-listed
// with wave1 5.38 / 7.17.
//
// The seed mixes three nodes and four timestamps so each sub-check
// has at least one entry to keep and one entry to drop — a bug that
// silently no-ops a filter would still pass a single-row fixture.
func TestErrorReportsListFilterNodeSinceLimit(t *testing.T) {
	srv := &Server{
		Addr:      pickFreeAddr(t),
		Store:     store.NewInMemory(),
		Client:    newFakeRESTClient(t),
		Namespace: testRESTNamespace,
	}

	// Four entries, deliberate timestamps in millis. RFC3339-encoded
	// reference points used by the since checks below:
	//   t0 = 1_700_000_000_000 → 2023-11-14T22:13:20Z
	//   t1 = 1_700_000_060_000 → 2023-11-14T22:14:20Z
	//   t2 = 1_700_000_120_000 → 2023-11-14T22:15:20Z
	//   t3 = 1_700_000_180_000 → 2023-11-14T22:16:20Z
	srv.RecordErrorReport(ErrorReportEntry{
		NodeName: "satellite-a", Filename: "a-old.log", ErrorTime: 1_700_000_000_000,
	})
	srv.RecordErrorReport(ErrorReportEntry{
		NodeName: "satellite-b", Filename: "b-mid.log", ErrorTime: 1_700_000_060_000,
	})
	srv.RecordErrorReport(ErrorReportEntry{
		NodeName: "satellite-a", Filename: "a-mid.log", ErrorTime: 1_700_000_120_000,
	})
	srv.RecordErrorReport(ErrorReportEntry{
		NodeName: "satellite-a", Filename: "a-new.log", ErrorTime: 1_700_000_180_000,
	})

	base, stop := startServerCustom(t, srv)
	defer stop()

	t.Run("node filter is case-insensitive and drops other nodes", func(t *testing.T) {
		got := decodeReports(t, base+"/v1/error-reports?node=SATELLITE-A")

		if len(got) != 3 {
			t.Fatalf("len: got %d, want 3 (a-old, a-mid, a-new); entries=%+v", len(got), got)
		}

		for i := range got {
			if !strings.EqualFold(got[i].NodeName, "satellite-a") {
				t.Errorf("entry %d: NodeName=%q, want satellite-a only", i, got[i].NodeName)
			}
		}
	})

	t.Run("since=RFC3339 drops entries strictly older than the cut-off", func(t *testing.T) {
		// 2023-11-14T22:15:00Z is between t1 (22:14:20) and t2 (22:15:20);
		// only the two newest entries (t2, t3) pass the >= filter.
		got := decodeReports(t, base+"/v1/error-reports?since=2023-11-14T22:15:00Z")

		if len(got) != 2 {
			t.Fatalf("len: got %d, want 2; entries=%+v", len(got), got)
		}

		for i := range got {
			if got[i].ErrorTime < 1_700_000_100_000 {
				t.Errorf("entry %d: ErrorTime=%d, want >= 1700000100000", i, got[i].ErrorTime)
			}
		}
	})

	t.Run("since=millis accepts the golinstor wire format", func(t *testing.T) {
		// 1_700_000_120_000 == t2; the >= filter keeps t2 and t3.
		got := decodeReports(t, base+"/v1/error-reports?since=1700000120000")

		if len(got) != 2 {
			t.Fatalf("len: got %d, want 2; entries=%+v", len(got), got)
		}
	})

	t.Run("limit caps the response after sorting newest-first", func(t *testing.T) {
		got := decodeReports(t, base+"/v1/error-reports?limit=2")

		if len(got) != 2 {
			t.Fatalf("len: got %d, want 2; entries=%+v", len(got), got)
		}

		// The two newest are a-new (t3) and a-mid (t2).
		if got[0].Filename != "a-new.log" || got[1].Filename != "a-mid.log" {
			t.Errorf("order: got [%q, %q], want [a-new.log, a-mid.log]",
				got[0].Filename, got[1].Filename)
		}
	})

	t.Run("combined filters apply in order: node → since → limit", func(t *testing.T) {
		// satellite-a has three entries (a-old, a-mid, a-new); the since
		// cut-off keeps a-mid + a-new; the limit caps to the newest one.
		got := decodeReports(t,
			base+"/v1/error-reports?node=satellite-a&since=2023-11-14T22:15:00Z&limit=1")

		if len(got) != 1 {
			t.Fatalf("len: got %d, want 1; entries=%+v", len(got), got)
		}

		if got[0].Filename != "a-new.log" {
			t.Errorf("Filename: got %q, want a-new.log", got[0].Filename)
		}
	})

	t.Run("invalid since payload rejects with 400", func(t *testing.T) {
		resp := httpGet(t, base+"/v1/error-reports?since=not-a-timestamp")
		_ = resp.Body.Close()

		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("status: got %d, want 400", resp.StatusCode)
		}
	})

	t.Run("invalid limit payload rejects with 400", func(t *testing.T) {
		resp := httpGet(t, base+"/v1/error-reports?limit=-3")
		_ = resp.Body.Close()

		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("status: got %d, want 400", resp.StatusCode)
		}
	})

	// Sanity-pin parseSinceMillis as the single point that translates
	// the wire form into the comparable int64 the ring stores. If this
	// shifts (e.g. UTC offset bug, off-by-1000 ms vs s), all the
	// `since=...` subtests above start lying about what they're testing.
	t.Run("parseSinceMillis round-trip", func(t *testing.T) {
		const wantMs int64 = 1_700_000_120_000

		raw := time.UnixMilli(wantMs).UTC().Format(time.RFC3339)

		gotMs, ok := parseSinceMillis(raw)
		if !ok {
			t.Fatalf("parse %q: not ok", raw)
		}

		if gotMs != wantMs {
			t.Errorf("parse %q: got %d ms, want %d ms", raw, gotMs, wantMs)
		}
	})
}

// decodeReports is a small wrapper that GETs the URL, asserts 200, and
// decodes into []ErrorReportEntry — pulled out so the multi-sub-test
// scenario above stays readable and each sub-test owns just its
// scenario-specific assertions.
func decodeReports(t *testing.T, url string) []ErrorReportEntry {
	t.Helper()

	resp := httpGet(t, url)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200 (url=%s)", resp.StatusCode, url)
	}

	var got []ErrorReportEntry
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v (url=%s)", err, url)
	}

	return got
}
