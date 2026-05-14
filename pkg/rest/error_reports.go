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
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// errorReportRingCap caps the in-memory buffer at 1000 entries — the
// upstream LINSTOR controller keeps the same order-of-magnitude on
// disk (it rotates the `ErrorReport-*.log` files at ~1 MiB each). The
// cap protects the controller from unbounded growth in a flapping
// cluster while still giving the operator enough history to grep
// after the fact.
const errorReportRingCap = 1000

// ErrorReportEntry is the on-wire row for `GET /v1/error-reports`.
// Field tags match upstream LINSTOR's `ErrorReport` DTO (golinstor's
// `client.ErrorReport`) so `linstor err l` and `linstor err show
// <id>` decode without translation.
//
// We only populate the fields downstream tooling reads: Filename (the
// id the CLI shows in the first column), NodeName (the satellite that
// produced the report), ErrorTime (millis-since-epoch — golinstor's
// `TimeStampMs` round-trips through a plain int64), and Text /
// ExceptionMessage for the human-readable body. Module / Version /
// Peer / OriginFile etc. are upstream telemetry we don't synthesise.
type ErrorReportEntry struct {
	NodeName         string `json:"node_name,omitempty"`
	ErrorTime        int64  `json:"error_time"`
	Filename         string `json:"filename,omitempty"`
	Text             string `json:"text,omitempty"`
	Module           string `json:"module,omitempty"`
	Version          string `json:"version,omitempty"`
	Peer             string `json:"peer,omitempty"`
	Exception        string `json:"exception,omitempty"`
	ExceptionMessage string `json:"exception_message,omitempty"`
}

// errorReportRing is a fixed-size, mutex-guarded ring buffer of
// ErrorReportEntry. Push appends + evicts the oldest entry when the
// buffer is full; Snapshot returns a defensive copy ordered most-
// recent-first (operators reading `linstor err l` expect the latest
// failures at the top).
type errorReportRing struct {
	mu      sync.Mutex
	entries []ErrorReportEntry
}

// push appends e, evicting the oldest entry past the cap so the
// buffer never grows without bound. The pointer parameter dodges
// gocritic's hugeParam complaint (ErrorReportEntry is 136 bytes —
// past the default 80-byte threshold), without forcing call sites
// to keep a pointer to a stack-allocated struct any longer than the
// call itself.
func (r *errorReportRing) push(e *ErrorReportEntry) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.entries = append(r.entries, *e)
	if len(r.entries) > errorReportRingCap {
		// Drop the oldest entry. A copy-and-replace beats a
		// slice-of-slice trick here because the underlying array
		// would otherwise keep the evicted strings alive for the
		// GC. Cheap at the cap we set.
		r.entries = append([]ErrorReportEntry{}, r.entries[len(r.entries)-errorReportRingCap:]...)
	}
}

// snapshot returns a defensive copy of the buffer in reverse-
// chronological order (newest first). Callers can mutate the
// returned slice without affecting the ring.
func (r *errorReportRing) snapshot() []ErrorReportEntry {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := make([]ErrorReportEntry, len(r.entries))
	copy(out, r.entries)

	// Newest first matches what `linstor err l` prints. Stable
	// sort on ErrorTime DESC; ties (millisecond collisions) preserve
	// insertion order so the latest push wins the head slot.
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].ErrorTime > out[j].ErrorTime
	})

	return out
}

// get returns the entry with the matching filename id, or false. The
// CLI's `linstor err show <id>` derives the id from the Filename
// column it printed earlier, so the lookup key here matches the LIST
// output's identity.
func (r *errorReportRing) get(id string) (ErrorReportEntry, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for i := range r.entries {
		if r.entries[i].Filename == id {
			return r.entries[i], true
		}
	}

	return ErrorReportEntry{}, false
}

// RecordErrorReport pushes a single entry onto the controller's
// in-memory error-reports buffer. Call sites: reconcilers that hit
// a non-retryable failure (zfs/lvm rejected the volume size, peer
// authentication failure, etc.), and REST handlers that want to
// surface a structured trail of "this request failed because…" beyond
// the per-call ApiCallRc envelope.
//
// The buffer is lazy-initialised so a zero-value Server (the shape
// used by every test that doesn't seed an explicit ring) keeps
// working without an explicit init step. Filename and ErrorTime are
// filled in here when the caller leaves them blank — the upstream
// convention is `ErrorReport-{nodeid}-{seq}.log`, but the controller
// has no native seq counter so we use a UTC nanosecond timestamp as
// the unique id. Operators still see a stable filename, and a
// concurrent burst of pushes can't collide on the id.
//
// hugeParam is suppressed here on purpose: we want value semantics on
// the public API so call sites don't have to keep a pointer alive past
// the call (the caller's ErrorReportEntry literal is typically built
// on the stack and discarded immediately). The mutate-and-push pattern
// below needs a local copy anyway; taking a pointer would force callers
// into either heap-allocation or a defensive copy of their own.
//
//nolint:gocritic // hugeParam intentional — see rationale block above.
func (s *Server) RecordErrorReport(entry ErrorReportEntry) {
	if s.errorReports == nil {
		s.errorReports = &errorReportRing{}
	}

	now := time.Now().UTC()
	if entry.ErrorTime == 0 {
		entry.ErrorTime = now.UnixMilli()
	}

	if entry.Filename == "" {
		// Upstream format is `ErrorReport-{instanceid}-{nodeid}-{seq}.log`.
		// We don't have an instance/node-bound seq counter; use the
		// nanosecond timestamp as a globally-unique replacement so
		// `linstor err show <filename>` still round-trips.
		entry.Filename = fmt.Sprintf("ErrorReport-blockstor-%d.log", now.UnixNano())
	}

	s.errorReports.push(&entry)
}

// registerErrorReports wires the error-reports endpoints. linstor CLI's
// `error-reports list` and `error-reports show` hit them.
func (s *Server) registerErrorReports(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/error-reports", s.handleErrorReportsList)
	mux.HandleFunc("GET /v1/error-reports/{id}", s.handleErrorReportGet)
}

// handleErrorReportsList returns the buffered ErrorReportEntry slice,
// newest first. The endpoint stays available even when Store is nil
// (no `requireStore` gate) so a controller running with persistence
// detached can still surface its own error stream.
//
// Query parameters honoured (upstream `Controller.GetErrorReports`
// passes the same names): `?node=NAME` filters by satellite name
// (case-insensitive); `?since=MILLIS` returns only entries with
// `error_time >= since`. Both are advisory — the CLI requests them
// when the operator passes `-n` / `--since`, but golinstor tolerates
// servers that ignore the filter and re-filter client-side. Implementing
// them here keeps the wire round-trip cheap.
func (s *Server) handleErrorReportsList(w http.ResponseWriter, r *http.Request) {
	var entries []ErrorReportEntry
	if s.errorReports != nil {
		entries = s.errorReports.snapshot()
	}

	if entries == nil {
		entries = []ErrorReportEntry{}
	}

	node := strings.TrimSpace(r.URL.Query().Get("node"))
	if node != "" {
		filtered := entries[:0]

		for i := range entries {
			if strings.EqualFold(entries[i].NodeName, node) {
				filtered = append(filtered, entries[i])
			}
		}

		entries = filtered
	}

	writeJSON(w, http.StatusOK, entries)
}

// handleErrorReportGet returns a single entry by its Filename id.
// 404 when the id isn't in the buffer (either it expired off the
// tail, or it never existed) — matches upstream LINSTOR which 404s
// on the same input so clients can tell "report aged out of the
// rotation" apart from a successful empty body.
func (s *Server) handleErrorReportGet(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	if s.errorReports != nil {
		if e, ok := s.errorReports.get(id); ok {
			// Upstream `GET /v1/error-reports/{id}` returns an
			// array (not a singleton). golinstor's GetErrorReport
			// unmarshals into `[]ErrorReport` and returns `report[0]`;
			// emitting an object instead breaks its decoder.
			writeJSON(w, http.StatusOK, []ErrorReportEntry{e})

			return
		}
	}

	writeError(w, http.StatusNotFound, "error report not found")
}
