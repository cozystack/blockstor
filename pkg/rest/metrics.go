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
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Bug 168 (P2 observability gap): pre-fix, /metrics exposed only the
// stdlib go_*/process_* collectors. There was no SLI signal for the
// REST surface itself — request rate, latency, error rate, JSON-decode
// failures (which silently observe Bug 158/161 regressions) were all
// invisible to SREs. This file wires three blockstor-specific series
// into the default Prometheus registry so the existing /metrics route
// surfaces them on the same port as the LINSTOR REST API.
//
// Cardinality discipline (per v6 report's "path SHOULD use the route
// pattern not the full URL"): the `path` label carries the ServeMux
// registered pattern (e.g. `/v1/nodes/{node}`), NOT the raw URL. Raw
// URLs would blow up cardinality — a cluster with 500 RDs would mint
// 500 distinct series for `/v1/resource-definitions/{name}` alone.
//
// Each series has a stable, documented label set. We promauto.With the
// default registerer so /metrics (which already gathers from
// prometheus.DefaultGatherer) picks them up without extra wiring.

// Stable label keys (extracted as constants so a rename is grep-safe
// and so goconst doesn't fire on repeated literals).
const (
	labelMethod = "method"
	labelPath   = "path"
	labelStatus = "status"
	labelKind   = "kind"

	// Decode-failure kinds — keep parallel to the switch in
	// decodeFailureKind so a new bucket has an obvious slot.
	decodeKindEOF          = "eof"
	decodeKindTruncated    = "truncated"
	decodeKindSyntaxError  = "syntax_error"
	decodeKindTypeMismatch = "type_mismatch"
	decodeKindUnknownField = "unknown_field"
	decodeKindTooLarge     = "too_large"
	decodeKindOther        = "other"

	// patternUnknown is the `path` label sentinel for requests that
	// did not match a registered route (404 from with404Envelope).
	patternUnknown = "(unknown)"
)

// Prometheus instrumentation lives at package scope by design —
// promauto registers each metric once with the default registerer at
// import time, and the /metrics handler reads from
// prometheus.DefaultGatherer. Package-level vars are the canonical
// pattern in the prometheus/client_golang ecosystem; the alternative
// (struct fields wired through every handler) buys nothing and breaks
// the standard scrape shape.
//
//nolint:gochecknoglobals // Prometheus collectors are package-scoped by convention; see comment above.
var (
	// requestsTotal counts every HTTP request the apiserver processed,
	// labelled by:
	//   method  — the HTTP verb actually received (uppercase).
	//   path    — the registered ServeMux pattern OR the literal "(unknown)"
	//             for requests that did not match any route (404 from
	//             with404Envelope). Keeping the unknown bucket explicit
	//             means "the operator is hitting paths we don't serve"
	//             is dashboard-visible.
	//   status  — final HTTP status the wire saw (after middleware), as
	//             a decimal string ("200", "404", "405", "413", "415",
	//             "500"). String label so Prometheus can group by status
	//             class via PromQL's `=~"4.."` regex.
	requestsTotal = promauto.With(prometheus.DefaultRegisterer).NewCounterVec(
		prometheus.CounterOpts{
			Name: "blockstor_requests_total",
			Help: "Total HTTP requests processed by the LINSTOR-compatible REST API, labelled by method, registered route pattern, and status code.",
		},
		[]string{labelMethod, labelPath, labelStatus},
	)

	// requestDurationSeconds is a histogram of request-handling latency.
	// Status is intentionally NOT a label — the cardinality would
	// multiply by 6 (the 2xx/4xx/5xx classes we serve) for marginal
	// dashboard value; SLOs are usually computed across all statuses.
	// The bucket boundaries cover sub-millisecond reads (cache-hit
	// view-API) through second-scale writes (cluster-wide spawn).
	requestDurationSeconds = promauto.With(prometheus.DefaultRegisterer).NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "blockstor_request_duration_seconds",
			Help:    "Latency of LINSTOR REST requests, in seconds, observed at the apiserver wire edge.",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		},
		[]string{labelMethod, labelPath},
	)

	// decodeFailuresTotal counts JSON-body decode failures by failure
	// shape. Without this signal the Bug 158/161 typed-envelope
	// invariants are silently regressing (operators see 400s in logs
	// but no per-shape rollup); a sudden spike in `unknown_field` would
	// indicate a client/server schema drift, a spike in `syntax_error`
	// indicates a misconfigured client or wrong-content-type body.
	//
	// kind values:
	//   eof            — empty body (io.EOF)
	//   truncated      — partial body (io.ErrUnexpectedEOF)
	//   syntax_error   — bytes are not JSON (*json.SyntaxError)
	//   type_mismatch  — value type does not fit target (*json.UnmarshalTypeError)
	//   unknown_field  — DisallowUnknownFields tripped (Bug 161)
	//   too_large      — body exceeded maxRequestBodyBytes (Bug 146)
	//   other          — anything else; fall-through bucket
	decodeFailuresTotal = promauto.With(prometheus.DefaultRegisterer).NewCounterVec(
		prometheus.CounterOpts{
			Name: "blockstor_decode_failures_total",
			Help: "JSON request-body decode failures, labelled by failure kind (eof, truncated, syntax_error, type_mismatch, unknown_field, too_large, other).",
		},
		[]string{labelKind},
	)
)

// instrumentRequests is the metrics middleware. It wraps the handler
// chain so each request mints exactly one (method, path, status)
// observation on requestsTotal plus a (method, path) latency sample
// on requestDurationSeconds.
//
// `path` is resolved against the mux via mux.Handler(r), which returns
// the registered ServeMux pattern (or "" when the request didn't
// match — we substitute "(unknown)" to keep the label non-empty).
// Resolving up-front pins cardinality independent of the raw URL and
// is cheap (a single radix lookup that ServeMux is going to do anyway).
//
// We re-use the existing `statusRecorder` to capture the status code
// after the inner chain has written it. The recorder is already used
// by withLogging; we keep this middleware separate so the recorder
// wrapping nests cleanly and the metrics observation happens AFTER
// the inner handlers run (so 4xx envelopes emitted by with404Envelope,
// withContentTypeJSON, etc. are reflected in the status label).
func instrumentRequests(mux *http.ServeMux, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// Resolve the route pattern up-front. mux.Handler returns the
		// empty string when no route matches; substitute a stable
		// sentinel so cardinality stays bounded AND the unknown-path
		// case is still visible in dashboards.
		//
		// Go 1.22's method-aware mux returns patterns like
		// `"GET /v1/nodes"` — we strip the leading verb so the `path`
		// label is just the path. The verb is already on the `method`
		// label; keeping it on `path` too would double-label and would
		// not match the v6 report's stated wire shape
		// (`path="/v1/nodes"`).
		//
		// For a wrong-verb request (e.g. `PUT /v1/controller/version`
		// when only GET is registered) the std-lib mux returns an
		// empty pattern from Handler(r) — we re-probe with GET so the
		// 405 observation still carries the registered path on the
		// `path` label instead of dropping it into `(unknown)`. The
		// status label (`"405"`) preserves the wrong-verb signal.
		pattern := resolvePattern(mux, r)

		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)

		status := strconv.Itoa(rec.status)
		requestsTotal.WithLabelValues(r.Method, pattern, status).Inc()
		requestDurationSeconds.WithLabelValues(r.Method, pattern).Observe(time.Since(start).Seconds())
	})
}

// observeDecodeFailure maps a decoder error to the matching
// `kind` label and increments decodeFailuresTotal. Called from
// writeDecodeError (the single funnel every decodeJSON failure routes
// through). Keeps the writeDecodeError code path readable — the switch
// on error types lives there for the operator-facing message; here we
// mirror the same switch for the metric label, so a regression in one
// is caught by a regression in the other.
func observeDecodeFailure(err error) {
	decodeFailuresTotal.WithLabelValues(decodeFailureKind(err)).Inc()
}

// resolvePattern returns the registered ServeMux pattern for r,
// stripped of any leading method token. If the (method, path) tuple
// does not match a registered route, the lookup is re-tried with a
// GET probe — that way a wrong-verb request (which would naturally
// 405) still has its path label attached. As a last resort returns
// "(unknown)" so the metric label stays non-empty.
func resolvePattern(mux *http.ServeMux, r *http.Request) string {
	if _, p := mux.Handler(r); p != "" {
		return stripMethodPrefix(p)
	}

	probe := r.Clone(r.Context())
	probe.Method = http.MethodGet

	if _, p := mux.Handler(probe); p != "" {
		return stripMethodPrefix(p)
	}

	return patternUnknown
}

// stripMethodPrefix removes the leading `METHOD ` token from a Go
// 1.22 mux pattern (`"GET /v1/nodes"` → `"/v1/nodes"`). Patterns that
// don't carry a method (verbless registration) flow through unchanged.
// Used by instrumentRequests so the `path` label is just the path —
// the verb already lives on the `method` label.
func stripMethodPrefix(pattern string) string {
	if pattern == "" {
		return ""
	}

	verb, rest, ok := strings.Cut(pattern, " ")
	if !ok {
		return pattern
	}

	switch verb {
	case http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut,
		http.MethodPatch, http.MethodDelete, http.MethodOptions, http.MethodConnect, http.MethodTrace:
		return rest
	}

	return pattern
}

// decodeFailureKind classifies err into one of the `kind` label
// buckets documented on decodeFailuresTotal. The switch mirrors
// writeDecodeError's error-type ladder — keeping them parallel means a
// new decode failure mode added to one site has an obvious slot in
// the other.
func decodeFailureKind(err error) string {
	if err == nil {
		return decodeKindOther
	}

	var maxErr *http.MaxBytesError
	if errors.As(err, &maxErr) {
		return decodeKindTooLarge
	}

	if errors.Is(err, io.EOF) {
		return decodeKindEOF
	}

	if errors.Is(err, io.ErrUnexpectedEOF) {
		return decodeKindTruncated
	}

	if _, ok := unknownFieldName(err); ok {
		return decodeKindUnknownField
	}

	var typeErr *json.UnmarshalTypeError
	if errors.As(err, &typeErr) {
		return decodeKindTypeMismatch
	}

	var syntaxErr *json.SyntaxError
	if errors.As(err, &syntaxErr) {
		return decodeKindSyntaxError
	}

	return decodeKindOther
}
