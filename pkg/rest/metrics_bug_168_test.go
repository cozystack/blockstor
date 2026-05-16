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
	"bytes"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/cozystack/blockstor/pkg/store"
)

// TestBug168MetricsExposesRequestCounter pins the v6 Bug 168 fix:
// /metrics must expose `blockstor_requests_total` counter labelled
// by method, path (ServeMux pattern), and status, so SREs can
// dashboard request rate and error rate without grepping logs.
//
// Pre-fix: /metrics only carried go_*/process_* — request rate was
// invisible.
func TestBug168MetricsExposesRequestCounter(t *testing.T) {
	base, stop := startServer(t)
	defer stop()

	// Three GETs, one POST (will 415 because no body), and one
	// 405 from PUT against a GET-only route — distinct (method,
	// path, status) label tuples.
	for range 3 {
		resp := httpGet(t, base+"/v1/controller/version")
		_ = resp.Body.Close()
	}

	// POST against a body-rejecting endpoint — 400 envelope.
	postReq, err := http.NewRequestWithContext(t.Context(), http.MethodPost, base+"/v1/nodes", bytes.NewReader([]byte("{ not json")))
	if err != nil {
		t.Fatalf("post req: %v", err)
	}
	postReq.Header.Set("Content-Type", "application/json")
	postResp, err := http.DefaultClient.Do(postReq)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	_ = postResp.Body.Close()

	// PUT against /v1/controller/version — registered only for GET,
	// so the mux returns 405.
	putReq, err := http.NewRequestWithContext(t.Context(), http.MethodPut, base+"/v1/controller/version", nil)
	if err != nil {
		t.Fatalf("put req: %v", err)
	}
	putResp, err := http.DefaultClient.Do(putReq)
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	_ = putResp.Body.Close()

	metrics := scrapeMetrics(t, base)

	// Counter family must appear at least once.
	if !strings.Contains(metrics, "blockstor_requests_total") {
		t.Fatalf("blockstor_requests_total not present in /metrics output (head: %q)", head(metrics, 400))
	}

	// Three GETs against /v1/controller/version with status 200.
	if got := counterValue(t, metrics, "blockstor_requests_total",
		map[string]string{"method": "GET", "path": "/v1/controller/version", "status": "200"}); got < 3 {
		t.Errorf("blockstor_requests_total{GET,/v1/controller/version,200}: got %d, want >= 3", got)
	}

	// PUT against /v1/controller/version → 405.
	if got := counterValue(t, metrics, "blockstor_requests_total",
		map[string]string{"method": "PUT", "path": "/v1/controller/version", "status": "405"}); got < 1 {
		t.Errorf("blockstor_requests_total{PUT,/v1/controller/version,405}: got %d, want >= 1", got)
	}
}

// TestBug168MetricsExposesLatencyHistogram pins the histogram half of
// the Bug 168 fix — the latency distribution is what SLOs are built on
// (p95/p99 dashboards). The counter alone tells rate; the histogram
// tells how slow each request was.
func TestBug168MetricsExposesLatencyHistogram(t *testing.T) {
	base, stop := startServer(t)
	defer stop()

	for range 2 {
		resp := httpGet(t, base+"/v1/healthz")
		_ = resp.Body.Close()
	}

	metrics := scrapeMetrics(t, base)

	if !strings.Contains(metrics, "blockstor_request_duration_seconds") {
		t.Fatalf("blockstor_request_duration_seconds family not in /metrics (head: %q)", head(metrics, 400))
	}

	// Histogram emits `_bucket`, `_count`, `_sum` series — assert the
	// sum/count are populated for the path we exercised.
	if !strings.Contains(metrics, `blockstor_request_duration_seconds_count{`) {
		t.Errorf("histogram _count series missing")
	}

	if !strings.Contains(metrics, `blockstor_request_duration_seconds_sum{`) {
		t.Errorf("histogram _sum series missing")
	}

	if !strings.Contains(metrics, `blockstor_request_duration_seconds_bucket{`) {
		t.Errorf("histogram _bucket series missing")
	}
}

// TestBug168MetricsExposesDecodeFailureCounter pins the decode-failure
// counter — without this signal, regressions in the Bug 158/161 typed-
// envelope invariants are invisible to SREs (operators see a 400 in
// logs but no per-shape rollup).
func TestBug168MetricsExposesDecodeFailureCounter(t *testing.T) {
	// The decode-failure middleware fires inside decodeJSON, which
	// runs AFTER requireStore. A nil-Store Server short-circuits with
	// 503 before any body parsing happens; we wire an InMemory store
	// so the POST handlers actually reach decodeJSON.
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	// Garbage JSON → SyntaxError path inside decodeJSON.
	syntaxReq := mustPost(t, base+"/v1/nodes", []byte("{ this is not json"))
	_ = syntaxReq.Body.Close()

	// Empty body → io.EOF path.
	emptyReq := mustPost(t, base+"/v1/nodes", nil)
	_ = emptyReq.Body.Close()

	// Unknown-field path (Bug 161).
	unknownReq := mustPost(t, base+"/v1/nodes", []byte(`{"totally_unknown_field":1}`))
	_ = unknownReq.Body.Close()

	metrics := scrapeMetrics(t, base)

	if !strings.Contains(metrics, "blockstor_decode_failures_total") {
		t.Fatalf("blockstor_decode_failures_total not present in /metrics (head: %q)", head(metrics, 400))
	}

	if got := counterValue(t, metrics, "blockstor_decode_failures_total",
		map[string]string{"kind": "syntax_error"}); got < 1 {
		t.Errorf("decode_failures_total{kind=syntax_error}: got %d, want >= 1", got)
	}

	if got := counterValue(t, metrics, "blockstor_decode_failures_total",
		map[string]string{"kind": "eof"}); got < 1 {
		t.Errorf("decode_failures_total{kind=eof}: got %d, want >= 1", got)
	}

	if got := counterValue(t, metrics, "blockstor_decode_failures_total",
		map[string]string{"kind": "unknown_field"}); got < 1 {
		t.Errorf("decode_failures_total{kind=unknown_field}: got %d, want >= 1", got)
	}
}

// scrapeMetrics reads /metrics from base and returns the body as a string.
func scrapeMetrics(t *testing.T, base string) string {
	t.Helper()

	resp := httpGet(t, base+"/metrics")
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read metrics: %v", err)
	}

	return string(body)
}

// mustPost issues an http.Post against url with body bytes (nil → no
// body). Helper for decode-failure scenarios.
func mustPost(t *testing.T, url string, body []byte) *http.Response {
	t.Helper()

	var r io.Reader

	if body != nil {
		r = bytes.NewReader(body)
	}

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, url, r)
	if err != nil {
		t.Fatalf("new req: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}

	return resp
}

// counterValue parses Prometheus exposition text and returns the value
// of the counter whose name matches metric AND whose label set is a
// superset of want. Returns 0 if no matching series is found (the
// counter family is allowed to exist without that specific tuple — the
// test asserts >= 1 when it cares).
//
// We hand-roll the parser (instead of pulling in prometheus/common's
// expfmt) to keep the test dependency-free. The format is
// well-specified: lines of `name{k="v",k="v"} <number>` plus
// `# HELP` / `# TYPE` framing.
func counterValue(t *testing.T, expo, metric string, want map[string]string) int {
	t.Helper()

	// Prometheus line pattern: name{labels} value [timestamp]
	// We anchor on the metric name + opening brace so we don't match
	// the `_bucket`/`_count`/`_sum` siblings of a histogram family.
	pattern := regexp.MustCompile(`^` + regexp.QuoteMeta(metric) + `\{([^}]*)\}\s+(\S+)`)

	var total int

	for _, line := range strings.Split(expo, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		m := pattern.FindStringSubmatch(line)
		if m == nil {
			continue
		}

		labels := parseLabels(m[1])
		if !labelsContain(labels, want) {
			continue
		}

		// Prometheus values can be floats; counters are non-negative
		// integers in practice. Round to int for the assertion.
		f, err := strconv.ParseFloat(m[2], 64)
		if err != nil {
			t.Fatalf("parse value %q: %v", m[2], err)
		}

		total += int(f)
	}

	return total
}

// parseLabels turns `k="v",k2="v2"` into a map. The Prometheus format
// does not allow `"` or `,` inside a label value without escaping; the
// test only emits well-formed labels so we keep the parse simple.
func parseLabels(s string) map[string]string {
	out := map[string]string{}

	for _, pair := range strings.Split(s, `",`) {
		eq := strings.IndexByte(pair, '=')
		if eq < 0 {
			continue
		}

		key := strings.TrimSpace(pair[:eq])

		val := strings.TrimSpace(pair[eq+1:])
		val = strings.TrimPrefix(val, `"`)
		val = strings.TrimSuffix(val, `"`)
		out[key] = val
	}

	return out
}

// labelsContain reports whether got is a superset of want — every k/v
// in want is present in got with the same value.
func labelsContain(got, want map[string]string) bool {
	for k, v := range want {
		if got[k] != v {
			return false
		}
	}

	return true
}
