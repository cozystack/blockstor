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
	"io"
	"net/http"
	"runtime"
	"strings"
	"testing"
)

// TestMetricsEndpointPrometheusFormat verifies scenario 7.W08's blockstor
// slice: the apiserver exposes /metrics on its REST listener and that
// endpoint returns the standard Prometheus exposition format. The
// ServiceMonitor that kube-prometheus-stack drops on the apiserver
// Service scrapes this path; the test pins the contract so a future
// refactor (e.g. moving back to controller-runtime's separate metrics
// port) cannot silently break it.
//
// Assertions:
//   - HTTP 200.
//   - Content-Type starts with `text/plain` (the canonical Prometheus
//     exposition MIME; OpenMetrics is also valid but only when the
//     client requests it via Accept).
//   - Body contains `go_*` metrics, which client_golang's default
//     registerer always registers via NewGoCollector.
//   - On Linux, body also contains `process_*` metrics — NewProcessCollector
//     is Linux-only (it reads /proc), so we skip the assertion elsewhere
//     (developer macOS, CI runners on Darwin) instead of pretending the
//     contract holds where the upstream collector can't emit.
func TestMetricsEndpointPrometheusFormat(t *testing.T) {
	base, stop := startServer(t)
	defer stop()

	resp := httpGet(t, base+"/metrics")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	ct := resp.Header.Get("Content-Type")
	if !strings.HasPrefix(ct, "text/plain") {
		t.Errorf("Content-Type: got %q, want prefix %q (Prometheus exposition format)", ct, "text/plain")
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	got := string(body)

	// `# HELP` / `# TYPE` framing is the Prometheus exposition format's
	// signature — assert at least one of each so we know promhttp is
	// actually encoding, not e.g. returning an empty 200.
	if !strings.Contains(got, "# HELP ") {
		t.Errorf("body missing `# HELP` lines (not Prometheus format?): first 200 chars = %q", head(got, 200))
	}

	if !strings.Contains(got, "# TYPE ") {
		t.Errorf("body missing `# TYPE` lines (not Prometheus format?): first 200 chars = %q", head(got, 200))
	}

	// go_* — always present, every platform.
	if !strings.Contains(got, "go_goroutines") {
		t.Errorf("body missing go_goroutines metric (Go collector not wired?)")
	}

	if !strings.Contains(got, "go_memstats_") {
		t.Errorf("body missing go_memstats_* metrics (Go collector not wired?)")
	}

	// process_* — Linux-only. NewProcessCollector reads /proc/self,
	// which doesn't exist on Darwin; client_golang silently degrades
	// to a no-op collector there, so the contract only holds on Linux.
	if runtime.GOOS == "linux" {
		if !strings.Contains(got, "process_cpu_seconds_total") {
			t.Errorf("body missing process_cpu_seconds_total (process collector not wired on Linux?)")
		}

		if !strings.Contains(got, "process_open_fds") {
			t.Errorf("body missing process_open_fds (process collector not wired on Linux?)")
		}
	}
}

// TestMetricsEndpointMethodNotAllowed pins the GET-only contract. The
// scrape side (Prometheus) only ever issues GET; any other method
// would mean a misconfigured client and should fail loudly with 405
// rather than silently succeed.
func TestMetricsEndpointMethodNotAllowed(t *testing.T) {
	base, stop := startServer(t)
	defer stop()

	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch} {
		t.Run(method, func(t *testing.T) {
			req, err := http.NewRequestWithContext(t.Context(), method, base+"/metrics", nil)
			if err != nil {
				t.Fatalf("new request: %v", err)
			}

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("do: %v", err)
			}
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != http.StatusMethodNotAllowed {
				t.Errorf("status: got %d, want 405", resp.StatusCode)
			}
		})
	}
}

// head returns the first n bytes of s, for error-message brevity.
func head(s string, n int) string {
	if len(s) <= n {
		return s
	}

	return s[:n]
}
