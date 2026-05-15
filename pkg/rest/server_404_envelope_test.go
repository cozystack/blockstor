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
	"io"
	"net/http"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestUnknownEndpointReturnsLINSTORJSONEnvelope pins Bug 103: when the
// python-linstor CLI hits an unwired REST endpoint, the Go
// http.ServeMux returns a plain-text "404 page not found\n" body.
// python-linstor's error-decoding path then falls back to XML and
// crashes with `xml.etree.ElementTree.ParseError: syntax error: line
// 1, column 0`.
//
// The wrapper installed in buildHandler captures the inner mux's
// reply; if it would have been a bare 404 with no JSON body, it
// rewrites the response to the LINSTOR `[]ApiCallRc` envelope so the
// CLI surfaces a typed `ERROR:` line instead of a Python traceback.
//
// Status code stays 404 — this is genuinely "endpoint not registered";
// the change is purely in the body shape and Content-Type. golinstor
// turns 404 into ErrNotFound with the body text preserved, so callers
// that already key on the status code keep working.
func TestUnknownEndpointReturnsLINSTORJSONEnvelope(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpGet(t, base+"/v1/nonexistent")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", resp.StatusCode)
	}

	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("Content-Type: got %q, want application/json (bug 103: python CLI parses non-JSON 404 bodies and crashes)", ct)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	if strings.Contains(string(body), "404 page not found") {
		t.Fatalf("body contains plain-text 404 marker — wrapper did not rewrite: %s", body)
	}

	var rc []apiv1.APICallRc
	if err := json.Unmarshal(body, &rc); err != nil {
		t.Fatalf("decode envelope: %v\nbody: %s", err, body)
	}

	if len(rc) == 0 {
		t.Fatalf("empty envelope — python-linstor crashes on replies[0]")
	}

	// MASK_ERROR bit must be set so the CLI classifies the reply as ERROR.
	if rc[0].RetCode >= 0 {
		t.Errorf("ret_code = %d, want negative (MASK_ERROR set)", rc[0].RetCode)
	}

	if rc[0].Message == "" {
		t.Errorf("empty message — operator log will be unreadable")
	}

	// The cause/correction should mention the actual path so the operator
	// can grep the apiserver logs for the right route.
	if !strings.Contains(rc[0].Cause, "/v1/nonexistent") {
		t.Errorf("cause %q does not reference the request path", rc[0].Cause)
	}
}

// TestWrongMethodOnExistingRouteStillReturns405 pins the other half of
// the Bug 103 fix: the 404 catch-all must NOT shadow the http.ServeMux
// 405 Method Not Allowed behaviour. A previous attempt at this fix
// registered a naked wildcard handler at "/" which broke the
// per-route method dispatch — POST /v1/nodes (a wired path with a
// different method) started returning 404 instead of 405.
//
// The wrapper approach captures the inner mux's status and only
// rewrites the body on a 404 outcome; 405 must pass through verbatim
// (no body rewrite, no Content-Type change).
func TestWrongMethodOnExistingRouteStillReturns405(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	// /v1/controller/version is registered as GET-only.
	req, err := http.NewRequestWithContext(t.Context(), http.MethodDelete,
		base+"/v1/controller/version", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}

	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("status: got %d, want 405 (catch-all must not shadow 405 dispatch)", resp.StatusCode)
	}
}

// TestWiredEndpointStillReturns200 pins that the wrapper does not
// disturb the happy path: a wired GET handler that writes 200 with a
// JSON body must reach the client unchanged.
func TestWiredEndpointStillReturns200(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpGet(t, base+"/v1/nodes")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	// Empty store → "[]" (not the envelope).
	if strings.TrimSpace(string(body)) != "[]" {
		t.Errorf("body: got %q, want \"[]\"", string(body))
	}
}

// TestHealthzEndpointStillReturns204 pins that the wrapper preserves
// 204 No Content — the readiness probe path. The wrapper must only
// special-case 404; other status codes (200, 201, 204, 4xx, 5xx) flow
// through verbatim with whatever body the handler wrote (which for 204
// is empty by HTTP definition).
func TestHealthzEndpointStillReturns204(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpGet(t, base+"/v1/healthz")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status: got %d, want 204", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	if len(body) != 0 {
		t.Errorf("body: got %q, want empty", string(body))
	}
}
