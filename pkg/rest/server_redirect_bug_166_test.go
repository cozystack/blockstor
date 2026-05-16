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
	"net/url"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Bug 166: Go's http.ServeMux auto-redirects "URL pathologies" — paths
// containing `//`, `..`, or a trailing slash on a route registered
// without one — with a 301 response that carries Content-Type
// `text/html` and an HTML body (`<a href="...">Moved Permanently</a>`).
//
// python-linstor's error-decoding path assumes every non-2xx body is a
// LINSTOR `[]ApiCallRc` JSON envelope; on the HTML body it falls
// through to XML and crashes with
// `xml.etree.ElementTree.ParseError: syntax error: line 1, column 0`.
//
// This is the same defect class as Bug 103 (404 plain-text) and Bug 109
// (405 plain-text): every non-2xx response from the apiserver must be a
// LINSTOR JSON envelope so the CLI can surface a typed `ERROR:` line
// instead of dying with a Python traceback.
//
// The LINSTOR REST contract does not need redirects: every path either
// exists (handler runs) or doesn't (404 envelope). The fix folds the
// ServeMux redirect into the same 404 envelope path so the wire-edge
// invariant ("every error is JSON") holds.

// rawClientNoRedirect returns an http.Client that does NOT follow
// redirects. We need to observe the 301 reply directly; the default
// client transparently follows it (turning the test into "did we
// eventually hit a 200"), masking the body-shape regression.
func rawClientNoRedirect() *http.Client {
	return &http.Client{
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

// rawGET issues a GET to addr with redirects disabled. Returns the raw
// response so the caller can assert on body bytes.
func rawGET(t *testing.T, addr string) *http.Response {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, addr, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}

	resp, err := rawClientNoRedirect().Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}

	return resp
}

// assertEnvelope is the shared body-shape check used by the Bug 166
// tests. Mirrors the contract pinned by Bug 103 and Bug 109: the body
// MUST be a non-empty LINSTOR `[]ApiCallRc` JSON array with MASK_ERROR
// set on the first entry; the Content-Type MUST be application/json;
// the body MUST NOT contain the HTML redirect marker that ServeMux
// emits by default.
func assertEnvelope(t *testing.T, resp *http.Response, wantPathInCause string) {
	t.Helper()

	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("Content-Type: got %q, want application/json (bug 166: python CLI crashes on HTML redirect body)", ct)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	if strings.Contains(string(body), "<a href=") || strings.Contains(strings.ToLower(string(body)), "<html") {
		t.Fatalf("body contains HTML redirect marker — wrapper did not rewrite: %s", body)
	}

	var rc []apiv1.APICallRc
	if err := json.Unmarshal(body, &rc); err != nil {
		t.Fatalf("decode envelope: %v\nbody: %s", err, body)
	}

	if len(rc) == 0 {
		t.Fatalf("empty envelope — python-linstor crashes on replies[0]")
	}

	if rc[0].RetCode >= 0 {
		t.Errorf("ret_code = %d, want negative (MASK_ERROR set)", rc[0].RetCode)
	}

	if rc[0].Message == "" {
		t.Errorf("empty message — operator log will be unreadable")
	}

	if wantPathInCause != "" && !strings.Contains(rc[0].Cause, wantPathInCause) {
		t.Errorf("cause %q does not reference the request path %q", rc[0].Cause, wantPathInCause)
	}
}

// TestBug166DoubleSlashRedirectReturnsEnvelope pins the double-slash
// case: GET `//v1/nodes`. http.ServeMux's default behaviour is a 301 to
// `/v1/nodes` with an HTML body. The wrapper must intercept that and
// emit a LINSTOR envelope (so python-linstor doesn't crash).
//
// The URL is constructed via url.URL with the raw `//v1/nodes` path to
// bypass the net/http client-side cleanup that might otherwise rewrite
// the path before it leaves the test process.
func TestBug166DoubleSlashRedirectReturnsEnvelope(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	baseURL, err := url.Parse(base)
	if err != nil {
		t.Fatalf("parse base: %v", err)
	}

	target := &url.URL{
		Scheme: baseURL.Scheme,
		Host:   baseURL.Host,
		Opaque: "//" + baseURL.Host + "//v1/nodes",
	}

	resp := rawGET(t, target.String())
	defer func() { _ = resp.Body.Close() }()

	// Status is either 404 (canonicalised-then-rewritten) or 301
	// (preserved, body still rewritten). Both are acceptable; the
	// invariant is body-shape, not status.
	if resp.StatusCode != http.StatusMovedPermanently && resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 301 or 404", resp.StatusCode)
	}

	assertEnvelope(t, resp, "//v1/nodes")
}

// TestBug166RelativePathRedirectReturnsEnvelope pins the parent-
// relative case: GET `/v1/../v1/nodes`. http.ServeMux runs path.Clean
// and redirects to `/v1/nodes`; the default reply is HTML.
func TestBug166RelativePathRedirectReturnsEnvelope(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	baseURL, err := url.Parse(base)
	if err != nil {
		t.Fatalf("parse base: %v", err)
	}

	target := &url.URL{
		Scheme: baseURL.Scheme,
		Host:   baseURL.Host,
		Opaque: "//" + baseURL.Host + "/v1/../v1/nodes",
	}

	resp := rawGET(t, target.String())
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusMovedPermanently && resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 301 or 404", resp.StatusCode)
	}

	assertEnvelope(t, resp, "")
}

// TestBug166TrailingSlashRedirectReturnsEnvelope pins the trailing-
// slash case: GET `/v1/nodes/` where the registered pattern is
// `GET /v1/nodes` (no trailing slash). Go's 1.22+ pattern matcher
// returns a 301 redirect-or-NotFound shape that python-linstor cannot
// decode as JSON.
func TestBug166TrailingSlashRedirectReturnsEnvelope(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := rawGET(t, base+"/v1/nodes/")
	defer func() { _ = resp.Body.Close() }()

	// 301 → wrapper rewrites body. 404 → already an envelope via
	// the existing 404 wrapper. Either status is acceptable.
	if resp.StatusCode != http.StatusMovedPermanently && resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 301 or 404", resp.StatusCode)
	}

	assertEnvelope(t, resp, "")
}

// TestBug166RegularGetUnaffected pins the regression guard: the wrapper
// must NOT touch a happy-path 200 reply. GET /v1/nodes on an empty
// store returns `[]` with status 200 — unchanged before and after the
// Bug 166 fix.
func TestBug166RegularGetUnaffected(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := rawGET(t, base+"/v1/nodes")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	if strings.TrimSpace(string(body)) != "[]" {
		t.Errorf("body: got %q, want \"[]\"", string(body))
	}
}
