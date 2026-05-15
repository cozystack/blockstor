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

// TestWrongMethodReturnsLINSTORJSONEnvelope pins Bug 109: when the
// python-linstor CLI hits an existing path with a verb the apiserver
// has not wired (e.g. `PUT /v1/controller/config` for
// `linstor c set-log-level`), Go's http.ServeMux replies with a bare
// `text/plain` body `Method Not Allowed\n` and status 405. The CLI's
// error-decoding path tries JSON, falls back to XML and crashes with
// `xml.etree.ElementTree.ParseError`. This is the same defect class as
// Bug 103, just for 405 instead of 404.
//
// The fix extends the existing with404Envelope wrapper to also catch
// 405 plaintext bodies and rewrite them to the LINSTOR `[]ApiCallRc`
// envelope, while preserving:
//   - the 405 status code (callers may key on it; golinstor maps 405
//     to ErrMethodNotAllowed),
//   - the `Allow:` header (HTTP requires it on 405).
func TestWrongMethodReturnsLINSTORJSONEnvelope(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	// Bug 109 endpoint list — each (method, path) pair is a wired
	// route on a different verb, so http.ServeMux currently returns
	// the plaintext 405 marker.
	cases := []struct {
		name   string
		method string
		path   string
	}{
		{"controller config PUT", http.MethodPut, "/v1/controller/config"},
		{"schedules POST", http.MethodPost, "/v1/schedules"},
		{"remotes s3 POST", http.MethodPost, "/v1/remotes/s3"},
		{"remotes ebs POST", http.MethodPost, "/v1/remotes/ebs"},
		{"node reconnect POST", http.MethodPost, "/v1/nodes/alpha/reconnect"},
		{"files POST", http.MethodPost, "/v1/files"},
		{"files PUT", http.MethodPut, "/v1/files/some-file"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequestWithContext(t.Context(), tc.method, base+tc.path, nil)
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

			if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
				t.Errorf("Content-Type: got %q, want application/json (bug 109: python CLI parses non-JSON 405 bodies and crashes)", ct)
			}

			// http.ServeMux sets Allow on its built-in 405; the
			// wrapper must preserve it because golinstor and other
			// HTTP-spec clients use it to retry the right verb.
			if allow := resp.Header.Get("Allow"); allow == "" {
				t.Errorf("Allow header: empty; want method list preserved by wrapper")
			}

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("read body: %v", err)
			}

			if strings.Contains(string(body), "Method Not Allowed") && !strings.HasPrefix(strings.TrimSpace(string(body)), "[") {
				t.Fatalf("body contains plain-text 405 marker — wrapper did not rewrite: %s", body)
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

			if !strings.Contains(strings.ToLower(rc[0].Message), "method not allowed") {
				t.Errorf("message %q does not mention method not allowed", rc[0].Message)
			}

			// Cause should mention the actual METHOD + path so the
			// operator can grep the apiserver logs for the right
			// route.
			if !strings.Contains(rc[0].Cause, tc.path) {
				t.Errorf("cause %q does not reference the request path %q", rc[0].Cause, tc.path)
			}

			if !strings.Contains(rc[0].Cause, tc.method) {
				t.Errorf("cause %q does not reference the request method %q", rc[0].Cause, tc.method)
			}
		})
	}
}

// TestWrongMethodPreservesAllowHeader pins the Allow-header passthrough
// in isolation: http.ServeMux populates it with the comma-separated
// list of verbs registered for a path, and our wrapper must copy that
// to the client unchanged. The Bug 109 envelope rewrite must NOT
// shadow the spec-mandated header.
func TestWrongMethodPreservesAllowHeader(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	// /v1/controller/version is GET-only; DELETE → 405 with Allow: GET.
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
		t.Fatalf("status: got %d, want 405", resp.StatusCode)
	}

	allow := resp.Header.Get("Allow")
	if allow == "" {
		t.Fatalf("Allow header empty; want GET")
	}

	if !strings.Contains(allow, "GET") {
		t.Errorf("Allow %q does not contain GET", allow)
	}
}

// TestWrongMethodEnvelopeHasCorrection pins that the rewritten 405
// envelope includes a correction string referencing the Allow header
// so operators following the typed ERROR: output have a clear next
// step.
func TestWrongMethodEnvelopeHasCorrection(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPut,
		base+"/v1/controller/config", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	var rc []apiv1.APICallRc
	if err := json.Unmarshal(body, &rc); err != nil {
		t.Fatalf("decode envelope: %v\nbody: %s", err, body)
	}

	if len(rc) == 0 {
		t.Fatalf("empty envelope")
	}

	if !strings.Contains(strings.ToLower(rc[0].Correc), "allow") {
		t.Errorf("correction %q does not mention the Allow header", rc[0].Correc)
	}
}
