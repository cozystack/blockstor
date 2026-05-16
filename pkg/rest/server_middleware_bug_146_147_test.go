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
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestBug146OversizedBodyReturns413WithEnvelope pins Bug 146: a POST
// body larger than the apiserver's accepted maximum must produce a
// 413 Request Entity Too Large + LINSTOR `[]ApiCallRc` envelope —
// NOT a 500 with an etcd/k8s implementation-detail string in the body.
//
// Before the fix the K8s-backed Resources/RD store happily proxied a
// 2MB POST down into etcd, which rejected it with `etcdserver: request
// is too large`. apiserver passed that string straight through as a
// 500 body, leaking the persistence backend identity AND crashing
// python-linstor's error-envelope decoder (it expects `[]ApiCallRc`).
func TestBug146OversizedBodyReturns413WithEnvelope(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	// 2MB payload — well past the 1MB cap chosen for the wire.
	// We don't need it to parse as JSON; the body-limit middleware
	// must reject it before any decode attempt sees a single byte.
	payload := bytes.Repeat([]byte("x"), 2<<20)

	resp := httpPost(t, base+"/v1/resource-definitions", payload)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Errorf("status: got %d, want 413 (Bug 146 — oversized body must not crash to 500)", resp.StatusCode)
	}

	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("Content-Type: got %q, want application/json (python-linstor crashes on non-JSON error bodies)", ct)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
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

	// No backend-implementation leakage allowed.
	for _, leak := range []string{"etcd", "etcdserver", "apimachinery", "k8s.io"} {
		if strings.Contains(strings.ToLower(string(body)), strings.ToLower(leak)) {
			t.Errorf("body leaks impl detail %q: %s", leak, body)
		}
	}
}

// TestBug146ImplDetailsNotLeakedOnDecodeError pins the scrub side of
// Bug 146: even for an in-cap body that fails to decode (e.g.
// malformed JSON), the error envelope must NOT contain etcd / k8s
// implementation-detail strings. The current json decoder errors
// (`invalid character 'x' looking for ...`) are fine; the scrub
// guards future regressions where a store-side decode forwards a
// backend error string.
func TestBug146ImplDetailsNotLeakedOnDecodeError(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpPost(t, base+"/v1/resource-definitions", []byte("not-json{"))
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400 (malformed JSON)", resp.StatusCode)
	}

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

	for _, leak := range []string{"etcd", "etcdserver", "apimachinery", "k8s.io"} {
		if strings.Contains(strings.ToLower(string(body)), strings.ToLower(leak)) {
			t.Errorf("body leaks impl detail %q: %s", leak, body)
		}
	}
}

// TestBug147ContentTypeRequiredForPOSTJSONEndpoints pins Bug 147: a
// POST against a JSON endpoint with `Content-Type: text/plain` must
// be rejected with 415 Unsupported Media Type + LINSTOR envelope.
// Before the fix any Content-Type (or none) was accepted; clients
// that forgot the header (or sent a wrong one) ended up with an
// HTTP 201 even when the body wasn't actually JSON.
func TestBug147ContentTypeRequiredForPOSTJSONEndpoints(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
		base+"/v1/resource-definitions",
		bytes.NewReader([]byte(`{"resource_definition":{"name":"x"}}`)))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}

	req.Header.Set("Content-Type", "text/plain")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}

	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusUnsupportedMediaType {
		t.Errorf("status: got %d, want 415", resp.StatusCode)
	}

	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("Content-Type: got %q, want application/json", ct)
	}

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

	if rc[0].RetCode >= 0 {
		t.Errorf("ret_code = %d, want negative (MASK_ERROR set)", rc[0].RetCode)
	}
}

// TestBug147ContentTypeRequiredForPOSTNoCharset pins the real-world
// variant: `application/json; charset=utf-8` must be accepted (curl
// and many HTTP clients add the parameter by default). The check is
// "starts with application/json" not exact-match.
func TestBug147ContentTypeRequiredForPOSTNoCharset(t *testing.T) {
	st := store.NewInMemory()

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.ResourceDefinitionCreate{
		ResourceDefinition: apiv1.ResourceDefinition{Name: "rd-charset"},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
		base+"/v1/resource-definitions", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json; charset=utf-8")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}

	defer func() { _ = resp.Body.Close() }()

	// Allow 201 (created) but explicitly disallow 415 — the middleware
	// must not reject a parametrised content type.
	if resp.StatusCode == http.StatusUnsupportedMediaType {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: got 415 — middleware must allow application/json; charset=utf-8 (body: %s)", respBody)
	}
}

// TestBug147MissingContentTypeRejected pins the no-header variant:
// a POST with NO Content-Type header at all must be rejected with
// 415. Plenty of broken clients (and `curl -d` without `-H`) omit
// the header entirely; we must not silently accept whatever bytes
// they send.
func TestBug147MissingContentTypeRejected(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
		base+"/v1/resource-definitions",
		bytes.NewReader([]byte(`{"resource_definition":{"name":"x"}}`)))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	// Explicitly remove the Content-Type header net/http may add.
	req.Header.Del("Content-Type")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}

	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusUnsupportedMediaType {
		t.Errorf("status: got %d, want 415 (missing Content-Type)", resp.StatusCode)
	}

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
}

// TestBug147GETStillAcceptsAnyContentType pins the regression guard:
// GET requests must NOT be subject to the Content-Type gate. CLI and
// HTTP clients routinely send GETs without any Content-Type header
// (there's no body), and a few set a residual `text/plain` from
// connection reuse. Either case must reach the wired handler.
func TestBug147GETStillAcceptsAnyContentType(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	// No Content-Type header.
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet,
		base+"/v1/nodes", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}

	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status: got %d, want 200 (GET must not require Content-Type)", resp.StatusCode)
	}

	// Now with a wrong-but-present Content-Type — also must pass.
	req2, err := http.NewRequestWithContext(t.Context(), http.MethodGet,
		base+"/v1/nodes", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}

	req2.Header.Set("Content-Type", "text/plain")

	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("do: %v", err)
	}

	defer func() { _ = resp2.Body.Close() }()

	if resp2.StatusCode != http.StatusOK {
		t.Errorf("status: got %d, want 200 (GET text/plain must pass)", resp2.StatusCode)
	}
}
