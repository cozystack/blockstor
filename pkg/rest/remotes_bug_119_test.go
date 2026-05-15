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

// Bug 119 — `linstor remote c linstor t1 not-a-url` accepts the
// garbage URL verbatim AND returns a bare object on success. Two
// distinct issues stacked:
//
//  1. URL is not validated. `not-a-url` (no scheme, no host) is
//     persisted as-is and shows up in `remote l`. The fix is a
//     `net/url.Parse` + scheme/host non-empty check at the request
//     boundary, returning 400 + standard `[]APICallRc` envelope.
//
//  2. Wire shape mismatch. The pre-fix handler returned 201 with the
//     bare object body `{"remote_name":"t1","url":"..."}`. python-
//     linstor 1.27.1's response decoder unconditionally treats the
//     body as a list of `ApiCallResponse` dicts (responses.py:124
//     `data[0]["ret_code"]`) — a bare object trips
//     `TypeError: string indices must be integers, not 'str'` and
//     the CLI crashes with a traceback instead of "remote created".
//
// The fix returns the upstream LINSTOR envelope `[{ret_code,message}]`
// on success too, matching the shape every other write-side handler
// in this package already uses (see Bug 101 / aa5134fcf for the
// node-connection precedent).

package rest

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestBug119RemoteCreateLinstorRefusesInvalidURL pins the URL
// validation: a body whose `url` field has no scheme or no host
// (e.g. the literal `not-a-url`) MUST be refused with 400 + envelope,
// not persisted onto the registry. Without this check the operator's
// typo lands in the cluster state and `remote l` reports a remote
// nothing can actually reach.
func TestBug119RemoteCreateLinstorRefusesInvalidURL(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	for _, tc := range []struct {
		name string
		url  string
	}{
		{name: "bare token", url: "not-a-url"},
		{name: "scheme only", url: "http://"},
		{name: "host only", url: "//host.example"},
		{name: "garbage with colon", url: "garbage:"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body, _ := json.Marshal(map[string]string{
				"remote_name": "t1",
				"url":         tc.url,
			})

			resp := httpPost(t, base+"/v1/remotes/linstor", body)
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status: got %d, want 400", resp.StatusCode)
			}

			var rcs []apiv1.APICallRc
			if err := json.NewDecoder(resp.Body).Decode(&rcs); err != nil {
				t.Fatalf("decode envelope: %v (must be []APICallRc, not bare object)", err)
			}

			if len(rcs) != 1 {
				t.Fatalf("envelope len: got %d, want 1", len(rcs))
			}

			if !strings.Contains(rcs[0].Message, "url") {
				t.Errorf("message: got %q, want substring mentioning 'url'", rcs[0].Message)
			}
		})
	}

	// And the registry must still be empty after every reject.
	listResp := httpGet(t, base+"/v1/remotes/linstor")
	defer func() { _ = listResp.Body.Close() }()

	var list []map[string]string
	if err := json.NewDecoder(listResp.Body).Decode(&list); err != nil {
		t.Fatalf("decode list: %v", err)
	}

	if len(list) != 0 {
		t.Errorf("registry leaked invalid entry: %+v", list)
	}
}

// TestBug119RemoteCreateLinstorAcceptsValidURL pins the success path:
// a well-formed URL (scheme + host) is accepted with 201 AND the body
// is the LINSTOR envelope `[{ret_code, message: "remote created..."}]`
// so the python CLI's `responses.py` happy path doesn't crash on a
// bare object.
func TestBug119RemoteCreateLinstorAcceptsValidURL(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	body, _ := json.Marshal(map[string]string{
		"remote_name": "t1",
		"url":         "https://other-controller.example/v1",
	})

	resp := httpPost(t, base+"/v1/remotes/linstor", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201", resp.StatusCode)
	}

	var rcs []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rcs); err != nil {
		t.Fatalf("decode envelope: %v (must be []APICallRc, not bare object)", err)
	}

	if len(rcs) != 1 {
		t.Fatalf("envelope len: got %d, want 1; got=%+v", len(rcs), rcs)
	}

	if rcs[0].RetCode&maskInfo == 0 {
		t.Errorf("ret_code: got %#x, want MASK_INFO (%#x) set", rcs[0].RetCode, maskInfo)
	}

	if !strings.Contains(rcs[0].Message, "remote created") {
		t.Errorf("message: got %q, want substring 'remote created'", rcs[0].Message)
	}

	if !strings.Contains(rcs[0].Message, "t1") {
		t.Errorf("message: got %q, want substring 't1' (remote_name)", rcs[0].Message)
	}

	// The entry must have actually landed on the registry — the
	// envelope is a contract on the response shape, but the
	// underlying state still has to persist.
	listResp := httpGet(t, base+"/v1/remotes/linstor")
	defer func() { _ = listResp.Body.Close() }()

	var list []map[string]string
	if err := json.NewDecoder(listResp.Body).Decode(&list); err != nil {
		t.Fatalf("decode list: %v", err)
	}

	if len(list) != 1 || list[0]["remote_name"] != "t1" {
		t.Errorf("registry: got %+v, want one entry named t1", list)
	}
}

// TestBug119RemoteCreateLinstorEnvelopeShape is a structural
// type-assertion pin: the success response MUST be a JSON array (not a
// bare object). This is the single byte-level invariant the python
// CLI's `data[0]["ret_code"]` decode depends on; the test would catch
// a regression that flipped back to the bare-object shape even if the
// body otherwise carried the right fields.
func TestBug119RemoteCreateLinstorEnvelopeShape(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	body, _ := json.Marshal(map[string]string{
		"remote_name": "shape-check",
		"url":         "https://other-controller.example/v1",
	})

	resp := httpPost(t, base+"/v1/remotes/linstor", body)
	defer func() { _ = resp.Body.Close() }()

	// Decode into a permissive shape first to assert the JSON type
	// is an array, not an object — `json.Unmarshal` into `any`
	// surfaces `[]any` vs `map[string]any` directly.
	var raw any
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if _, ok := raw.([]any); !ok {
		t.Fatalf("response shape: got %T (%+v), want []any (LINSTOR envelope array)", raw, raw)
	}
}

// TestBug119RemoteCreateS3SimilarShape: the S3 typed POST endpoint is
// stubbed today (no Cozystack workflow drives it). If a future change
// wires it, the success body must be the LINSTOR envelope, not a bare
// object — this test guards the shape preemptively. The handler is
// allowed to return 501 / 404 / 405 in the meantime; the assertion
// only fires on a 2xx (i.e. when the handler claims success).
func TestBug119RemoteCreateS3SimilarShape(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	body, _ := json.Marshal(map[string]string{
		"remote_name": "s3-shape",
		"endpoint":    "https://s3.example",
		"bucket":      "test",
	})

	resp := httpPost(t, base+"/v1/remotes/s3", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// Stub today — nothing to enforce until S3 create is wired.
		t.Skipf("S3 create not yet implemented (status %d); shape pin will fire when it is", resp.StatusCode)

		return
	}

	var raw any
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if _, ok := raw.([]any); !ok {
		t.Errorf("S3 create response shape: got %T (%+v), want []any (LINSTOR envelope array)", raw, raw)
	}
}
