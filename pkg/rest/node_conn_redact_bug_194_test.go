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
	"net/http"
	"strings"
	"testing"
)

// bug194SharedSecret is the v11 canary value the deny-list MUST scrub
// from `GET /v1/node-connections/...` (Bug 194). Distinct from the
// v10 canary so a regression on Bugs 187-190 vs. Bug 194 surfaces
// without spillover across reports.
const bug194SharedSecret = "hunter2-v11-poc"

// seedNodeConnectionSharedSecret stages an (A, B) pair with the
// upstream-legitimate `DrbdOptions/Net/shared-secret` key set to the
// v11 canary. Mirrors the operator workflow:
//
//	linstor node-connection set-property A B \
//	    DrbdOptions/Net/shared-secret hunter2-v11-poc
//
// Uses the modify wire surface so persistence flows through the same
// path the operator's `linstor node-connection set-property` would
// exercise — guarantees the leak surface under test reads back the
// real on-cluster shape and not a hand-crafted bag.
func seedNodeConnectionSharedSecret(t *testing.T, base, src, dst, key, value string) {
	t.Helper()

	body, err := json.Marshal(nodeConnectionPutBody{
		OverrideProps: map[string]string{key: value},
	})
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPut,
		base+"/v1/node-connections/"+src+"/"+dst, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}

	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("seed PUT: got %d, want 200", resp.StatusCode)
	}
}

// TestBug194NodeConnectionGetRedactsSharedSecret covers
// `GET /v1/node-connections/{src}/{dst}`. The Bug 115 + 181-190
// redaction sweep wired every other read surface, but the
// node-connection per-pair GET was missed — operators legitimately
// set `DrbdOptions/Net/shared-secret` per-pair (upstream UG9), so
// the raw shared-secret leaks on any read-only LINSTOR access.
func TestBug194NodeConnectionGetRedactsSharedSecret(t *testing.T) {
	t.Parallel()

	base, stop := startServerWithStore(t,
		seedNodeConnectionEndpoints(t, "n194a", "n194b"))
	defer stop()

	seedNodeConnectionSharedSecret(t, base, "n194a", "n194b",
		"DrbdOptions/Net/shared-secret", bug194SharedSecret)

	resp := httpGet(t, base+"/v1/node-connections/n194a/n194b")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	body, err := readAllBody(resp)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	if strings.Contains(string(body), bug194SharedSecret) {
		t.Errorf("GET /v1/node-connections/{a}/{b} leaked shared-secret %q in body: %s",
			bug194SharedSecret, body)
	}

	raw := string(body)
	if !strings.Contains(raw, redactedPropValue) &&
		!strings.Contains(raw, htmlEscapedRedactedMarker) {
		t.Errorf("GET /v1/node-connections/{a}/{b} missing redaction marker in body: %s", body)
	}

	// Key preservation — operators must still see "shared-secret IS
	// configured" so the redacted view doesn't look like an absent
	// secret.
	if !strings.Contains(raw, "DrbdOptions/Net/shared-secret") {
		t.Errorf("GET /v1/node-connections/{a}/{b} dropped shared-secret key entirely; body=%s", body)
	}
}

// TestBug194NodeConnectionListRedactsSharedSecret covers the list
// sibling `GET /v1/node-connections` (and the optional-filter form
// `GET /v1/node-connections/{src}`). Same registry, same leak — pin
// both surfaces so a partial fix (Get but not List, or vice versa)
// fails the test.
func TestBug194NodeConnectionListRedactsSharedSecret(t *testing.T) {
	t.Parallel()

	base, stop := startServerWithStore(t,
		seedNodeConnectionEndpoints(t, "n194c", "n194d"))
	defer stop()

	seedNodeConnectionSharedSecret(t, base, "n194c", "n194d",
		"DrbdOptions/Net/shared-secret", bug194SharedSecret)

	resp := httpGet(t, base+"/v1/node-connections")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	body, err := readAllBody(resp)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	if strings.Contains(string(body), bug194SharedSecret) {
		t.Errorf("GET /v1/node-connections leaked shared-secret %q in body: %s",
			bug194SharedSecret, body)
	}

	raw := string(body)
	if !strings.Contains(raw, redactedPropValue) &&
		!strings.Contains(raw, htmlEscapedRedactedMarker) {
		t.Errorf("GET /v1/node-connections missing redaction marker in body: %s", body)
	}

	if !strings.Contains(raw, "DrbdOptions/Net/shared-secret") {
		t.Errorf("GET /v1/node-connections dropped shared-secret key entirely; body=%s", body)
	}
}

// TestBug194NodeConnectionPreservesNonSensitiveProps pins the
// non-regression half of the fix: the deny-list is a key-substring
// match, NOT a blanket redaction. A benign key like `Sites/Site`
// must come back verbatim through both Get and List so operators
// who use node-connections for non-secret tagging (site labels,
// topology hints) keep their on-wire shape.
func TestBug194NodeConnectionPreservesNonSensitiveProps(t *testing.T) {
	t.Parallel()

	base, stop := startServerWithStore(t,
		seedNodeConnectionEndpoints(t, "n194e", "n194f"))
	defer stop()

	seedNodeConnectionSharedSecret(t, base, "n194e", "n194f", "Sites/Site", "site-a")

	resp := httpGet(t, base+"/v1/node-connections/n194e/n194f")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got nodeConnectionWire

	if decodeErr := json.NewDecoder(resp.Body).Decode(&got); decodeErr != nil {
		t.Fatalf("decode: %v", decodeErr)
	}

	if got.Props["Sites/Site"] != "site-a" {
		t.Errorf("Sites/Site: got %q, want %q (deny-list must not over-redact benign keys)",
			got.Props["Sites/Site"], "site-a")
	}

	// And the list sibling must agree.
	listResp := httpGet(t, base+"/v1/node-connections")
	defer func() { _ = listResp.Body.Close() }()

	var pairs []nodeConnectionWire

	if decodeErr := json.NewDecoder(listResp.Body).Decode(&pairs); decodeErr != nil {
		t.Fatalf("decode list: %v", decodeErr)
	}

	if len(pairs) != 1 {
		t.Fatalf("len: got %d entries, want 1", len(pairs))
	}

	if pairs[0].Props["Sites/Site"] != "site-a" {
		t.Errorf("list Sites/Site: got %q, want %q",
			pairs[0].Props["Sites/Site"], "site-a")
	}
}
