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
	"strconv"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// seedNodesForHEAD populates the store with enough nodes that the JSON
// response body exceeds net/http's 4KB sniff buffer. Below the buffer,
// net/http auto-derives Content-Length on a single buffered Write so
// the bug doesn't manifest; above it, GET switches to chunked and the
// HEAD response loses Content-Length entirely (Bug 160). Replicating
// the production stand's >4KB body is what makes the test deterministic.
func seedNodesForHEAD(t *testing.T, st store.Store) {
	t.Helper()

	// 5 nodes × ~1.2KB props each ≈ 6KB JSON — comfortably past the 4KB
	// chunked-transition threshold.
	for i := range 5 {
		name := "bug160-node-" + strconv.Itoa(i)
		props := map[string]string{}

		for j := range 20 {
			props["FillerKey"+strconv.Itoa(j)] = strings.Repeat("v", 50)
		}

		if err := st.Nodes().Create(t.Context(), &apiv1.Node{
			Name:  name,
			Type:  apiv1.NodeTypeSatellite,
			Props: props,
			NetInterfaces: []apiv1.NetInterface{
				{Name: "default", Address: "10.0.0." + strconv.Itoa(i+1)},
			},
		}); err != nil {
			t.Fatalf("seed node %s: %v", name, err)
		}
	}
}

// TestBug160HEADReturnsContentLength pins Bug 160: HEAD on a wired GET
// endpoint MUST return the same headers as the equivalent GET, including
// Content-Length. RFC 9110 §9.3.2: "The server SHOULD send the same
// header fields in response to a HEAD request as it would have sent if
// the request method had been GET." Some curl probes, HTTP/1.0
// clients, and naive proxy/LB health-checkers rely on Content-Length to
// size their read buffer; chunked-only or header-less HEAD responses
// either degenerate or hang.
//
// Before the fix: HEAD /v1/nodes returned 200 + Content-Type but no
// Content-Length and no Transfer-Encoding (net/http strips chunked on
// HEAD without substituting a length).
func TestBug160HEADReturnsContentLength(t *testing.T) {
	st := store.NewInMemory()
	seedNodesForHEAD(t, st)

	base, stop := startServerWithStore(t, st)
	defer stop()

	// Sample GET first so we can compare body length.
	getResp := httpGet(t, base+"/v1/nodes")
	defer func() { _ = getResp.Body.Close() }()

	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("GET status: got %d, want 200", getResp.StatusCode)
	}

	getBody, err := io.ReadAll(getResp.Body)
	if err != nil {
		t.Fatalf("GET read body: %v", err)
	}

	// HEAD must return same status, same Content-Type, AND Content-Length
	// equal to the GET body length.
	req, err := http.NewRequestWithContext(t.Context(), http.MethodHead, base+"/v1/nodes", nil)
	if err != nil {
		t.Fatalf("new HEAD request: %v", err)
	}

	headResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do HEAD: %v", err)
	}

	defer func() { _ = headResp.Body.Close() }()

	if headResp.StatusCode != http.StatusOK {
		t.Errorf("HEAD status: got %d, want 200", headResp.StatusCode)
	}

	cl := headResp.Header.Get("Content-Length")
	if cl == "" {
		t.Fatalf("HEAD missing Content-Length header (RFC 9110 §9.3.2). Headers: %v", headResp.Header)
	}

	clInt, err := strconv.Atoi(cl)
	if err != nil {
		t.Fatalf("Content-Length not numeric: %q", cl)
	}

	if clInt != len(getBody) {
		t.Errorf("Content-Length %d != GET body length %d (HEAD must report the byte count GET would have sent)",
			clInt, len(getBody))
	}
}

// TestBug160HEADStripsBody pins the body-stripping side of HEAD: the
// response MUST be header-only, no body bytes on the wire. This guards
// against a naive fix that returns Content-Length but ALSO leaks the
// body (which would double-count bytes for clients that respect
// Content-Length and read that many bytes).
func TestBug160HEADStripsBody(t *testing.T) {
	st := store.NewInMemory()
	seedNodesForHEAD(t, st)

	base, stop := startServerWithStore(t, st)
	defer stop()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodHead, base+"/v1/nodes", nil)
	if err != nil {
		t.Fatalf("new HEAD request: %v", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do HEAD: %v", err)
	}

	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	if len(body) != 0 {
		t.Errorf("HEAD body: got %d bytes (%q), want 0 (HEAD must be header-only)", len(body), body)
	}

	// Same content-type as GET.
	if ct := resp.Header.Get("Content-Type"); ct == "" {
		t.Errorf("HEAD missing Content-Type header")
	}
}

// TestBug160GETStillWorks is a regression guard: the HEAD wrapper must
// not interfere with normal GET requests. Body must still be present,
// Content-Length OR Transfer-Encoding present, status 200.
func TestBug160GETStillWorks(t *testing.T) {
	st := store.NewInMemory()
	seedNodesForHEAD(t, st)

	base, stop := startServerWithStore(t, st)
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

	if len(body) == 0 {
		t.Errorf("GET body is empty — expected at least an empty JSON array")
	}
}
