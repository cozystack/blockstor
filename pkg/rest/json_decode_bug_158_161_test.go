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
	"compress/gzip"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Bug 158 (P1) — typed envelope invariant breaks on malformed JSON.
//
// Every JSON decode site in pkg/rest/ used the std-lib decoder's
// error string verbatim. python-linstor expects an `[]ApiCallRc`
// envelope on every reply, so the bare-text errors below crashed
// the CLI before the operator saw any signal:
//
//   - empty body          → wire body literally `"EOF"`
//   - wrong JSON shape    → `"json: cannot unmarshal array into Go value of type v1.Node"`
//                           (leaks the Go type name `v1.Node` — internal API).
//   - garbage             → `"invalid character 'o' in literal null"`
//   - gzip body           → `"invalid character '\x1f'"`
//
// The fix introduces a `decodeJSON` helper that maps every
// known decoder failure to a typed LINSTOR `[]ApiCallRc` envelope
// with an operator-friendly cause/correction — and refuses unknown
// top-level fields (Bug 161, below).

// TestBug158EmptyBodyReturnsEnvelope: POST with an empty body must
// not return the literal string "EOF" on the wire. python-linstor
// surfaces such replies as `Unable to parse REST json data: Expecting
// value` and dies; operators get no signal that the client forgot a
// payload.
func TestBug158EmptyBodyReturnsEnvelope(t *testing.T) {
	t.Parallel()

	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpPost(t, base+"/v1/nodes", nil)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400 (empty body must be a 400)", resp.StatusCode)
	}

	body := mustReadBody(t, resp)

	if strings.TrimSpace(string(body)) == `"EOF"` || string(body) == "EOF" {
		t.Fatalf("body leaks raw decoder EOF: %s", body)
	}

	rc := mustDecodeEnvelope(t, body)
	assertErrorEnvelope(t, rc, body)

	// Operator-friendly cause/message. The exact wording is implementation
	// choice; we pin only that "EOF" / "json:" / "unmarshal" don't leak.
	for _, leak := range []string{"EOF", "json:", "json.", "unmarshal", "literal null"} {
		if strings.Contains(string(body), leak) {
			t.Errorf("body leaks decoder impl detail %q: %s", leak, body)
		}
	}
}

// TestBug158WrongTypeReturnsEnvelopeNoGoTypeLeak: POST a JSON array
// to an endpoint that expects a single object. The std-lib decoder
// returns `"json: cannot unmarshal array into Go value of type v1.Node"`
// — leaking the Go package + type name (`v1.Node`). The fix must
// wrap this in a LINSTOR envelope with NO Go-side identifiers.
func TestBug158WrongTypeReturnsEnvelopeNoGoTypeLeak(t *testing.T) {
	t.Parallel()

	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	// POST a JSON array to /v1/nodes — handler decodes into a single
	// apiv1.Node value, so the decoder returns UnmarshalTypeError with
	// the Go type name in its message.
	resp := httpPost(t, base+"/v1/nodes", []byte("[]"))
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400 (wrong JSON shape)", resp.StatusCode)
	}

	body := mustReadBody(t, resp)

	for _, leak := range []string{
		"v1.Node", "apiv1.Node", "linstor.v1", "json:", "json.UnmarshalTypeError",
		"Go value of type",
	} {
		if strings.Contains(string(body), leak) {
			t.Errorf("body leaks Go-side identifier %q: %s", leak, body)
		}
	}

	rc := mustDecodeEnvelope(t, body)
	assertErrorEnvelope(t, rc, body)
}

// TestBug158GarbageReturnsEnvelope: POST non-JSON bytes — `"not-json"`
// without leading quote, `foo`, etc. The decoder emits
// `"invalid character 'o' in literal null"`; the fix must wrap that
// in a LINSTOR envelope with a stable, operator-friendly message.
func TestBug158GarbageReturnsEnvelope(t *testing.T) {
	t.Parallel()

	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpPost(t, base+"/v1/nodes", []byte("not-json"))
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400 (garbage JSON)", resp.StatusCode)
	}

	body := mustReadBody(t, resp)

	for _, leak := range []string{"invalid character", "literal null", "json:"} {
		if strings.Contains(string(body), leak) {
			t.Errorf("body leaks raw decoder string %q: %s", leak, body)
		}
	}

	rc := mustDecodeEnvelope(t, body)
	assertErrorEnvelope(t, rc, body)
}

// TestBug158GzipBodyReturnsEnvelope: gzip-encoded body. Operator sends
// `curl --data-binary @file.gz` without `--data-raw` and forgets the
// Content-Encoding. The decoder sees the gzip magic bytes (0x1f 0x8b)
// and returns `"invalid character '\x1f'"`. The fix must wrap that in a
// typed envelope. The status may be 400 (the wire view: garbage JSON)
// or 415 (the semantic view: wrong media type) — either is acceptable,
// the load-bearing invariant is "no `invalid character` leak".
func TestBug158GzipBodyReturnsEnvelope(t *testing.T) {
	t.Parallel()

	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	var buf bytes.Buffer

	gz := gzip.NewWriter(&buf)

	_, err := gz.Write([]byte(`{"name":"alpha"}`))
	if err != nil {
		t.Fatalf("gzip write: %v", err)
	}

	if err := gz.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}

	resp := httpPost(t, base+"/v1/nodes", buf.Bytes())
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest && resp.StatusCode != http.StatusUnsupportedMediaType {
		t.Errorf("status: got %d, want 400 or 415", resp.StatusCode)
	}

	body := mustReadBody(t, resp)

	for _, leak := range []string{"invalid character", "\\x1f", "\x1f", "json:"} {
		if strings.Contains(string(body), leak) {
			t.Errorf("body leaks raw decoder string %q: %s", leak, body)
		}
	}

	rc := mustDecodeEnvelope(t, body)
	assertErrorEnvelope(t, rc, body)
}

// Bug 161 (P2) — `json.Decoder` silently drops unknown top-level fields.
//
// `{"resource":{"node_name":"..."},"props":{"StorPoolName":"nonexistent-sp"}}`
// (top-level `props` instead of `resource.props`) is silently accepted;
// the SP-existence gate (Bug 117/118) never fires because the wrong
// field carries the pool reference; Resource is created in Unknown state.
//
// The fix calls `dec.DisallowUnknownFields()` on every decoder in
// pkg/rest/ and rejects unknown fields with 400 + envelope.

// TestBug161UnknownTopLevelFieldRefused: a bogus top-level field
// (not part of the wire contract) must be refused — and no Resource
// CRD may be created.
func TestBug161UnknownTopLevelFieldRefused(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "rd161a"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n1"}); err != nil {
		t.Fatalf("seed Node: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body := []byte(`{"resource":{"node_name":"n1"},"unknown_field":"x"}`)

	resp := httpPost(t, base+"/v1/resource-definitions/rd161a/resources", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		got := mustReadBody(t, resp)
		t.Fatalf("status: got %d, want 400 (Bug 161 unknown top-level field). Body: %s", resp.StatusCode, got)
	}

	got := mustReadBody(t, resp)

	rc := mustDecodeEnvelope(t, got)
	assertErrorEnvelope(t, rc, got)

	// The envelope must name the offending field so operators can fix the call.
	if !strings.Contains(string(got), "unknown_field") {
		t.Errorf("envelope must cite the unknown field name: %s", got)
	}

	// No phantom Resource CRD.
	if _, err := st.Resources().Get(ctx, "rd161a", "n1"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("Resource rd161a.n1 persisted despite 400: err=%v", err)
	}
}

// TestBug161PropsAtTopLevelRefused: exact v5-report repro. Operator
// (or buggy client) puts `props` at the top level instead of inside
// `resource`. Pre-fix the SP gate never fires (wrong path for
// StorPoolName) and the Resource lands with no SP. The fix refuses
// the body outright.
func TestBug161PropsAtTopLevelRefused(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "rd161b"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n1"}); err != nil {
		t.Fatalf("seed Node: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body := []byte(`{"resource":{"node_name":"n1"},"props":{"StorPoolName":"bogus-sp"}}`)

	resp := httpPost(t, base+"/v1/resource-definitions/rd161b/resources", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		got := mustReadBody(t, resp)
		t.Fatalf("status: got %d, want 400 (Bug 161 props at top level). Body: %s", resp.StatusCode, got)
	}

	got := mustReadBody(t, resp)
	rc := mustDecodeEnvelope(t, got)
	assertErrorEnvelope(t, rc, got)

	// Envelope must name the offending key.
	if !strings.Contains(string(got), "props") {
		t.Errorf("envelope must cite the unknown field %q: %s", "props", got)
	}

	// No phantom Resource CRD.
	if _, err := st.Resources().Get(ctx, "rd161b", "n1"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("Resource rd161b.n1 persisted despite 400: err=%v", err)
	}
}

// TestBug161UnknownNestedFieldRefused: an unknown field nested inside
// `resource` must also be refused — DisallowUnknownFields is recursive
// on the Go side, so the same gate fires.
func TestBug161UnknownNestedFieldRefused(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "rd161c"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n1"}); err != nil {
		t.Fatalf("seed Node: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body := []byte(`{"resource":{"node_name":"n1","wat":"x"}}`)

	resp := httpPost(t, base+"/v1/resource-definitions/rd161c/resources", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		got := mustReadBody(t, resp)
		t.Fatalf("status: got %d, want 400 (Bug 161 unknown nested field). Body: %s", resp.StatusCode, got)
	}

	got := mustReadBody(t, resp)
	rc := mustDecodeEnvelope(t, got)
	assertErrorEnvelope(t, rc, got)

	if !strings.Contains(string(got), "wat") {
		t.Errorf("envelope must cite the unknown field %q: %s", "wat", got)
	}
}

// TestBug161WellFormedBodyStillAccepted: happy-path regression guard.
// A well-shaped POST that only carries known fields must still reach
// the existing Bug 117/118 SP gate (which 4xxs because the pool is
// unseeded). The signal is "we got past the JSON decode" — anything
// other than a successful decode would 400 with a different message.
func TestBug161WellFormedBodyStillAccepted(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "rd161d"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n1"}); err != nil {
		t.Fatalf("seed Node: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.ResourceCreate{
		Resource: apiv1.Resource{
			NodeName: "n1",
			Props:    map[string]string{"StorPoolName": "nonexistent-pool"},
		},
	})

	resp := httpPost(t, base+"/v1/resource-definitions/rd161d/resources", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 400 || resp.StatusCode >= 500 {
		got := mustReadBody(t, resp)
		t.Fatalf("status: got %d, want 4xx (Bug 118 SP gate). Body: %s", resp.StatusCode, got)
	}

	got := mustReadBody(t, resp)
	rc := mustDecodeEnvelope(t, got)
	assertErrorEnvelope(t, rc, got)

	// The Bug 118 envelope cites the missing pool — that's how we know
	// the decode succeeded and we landed on the right gate.
	if !strings.Contains(rc[0].Message, "nonexistent-pool") {
		t.Errorf("expected Bug 118 SP-gate envelope (cites 'nonexistent-pool'), got: %s", got)
	}
}

// --- helpers ---

func mustReadBody(t *testing.T, resp *http.Response) []byte {
	t.Helper()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	return body
}

func mustDecodeEnvelope(t *testing.T, body []byte) []apiv1.APICallRc {
	t.Helper()

	var rc []apiv1.APICallRc
	if err := json.Unmarshal(body, &rc); err != nil {
		t.Fatalf("body is not a []ApiCallRc envelope: %v\nbody: %s", err, body)
	}

	if len(rc) == 0 {
		t.Fatalf("empty envelope — python-linstor crashes on replies[0]\nbody: %s", body)
	}

	return rc
}

func assertErrorEnvelope(t *testing.T, rc []apiv1.APICallRc, body []byte) {
	t.Helper()

	if rc[0].RetCode&apiCallRcError == 0 {
		t.Errorf("envelope ret_code %#x does not carry MASK_ERROR: %s", rc[0].RetCode, body)
	}

	if rc[0].Message == "" {
		t.Errorf("envelope message is empty: %s", body)
	}
}
