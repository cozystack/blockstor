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

// TestBug126ControllerPropertiesInfoReturnsEnvelopeOr200WithSchema pins
// Bug 126: `GET /v1/controller/properties/info` used to return a bare
// `[]` — neither a real property catalogue nor a typed not-implemented
// envelope. The CLI consumed the empty array silently and operators got
// no signal that the catalogue isn't actually populated.
//
// We pick option (A): unregister the route so the Bug 103 catch-all
// emits the LINSTOR `[]ApiCallRc` "endpoint not implemented" envelope.
// That mirrors how every other unfinished endpoint surfaces and gives
// `linstor c lp -i` a real typed ERROR line instead of "no properties".
//
// If a future commit decides to populate the catalogue (option B), this
// test also accepts 200 + a JSON array whose entries carry at least the
// upstream `PropsInfo` `name`/`info` fields — but the current shape MUST
// stop being a bare empty array.
func TestBug126ControllerPropertiesInfoReturnsEnvelopeOr200WithSchema(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpGet(t, base+"/v1/controller/properties/info")
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	switch resp.StatusCode {
	case http.StatusNotFound, http.StatusMethodNotAllowed:
		// Option (A): catch-all envelope. Verify the wire shape so the
		// python CLI sees a typed ERROR instead of crashing on XML decode.
		//
		// We accept both 404 and 405 here: `DELETE /v1/controller/
		// properties/{key...}` is wired for the prop-delete path, and
		// http.ServeMux treats GET on that pattern as 405 (path matches,
		// verb does not). Either envelope tells `linstor c lp -i` "this
		// endpoint isn't implemented" with a typed ret_code, which is
		// the actual user-visible fix for Bug 126.
		if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
			t.Errorf("Content-Type: got %q, want application/json", ct)
		}

		var rc []apiv1.APICallRc
		if err := json.Unmarshal(body, &rc); err != nil {
			t.Fatalf("decode envelope: %v\nbody: %s", err, body)
		}

		if len(rc) == 0 {
			t.Fatalf("empty envelope — python-linstor crashes on replies[0]")
		}

		if rc[0].RetCode >= 0 {
			t.Errorf("ret_code = %d, want negative (MASK_ERROR)", rc[0].RetCode)
		}

		if !strings.Contains(rc[0].Cause, "/v1/controller/properties/info") {
			t.Errorf("cause %q does not reference the request path", rc[0].Cause)
		}
	case http.StatusOK:
		// Option (B): populated catalogue. We do NOT accept the bare `[]`
		// shape that Bug 126 reported — the array must be non-empty AND
		// entries must look like upstream `PropsInfo` (name + info).
		var got []map[string]any
		if err := json.Unmarshal(body, &got); err != nil {
			t.Fatalf("decode catalogue: %v\nbody: %s", err, body)
		}

		if len(got) == 0 {
			t.Fatalf("Bug 126 regression: response is still bare [] — must be 404+envelope OR a populated catalogue")
		}

		for i, entry := range got {
			if _, ok := entry["name"]; !ok {
				t.Errorf("entry[%d] missing `name` field — not a PropsInfo shape", i)
			}
		}
	default:
		t.Fatalf("unexpected status %d (want 404+envelope or 200+populated array)\nbody: %s",
			resp.StatusCode, body)
	}
}

// TestBug127SosReportDownloadEnvelopeConsistent pins Bug 127: before
// the fix, `linstor sos-report create` (GET /v1/sos-report) returned a
// canned "not yet implemented" envelope while `linstor sos-report
// download` (GET /v1/sos-report/download) fell through to the generic
// catch-all envelope, which has a different ret_code/cause/correction
// shape. Operators saw two different error stories for the same
// half-implemented feature.
//
// Fix: register a `download` handler that returns the same canned
// envelope shape (LINSTOR `[]ApiCallRc`, MASK_ERROR ret_code, message
// containing "sos-report download not yet implemented"). The CLI's
// surfaced ERROR line is now identical to the create side.
func TestBug127SosReportDownloadEnvelopeConsistent(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpGet(t, base+"/v1/sos-report/download")
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("Content-Type: got %q, want application/json", ct)
	}

	// Status must be the same 501 the create endpoint returns — both are
	// "not implemented", not "not found".
	if resp.StatusCode != http.StatusNotImplemented {
		t.Errorf("status: got %d, want 501 (must match sos-report create)", resp.StatusCode)
	}

	var rc []apiv1.APICallRc
	if err := json.Unmarshal(body, &rc); err != nil {
		t.Fatalf("decode envelope: %v\nbody: %s", err, body)
	}

	if len(rc) == 0 {
		t.Fatalf("empty envelope — python-linstor crashes on replies[0]")
	}

	if rc[0].RetCode >= 0 {
		t.Errorf("ret_code = %d, want negative (MASK_ERROR)", rc[0].RetCode)
	}

	if !strings.Contains(rc[0].Message, "not yet implemented") {
		t.Errorf("message %q does not advertise the half-implemented state", rc[0].Message)
	}

	if !strings.Contains(rc[0].Message, "download") {
		t.Errorf("message %q does not mention the `download` action", rc[0].Message)
	}
}

// TestBug127SosReportCreateStillEnvelope is a regression guard: while
// fixing the `download` side we must not regress the already-canned
// `create` side. GET /v1/sos-report must still return the same 501 +
// `[]ApiCallRc` envelope it did before this commit.
func TestBug127SosReportCreateStillEnvelope(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpGet(t, base+"/v1/sos-report")
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	if resp.StatusCode != http.StatusNotImplemented {
		t.Errorf("status: got %d, want 501", resp.StatusCode)
	}

	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("Content-Type: got %q, want application/json", ct)
	}

	var rc []apiv1.APICallRc
	if err := json.Unmarshal(body, &rc); err != nil {
		t.Fatalf("decode envelope: %v\nbody: %s", err, body)
	}

	if len(rc) == 0 {
		t.Fatalf("empty envelope — python-linstor crashes on replies[0]")
	}

	if rc[0].RetCode >= 0 {
		t.Errorf("ret_code = %d, want negative (MASK_ERROR)", rc[0].RetCode)
	}

	if !strings.Contains(rc[0].Message, "not yet implemented") {
		t.Errorf("message %q lost the canned not-implemented phrasing", rc[0].Message)
	}
}
