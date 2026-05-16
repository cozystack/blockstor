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

// Bug 203 (P3) — `decodeJSON` accepts trailing JSON garbage.
//
// `pkg/rest/server.go` `decodeJSON` calls `dec.Decode(target)` and
// returns true on a nil error. The std-lib decoder is happy after a
// single complete JSON value: `{"valid":"json"}garbage` decodes
// successfully into the target even though `garbage` is invalid JSON
// following a complete object. Same shape on the Bug 173 bare-string
// passphrase decoder (`pkg/rest/encryption.go` `decodePassphraseEnterBody`).
//
// Wire impact: a malformed client putting an extra `}` or a trailing
// blob slips past the decode gate. linstor-csi never does this, but a
// hand-rolled `curl -d '{"foo":"bar"}\nbody-from-template-engine'`
// silently succeeds. The fix calls `dec.More()` after the primary
// Decode — `true` means residual bytes remain and we surface 400 +
// envelope with the same shape Bug 158/161 emits for malformed input.

// TestBug203DecodeJSONRejectsTrailingGarbage POSTs a body that's a
// complete JSON object followed by garbage. Pre-fix the std-lib
// decoder happily consumes the leading object and returns nil, the
// handler proceeds with the partially-decoded value, and the wire
// reply is 200 / per-handler success. Post-fix the helper sees the
// `dec.More()` flag and routes through `writeDecodeError` to surface
// 400 + the standard envelope citing "trailing JSON data".
func TestBug203DecodeJSONRejectsTrailingGarbage(t *testing.T) {
	t.Parallel()

	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	// POST /v1/nodes is a decodeJSON callsite (handleNodeCreate routes
	// the body through decodeJSON). A valid node object followed by
	// `extra` bytes must NOT decode cleanly. Pre-fix the std-lib
	// decoder returns nil after the closing brace and the handler runs
	// with the parsed Node.
	body := []byte(`{"name":"trailing-garbage","type":"SATELLITE"}extra`)

	resp := httpPost(t, base+"/v1/nodes", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: got %d, want 400 (trailing garbage must be rejected, body=%q)",
			resp.StatusCode, string(raw))
	}

	raw, _ := io.ReadAll(resp.Body)

	var rcs []apiv1.APICallRc
	if err := json.Unmarshal(raw, &rcs); err != nil {
		t.Fatalf("body is not a []ApiCallRc envelope: %v\nbody: %s", err, raw)
	}

	if len(rcs) == 0 || rcs[0].Message == "" {
		t.Fatalf("envelope empty / missing message: %s", raw)
	}

	if rcs[0].RetCode&apiCallRcError == 0 {
		t.Errorf("envelope ret_code %#x does not carry MASK_ERROR: %s", rcs[0].RetCode, raw)
	}

	// The exact wording is the fix author's choice; we pin only that
	// the envelope cites trailing data so operators know what's wrong.
	hay := strings.ToLower(rcs[0].Message + "\n" + rcs[0].Cause)
	if !strings.Contains(hay, "trailing") {
		t.Errorf("envelope doesn't mention 'trailing' data: %s", raw)
	}
}

// TestBug203PassphraseEnterRejectsTrailingGarbage PATCHes a bare
// string body with trailing garbage. Pre-fix the bare-string decoder
// in `decodePassphraseEnterBody` decodes the leading `"valid-pass"`
// happily, stuffs it into NewPassphrase, and the downstream auth path
// runs on the partial value. Post-fix the helper sees the residual
// bytes and surfaces 400 + envelope. Mirrors the Bug 173 malformed-
// string guard.
func TestBug203PassphraseEnterRejectsTrailingGarbage(t *testing.T) {
	t.Parallel()

	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	seedPassphrase(t, base, "seed-203")

	// Bare-string body with trailing garbage. The std-lib decoder
	// returns nil after the closing quote; pre-fix the handler then
	// stuffs the parsed value into NewPassphrase and runs the auth
	// path. Post-fix the helper rejects with 400.
	body := []byte(`"valid-pass"trailing`)

	resp := httpPatch(t, base+"/v1/encryption/passphrase", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("PATCH bare-string with trailing garbage: got %d, want 400 (body=%q)",
			resp.StatusCode, string(raw))
	}

	raw, _ := io.ReadAll(resp.Body)

	var rcs []apiv1.APICallRc
	if err := json.Unmarshal(raw, &rcs); err != nil {
		t.Fatalf("body is not a []ApiCallRc envelope: %v\nbody: %s", err, raw)
	}

	if len(rcs) == 0 || rcs[0].Message == "" {
		t.Fatalf("envelope empty / missing message: %s", raw)
	}

	if rcs[0].RetCode&apiCallRcError == 0 {
		t.Errorf("envelope ret_code %#x does not carry MASK_ERROR: %s", rcs[0].RetCode, raw)
	}

	hay := strings.ToLower(rcs[0].Message + "\n" + rcs[0].Cause)
	if !strings.Contains(hay, "trailing") {
		t.Errorf("envelope doesn't mention 'trailing' data: %s", raw)
	}
}
