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

// TestRemotesEnvelopeShape pins the /v1/remotes (no type suffix)
// response: a JSON object with three named empty arrays
// (s3_remotes, linstor_remotes, ebs_remotes). golinstor's
// client.RemoteList decodes into this shape — a bare `[]` errors
// with "cannot unmarshal array into Go value of type
// client.RemoteList" and would break every linstor-csi
// DeleteSnapshot flow.
func TestRemotesEnvelopeShape(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpGet(t, base+"/v1/remotes")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got emptyRemoteList
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v (response is bare array, not RemoteList object)", err)
	}

	if got.S3Remotes == nil {
		t.Errorf("s3_remotes: nil, want []")
	}

	if got.LinstorRemotes == nil {
		t.Errorf("linstor_remotes: nil, want []")
	}

	if got.EbsRemotes == nil {
		t.Errorf("ebs_remotes: nil, want []")
	}
}

// TestRemotesTypedEndpointsBareArray pins the /v1/remotes/{type}
// shape: a flat array of that type, NOT the envelope. golinstor's
// GetAllLinstor / GetAllS3 / GetAllEbs decode into `[]Type{...}` —
// the envelope shape breaks their decoder.
func TestRemotesTypedEndpointsBareArray(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	for _, path := range []string{
		"/v1/remotes/s3",
		"/v1/remotes/linstor",
		"/v1/remotes/ebs",
	} {
		t.Run(path, func(t *testing.T) {
			resp := httpGet(t, base+path)
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status: got %d, want 200", resp.StatusCode)
			}

			var got []map[string]any

			if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
				t.Fatalf("decode: %v (response is envelope object, not bare array)", err)
			}

			if got == nil {
				t.Errorf("got nil; want []")
			}
		})
	}
}

// TestLinstorRemoteCRUD pins the REST surface for scenario 4.17:
// POST /v1/remotes/linstor with the envelope `{remote_name, url,
// passphrase}` accepts and stores the entry; GET /v1/remotes/linstor
// surfaces it as a single-element array; DELETE /v1/remotes
// ?remote_name=... removes it; the follow-up GET goes back to `[]`.
//
// The body wire-shape matches upstream LINSTOR's `LinstorRemote`
// (see pkg/api/openapi/types.gen.go) so golinstor's typed client can
// drive this surface without bespoke decoders. We assert the exact
// JSON keys, not Go field names, because the persistence layer is
// what tooling sees.
func TestLinstorRemoteCRUD(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	const (
		remoteName = "peer-cluster-east"
		remoteURL  = "http://controller.peer.east.example:3370"
		remotePass = "shared-test-passphrase"
	)

	// --- POST: create the remote ---
	body, err := json.Marshal(map[string]string{
		"remote_name": remoteName,
		"url":         remoteURL,
		"passphrase":  remotePass,
	})
	if err != nil {
		t.Fatalf("marshal create body: %v", err)
	}

	createResp := httpPost(t, base+"/v1/remotes/linstor", body)
	defer func() { _ = createResp.Body.Close() }()

	if createResp.StatusCode != http.StatusCreated {
		gotBody, _ := io.ReadAll(createResp.Body)
		t.Fatalf("POST status: got %d, want 201; body=%s", createResp.StatusCode, gotBody)
	}

	// Bug 119: the create response is the LINSTOR `[]APICallRc`
	// envelope, not the bare entry object. python-linstor decodes
	// the body as a list of ApiCallResponses unconditionally — a
	// bare object trips `TypeError: string indices must be integers`
	// in `responses.py:124`. The remote_name is verified by reading
	// it back from the GET list below.
	var createdRcs []apiv1.APICallRc
	if err := json.NewDecoder(createResp.Body).Decode(&createdRcs); err != nil {
		t.Fatalf("decode create envelope: %v", err)
	}

	if len(createdRcs) != 1 {
		t.Fatalf("create envelope len: got %d, want 1; got=%+v", len(createdRcs), createdRcs)
	}

	if !strings.Contains(createdRcs[0].Message, remoteName) {
		t.Errorf("create message: got %q, want substring %q", createdRcs[0].Message, remoteName)
	}

	// --- GET /v1/remotes/linstor: typed-array list ---
	listResp := httpGet(t, base+"/v1/remotes/linstor")
	defer func() { _ = listResp.Body.Close() }()

	if listResp.StatusCode != http.StatusOK {
		t.Fatalf("GET typed status: got %d, want 200", listResp.StatusCode)
	}

	var list []map[string]string
	if err := json.NewDecoder(listResp.Body).Decode(&list); err != nil {
		t.Fatalf("decode list: %v", err)
	}

	if len(list) != 1 {
		t.Fatalf("typed list len: got %d, want 1; entries=%+v", len(list), list)
	}

	if list[0]["remote_name"] != remoteName {
		t.Errorf("list[0].remote_name: got %q, want %q", list[0]["remote_name"], remoteName)
	}

	if list[0]["url"] != remoteURL {
		t.Errorf("list[0].url: got %q, want %q", list[0]["url"], remoteURL)
	}

	// passphrase round-trips on the typed-array view (the operator
	// posted it, the operator can read it back). A future hardening
	// pass may redact it; pin the current behaviour so the change is
	// intentional.
	if list[0]["passphrase"] != remotePass {
		t.Errorf("list[0].passphrase: got %q, want %q", list[0]["passphrase"], remotePass)
	}

	// --- GET /v1/remotes: envelope view also sees the entry ---
	envResp := httpGet(t, base+"/v1/remotes")
	defer func() { _ = envResp.Body.Close() }()

	var env remoteListEnvelope
	if err := json.NewDecoder(envResp.Body).Decode(&env); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}

	if len(env.LinstorRemotes) != 1 || env.LinstorRemotes[0].RemoteName != remoteName {
		t.Errorf("envelope linstor_remotes: got %+v, want one entry named %q",
			env.LinstorRemotes, remoteName)
	}

	// --- DELETE: remove via query param ---
	delResp := httpDelete(t, base+"/v1/remotes?remote_name="+remoteName)
	defer func() { _ = delResp.Body.Close() }()

	if delResp.StatusCode != http.StatusOK {
		t.Fatalf("DELETE status: got %d, want 200", delResp.StatusCode)
	}

	// --- Final GET: list empty again ---
	finalResp := httpGet(t, base+"/v1/remotes/linstor")
	defer func() { _ = finalResp.Body.Close() }()

	var finalList []map[string]string
	if err := json.NewDecoder(finalResp.Body).Decode(&finalList); err != nil {
		t.Fatalf("decode final list: %v", err)
	}

	if len(finalList) != 0 {
		t.Errorf("post-delete list: got %+v, want []", finalList)
	}
}

// TestLinstorRemoteCreateValidatesRequiredFields: POSTing a body
// missing `remote_name` or `url` produces 400 with an
// upstream-shaped ApiCallRc message — pinned so a future refactor
// that drops the validation doesn't slip a half-formed entry past
// the JSON decoder onto the registry.
func TestLinstorRemoteCreateValidatesRequiredFields(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	for _, tc := range []struct {
		name     string
		body     map[string]string
		wantMsg  string
		wantCode int
	}{
		{
			name:     "missing remote_name",
			body:     map[string]string{"url": "http://x"},
			wantMsg:  "remote_name is required",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing url",
			body:     map[string]string{"remote_name": "r1"},
			wantMsg:  "url is required",
			wantCode: http.StatusBadRequest,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body, _ := json.Marshal(tc.body)

			resp := httpPost(t, base+"/v1/remotes/linstor", body)
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != tc.wantCode {
				t.Fatalf("status: got %d, want %d", resp.StatusCode, tc.wantCode)
			}

			var rcs []apiv1.APICallRc
			if err := json.NewDecoder(resp.Body).Decode(&rcs); err != nil {
				t.Fatalf("decode ApiCallRc: %v", err)
			}

			if len(rcs) != 1 {
				t.Fatalf("ApiCallRc len: got %d, want 1", len(rcs))
			}

			if !strings.Contains(rcs[0].Message, tc.wantMsg) {
				t.Errorf("message: got %q, want substring %q", rcs[0].Message, tc.wantMsg)
			}
		})
	}
}

// TestLinstorRemoteDeleteMissingReturns200Warning pins the wire shape
// for "delete unknown remote" (Bug 66): 200 + ApiCallRc envelope with
// the warn-mask bit set. Previously asserted 404, but the in-memory
// remote registry is wiped on every controller restart, so retries
// against a fresh controller would crash the python CLI's XML decoder
// fallback. The warn-mask distinguishes the no-op replay from a real
// drop so downstream tooling can still detect the dangling reference
// if it wants — but the HTTP layer stays exit-0.
func TestLinstorRemoteDeleteMissingReturns200Warning(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpDelete(t, base+"/v1/remotes?remote_name=ghost")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var rc []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rc); err != nil {
		t.Fatalf("decode ApiCallRc envelope: %v", err)
	}

	if len(rc) == 0 {
		t.Fatalf("ApiCallRc envelope: got empty, want one entry")
	}

	if rc[0].RetCode&maskWarn == 0 {
		t.Errorf("ret_code: got %#x, want WARN bit (%#x) set", rc[0].RetCode, maskWarn)
	}

	if !strings.Contains(rc[0].Message, "already absent") || !strings.Contains(rc[0].Message, "ghost") {
		t.Errorf("message: got %q, want 'already absent' + 'ghost'", rc[0].Message)
	}
}

// TestLinstorRemoteShipReturns501WithText is the core spec pin for
// scenario 4.17: POST /v1/remotes/{remote}/backups/ship MUST return
// 501 (Not Implemented) with a body that names the supported
// in-cluster alternative (`snapshot-restore-resource`). golinstor
// turns 501 into `ApiCallError` and surfaces the `message` field
// verbatim — that's what the operator sees on the CLI.
//
// The text MUST mention "snapshot-restore-resource" so an operator
// searching for the fallback can find it without grep-ing source.
// Without this pin a future refactor returning a bare 501 would
// drop the actionable hint and force the operator into the bug
// tracker.
func TestLinstorRemoteShipReturns501WithText(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	// Pre-create a remote so the URL is plausibly addressable. The
	// 501 must fire regardless of whether the remote exists — the
	// feature is missing on the satellite side, not on the routing
	// layer — but exercising the realistic call path catches a
	// future regression where someone wires the registry check
	// before the not-implemented return and accidentally turns the
	// 501 into a 404.
	createBody, _ := json.Marshal(map[string]string{
		"remote_name": "peer-east",
		"url":         "http://peer.example:3370",
	})
	createResp := httpPost(t, base+"/v1/remotes/linstor", createBody)
	_ = createResp.Body.Close()

	shipBody, _ := json.Marshal(map[string]any{
		"src_rsc_name":  "pvc-source",
		"dst_rsc_name":  "pvc-target",
		"src_snap_name": "snap-1",
	})

	resp := httpPost(t, base+"/v1/remotes/peer-east/backups/ship", shipBody)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusNotImplemented {
		t.Fatalf("status: got %d, want 501", resp.StatusCode)
	}

	var rcs []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rcs); err != nil {
		t.Fatalf("decode ApiCallRc: %v", err)
	}

	if len(rcs) != 1 {
		t.Fatalf("ApiCallRc len: got %d, want 1; got=%+v", len(rcs), rcs)
	}

	// Hard-pinned substrings — change here means the operator-facing
	// hint changed. That's intentional; require an explicit test
	// edit when it does.
	for _, want := range []string{
		"linstor-remote ship not implemented",
		"snapshot-restore-resource",
	} {
		if !strings.Contains(rcs[0].Message, want) {
			t.Errorf("ship 501 message %q is missing required substring %q",
				rcs[0].Message, want)
		}
	}
}
