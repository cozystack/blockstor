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

// TestBug141SchedulePOSTReturns501Envelope pins Bug 141: Bug 100 wired
// `GET /v1/schedules` to return `{"data": []}` so `linstor schedule
// list` no longer crashed the python CLI on XML fallback decode. But
// `POST /v1/schedules` and `DELETE /v1/schedules/{name}` stayed unwired
// — they fell through to the Bug 109 405 / Bug 103 404 catch-all
// envelope, which is structurally correct but surfaces a generic
// "method not allowed" / "endpoint not implemented" message. Operators
// running `linstor schedule create` saw a non-actionable error.
//
// The fix mirrors Bug 127's `sos-report create / download` pattern:
// register dedicated handlers that emit a canned 501 envelope naming
// the verb ("schedule create not yet implemented" etc.) so the CLI
// surfaces a typed ERROR line that explicitly tells the operator the
// half-implemented feature gap.
func TestBug141SchedulePOSTReturns501Envelope(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	body := []byte(`{"schedule_name":"sched1","full_cron":"0 * * * *"}`)

	resp := httpPost(t, base+"/v1/schedules", body)
	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	if resp.StatusCode != http.StatusNotImplemented {
		t.Errorf("status: got %d, want 501 (matches sos-report create/download canned envelope)",
			resp.StatusCode)
	}

	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("Content-Type: got %q, want application/json", ct)
	}

	var rc []apiv1.APICallRc

	if err := json.Unmarshal(raw, &rc); err != nil {
		t.Fatalf("decode []ApiCallRc envelope: %v\nbody: %s", err, raw)
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

	if !strings.Contains(rc[0].Message, "create") {
		t.Errorf("message %q does not mention the `create` verb", rc[0].Message)
	}
}

// TestBug141ScheduleDELETEReturns501Envelope is the DELETE half of Bug
// 141. `linstor schedule delete <name>` posts a DELETE to
// /v1/schedules/{name}; the canned envelope must name the `delete`
// verb so the CLI's ERROR line is distinguishable from the create one.
func TestBug141ScheduleDELETEReturns501Envelope(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpDelete(t, base+"/v1/schedules/sched1")
	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(resp.Body)
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

	if err := json.Unmarshal(raw, &rc); err != nil {
		t.Fatalf("decode []ApiCallRc envelope: %v\nbody: %s", err, raw)
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

	if !strings.Contains(rc[0].Message, "delete") {
		t.Errorf("message %q does not mention the `delete` verb", rc[0].Message)
	}
}

// TestBug141ScheduleModifyReturns501Envelope is the PUT/modify half of
// Bug 141. `linstor schedule modify <name>` posts a PUT to
// /v1/schedules/{name}; the canned envelope must name the `modify`
// verb so all three write verbs surface a consistent typed ERROR.
func TestBug141ScheduleModifyReturns501Envelope(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	body := []byte(`{"full_cron":"30 * * * *"}`)

	resp := httpPut(t, base+"/v1/schedules/sched1", body)
	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	if resp.StatusCode != http.StatusNotImplemented {
		t.Errorf("status: got %d, want 501", resp.StatusCode)
	}

	var rc []apiv1.APICallRc

	if err := json.Unmarshal(raw, &rc); err != nil {
		t.Fatalf("decode []ApiCallRc envelope: %v\nbody: %s", err, raw)
	}

	if len(rc) == 0 {
		t.Fatalf("empty envelope")
	}

	if rc[0].RetCode >= 0 {
		t.Errorf("ret_code = %d, want negative (MASK_ERROR)", rc[0].RetCode)
	}

	if !strings.Contains(rc[0].Message, "modify") {
		t.Errorf("message %q does not mention the `modify` verb", rc[0].Message)
	}
}

// TestBug141ScheduleGETStillReturnsEmptyList is the regression guard
// for Bug 100. While adding the POST / DELETE / PUT handlers we must
// not regress the already-canned `GET /v1/schedules` -> `{"data": []}`
// reply that python-linstor needs to decode `ScheduleListResponse`
// without crashing.
func TestBug141ScheduleGETStillReturnsEmptyList(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpGet(t, base+"/v1/schedules")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200 (Bug 100 regression)", resp.StatusCode)
	}

	var body struct {
		Data []map[string]any `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode ScheduleListResponse: %v", err)
	}

	if body.Data == nil {
		t.Fatalf("expected `data: []`, got nil — ScheduleListResponse would crash")
	}

	if len(body.Data) != 0 {
		t.Errorf("expected empty schedule list, got %d entries", len(body.Data))
	}
}

// TestBug143RLPBogusRDReturns404Envelope pins Bug 143: `linstor r lp
// <bogus-rd> <bogus-node>` used to surface a generic LINSTOR-style
// error string ("No property map found") instead of a typed 404 +
// envelope naming the missing object.
//
// The CLI reads `r lp` from `GET /v1/resource-definitions/{rd}/
// resources/{node}` and consumes the response's `props` field. When
// the RD doesn't exist at all, the handler must 404 with an envelope
// that explicitly names the missing resource definition — same pattern
// as Bug 94 (unknown node on r c), Bug 118 (unknown pool on r c).
func TestBug143RLPBogusRDReturns404Envelope(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpGet(t, base+"/v1/resource-definitions/bogus-rd/resources/bogus-node")
	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404 (bogus RD must be reported as missing)",
			resp.StatusCode)
	}

	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("Content-Type: got %q, want application/json", ct)
	}

	var rc []apiv1.APICallRc

	if err := json.Unmarshal(raw, &rc); err != nil {
		t.Fatalf("decode []ApiCallRc envelope: %v\nbody: %s", err, raw)
	}

	if len(rc) == 0 {
		t.Fatalf("empty envelope — python-linstor crashes on replies[0]")
	}

	if rc[0].RetCode >= 0 {
		t.Errorf("ret_code = %d, want negative (MASK_ERROR)", rc[0].RetCode)
	}

	// Envelope must name the missing RD so the operator's eye lands on
	// the right typo. A generic "resource X on node Y not found" hides
	// the precondition failure under a derived statement.
	if !strings.Contains(rc[0].Message, "bogus-rd") {
		t.Errorf("envelope message %q does not name the missing RD `bogus-rd`",
			rc[0].Message)
	}

	if !strings.Contains(rc[0].Message, "resource definition") {
		t.Errorf("envelope message %q does not label the missing object as a `resource definition`",
			rc[0].Message)
	}
}

// TestBug143RLPKnownRDBogusNodeReturns404Envelope pins the second
// limb of Bug 143: RD exists but the node doesn't. The envelope must
// name the missing node specifically (not the RD) so the operator
// understands the typo is on the node-name half of the command.
func TestBug143RLPKnownRDBogusNodeReturns404Envelope(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "rdok143"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/resource-definitions/rdok143/resources/bogus-node")
	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404 (bogus node must be reported as missing)",
			resp.StatusCode)
	}

	var rc []apiv1.APICallRc

	if err := json.Unmarshal(raw, &rc); err != nil {
		t.Fatalf("decode []ApiCallRc envelope: %v\nbody: %s", err, raw)
	}

	if len(rc) == 0 {
		t.Fatalf("empty envelope")
	}

	if rc[0].RetCode >= 0 {
		t.Errorf("ret_code = %d, want negative (MASK_ERROR)", rc[0].RetCode)
	}

	if !strings.Contains(rc[0].Message, "bogus-node") {
		t.Errorf("envelope message %q does not name the missing node `bogus-node`",
			rc[0].Message)
	}
}
