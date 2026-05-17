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

// Bug 239 (P3): RD-clone `src_snap_name` silently dropped.
//
// Bug 232 added `src_snap_name` to `rdCloneRequest` so the decoder
// would stop 400'ing on the 1.27-era CLI's snapshot-based clone
// path. The field was accepted-and-no-op: `cloneEmptyRDShell`
// shallow-copied the source RD shell without consulting the
// snapshot. Operators asking for `clone --src-snap-name X` got a
// fresh empty shell with no error — the "snap" intent vanished.
//
// Production impact is bounded by Bug 114's existing 501 for
// VD-bearing sources, so the snapshot path can never silently
// corrupt data via this route — but it CAN silently mis-shape the
// clone (e.g. an operator scripting a workflow expects the snap to
// land and continues to the next step). The honest answer is an
// explicit 501 + envelope when `src_snap_name != ""`, so the
// operator either learns the gap immediately or falls back to a
// different workflow.
//
// We mirror the Bug 114 envelope shape exactly:
// `ResourceDefinitionCloneStarted` object with the operator-
// actionable message inside `messages` — NOT a bare `[]ApiCallRc`
// (the python CLI's `resource_dfn_clone` decodes the body straight
// into CloneStarted and crashes on a list).

// TestBug239RDCloneSrcSnapNameReturns501 pins the corrected
// behaviour: a `src_snap_name`-bearing clone POST surfaces HTTP 501
// + the CloneStarted-envelope refusal so the operator sees the gap
// instead of a silent empty-shell clone.
//
// Pre-fix: Bug 232's accept-and-no-op returns HTTP 201 +
// CloneStarted success envelope, the test fails on the status code
// AND on the absence of the "snapshot-based clone" wording in the
// body.
func TestBug239RDCloneSrcSnapNameReturns501(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: "rd-src",
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body := []byte(`{"name":"rd-clone","src_snap_name":"snap-1"}`)

	resp := httpPost(t, base+"/v1/resource-definitions/rd-src/clone", body)
	respBody, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	if resp.StatusCode != http.StatusNotImplemented {
		t.Fatalf("status: got %d, want 501 (snapshot-based clone must be explicit 501, NOT silently drop the src_snap_name)\nbody=%s",
			resp.StatusCode, string(respBody))
	}

	// Body MUST be the CloneStarted envelope shape (object with
	// `messages` array), not a bare []ApiCallRc — the python CLI
	// crashes with `'list' object has no attribute 'get'` on the
	// latter (see writeCloneNotImplemented docstring).
	var env struct {
		Location   string             `json:"location"`
		SourceName string             `json:"source_name"`
		CloneName  string             `json:"clone_name"`
		Messages   *[]apiv1.APICallRc `json:"messages,omitempty"`
	}

	err = json.Unmarshal(respBody, &env)
	if err != nil {
		t.Fatalf("response is not a CloneStarted object — python CLI will crash decoding: %v (body=%s)",
			err, string(respBody))
	}

	if env.SourceName != "rd-src" {
		t.Errorf("source_name: got %q, want %q", env.SourceName, "rd-src")
	}

	if env.CloneName != "rd-clone" {
		t.Errorf("clone_name: got %q, want %q", env.CloneName, "rd-clone")
	}

	if env.Messages == nil || len(*env.Messages) == 0 {
		t.Fatalf("messages missing — operator gets no actionable error: body=%s", string(respBody))
	}

	msg := (*env.Messages)[0].Message
	if !strings.Contains(strings.ToLower(msg), "snapshot") {
		t.Errorf("envelope message %q must mention snapshot so the operator understands the gap", msg)
	}

	// And — critically — the clone RD MUST NOT have been created.
	// The whole point of the 501 is "don't pretend we did the work".
	_, err = st.ResourceDefinitions().Get(ctx, "rd-clone")
	if err == nil {
		t.Errorf("rd-clone was created despite the 501 — operator now has a phantom empty shell that lies about the snapshot")
	}
}

// TestBug239RDCloneWithoutSrcSnapNameStillWorks pins that the
// non-snapshot clone path (empty `src_snap_name`) is unchanged by
// the 239 fix. Bug 232's override_props / delete_props wire-through
// is preserved, the empty-VD clone shell still lands, and the
// CloneStarted success envelope still flows.
//
// Without this guardrail a too-eager fix could regress the
// already-shipped empty-shell clone path that Group D's smoke test
// depends on.
func TestBug239RDCloneWithoutSrcSnapNameStillWorks(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: "rd-src",
		Props: map[string]string{
			"keep-me": "yes",
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body := []byte(`{"name":"rd-clone"}`)

	resp := httpPost(t, base+"/v1/resource-definitions/rd-src/clone", body)
	respBody, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("non-snapshot clone must still return 201; got %d (body=%s)", resp.StatusCode, respBody)
	}

	got, err := st.ResourceDefinitions().Get(ctx, "rd-clone")
	if err != nil {
		t.Fatalf("non-snapshot clone RD missing: %v", err)
	}

	if got.Props["keep-me"] != "yes" {
		t.Errorf("Props not copied through non-snapshot path: %v", got.Props)
	}
}
