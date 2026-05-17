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

// Bug 232: python-linstor 1.27.0's `_require_version()` gates opened
// after Bug 222 bumped the wire-advertised `rest_api_version` from
// 1.23.0 to 1.27.0. The CLI now sends fields that
// DisallowUnknownFields (Bug 158/161) decoders rejected as
// 400 + "unknown field" envelopes:
//
//   - PUT /v1/nodes/{node}/evacuate          → target / do_not_target
//   - POST /v1/nodes/evacuate                → target / do_not_target
//   - PUT /v1/resource-definitions/{rd}      → dst_rsc_grp / force_mv_rsc_grp
//   - POST /v1/resource-definitions/{rd}/clone
//       → override_props / delete_props / src_snap_name
//
// The contract these tests pin is "the decoder accepts the field
// without 400". Whether the apiserver wires the field through to
// real behaviour (autoplace narrowing, cross-RG move with rebalance,
// snapshot-based clone) is a follow-up — the immediate goal is the
// CLI stops crashing on otherwise-valid commands.

// TestBug232NodeEvacuateSingleAcceptsTargetFields pins that the
// PUT single-node evacuate handler accepts `target` and
// `do_not_target` arrays in the body. python-linstor 1.27.0's
// `node_evacuate(target=..., do_not_target=...)` PUTs to this path
// after the Bug 222 version-string bump opens the `_require_version`
// gate; without this acceptance the CLI's evacuate-with-target form
// can never reach the handler if a future decode lands on the path.
func TestBug232NodeEvacuateSingleAcceptsTargetFields(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n1"}); err != nil {
		t.Fatalf("seed n1: %v", err)
	}

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n2"}); err != nil {
		t.Fatalf("seed n2: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body := []byte(`{"target":["n2"]}`)

	resp := httpPut(t, base+"/v1/nodes/n1/evacuate", body)
	respBody, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	if resp.StatusCode == http.StatusBadRequest {
		t.Fatalf("evacuate with target should not 400; got 400 body=%s", respBody)
	}

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200 (body=%s)", resp.StatusCode, respBody)
	}

	// do_not_target variant — python-linstor disallows mixing the two
	// client-side, but the wire shape must accept either independently.
	body = []byte(`{"do_not_target":["n2"]}`)

	resp = httpPut(t, base+"/v1/nodes/n1/evacuate", body)
	respBody, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	if resp.StatusCode == http.StatusBadRequest {
		t.Fatalf("evacuate with do_not_target should not 400; got 400 body=%s", respBody)
	}

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200 (body=%s)", resp.StatusCode, respBody)
	}
}

// TestBug232NodeEvacuateMultiAcceptsTargetFields pins the same
// acceptance on the variadic-evacuate POST. The multi-node handler
// DOES decode through decodeJSON today, so a python-linstor body
// with target/do_not_target sneaking into the same path returns
// 400 + "unknown field" without this fix. The decoder must
// silently absorb the extra fields and route to the existing
// multi-evacuate path.
func TestBug232NodeEvacuateMultiAcceptsTargetFields(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n1"}); err != nil {
		t.Fatalf("seed n1: %v", err)
	}

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n2"}); err != nil {
		t.Fatalf("seed n2: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body := []byte(`{"nodes":["n1"],"target":["n2"],"do_not_target":["n3"]}`)

	resp := httpPost(t, base+"/v1/nodes/evacuate", body)
	respBody, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	if resp.StatusCode == http.StatusBadRequest {
		t.Fatalf("multi-evacuate with target/do_not_target should not 400; got 400 body=%s", respBody)
	}

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200 (body=%s)", resp.StatusCode, respBody)
	}
}

// TestBug232RDModifyAcceptsDstRscGrp pins that the RD-modify
// PUT handler accepts the `dst_rsc_grp` + `force_mv_rsc_grp`
// fields python-linstor sends on `rd modify --resource-group`
// after the 1.27 version-string bump. Pre-fix the
// DisallowUnknownFields decode returns 400 + "json: unknown field
// \"dst_rsc_grp\"", and the CLI crashes on the malformed envelope.
//
// Wiring contract: `dst_rsc_grp` non-empty MUST update the RD's
// `ResourceGroupName` (the existing `resource_group` alias path),
// `force_mv_rsc_grp` is accepted-and-no-op until the rebalance
// orchestration lands in a follow-up.
func TestBug232RDModifyAcceptsDstRscGrp(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:              "rd1",
		ResourceGroupName: "rg-old",
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body := []byte(`{"dst_rsc_grp":"rg-new","force_mv_rsc_grp":true}`)

	resp := httpPut(t, base+"/v1/resource-definitions/rd1", body)
	respBody, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	if resp.StatusCode == http.StatusBadRequest {
		t.Fatalf("rd modify with dst_rsc_grp/force_mv_rsc_grp should not 400; got 400 body=%s", respBody)
	}

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200 (body=%s)", resp.StatusCode, respBody)
	}

	got, err := st.ResourceDefinitions().Get(ctx, "rd1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if got.ResourceGroupName != "rg-new" {
		t.Errorf("ResourceGroupName: got %q, want %q (dst_rsc_grp should drive the move)",
			got.ResourceGroupName, "rg-new")
	}
}

// TestBug232RDCloneAcceptsOverrideDeleteProps pins that the RD-clone
// POST handler accepts the two non-snapshot 1.27 fields python-linstor
// sends on `linstor resource-definition clone --override-prop`
// / `--delete-prop`:
//
//   - override_props (map[string]string) — apply over the cloned RD's Props
//   - delete_props ([]string)            — remove from the cloned RD's Props
//
// Pre-fix DisallowUnknownFields returns 400 + "json: unknown field
// \"override_props\"" and the CLI crashes before any clone can land.
//
// Bug 239 supersession: `src_snap_name` USED to ride along in the
// original Bug 232 fixture as accept-and-no-op, but that turned out
// to be a silent-drop trap (operator asks for snapshot clone, gets
// a non-snapshot empty shell). Bug 239 now refuses src_snap_name
// with an explicit 501 — see TestBug239RDCloneSrcSnapNameReturns501
// for that contract. This Bug 232 fixture is the override/delete-
// only happy path; src_snap_name is exercised separately.
func TestBug232RDCloneAcceptsOverrideDeleteProps(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: "rd-src",
		Props: map[string]string{
			"keep-me":     "yes",
			"drop-me":     "remove",
			"override-me": "old",
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// NB: src_snap_name deliberately absent — Bug 239 now refuses it
	// with 501; the override/delete contract pinned here is the live-
	// RD-shell clone path, unchanged.
	body, err := json.Marshal(map[string]any{
		"name":           "rd-clone",
		"override_props": map[string]string{"override-me": "new", "extra": "added"},
		"delete_props":   []string{"drop-me"},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-definitions/rd-src/clone", body)
	respBody, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	if resp.StatusCode == http.StatusBadRequest {
		t.Fatalf("rd clone with override_props/delete_props should not 400; got 400 body=%s",
			respBody)
	}

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201 (body=%s)", resp.StatusCode, respBody)
	}

	// override_props + delete_props MUST be wired through to the
	// cloned RD's Props — pre-fix this branch was unreachable because
	// the decode 400'd before the handler ran.
	got, err := st.ResourceDefinitions().Get(ctx, "rd-clone")
	if err != nil {
		t.Fatalf("get clone: %v", err)
	}

	if got.Props["override-me"] != "new" {
		t.Errorf("Props[override-me]: got %q, want %q (override_props should apply)",
			got.Props["override-me"], "new")
	}

	if got.Props["extra"] != "added" {
		t.Errorf("Props[extra]: got %q, want %q (override_props should add new keys)",
			got.Props["extra"], "added")
	}

	if _, ok := got.Props["drop-me"]; ok {
		t.Errorf("Props[drop-me] still present: %v (delete_props should remove it)", got.Props)
	}

	if got.Props["keep-me"] != "yes" {
		t.Errorf("Props[keep-me]: got %q, want %q (untouched keys must survive)",
			got.Props["keep-me"], "yes")
	}
}

// TestBug232RDCloneSrcSnapNameDecodes pins that the
// `src_snap_name` JSON tag still resolves on the wire — even though
// Bug 239 now refuses the request semantically (501), the decoder
// MUST still parse the field, not 400 on it. Without this guard a
// future refactor could remove the field from rdCloneRequest and
// the decoder would slip back to 400 + "unknown field" instead of
// the operator-actionable 501 Bug 239 surfaces.
//
// The contract: `src_snap_name` decodes (no 400) AND surfaces 501
// (the Bug 239 refusal). The detailed envelope-shape assertion lives
// in TestBug239RDCloneSrcSnapNameReturns501 — here we just want to
// know the field name is wired into the struct.
func TestBug232RDCloneSrcSnapNameDecodes(t *testing.T) {
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

	if resp.StatusCode == http.StatusBadRequest {
		t.Fatalf("rd clone with src_snap_name should not 400 (Bug 232 decode contract); got 400 body=%s",
			respBody)
	}

	// Bug 239 supersession: this body MUST now surface 501. The
	// detailed envelope assertion lives in the Bug 239 test; here
	// we only need to know the decoder doesn't 400.
	if resp.StatusCode != http.StatusNotImplemented {
		t.Fatalf("status: got %d, want 501 (Bug 239 supersedes Bug 232's accept-and-no-op for src_snap_name; body=%s)",
			resp.StatusCode, string(respBody))
	}

	// Sanity: the response is the CloneStarted envelope (Bug 239's
	// shape) — proves the decoder routed through the snapshot-clone
	// 501 branch rather than some unrelated 501.
	if !strings.Contains(string(respBody), `"clone_name":"rd-clone"`) {
		t.Errorf("response missing clone_name=rd-clone in CloneStarted envelope: %s", respBody)
	}
}
