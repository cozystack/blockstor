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
	"net/http"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Bug 111 — `linstor n lost <node>` deleted a live, online node with
// Resource CRDs still referencing it. Upstream LINSTOR documents
// `lost` as the "satellite is unreachable, force-cleanup" escape
// hatch — running it against an online satellite orphans the
// resources, leaves the DRBD kernel state on the host, and unregisters
// a still-running satellite. The handler must refuse by default when
// the satellite is `ONLINE` and/or any Resource CRD references the
// node, and accept `?force=true` for the rare case where the operator
// truly wants to override (mirrors the Bug 92 pattern on `n d`).

// TestBug111NodeLostRefusedWhenSatelliteOnline pins refusal when the
// node's ConnectionStatus is ONLINE — the satellite is still
// reporting, so `n lost` is the wrong tool. Operator should use
// `n d` (clean delete) or `n evacuate` (drain first). 409 + envelope.
func TestBug111NodeLostRefusedWhenSatelliteOnline(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n1"}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	if err := st.Nodes().SetConnectionStatus(ctx, "n1", apiv1.NodeTypeOnline); err != nil {
		t.Fatalf("seed connection status: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/nodes/n1/lost", nil)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status: got %d, want 409", resp.StatusCode)
	}

	var rcs []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rcs); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}

	if len(rcs) == 0 {
		t.Fatalf("envelope: got empty, want one entry")
	}

	// FAIL_IN_USE sub-code 997, mask error bit set — same shape as
	// Bug 92's `n d` refusal so the Python CLI surfaces the line as
	// an error and operator-side audit-log filters classify it
	// alongside other "in-use" refusals.
	if rcs[0].RetCode&apiCallRcError == 0 {
		t.Errorf("ret_code: got %#x, want MASK_ERROR bit set", rcs[0].RetCode)
	}

	if rcs[0].RetCode&0xFFFF != apiCallRcFailInUse {
		t.Errorf("ret_code sub-code: got %d, want FAIL_IN_USE (%d)",
			rcs[0].RetCode&0xFFFF, apiCallRcFailInUse)
	}

	low := strings.ToLower(rcs[0].Message + " " + rcs[0].Cause + " " + rcs[0].Correc)
	if !strings.Contains(low, "online") && !strings.Contains(low, "heartbeat") {
		t.Errorf("envelope: missing online/heartbeat marker; got message=%q cause=%q correction=%q",
			rcs[0].Message, rcs[0].Cause, rcs[0].Correc)
	}

	if !strings.Contains(rcs[0].Correc, "force=true") {
		t.Errorf("correction: missing force=true escape hatch; got %q", rcs[0].Correc)
	}

	// The node MUST still exist — a refused operation that nevertheless
	// dropped the Node row would leave the cluster in a half-deleted
	// state and breaks the retry contract operators rely on.
	if _, err := st.Nodes().Get(ctx, "n1"); err != nil {
		t.Errorf("node removed despite 409 refusal: %v", err)
	}
}

// TestBug111NodeLostRefusedWhenResourcesExist pins the operator-
// poke-report repro shape: satellite is ONLINE and Resource CRDs
// reference the node. The refusal envelope MUST surface the
// referencing Resource names so the operator can target them with
// `r d` before retrying. An OFFLINE satellite with Resources is
// the documented `n lost` use case and is covered by
// TestNodeLostCascadeDeletesResources — the Bug 111 gate is
// specifically the live-satellite footgun.
func TestBug111NodeLostRefusedWhenResourcesExist(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n1"}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	if err := st.Nodes().SetConnectionStatus(ctx, "n1", apiv1.NodeTypeOnline); err != nil {
		t.Fatalf("seed connection status: %v", err)
	}

	for _, rd := range []string{"rd-a", "rd-b"} {
		if err := st.Resources().Create(ctx, &apiv1.Resource{
			Name:     rd,
			NodeName: "n1",
		}); err != nil {
			t.Fatalf("seed resource %s: %v", rd, err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/nodes/n1/lost", nil)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status: got %d, want 409", resp.StatusCode)
	}

	var rcs []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rcs); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}

	if len(rcs) == 0 {
		t.Fatalf("envelope: got empty, want one entry")
	}

	if rcs[0].RetCode&apiCallRcError == 0 {
		t.Errorf("ret_code: got %#x, want MASK_ERROR bit set", rcs[0].RetCode)
	}

	if rcs[0].RetCode&0xFFFF != apiCallRcFailInUse {
		t.Errorf("ret_code sub-code: got %d, want FAIL_IN_USE (%d)",
			rcs[0].RetCode&0xFFFF, apiCallRcFailInUse)
	}

	// Operator must see WHICH resources are blocking so they can
	// target them with `r d` before retrying `n lost --force`.
	cause := rcs[0].Cause
	if !strings.Contains(cause, "rd-a") || !strings.Contains(cause, "rd-b") {
		t.Errorf("cause: got %q, want it to name rd-a + rd-b", cause)
	}

	if !strings.Contains(rcs[0].Correc, "force=true") {
		t.Errorf("correction: missing force=true escape hatch; got %q", rcs[0].Correc)
	}

	// Node and Resource CRDs must all survive the refusal — a half-
	// applied cascade would brick the retry path.
	if _, err := st.Nodes().Get(ctx, "n1"); err != nil {
		t.Errorf("node removed despite 409 refusal: %v", err)
	}

	for _, rd := range []string{"rd-a", "rd-b"} {
		if _, err := st.Resources().Get(ctx, rd, "n1"); err != nil {
			t.Errorf("resource %s removed despite 409 refusal: %v", rd, err)
		}
	}
}

// TestBug111NodeLostAllowsOfflineWithResources pins the inverse:
// when the satellite is OFFLINE (truly unreachable) but Resource
// CRDs still reference it, the cascade MUST proceed without
// requiring `?force=true`. That is the documented `n lost` use
// case — clean up orphans of a permanently dead satellite.
// Cross-references TestNodeLostCascadeDeletesResources which pins
// the cascade behaviour; this test pins that the Bug 111 gate
// does not over-trigger on the legitimate path.
func TestBug111NodeLostAllowsOfflineWithResources(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n1"}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	if err := st.Nodes().SetConnectionStatus(ctx, "n1", apiv1.NodeTypeOffline); err != nil {
		t.Fatalf("seed connection status: %v", err)
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name:     "rd-orphan",
		NodeName: "n1",
	}); err != nil {
		t.Fatalf("seed resource: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/nodes/n1/lost", nil)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200 (offline satellite is the documented `n lost` path)",
			resp.StatusCode)
	}

	// Cascade fires — orphan resource swept on the dead satellite's behalf.
	if _, err := st.Resources().Get(ctx, "rd-orphan", "n1"); err == nil {
		t.Errorf("orphan resource not cascaded on offline-satellite `n lost`")
	}
}

// TestBug111NodeLostAllowsForceTrue pins the escape hatch: an
// operator who explicitly accepts the orphan-cascade-on-online-node
// risk (rare disaster-recovery flow — "the satellite is wedged but
// reporting heartbeats, I need it gone NOW") can opt in via
// ?force=true. Mirrors the Bug 92 `n d --force` pattern and
// handleNodeEvacuate's `?force=true` semantics.
func TestBug111NodeLostAllowsForceTrue(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n1"}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	if err := st.Nodes().SetConnectionStatus(ctx, "n1", apiv1.NodeTypeOnline); err != nil {
		t.Fatalf("seed connection status: %v", err)
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name:     "rd-a",
		NodeName: "n1",
	}); err != nil {
		t.Fatalf("seed resource: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/nodes/n1/lost?force=true", nil)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200 (force bypass)", resp.StatusCode)
	}

	// Cascade should have fired — node gone, resource gone.
	if _, err := st.Nodes().Get(ctx, "n1"); err == nil {
		t.Errorf("node still present after force=true `n lost`")
	}

	if _, err := st.Resources().Get(ctx, "rd-a", "n1"); err == nil {
		t.Errorf("resource still present after force=true `n lost` cascade")
	}
}

// TestBug111NodeLostHappyPath pins the legitimate `n lost` use
// case: satellite is offline AND no Resources reference the node.
// This is the scenario the upstream LINSTOR `n lost` is documented
// for ("the node is permanently dead"). Pre-Bug-111 the gate didn't
// exist, so any input passed through; after the fix this remains
// the one path that should succeed without `?force=true`.
func TestBug111NodeLostHappyPath(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n1"}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	// Satellite explicitly OFFLINE — the documented `n lost` precondition.
	if err := st.Nodes().SetConnectionStatus(ctx, "n1", apiv1.NodeTypeOffline); err != nil {
		t.Fatalf("seed connection status: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/nodes/n1/lost", nil)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200 (legitimate lost path)", resp.StatusCode)
	}

	if _, err := st.Nodes().Get(ctx, "n1"); err == nil {
		t.Errorf("node still present after legitimate `n lost`")
	}
}
