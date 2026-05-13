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
	"slices"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestNodeEvacuateMarksFlag: POST /v1/nodes/{node}/evacuate adds the
// EVICTED flag to the Node. Replica migration is the reconciler's job;
// the REST endpoint only marks intent.
func TestNodeEvacuateMarksFlag(t *testing.T) {
	st := store.NewInMemory()
	if err := st.Nodes().Create(t.Context(), &apiv1.Node{Name: "n1"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/nodes/n1/evacuate", nil)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.Nodes().Get(t.Context(), "n1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if !slices.Contains(got.Flags, "EVICTED") {
		t.Errorf("expected EVICTED flag; got %v", got.Flags)
	}
}

// TestNodeRestoreClearsFlag: POST /v1/nodes/{node}/restore removes
// the EVICTED flag.
func TestNodeRestoreClearsFlag(t *testing.T) {
	st := store.NewInMemory()
	if err := st.Nodes().Create(t.Context(), &apiv1.Node{
		Name:  "n1",
		Flags: []string{"EVICTED"},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/nodes/n1/restore", nil)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.Nodes().Get(t.Context(), "n1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if slices.Contains(got.Flags, "EVICTED") {
		t.Errorf("EVICTED still present: %v", got.Flags)
	}
}

// TestNodeLostDeletesNode pins the upstream-LINSTOR semantic:
// `controller drop-node` removes the Node entry entirely (not
// "mark with LOST/EVICTED flags"). Orphan Resources are re-placed
// by the reconciler on the next cycle (Phase 6 work). The
// recorder-corpus comparison against the oracle relied on this —
// keeping the old "mark flags" behaviour made the post-Lost
// /v1/nodes list diverge from upstream.
func TestNodeLostDeletesNode(t *testing.T) {
	st := store.NewInMemory()
	if err := st.Nodes().Create(t.Context(), &apiv1.Node{Name: "n1"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/nodes/n1/lost", nil)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	_, err := st.Nodes().Get(t.Context(), "n1")
	if err == nil {
		t.Errorf("expected node to be deleted, but Get succeeded")
	}
}

// TestNodeEvacuateUnknown: 404 if the node doesn't exist.
func TestNodeEvacuateUnknown(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpPost(t, base+"/v1/nodes/ghost/evacuate", nil)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", resp.StatusCode)
	}
}

// TestNodeEvacuateIdempotent: a second POST to /evacuate must not
// duplicate the EVICTED flag — addFlag's slices.Contains branch
// short-circuits on already-set flags. Pinned because operators
// often re-fire evacuate after a controller restart; without
// idempotency the flag list would grow unbounded.
func TestNodeEvacuateIdempotent(t *testing.T) {
	st := store.NewInMemory()
	if err := st.Nodes().Create(t.Context(), &apiv1.Node{
		Name:  "n1",
		Flags: []string{"EVICTED"},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/nodes/n1/evacuate", nil)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	got, _ := st.Nodes().Get(t.Context(), "n1")

	count := 0

	for _, f := range got.Flags {
		if f == "EVICTED" {
			count++
		}
	}

	if count != 1 {
		t.Errorf("EVICTED count after re-evacuate: got %d, want 1; flags=%v", count, got.Flags)
	}
}

// TestNodeRestoreUnknown: 404 if the node doesn't exist on restore.
// Pinned alongside Evacuate's 404 so the entire lifecycle matrix
// has consistent error surfaces — operator scripts that loop
// restore + evacuate calls don't have to special-case which
// endpoint returns what.
func TestNodeRestoreUnknown(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpPost(t, base+"/v1/nodes/ghost/restore", nil)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", resp.StatusCode)
	}
}

// TestNodeLostUnknownIsIdempotent: `lost` on a non-existent node is
// folded into success — matches upstream LINSTOR's "drop-node is
// idempotent" semantic so re-running an operator teardown script
// doesn't fail on an already-cleaned node.
func TestNodeLostUnknownIsIdempotent(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpPost(t, base+"/v1/nodes/ghost/lost", nil)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status: got %d, want 200", resp.StatusCode)
	}
}

// boolPtr returns a pointer to b — sugar for seeding
// ResourceState.InUse, which is intentionally *bool so the satellite
// can distinguish "Primary" / "Secondary" / "not observed yet".
func boolPtr(b bool) *bool { return &b }

// TestNodeEvacuateRefusedWhenInUse pins UG9 §"Evacuating a node":
// the REST endpoint MUST refuse the evacuate when any resource on
// the target node has observed state.in_use=true (Primary, with a
// consumer mounting it). Stamping EVICTED silently in that state
// would let the autoplacer/migrator strand an actively-mounted
// volume — a data-availability hazard the operator must
// consciously accept (via ?force=true).
//
// The 409 + actionable body text matches the precedent set by
// handleRGDelete's RD-cross-check (Bug 11): same status code, same
// "X cannot Y: <N> reason; corrective action" shape, so operator
// scripts can pattern-match the response across lifecycle endpoints.
func TestNodeEvacuateRefusedWhenInUse(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n1"}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	// rd-busy: Primary on n1 (in_use=true) → must refuse.
	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name:     "rd-busy",
		NodeName: "n1",
		State:    apiv1.ResourceState{InUse: boolPtr(true)},
	}); err != nil {
		t.Fatalf("seed busy resource: %v", err)
	}

	// rd-idle: Secondary on n1 (in_use=false) → not a blocker.
	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name:     "rd-idle",
		NodeName: "n1",
		State:    apiv1.ResourceState{InUse: boolPtr(false)},
	}); err != nil {
		t.Fatalf("seed idle resource: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/nodes/n1/evacuate", nil)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status: got %d, want 409", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)

	low := strings.ToLower(string(body))
	if !strings.Contains(low, "cannot evacuate") {
		t.Errorf("body: missing 'cannot evacuate'; got %q", string(body))
	}

	if !strings.Contains(low, "in use") {
		t.Errorf("body: missing 'in use'; got %q", string(body))
	}

	if !strings.Contains(low, "demote or stop") {
		t.Errorf("body: missing 'demote or stop' guidance; got %q", string(body))
	}

	// Operator needs to see WHICH resource is in use, not just the
	// count — otherwise they can't target the right consumer to stop.
	if !strings.Contains(string(body), "rd-busy") {
		t.Errorf("body: missing in-use resource name 'rd-busy'; got %q", string(body))
	}

	// rd-idle is Secondary; surfacing it in the refusal would lie
	// to the operator about which workload is blocking the drain.
	if strings.Contains(string(body), "rd-idle") {
		t.Errorf("body: surfaced non-blocking resource 'rd-idle'; got %q", string(body))
	}

	// EVICTED must NOT be stamped — refused operations don't
	// half-apply, otherwise a retry-loop client races itself.
	got, err := st.Nodes().Get(ctx, "n1")
	if err != nil {
		t.Fatalf("get node: %v", err)
	}

	if slices.Contains(got.Flags, "EVICTED") {
		t.Errorf("EVICTED stamped despite refusal: %v", got.Flags)
	}
}

// TestNodeEvacuateForcedBypassesInUseCheck pins the escape hatch:
// ?force=true MUST stamp EVICTED even when an InUse resource exists
// on the node. Matches the precedent from the Bug 11 fix on
// handleRGDelete (?force=true bypasses the RD-cross-check) and
// mirrors upstream LINSTOR's `--force` flag — an operator who
// understands the data-availability risk can override.
func TestNodeEvacuateForcedBypassesInUseCheck(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n1"}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name:     "rd-busy",
		NodeName: "n1",
		State:    apiv1.ResourceState{InUse: boolPtr(true)},
	}); err != nil {
		t.Fatalf("seed busy resource: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/nodes/n1/evacuate?force=true", nil)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.Nodes().Get(ctx, "n1")
	if err != nil {
		t.Fatalf("get node: %v", err)
	}

	if !slices.Contains(got.Flags, "EVICTED") {
		t.Errorf("EVICTED not stamped despite force=true: %v", got.Flags)
	}
}

// TestNodeEvacuateUnobservedInUseAccepts pins the tri-state
// semantic of ResourceState.InUse: nil means "the satellite hasn't
// reported state for this replica yet", NOT "in use". An operator
// MUST be able to evacuate a freshly-created node before any
// satellite observation has landed — refusing in that case would
// brick the lifecycle on a cold cluster.
//
// Only `*InUse == true` is a refusal; nil and `*InUse == false`
// both pass.
func TestNodeEvacuateUnobservedInUseAccepts(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n1"}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	// InUse is the zero value (nil) — no observation yet.
	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name:     "rd-fresh",
		NodeName: "n1",
	}); err != nil {
		t.Fatalf("seed fresh resource: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/nodes/n1/evacuate", nil)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200 (nil InUse is not a refusal)", resp.StatusCode)
	}

	got, err := st.Nodes().Get(ctx, "n1")
	if err != nil {
		t.Fatalf("get node: %v", err)
	}

	if !slices.Contains(got.Flags, "EVICTED") {
		t.Errorf("EVICTED not stamped with unobserved InUse: %v", got.Flags)
	}
}
