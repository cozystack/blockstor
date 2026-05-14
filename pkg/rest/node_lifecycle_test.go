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
	"encoding/json"
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

// TestNodeRestorePreservesPropsAndPools pins scenario 4.W07 (UG9
// §"Restoring an evacuating node"): a `node restore` un-evicts the
// node WITHOUT touching its other state — storage pools, Props
// (including any operator-set `AutoplaceTarget`), and replicas that
// already migrated to peer nodes during the evacuate window MUST
// stay exactly where they are. The restore endpoint is a flag
// flip, not a full lifecycle reset; auto-balance-back is
// explicitly out-of-scope (UG9 lines 2424-2443).
//
// Pinned because a naive "rebuild Node from name" path would zero
// Props on every restore — which would silently drop the
// AutoplaceTarget override the operator stamped before evacuate
// (4.W06 sequence: prop-then-evacuate), inviting the next
// autoplace cycle to repopulate the just-restored node and undo
// the operator's drain.
func TestNodeRestorePreservesPropsAndPools(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	// Seed: evicted node with operator-set props + a peer node.
	if err := st.Nodes().Create(ctx, &apiv1.Node{
		Name:  "n1",
		Flags: []string{"EVICTED"},
		Props: map[string]string{
			// Mirrors the 4.W06 sequence: operator pinned this before
			// evacuate to keep the autoplacer off the node. Must
			// survive the restore so a follow-up autoplace doesn't
			// immediately repopulate.
			"DrbdOptions/AutoplaceTarget": "false",
			"Aux/operator-note":           "drained-2026-05-14",
		},
	}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "peer"}); err != nil {
		t.Fatalf("seed peer: %v", err)
	}

	// Pool on the evicted node — must survive the restore. A
	// rebuild-from-name path would lose this and brick the next
	// `linstor sp l` against the freshly-restored node.
	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		NodeName:        "n1",
		StoragePoolName: "pool-ssd",
	}); err != nil {
		t.Fatalf("seed pool: %v", err)
	}

	// Replica that migrated to `peer` during the evacuate window.
	// 4.W07 contract: no auto-balance-back. The replica MUST NOT
	// move on restore; it stays on `peer`.
	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name: "rd-moved", NodeName: "peer",
	}); err != nil {
		t.Fatalf("seed migrated replica: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/nodes/n1/restore", nil)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.Nodes().Get(ctx, "n1")
	if err != nil {
		t.Fatalf("get node: %v", err)
	}

	if slices.Contains(got.Flags, "EVICTED") {
		t.Errorf("EVICTED still present after restore: %v", got.Flags)
	}

	// Props survive verbatim — operator's AutoplaceTarget pin and
	// any Aux/* annotations are part of the operator's drain
	// posture, not part of the eviction state machine.
	if got.Props["DrbdOptions/AutoplaceTarget"] != "false" {
		t.Errorf("AutoplaceTarget lost on restore: got %q, want %q",
			got.Props["DrbdOptions/AutoplaceTarget"], "false")
	}

	if got.Props["Aux/operator-note"] != "drained-2026-05-14" {
		t.Errorf("Aux annotation lost on restore: %v", got.Props)
	}

	// Storage pool on the restored node still present.
	pool, err := st.StoragePools().Get(ctx, "n1", "pool-ssd")
	if err != nil {
		t.Errorf("pool dropped by restore: %v", err)
	} else if pool.StoragePoolName != "pool-ssd" {
		t.Errorf("pool mutated by restore: %+v", pool)
	}

	// Migrated replica still on `peer` — restore MUST NOT trigger
	// an auto-balance-back (UG9 4.W07 explicit contract).
	moved, err := st.Resources().Get(ctx, "rd-moved", "peer")
	if err != nil {
		t.Errorf("migrated replica vanished after restore: %v", err)
	} else if moved.NodeName != "peer" {
		t.Errorf("replica auto-balanced back on restore: NodeName=%q", moved.NodeName)
	}

	// And the inverse: no replica was magically materialised back
	// on n1 by the restore handler.
	if _, err := st.Resources().Get(ctx, "rd-moved", "n1"); err == nil {
		t.Errorf("restore re-seeded replica on n1; expected no-op for replica placement")
	}
}

// TestNodeRestoreIdempotent: a second POST to /restore on a node
// that's already un-evicted MUST succeed without flapping any
// other flag. addFlag/removeFlag are pure idempotent set
// operations, but the lifecycle endpoint is the contract the
// operator sees — pinned so a controller-restart-then-retry loop
// doesn't 404 or 5xx on the second pass.
func TestNodeRestoreIdempotent(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{
		Name:  "n1",
		Flags: []string{"SOME_OTHER_FLAG"},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/nodes/n1/restore", nil)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200 (restore on un-evicted node)", resp.StatusCode)
	}

	got, err := st.Nodes().Get(ctx, "n1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	// Pre-existing unrelated flag MUST survive — removeFlag scopes
	// to EVICTED only. A clobbering implementation would corrupt
	// neighbouring flags and (e.g.) accidentally drop LOST on a
	// half-cleaned-up node.
	if !slices.Contains(got.Flags, "SOME_OTHER_FLAG") {
		t.Errorf("unrelated flag clobbered by restore: %v", got.Flags)
	}

	if slices.Contains(got.Flags, "EVICTED") {
		t.Errorf("EVICTED appeared on restore of un-evicted node: %v", got.Flags)
	}
}

// TestNodeLifecyclePUTRoutes pins Bug 78: golinstor and python-linstor
// hit /v1/nodes/{n}/restore, /evacuate, /evict with HTTP PUT (the
// OpenAPI spec method). Without an explicit PUT route the Go-1.22
// ServeMux returns 405 Method Not Allowed with an empty body, and
// python-linstor crashes on `xml.etree.ElementTree.ParseError: syntax
// error: line 1, column 0` because its error path tries to parse the
// empty 405 body as an XML APICallRc envelope.
//
// Pin every PUT verb the upstream contract guarantees so a vendored
// client (legacy script, OpenAPI-generated SDK, the official Python
// CLI) gets a structured JSON response on success rather than a
// 405-with-empty-body that crashes its response parser.
func TestNodeLifecyclePUTRoutes(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{
		Name:  "n1",
		Flags: []string{"EVICTED"},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// PUT /restore — un-evict.
	resp := httpPut(t, base+"/v1/nodes/n1/restore", nil)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT restore: got %d, want 200 (route missing → python-linstor crash)", resp.StatusCode)
	}

	got, err := st.Nodes().Get(ctx, "n1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if slices.Contains(got.Flags, "EVICTED") {
		t.Errorf("PUT restore left EVICTED in place: %v", got.Flags)
	}

	// PUT /evacuate — re-stamp EVICTED. ?force=true bypasses the
	// in-use guard (no resources seeded so the guard would pass
	// anyway, but explicit is safer against future seed changes).
	resp = httpPut(t, base+"/v1/nodes/n1/evacuate?force=true", nil)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT evacuate: got %d, want 200", resp.StatusCode)
	}

	got, err = st.Nodes().Get(ctx, "n1")
	if err != nil {
		t.Fatalf("get after evacuate: %v", err)
	}

	if !slices.Contains(got.Flags, "EVICTED") {
		t.Errorf("PUT evacuate did not stamp EVICTED: %v", got.Flags)
	}

	// PUT /evict — alias of /evacuate (blockstor folds offline-drain
	// into the online-drain handler since ?force already covers the
	// semantic difference upstream LINSTOR splits across two paths).
	resp = httpPut(t, base+"/v1/nodes/n1/evict?force=true", nil)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT evict: got %d, want 200 (alias missing breaks golinstor NodeService.Evict)", resp.StatusCode)
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

// TestNodeLostCascadeDeletesResources pins scenario 4.W04: every
// Resource CRD with Spec.NodeName == lost MUST be removed by the
// REST handler itself — NOT by the satellite finalizer.
//
// The node is irrecoverable by definition (that's what `node lost`
// means; UG9 §"Auto-evict" calls it "aggressive — never run on a
// recoverable node"). The satellite that owns
// SatelliteResourceFinalizer is gone with the node, so a plain
// DeletionTimestamp stamp would hang Resources forever and brick
// the next RD-create that recycles the name/port. The handler
// drives the cascade through the store directly.
//
// Surviving peers on a healthy node are left alone — they're how
// the TieBreaker reconciler maintains quorum after node loss.
func TestNodeLostCascadeDeletesResources(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "lost-node"}); err != nil {
		t.Fatalf("seed lost node: %v", err)
	}

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "peer-node"}); err != nil {
		t.Fatalf("seed peer node: %v", err)
	}

	for _, rd := range []string{"rd-a", "rd-b"} {
		if err := st.Resources().Create(ctx, &apiv1.Resource{
			Name: rd, NodeName: "lost-node",
		}); err != nil {
			t.Fatalf("seed replica on lost node for %s: %v", rd, err)
		}

		if err := st.Resources().Create(ctx, &apiv1.Resource{
			Name: rd, NodeName: "peer-node",
		}); err != nil {
			t.Fatalf("seed replica on peer node for %s: %v", rd, err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/nodes/lost-node/lost", nil)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	for _, rd := range []string{"rd-a", "rd-b"} {
		_, err := st.Resources().Get(ctx, rd, "lost-node")
		if err == nil {
			t.Errorf("replica %s on lost-node still present; expected cascade delete", rd)
		}
	}

	for _, rd := range []string{"rd-a", "rd-b"} {
		_, err := st.Resources().Get(ctx, rd, "peer-node")
		if err != nil {
			t.Errorf("peer replica %s on peer-node missing: %v", rd, err)
		}
	}
}

// TestNodeLostCascadeDeletesStoragePools pins the SP half of
// scenario 4.W04: every StoragePool with NodeName == lost MUST be
// dropped from the store. StoragePools on the lost node can never
// be probed again (no satellite to talk to), so leaving them
// indefinitely makes `linstor sp l` report stale capacity and
// pollutes the autoplacer's free-space ranking. SPs on the
// surviving peer node must NOT be touched.
func TestNodeLostCascadeDeletesStoragePools(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "lost-node"}); err != nil {
		t.Fatalf("seed lost node: %v", err)
	}

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "peer-node"}); err != nil {
		t.Fatalf("seed peer node: %v", err)
	}

	for _, node := range []string{"lost-node", "peer-node"} {
		for _, pool := range []string{"pool-ssd", "pool-hdd"} {
			if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
				NodeName:        node,
				StoragePoolName: pool,
			}); err != nil {
				t.Fatalf("seed SP %s on %s: %v", pool, node, err)
			}
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/nodes/lost-node/lost", nil)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	for _, pool := range []string{"pool-ssd", "pool-hdd"} {
		_, err := st.StoragePools().Get(ctx, "lost-node", pool)
		if err == nil {
			t.Errorf("SP %s on lost-node still present; expected cascade delete", pool)
		}
	}

	for _, pool := range []string{"pool-ssd", "pool-hdd"} {
		_, err := st.StoragePools().Get(ctx, "peer-node", pool)
		if err != nil {
			t.Errorf("peer SP %s on peer-node missing: %v", pool, err)
		}
	}
}

// postEvacuateMulti sugar wraps the `POST /v1/nodes/evacuate` body
// shape — `{"nodes":[...]}` — so individual tests stay focused on
// the assertion under check, not on JSON plumbing.
func postEvacuateMulti(t *testing.T, base string, nodes []string) *http.Response {
	t.Helper()

	body, err := json.Marshal(struct {
		Nodes []string `json:"nodes"`
	}{Nodes: nodes})
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}

	return httpPost(t, base+"/v1/nodes/evacuate", body)
}

// TestNodeEvacuateMultiStampsAll pins scenario 4.W06 (cross-listed
// wave1 4.21): `POST /v1/nodes/evacuate` with `{"nodes":[n1,n2]}`
// MUST stamp EVICTED on every named node in a single call.
// Upstream LINSTOR's `linstor node evacuate worker-3 worker-4`
// (variadic) controller-side picks a sequence that doesn't lose
// redundancy at any point; the REST surface's responsibility is the
// atomic intent stamp — replica migration is the reconciler's job
// (Phase 6). Operators rely on this for rolling-drain windows where
// half the cluster goes offline for maintenance: a single API call
// must mark the whole set so the autoplacer's `AutoplaceTarget=false`
// short-circuit fires consistently against all targets at once.
func TestNodeEvacuateMultiStampsAll(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	for _, n := range []string{"n1", "n2", "n3"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{Name: n}); err != nil {
			t.Fatalf("seed %s: %v", n, err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := postEvacuateMulti(t, base, []string{"n1", "n2"})
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	for _, n := range []string{"n1", "n2"} {
		got, err := st.Nodes().Get(ctx, n)
		if err != nil {
			t.Fatalf("get %s: %v", n, err)
		}

		if !slices.Contains(got.Flags, "EVICTED") {
			t.Errorf("node %s missing EVICTED: %v", n, got.Flags)
		}
	}

	// n3 was not in the body — must NOT be touched.
	got, err := st.Nodes().Get(ctx, "n3")
	if err != nil {
		t.Fatalf("get n3: %v", err)
	}

	if slices.Contains(got.Flags, "EVICTED") {
		t.Errorf("n3 stamped EVICTED despite being outside the request: %v", got.Flags)
	}
}

// TestNodeEvacuateMultiRefusesNoCandidate pins the no-candidate
// guard from scenario 4.W06: refuse if no candidate target exists
// for the evacuating set — i.e. every remaining cluster node is
// either in the evacuating set or already EVICTED. Without this
// guard the autoplacer would silently strand every replica that
// previously lived on the evacuated nodes, because there's nowhere
// to migrate to. The refusal MUST be atomic — NO node receives
// EVICTED — so a retry loop after the operator brings up a fresh
// target node behaves identically to a first attempt.
func TestNodeEvacuateMultiRefusesNoCandidate(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	// Only two nodes in the cluster — evacuating both leaves no
	// candidate target. (n3 absent.)
	for _, n := range []string{"n1", "n2"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{Name: n}); err != nil {
			t.Fatalf("seed %s: %v", n, err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := postEvacuateMulti(t, base, []string{"n1", "n2"})
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status: got %d, want 409", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)

	low := strings.ToLower(string(body))
	if !strings.Contains(low, "no candidate") {
		t.Errorf("body: missing 'no candidate' guidance; got %q", string(body))
	}

	// Atomicity: refused operations don't half-apply.
	for _, n := range []string{"n1", "n2"} {
		got, err := st.Nodes().Get(ctx, n)
		if err != nil {
			t.Fatalf("get %s: %v", n, err)
		}

		if slices.Contains(got.Flags, "EVICTED") {
			t.Errorf("node %s stamped EVICTED despite refusal: %v", n, got.Flags)
		}
	}
}

// TestNodeEvacuateMultiCandidateExcludesAlreadyEvicted pins the
// candidate-set semantic: a node already carrying EVICTED is NOT a
// valid migration target. The reconciler treats EVICTED as
// "AutoplaceTarget=false"; counting it as a candidate would let a
// follow-up evacuate call succeed while leaving nowhere for replicas
// to land. Mirrors UG9 §"Evacuating multiple nodes" — operators
// staging a rolling-drain often pre-evict one node, observe
// migration, then evacuate the next batch; we must refuse the next
// batch if every remaining live node is already evicted.
func TestNodeEvacuateMultiCandidateExcludesAlreadyEvicted(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	// n3 already evicted (prior drain in flight). Only n1+n2 are
	// live candidates; evacuating both leaves no live candidate
	// target even though n3 still exists as a Node row.
	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n1"}); err != nil {
		t.Fatalf("seed n1: %v", err)
	}

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n2"}); err != nil {
		t.Fatalf("seed n2: %v", err)
	}

	if err := st.Nodes().Create(ctx, &apiv1.Node{
		Name:  "n3",
		Flags: []string{"EVICTED"},
	}); err != nil {
		t.Fatalf("seed n3: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := postEvacuateMulti(t, base, []string{"n1", "n2"})
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status: got %d, want 409", resp.StatusCode)
	}

	for _, n := range []string{"n1", "n2"} {
		got, err := st.Nodes().Get(ctx, n)
		if err != nil {
			t.Fatalf("get %s: %v", n, err)
		}

		if slices.Contains(got.Flags, "EVICTED") {
			t.Errorf("node %s stamped EVICTED despite no live candidate: %v", n, got.Flags)
		}
	}
}

// TestNodeEvacuateMultiRefusesInUseAtomic pins that the existing
// per-node InUse guard (UG9 §"Evacuating a node": refuse when any
// resource on the target node is Primary) MUST also apply across
// the multi-node call AND MUST be atomic — if ANY node in the set
// holds an in-use resource, the entire call refuses with 409 and
// NO node receives EVICTED. A partial application would brick the
// healthy nodes into a half-drained state while the operator has
// to chase down whichever consumer is holding the Primary, with no
// way to roll back the already-stamped flags except a follow-up
// `node restore` per node.
func TestNodeEvacuateMultiRefusesInUseAtomic(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	for _, n := range []string{"n1", "n2", "n3"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{Name: n}); err != nil {
			t.Fatalf("seed %s: %v", n, err)
		}
	}

	// rd-busy on n2 is Primary → must block the whole call.
	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name:     "rd-busy",
		NodeName: "n2",
		State:    apiv1.ResourceState{InUse: boolPtr(true)},
	}); err != nil {
		t.Fatalf("seed busy: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := postEvacuateMulti(t, base, []string{"n1", "n2"})
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status: got %d, want 409", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)

	if !strings.Contains(string(body), "rd-busy") {
		t.Errorf("body: missing offending resource 'rd-busy'; got %q", string(body))
	}

	// Atomicity: even the in-use-free n1 must NOT be stamped.
	for _, n := range []string{"n1", "n2"} {
		got, err := st.Nodes().Get(ctx, n)
		if err != nil {
			t.Fatalf("get %s: %v", n, err)
		}

		if slices.Contains(got.Flags, "EVICTED") {
			t.Errorf("node %s stamped EVICTED despite refusal on peer: %v", n, got.Flags)
		}
	}
}

// TestNodeEvacuateMultiUnknownNodeRefuses pins the pre-validation:
// every name in `nodes` MUST exist before any flag is stamped. A
// typo or stale operator script that lists a deleted node must
// fail the entire call with 404 — NOT silently evict the valid
// names while the typo is reported via a separate error. Mirrors
// the atomicity guarantee of the in-use refusal: refused calls
// don't half-apply.
func TestNodeEvacuateMultiUnknownNodeRefuses(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	for _, n := range []string{"n1", "n2"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{Name: n}); err != nil {
			t.Fatalf("seed %s: %v", n, err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := postEvacuateMulti(t, base, []string{"n1", "ghost"})
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status: got %d, want 404", resp.StatusCode)
	}

	// n1 exists and would otherwise be a fine target — must NOT
	// be stamped, the call refused as a unit.
	got, err := st.Nodes().Get(ctx, "n1")
	if err != nil {
		t.Fatalf("get n1: %v", err)
	}

	if slices.Contains(got.Flags, "EVICTED") {
		t.Errorf("n1 stamped EVICTED despite unknown peer in request: %v", got.Flags)
	}
}

// TestNodeEvacuateMultiEmptyBodyRefuses pins the input-validation:
// an empty `nodes` slice (or a missing field) MUST 400. A no-op
// 200 would mask client bugs that build the body from a faulty
// variadic CLI expansion — `linstor node evacuate` with zero
// positional args should never reach the REST surface, but if it
// does we want the loud failure, not a silent success.
func TestNodeEvacuateMultiEmptyBodyRefuses(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := postEvacuateMulti(t, base, nil)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400", resp.StatusCode)
	}
}

// TestNodeEvacuateMultiMalformedBodyRefuses pins the JSON-decode
// failure path. A non-JSON body MUST 400 with a decode error, not
// a 200 with the implicit empty `nodes` slice. Symmetric with
// every other POST-with-body handler in the package.
func TestNodeEvacuateMultiMalformedBodyRefuses(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpPost(t, base+"/v1/nodes/evacuate",
		bytes.NewBufferString("not json").Bytes())
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400", resp.StatusCode)
	}
}

// TestNodeEvacuateMultiIdempotent pins idempotency over the
// variadic shape: re-firing the same multi-evacuate must NOT
// duplicate EVICTED on any node. Inherits the single-node
// idempotency contract — addFlag's slices.Contains short-circuit
// runs per-node inside the multi handler too. Operators retry
// drains after controller restarts; without idempotency a
// re-fired multi-call would balloon every node's flag list.
func TestNodeEvacuateMultiIdempotent(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	for _, n := range []string{"n1", "n2", "n3"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{Name: n}); err != nil {
			t.Fatalf("seed %s: %v", n, err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// First call stamps both.
	resp := postEvacuateMulti(t, base, []string{"n1", "n2"})
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("first call status: got %d, want 200", resp.StatusCode)
	}

	// Second call — same set, must remain 200 and not duplicate.
	resp = postEvacuateMulti(t, base, []string{"n1", "n2"})
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("second call status: got %d, want 200", resp.StatusCode)
	}

	for _, n := range []string{"n1", "n2"} {
		got, err := st.Nodes().Get(ctx, n)
		if err != nil {
			t.Fatalf("get %s: %v", n, err)
		}

		count := 0

		for _, f := range got.Flags {
			if f == "EVICTED" {
				count++
			}
		}

		if count != 1 {
			t.Errorf("node %s EVICTED count: got %d, want 1; flags=%v",
				n, count, got.Flags)
		}
	}
}

// TestNodeLostCascadeIgnoresMissingChildren pins the idempotency
// of the cascade: a re-run of `node lost` (or a partial prior
// teardown) must succeed even when no Resources / StoragePools
// remain to cascade. Matches the parent handler's
// already-NotFound-is-success semantic so an operator teardown
// loop is safe to retry.
func TestNodeLostCascadeIgnoresMissingChildren(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "lost-node"}); err != nil {
		t.Fatalf("seed lost node: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/nodes/lost-node/lost", nil)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200 (cascade with no children must succeed)", resp.StatusCode)
	}

	_, err := st.Nodes().Get(ctx, "lost-node")
	if err == nil {
		t.Errorf("node still present after lost: expected removal")
	}
}

// TestNodeEvacuateSingleInUseWireCompatibleWithMulti pins scenario
// 4.W05 (cross-listed wave1 4.18 + Bug 18, paired with 4.W06 already
// closed): the single-node form `POST /v1/nodes/{node}/evacuate` MUST
// produce the same on-the-wire refusal shape as the multi-node form
// `POST /v1/nodes/evacuate` for the InUse guard — same HTTP status
// (409), same `APICallRc` envelope (negative RetCode = MASK_ERROR
// bit, non-empty Message), same atomic non-stamp on refusal.
//
// Both forms route through the same UG9 §"Evacuating a node" intent;
// the variadic form just folds a set into one call. An operator
// script that pattern-matches the refusal body (or status code +
// envelope) for one form MUST work identically against the other,
// so the controller-side rolling-drain orchestrator can swap between
// per-node loops and batched calls without rewriting its error
// handling. If the two paths diverge — different status, different
// envelope, partial stamping — clients have to special-case which
// form they hit and the abstraction leaks back into the operator.
//
// Pinned because the single-node guard lives in handleNodeEvacuate
// (inline) while the multi-node guard lives in checkEvacuateInUse
// (extracted helper); a refactor that drifts one without the other
// would silently break wire-compat with no other test catching it.
func TestNodeEvacuateSingleInUseWireCompatibleWithMulti(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	// Two-node cluster so the multi-form's no-candidate guard is
	// satisfied: evacuating n1 leaves n2 as a candidate target.
	// Both forms must reach the InUse guard, not bail earlier.
	for _, n := range []string{"n1", "n2"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{Name: n}); err != nil {
			t.Fatalf("seed %s: %v", n, err)
		}
	}

	// rd-busy Primary on n1 — must trigger the InUse refusal for
	// either form.
	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name:     "rd-busy",
		NodeName: "n1",
		State:    apiv1.ResourceState{InUse: boolPtr(true)},
	}); err != nil {
		t.Fatalf("seed busy: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// Fire the single-node form.
	singleResp := httpPost(t, base+"/v1/nodes/n1/evacuate", nil)
	defer func() { _ = singleResp.Body.Close() }()

	singleBody, _ := io.ReadAll(singleResp.Body)

	// Fire the multi-node form with the same {n1} set.
	multiResp := postEvacuateMulti(t, base, []string{"n1"})
	defer func() { _ = multiResp.Body.Close() }()

	multiBody, _ := io.ReadAll(multiResp.Body)

	// Wire-compat #1: identical HTTP status.
	if singleResp.StatusCode != multiResp.StatusCode {
		t.Errorf("status divergence: single=%d, multi=%d (both must be 409)",
			singleResp.StatusCode, multiResp.StatusCode)
	}

	if singleResp.StatusCode != http.StatusConflict {
		t.Errorf("single status: got %d, want 409", singleResp.StatusCode)
	}

	// Wire-compat #2: identical APICallRc envelope shape — both
	// MUST decode as []APICallRc with RetCode carrying MASK_ERROR
	// (negative) and a non-empty Message. golinstor's
	// client.ApiCallError discriminates on RetCode sign; a single-
	// form 409 that lacks the negative RetCode would surface as
	// "unexpected end of JSON input" in the client.
	var singleEnv, multiEnv []apiv1.APICallRc

	if err := json.Unmarshal(singleBody, &singleEnv); err != nil {
		t.Fatalf("single body not []APICallRc: %v; body=%q", err, string(singleBody))
	}

	if err := json.Unmarshal(multiBody, &multiEnv); err != nil {
		t.Fatalf("multi body not []APICallRc: %v; body=%q", err, string(multiBody))
	}

	if len(singleEnv) == 0 || len(multiEnv) == 0 {
		t.Fatalf("empty envelopes: single=%d, multi=%d", len(singleEnv), len(multiEnv))
	}

	if (singleEnv[0].RetCode < 0) != (multiEnv[0].RetCode < 0) {
		t.Errorf("RetCode sign divergence: single=%#x, multi=%#x (both must be MASK_ERROR negative)",
			singleEnv[0].RetCode, multiEnv[0].RetCode)
	}

	if singleEnv[0].RetCode >= 0 {
		t.Errorf("single RetCode not negative (MASK_ERROR missing): %#x", singleEnv[0].RetCode)
	}

	if singleEnv[0].Message == "" {
		t.Errorf("single Message empty; operator gets no actionable text")
	}

	// Wire-compat #3: both refusals MUST name the offending
	// resource so an operator script parsing either form can target
	// the same consumer to demote.
	if !strings.Contains(singleEnv[0].Message, "rd-busy") {
		t.Errorf("single message missing 'rd-busy': %q", singleEnv[0].Message)
	}

	if !strings.Contains(multiEnv[0].Message, "rd-busy") {
		t.Errorf("multi message missing 'rd-busy': %q", multiEnv[0].Message)
	}

	// Wire-compat #4: atomic non-stamp — neither refusal half-
	// applies EVICTED. A divergence here (one stamps, the other
	// doesn't) is the worst kind of leak: same wire shape, different
	// side effects.
	got, err := st.Nodes().Get(ctx, "n1")
	if err != nil {
		t.Fatalf("get n1: %v", err)
	}

	if slices.Contains(got.Flags, "EVICTED") {
		t.Errorf("n1 stamped EVICTED despite refusal from both forms: %v", got.Flags)
	}
}
