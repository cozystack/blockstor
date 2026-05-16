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
	"errors"
	"net/http"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Bug 179 (P1) — `n d` orphans StoragePool CRDs.
//
// `refuseNodeDeleteIfReferenced` only walked the Resources store
// while checking whether a node was still referenced (Bug 92
// closed the Resource half). StoragePools on the same node were
// completely invisible to the gate: `linstor n d <node>` with
// zero Resources but one or more SPs still on the node returned
// SUCCESS and the SP CRDs survived pointing at a deleted Node row
// — the autoplacer's free-space ranking then crashed on a nil
// `Node` lookup the next reconcile.
//
// `n lost` (handleNodeLost / cascadeOrphansForLostNode) already
// walks BOTH Resources and StoragePools — Bug 179 is the missing
// half of the `n d` parity. Fix: extend the pre-walk refusal in
// `refuseNodeDeleteIfReferenced` to ALSO list StoragePools on
// the node and surface both lists in the 409 envelope.
//
// `?force=true` keeps its Bug 92 contract: bypass the refusal
// (cascade-delete SPs + Resources before dropping the Node so the
// operator never ends up with orphan StoragePool CRDs).

// TestBug179NodeDeleteRefusesWithStoragePools pins the missing
// SP half of the Bug 92 refusal. Node + 1 StoragePool on the
// node, no Resources → `DELETE /v1/nodes/<node>` MUST refuse
// with 409 + FAIL_IN_USE and surface the SP name in the
// envelope cause. The Node and the SP MUST both survive — a
// refused delete that nevertheless dropped the Node row would
// be the very orphan we're closing.
func TestBug179NodeDeleteRefusesWithStoragePools(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{
		Name: "poke179",
		Type: apiv1.NodeTypeSatellite,
	}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		StoragePoolName: "mypool",
		NodeName:        "poke179",
		ProviderKind:    apiv1.StoragePoolKindLVMThin,
		Props: map[string]string{
			"StorDriver/LvmVg":    "vg1",
			"StorDriver/ThinPool": "thin",
		},
	}); err != nil {
		t.Fatalf("seed pool: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t, base+"/v1/nodes/poke179")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status: got %d, want 409 (still-referenced refusal)", resp.StatusCode)
	}

	var rc []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rc); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}

	if len(rc) == 0 {
		t.Fatalf("envelope: got empty, want one entry")
	}

	// FAIL_IN_USE sub-code OR'd with MASK_ERROR (high bit set) —
	// same shape Bug 92 uses for the Resource-side refusal.
	if rc[0].RetCode&0xFFFF != apiCallRcFailInUse {
		t.Errorf("ret_code sub-code: got %#x, want FAIL_IN_USE (%d)",
			rc[0].RetCode&0xFFFF, apiCallRcFailInUse)
	}

	if rc[0].RetCode >= 0 {
		t.Errorf("ret_code: got %#x, want MASK_ERROR (negative) bit set", rc[0].RetCode)
	}

	// Cause must name the SP so the operator knows which pool is
	// holding the node hostage. Without this signal the operator
	// retries `n d` indefinitely with no idea what to drop first.
	if !strings.Contains(rc[0].Cause, "mypool") {
		t.Errorf("cause: got %q, want it to name storage pool 'mypool'", rc[0].Cause)
	}

	// CRITICAL: Node row MUST still exist — a refused delete that
	// nevertheless dropped the Node would be the orphan we're
	// closing.
	if _, err := st.Nodes().Get(ctx, "poke179"); err != nil {
		t.Errorf("Node was deleted despite refusal: %v", err)
	}

	// The SP must survive too — Bug 179 is precisely the orphan-SP
	// state the refusal exists to prevent.
	if _, err := st.StoragePools().Get(ctx, "poke179", "mypool"); err != nil {
		t.Errorf("StoragePool was deleted despite refusal: %v", err)
	}
}

// TestBug179NodeDeleteRefusesWithBothSPsAndResources pins that
// when the node is referenced by BOTH a Resource and a
// StoragePool, the refusal envelope surfaces BOTH lists — a
// single round-trip tells the operator everything they have to
// clean up. Without this the operator drops the Resource, retries
// `n d`, and re-hits the same refusal for the SP they didn't know
// about. Mirrors the `n lost` cause-line shape which already
// surfaces resources + (implicitly) cascaded SPs together.
func TestBug179NodeDeleteRefusesWithBothSPsAndResources(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{
		Name: "poke179both",
		Type: apiv1.NodeTypeSatellite,
	}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		StoragePoolName: "mypool",
		NodeName:        "poke179both",
		ProviderKind:    apiv1.StoragePoolKindLVMThin,
		Props: map[string]string{
			"StorDriver/LvmVg":    "vg1",
			"StorDriver/ThinPool": "thin",
		},
	}); err != nil {
		t.Fatalf("seed pool: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: "rd179",
	}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name:     "rd179",
		NodeName: "poke179both",
	}); err != nil {
		t.Fatalf("seed resource: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t, base+"/v1/nodes/poke179both")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status: got %d, want 409 (still-referenced refusal)", resp.StatusCode)
	}

	var rc []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rc); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}

	if len(rc) == 0 {
		t.Fatalf("envelope: got empty, want one entry")
	}

	// Combined envelope: cause must reference BOTH lists. Operator
	// sees the full cleanup workload in one round-trip.
	if !strings.Contains(rc[0].Cause, "mypool") {
		t.Errorf("cause: got %q, want it to name SP 'mypool'", rc[0].Cause)
	}

	if !strings.Contains(rc[0].Cause, "rd179") {
		t.Errorf("cause: got %q, want it to name resource 'rd179'", rc[0].Cause)
	}

	// All three children must survive a refused delete.
	if _, err := st.Nodes().Get(ctx, "poke179both"); err != nil {
		t.Errorf("Node was deleted despite refusal: %v", err)
	}

	if _, err := st.StoragePools().Get(ctx, "poke179both", "mypool"); err != nil {
		t.Errorf("StoragePool was deleted despite refusal: %v", err)
	}

	resources, err := st.Resources().List(ctx)
	if err != nil {
		t.Fatalf("list resources: %v", err)
	}

	if len(resources) != 1 {
		t.Errorf("resources: got %d, want 1 (refused delete must not cascade)", len(resources))
	}
}

// TestBug179NodeDeleteForceTrueBypasses pins the escape hatch:
// `?force=true` MUST succeed even when StoragePools reference the
// node, and the Node row MUST be gone afterward. The blockstor
// choice is cascade-delete the StoragePool CRDs along with the
// Node so the operator never ends up with orphan SP rows pointing
// at a deleted node — same semantic as `n lost` already provides
// via cascadeOrphansForLostNode. Documented in
// `refuseNodeDeleteIfReferenced` / `handleNodeDelete` doc-comments.
func TestBug179NodeDeleteForceTrueBypasses(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{
		Name: "poke179force",
		Type: apiv1.NodeTypeSatellite,
	}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		StoragePoolName: "mypool",
		NodeName:        "poke179force",
		ProviderKind:    apiv1.StoragePoolKindLVMThin,
		Props: map[string]string{
			"StorDriver/LvmVg":    "vg1",
			"StorDriver/ThinPool": "thin",
		},
	}); err != nil {
		t.Fatalf("seed pool: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t, base+"/v1/nodes/poke179force?force=true")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200 (force=true bypasses refusal)", resp.StatusCode)
	}

	// Node must be gone.
	_, err := st.Nodes().Get(ctx, "poke179force")
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("Node not deleted despite force=true: %v", err)
	}

	// SP must be cascade-deleted — no orphan rows pointing at the
	// dropped Node. (Same shape `n lost` enforces via
	// cascadeOrphansForLostNode.)
	pools, err := st.StoragePools().ListByNode(ctx, "poke179force")
	if err != nil {
		t.Fatalf("list pools by node: %v", err)
	}

	if len(pools) != 0 {
		t.Errorf("orphan StoragePools after force-delete: got %d, want 0", len(pools))
	}
}

// TestBug179NodeDeleteHappyPath pins the no-reference happy path:
// a node with neither Resources nor StoragePools deletes cleanly
// with a 200 + maskInfo envelope. Guards against the SP pre-walk
// accidentally refusing on an empty-reference happy path.
func TestBug179NodeDeleteHappyPath(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{
		Name: "happy179",
		Type: apiv1.NodeTypeSatellite,
	}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t, base+"/v1/nodes/happy179")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200 (happy path, no references)", resp.StatusCode)
	}

	_, err := st.Nodes().Get(ctx, "happy179")
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("Node not deleted: %v", err)
	}
}
