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
	"fmt"
	"net/http"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Bug 355 (P2) — `linstor vd d <rd> 0` on a multi-replica RD refused
// unconditionally with "resource replicas still reference VolumeNumber"
// + a Correction suggesting `?force=true`. The query parameter existed
// on the REST handler but linstor-client never grew a `--force` flag
// for `vd d`, so the operator had no escape hatch via the standard
// CLI. The catcher cell is `tests/e2e/cli-matrix/vd-d-cascades-
// replicas.sh` (commit 5f36ad7cb).
//
// Upstream LINSTOR's CtrlVlmDfnDeleteApiCallHandler cascades:
//
//   1. `anyResourceInUsePrivileged(rscDfn)` — refuse ONLY when at
//      least one Resource is observed in-use (Primary + mounted
//      consumer). Returns FAIL_IN_USE.
//   2. Otherwise iterate `getVolumeIteratorPrivileged(vlmDfn)` →
//      `markDeleted(vlm)` per replica.
//   3. `markDeleted(vlmDfn)`.
//   4. `updateSatellites(vlmDfn.getResourceDefinition(),
//      deleteDataFlux)` — per-node teardown (`zfs destroy`, `lvremove`).
//
// Fix shape: narrow `refuseVDDeleteIfReferenced` to "any in-use refuses"
// (matching `anyResourceInUsePrivileged`); leave the cascade path
// (existing `pruneVolumesFromResources` + the satellite reconciler's
// VD-watch) responsible for steps 2–4. Drop the misleading
// `?force=true` line from the surfaced Correction — keep the query
// param as a transport-level escape for curl scripts but stop
// advertising it as the operator remedy.

// TestBug355VDDeleteCascadesSecondaryReplicas pins the cascade happy
// path: a 2-replica RD with both replicas Secondary (`state.in_use`
// unset OR false) MUST allow `vd d` to succeed with 200 + MASK_INFO,
// drop the VD from the store, and prune the dropped VolumeNumber from
// each Resource's Status.Volumes so `view/resources` is read-your-
// writes against the deletion.
//
// Pre-fix this returned 409 + FAIL_IN_USE with the misleading
// `?force=true` Correction because the pre-Delete walk treated any
// Resource reference as a refusal cause.
func TestBug355VDDeleteCascadesSecondaryReplicas(t *testing.T) {
	t.Parallel()

	const (
		rdName  = "pvc-bug355-cascade"
		nodeA   = "node-a"
		nodeB   = "node-b"
		volNum  = int32(0)
		sizeKib = int64(32 * 1024)
	)

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rdName}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.VolumeDefinitions().Create(ctx, rdName,
		&apiv1.VolumeDefinition{VolumeNumber: volNum, SizeKib: sizeKib}); err != nil {
		t.Fatalf("seed VD: %v", err)
	}

	// Two Secondary replicas. State.InUse == nil mirrors the wire
	// shape upstream LINSTOR emits before the satellite has reported
	// (and the prior "any reference refuses" gate would have caught
	// it on the implicit-reference branch). Bug 355 narrows the gate
	// to in-use only, so this case MUST cascade.
	for _, node := range []string{nodeA, nodeB} {
		err := st.Resources().Create(ctx, &apiv1.Resource{
			Name:     rdName,
			NodeName: node,
			Volumes:  []apiv1.Volume{{VolumeNumber: volNum, DevicePath: "/dev/fake/" + rdName + "_00000"}},
		})
		if err != nil {
			t.Fatalf("seed Resource %s/%s: %v", rdName, node, err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t,
		fmt.Sprintf("%s/v1/resource-definitions/%s/volume-definitions/%d",
			base, rdName, volNum))
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DELETE status: got %d, want 200 (Bug 355: Secondary-only RD must cascade)", resp.StatusCode)
	}

	var rcs []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rcs); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}

	if len(rcs) == 0 {
		t.Fatalf("envelope empty on success; want one entry")
	}

	if rcs[0].RetCode&apiCallRcError != 0 {
		t.Errorf("ret_code: got %#x, want MASK_ERROR bit clear on cascade success", rcs[0].RetCode)
	}

	// VD MUST be gone from the store after a successful cascade.
	if _, err := st.VolumeDefinitions().Get(ctx, rdName, volNum); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("VolumeDefinition %s/%d still present after successful cascade: err=%v",
			rdName, volNum, err)
	}

	// Per-node Status.Volumes MUST have the dropped VolumeNumber
	// pruned synchronously — Bug 139's invariant survives Bug 355
	// because pruneVolumesFromResources runs on the cascade path.
	for _, node := range []string{nodeA, nodeB} {
		res, err := st.Resources().Get(ctx, rdName, node)
		if err != nil {
			t.Fatalf("re-get Resource %s/%s: %v", rdName, node, err)
		}

		for i := range res.Volumes {
			if res.Volumes[i].VolumeNumber == volNum {
				t.Errorf("Resource %s/%s: VolumeNumber %d still present after cascade: volumes=%+v",
					rdName, node, volNum, res.Volumes)
			}
		}
	}
}

// TestBug355VDDeleteRefusesOnInUsePrimary pins the narrowed refusal
// gate: when at least one Resource on the parent RD reports
// `state.in_use == true` (DRBD Primary with a mounted consumer),
// `vd d` MUST refuse with 409 + FAIL_IN_USE | MASK_ERROR. The
// envelope's Correction MUST NOT mention `?force=true` (the dead-end
// remedy the prior envelope surfaced) — it MUST point at
// `role-demote` and consumer unmount.
//
// Mirrors upstream LINSTOR's `anyResourceInUsePrivileged` refusal
// shape (Bug 92 / Bug 152 wire-shape parity).
func TestBug355VDDeleteRefusesOnInUsePrimary(t *testing.T) {
	t.Parallel()

	const (
		rdName  = "pvc-bug355-inuse"
		nodeA   = "node-a"
		nodeB   = "node-b"
		volNum  = int32(0)
		sizeKib = int64(32 * 1024)
	)

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rdName}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.VolumeDefinitions().Create(ctx, rdName,
		&apiv1.VolumeDefinition{VolumeNumber: volNum, SizeKib: sizeKib}); err != nil {
		t.Fatalf("seed VD: %v", err)
	}

	// nodeA is Primary (in_use=true) → refusal cause.
	// nodeB is Secondary (in_use=false) → not a refusal cause on its
	// own; the cascade would happily drop a Secondary-only RD.
	err := st.Resources().Create(ctx, &apiv1.Resource{
		Name:     rdName,
		NodeName: nodeA,
		State:    apiv1.ResourceState{InUse: boolPtr(true)},
		Volumes:  []apiv1.Volume{{VolumeNumber: volNum}},
	})
	if err != nil {
		t.Fatalf("seed primary Resource: %v", err)
	}

	err = st.Resources().Create(ctx, &apiv1.Resource{
		Name:     rdName,
		NodeName: nodeB,
		State:    apiv1.ResourceState{InUse: boolPtr(false)},
		Volumes:  []apiv1.Volume{{VolumeNumber: volNum}},
	})
	if err != nil {
		t.Fatalf("seed secondary Resource: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t,
		fmt.Sprintf("%s/v1/resource-definitions/%s/volume-definitions/%d",
			base, rdName, volNum))
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("DELETE status: got %d, want 409 (Bug 355: in-use Primary MUST refuse)", resp.StatusCode)
	}

	var rcs []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rcs); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}

	if len(rcs) == 0 {
		t.Fatalf("envelope empty on refusal; want one entry")
	}

	if rcs[0].RetCode&apiCallRcError == 0 {
		t.Errorf("ret_code: got %#x, want MASK_ERROR bit set", rcs[0].RetCode)
	}

	if rcs[0].RetCode&0xFFFF != apiCallRcFailInUse {
		t.Errorf("ret_code sub-code: got %d, want FAIL_IN_USE (%d)",
			rcs[0].RetCode&0xFFFF, apiCallRcFailInUse)
	}

	// Envelope MUST name the in-use node so the operator knows which
	// `role-demote` to issue. The Secondary node MUST NOT show up —
	// surfacing it would re-introduce the "blames every replica"
	// noise the prior envelope had.
	hay := rcs[0].Message + "\n" + rcs[0].Cause + "\n" + rcs[0].Correc
	if !strings.Contains(hay, nodeA) {
		t.Errorf("envelope omits in-use node %q; envelope=%+v", nodeA, rcs[0])
	}

	if strings.Contains(hay, nodeB) {
		t.Errorf("envelope mentions Secondary node %q; only in-use nodes should be cited. envelope=%+v",
			nodeB, rcs[0])
	}

	// Wire contract: NO `force=true` suggestion. linstor-client has
	// no `--force` flag for `vd d`, so surfacing it gave the operator
	// a dead-end remedy. The catcher cell `vd-d-cascades-replicas.sh`
	// asserts the same invariant at the e2e layer.
	if strings.Contains(hay, "force=true") || strings.Contains(hay, "?force=") {
		t.Errorf("envelope still suggests dead-end ?force=true remedy; envelope=%+v", rcs[0])
	}

	// The Correction MUST point at the operator-actionable remedy.
	// Either "role-demote" or "unmount" is acceptable — both are
	// the legitimate paths out of an in-use refusal.
	if !strings.Contains(rcs[0].Correc, "role-demote") && !strings.Contains(rcs[0].Correc, "unmount") {
		t.Errorf("Correction missing actionable remedy (role-demote / unmount): %q", rcs[0].Correc)
	}

	// VD MUST still be present on a refused call — partial-state
	// after a 409 would be worse than the bug itself.
	if _, err := st.VolumeDefinitions().Get(ctx, rdName, volNum); err != nil {
		t.Errorf("VD %s/%d disappeared after refused DELETE: %v", rdName, volNum, err)
	}
}

// TestBug355VDDeleteIdempotentOnRepeatedDelete pins the Bug 65/186
// ordering pattern: a second `vd d` against an already-deleted VD
// MUST be a no-op (200 + WARN/NOT_FOUND envelope) regardless of
// whether the parent RD still has Resources around. The cascade is
// idempotent on retry — linstor-csi's expand/shrink retry loops
// re-issue `vd d` and would otherwise wedge on the second call.
func TestBug355VDDeleteIdempotentOnRepeatedDelete(t *testing.T) {
	t.Parallel()

	const (
		rdName  = "pvc-bug355-idem"
		nodeA   = "node-a"
		volNum  = int32(0)
		sizeKib = int64(32 * 1024)
	)

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rdName}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.VolumeDefinitions().Create(ctx, rdName,
		&apiv1.VolumeDefinition{VolumeNumber: volNum, SizeKib: sizeKib}); err != nil {
		t.Fatalf("seed VD: %v", err)
	}

	// Surviving Secondary replica — the cascade should drop the VD
	// once, then the second call hits the NotFound idempotent branch.
	err := st.Resources().Create(ctx, &apiv1.Resource{
		Name:     rdName,
		NodeName: nodeA,
		Volumes:  []apiv1.Volume{{VolumeNumber: volNum}},
	})
	if err != nil {
		t.Fatalf("seed Resource: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	// First DELETE — cascades.
	first := httpDelete(t,
		fmt.Sprintf("%s/v1/resource-definitions/%s/volume-definitions/%d",
			base, rdName, volNum))
	_ = first.Body.Close()

	if first.StatusCode != http.StatusOK {
		t.Fatalf("first DELETE status: got %d, want 200", first.StatusCode)
	}

	// Second DELETE — already-absent, idempotent.
	second := httpDelete(t,
		fmt.Sprintf("%s/v1/resource-definitions/%s/volume-definitions/%d",
			base, rdName, volNum))
	defer func() { _ = second.Body.Close() }()

	if second.StatusCode != http.StatusOK {
		t.Fatalf("second DELETE status: got %d, want 200 (Bug 65/186 idempotent shape)", second.StatusCode)
	}

	var rcs []apiv1.APICallRc
	if err := json.NewDecoder(second.Body).Decode(&rcs); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}

	if len(rcs) == 0 {
		t.Fatalf("envelope empty on idempotent replay; want one entry")
	}

	// WARN-bit envelope with "already absent" marker (warnVDNotFound).
	if rcs[0].RetCode&maskWarn == 0 {
		t.Errorf("ret_code: got %#x, want WARN bit (%#x) set on idempotent replay",
			rcs[0].RetCode, maskWarn)
	}

	if !strings.Contains(rcs[0].Message, "already absent") {
		t.Errorf("idempotent replay message: got %q, want 'already absent' marker", rcs[0].Message)
	}
}
