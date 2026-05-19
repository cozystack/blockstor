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
	"net/http/httptest"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Bug 202 (P3) — `handleVDDelete` has the Bug 355 pre-Delete walk
// but needed a Bug 174-style post-Delete re-walk too.
//
// Bug 355 narrowed the pre-walk to refuse with 409 + FAIL_IN_USE
// only when a Resource on the parent RD reports
// `state.in_use == true` (DRBD Primary with a mounted consumer).
// The pre-walk is check-then-write with no atomicity, so a
// concurrent `r c <rd>.<node>` + Primary promotion that slips
// between the pre-walk and the store-level Delete drops the VD
// spec out from under a now-mounted Primary. Post-Delete the
// Resource is orphaned: it references a VolumeNumber whose VD row
// no longer exists, AND the Primary still has a mounted consumer.
//
// Self-heals via the satellite reconciler eventually (the VD-watch
// re-applies the RD spec), hence P3 — but the wire-side `vd d` reply
// is 200 + MASK_INFO while the cluster is mid-orphan, which is the
// exact misleading-success signal Bug 174 closed for `n d` / `rg d`.
//
// Fix shape mirrors Bug 174 on `n d` / `rg d` / `sp d`: capture the
// pre-Delete VD via Get, run Delete, re-walk the in-use Resources,
// restore the captured VD via Create if an in-use racer appeared
// during the window, return the same 409 envelope the pre-walk
// would have emitted.

// TestBug202VDDeletePostWalkRollsBackOnRace pins the post-Delete walk
// + rollback behaviour deterministically: a Resource that appears
// AFTER the Bug 186 pre-walk but BEFORE the post-Delete re-walk MUST
// trigger the rollback path — restore the captured VD, return 409 +
// FAIL_IN_USE, no orphan Volume row on the wire.
//
// Pre-fix, `handleVDDelete` only had the pre-walk; the post-Delete
// re-walk did not exist, so the racing-Resource scenario passed
// silently: Delete landed, the orphaned Resource lived on, and the
// wire reply was 200 + MASK_INFO. Post-fix, the captured VD is
// restored via Create and the 409 envelope cites the racing replica.
//
// The race is driven directly against the rollback helper so the
// test is deterministic — the live-cluster TOCTOU window has timing
// the in-memory store can't reliably reproduce without a hook (every
// run there are residual orderings where the racing `r c` lands
// after the post-walk; the satellite reconciler is the eventual
// remediator on those, see Bug 202's P3 designation). Driving the
// helper directly pins the wire-side fix contract: rollback fires
// when it CAN see the racing Resource, restoring the VD and writing
// the 409. The Bug-174 sibling tests use the same shape for `n d` /
// `rg d` post-Delete close.
func TestBug202VDDeletePostWalkRollsBackOnRace(t *testing.T) {
	t.Parallel()

	const (
		rdName   = "pvc-bug202-race"
		nodeName = "node-bug202"
		volNum   = int32(0)
		sizeKib  = int64(32 * 1024)
	)

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rdName}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	captured := &apiv1.VolumeDefinition{VolumeNumber: volNum, SizeKib: sizeKib}

	// Simulate the post-Delete state: VD spec gone (the handler's
	// store.Delete already ran), a racing `r c` + Primary promotion
	// already persisted an in-use Resource. The captured VD snapshot
	// is what the handler held before its Delete; the rollback's job
	// is to restore it. Bug 355 narrowed the gate to in-use only, so
	// the racer MUST carry `state.in_use=true` for rollback to fire.
	err := st.Resources().Create(ctx, &apiv1.Resource{
		Name:     rdName,
		NodeName: nodeName,
		State:    apiv1.ResourceState{InUse: boolPtr(true)},
	})
	if err != nil {
		t.Fatalf("seed racing Resource: %v", err)
	}

	srv := &Server{Store: st}

	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(ctx, http.MethodDelete,
		fmt.Sprintf("/v1/resource-definitions/%s/volume-definitions/%d", rdName, volNum),
		nil)

	rolled := srv.rollbackVDDeleteIfRaced(rec, req, rdName, volNum, captured)
	if !rolled {
		t.Fatalf("rollbackVDDeleteIfRaced: got false, want true (Bug 202: racing Resource MUST trigger rollback)")
	}

	if rec.Code != http.StatusConflict {
		t.Errorf("status: got %d, want 409 (Bug 202: rollback envelope mirrors Bug 186 pre-walk shape)", rec.Code)
	}

	// VD MUST be restored — that's the whole point of capture +
	// rollback. A pre-fix run never reached this branch (no rollback
	// existed), so the VD stayed gone and the racing Resource was
	// the orphan.
	got, err := st.VolumeDefinitions().Get(ctx, rdName, volNum)
	if err != nil {
		t.Fatalf("VD %s/%d not restored after rollback: %v", rdName, volNum, err)
	}

	if got.VolumeNumber != volNum || got.SizeKib != sizeKib {
		t.Errorf("restored VD content drift: got %+v, want VolumeNumber=%d SizeKib=%d",
			got, volNum, sizeKib)
	}

	// Envelope contract: 409 + FAIL_IN_USE | MASK_ERROR, names the
	// racing node so the operator knows which `r d` to issue next.
	var rcs []apiv1.APICallRc
	if err := json.Unmarshal(rec.Body.Bytes(), &rcs); err != nil {
		t.Fatalf("decode envelope: %v\nbody: %s", err, rec.Body.String())
	}

	if len(rcs) == 0 {
		t.Fatalf("envelope empty on rollback; want at least one entry")
	}

	if rcs[0].RetCode&apiCallRcError == 0 {
		t.Errorf("ret_code: got %#x, want MASK_ERROR bit set", rcs[0].RetCode)
	}

	if rcs[0].RetCode&0xFFFF != apiCallRcFailInUse {
		t.Errorf("ret_code sub-code: got %d, want FAIL_IN_USE (%d)",
			rcs[0].RetCode&0xFFFF, apiCallRcFailInUse)
	}

	hay := rcs[0].Message + "\n" + rcs[0].Cause + "\n" + rcs[0].Correc
	if !strings.Contains(hay, nodeName) {
		t.Errorf("envelope omits racing node %q; envelope=%+v", nodeName, rcs[0])
	}
}

// TestBug202VDDeletePostWalkNoFireWhenNoRace pins the no-race branch:
// when no referencing Resource exists at post-walk time, the rollback
// MUST return false so the delete commits cleanly. Pre-fix the helper
// didn't exist; post-fix a regression that always-rolled-back would
// break the happy path of every `vd d` call.
func TestBug202VDDeletePostWalkNoFireWhenNoRace(t *testing.T) {
	t.Parallel()

	const (
		rdName  = "pvc-bug202-norace"
		volNum  = int32(0)
		sizeKib = int64(32 * 1024)
	)

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rdName}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	captured := &apiv1.VolumeDefinition{VolumeNumber: volNum, SizeKib: sizeKib}

	srv := &Server{Store: st}

	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(ctx, http.MethodDelete,
		fmt.Sprintf("/v1/resource-definitions/%s/volume-definitions/%d", rdName, volNum),
		nil)

	rolled := srv.rollbackVDDeleteIfRaced(rec, req, rdName, volNum, captured)
	if rolled {
		t.Fatalf("rollbackVDDeleteIfRaced: got true on empty reference set, want false")
	}

	// VD must NOT have been restored — there was nothing to roll back
	// to. (The handler's caller still has the responsibility to write
	// the success envelope on its happy path.)
	if _, err := st.VolumeDefinitions().Get(ctx, rdName, volNum); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("rollback unexpectedly Created VD on empty-reference path: err=%v", err)
	}
}

// TestBug202VDDeleteHappyPath pins the end-to-end no-race case via the
// HTTP wire: a VD with no referencing Resources deletes cleanly with a
// 200 + MASK_INFO envelope. Guards against the post-walk accidentally
// refusing on an empty-reference happy path.
func TestBug202VDDeleteHappyPath(t *testing.T) {
	t.Parallel()

	const (
		rdName  = "rd202-happy"
		volNum  = int32(0)
		sizeKib = int64(32 * 1024)
	)

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rdName}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	err := st.VolumeDefinitions().Create(ctx, rdName,
		&apiv1.VolumeDefinition{VolumeNumber: volNum, SizeKib: sizeKib})
	if err != nil {
		t.Fatalf("seed VD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t,
		fmt.Sprintf("%s/v1/resource-definitions/%s/volume-definitions/%d",
			base, rdName, volNum))
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DELETE status: got %d, want 200", resp.StatusCode)
	}

	if _, err := st.VolumeDefinitions().Get(ctx, rdName, volNum); err == nil {
		t.Errorf("VD %s/%d still present after successful DELETE", rdName, volNum)
	}
}
