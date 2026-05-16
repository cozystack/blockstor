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
	"sync"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Bug 180 (P1) — TOCTOU race in `handleSnapshotCreate` vs concurrent
// `linstor rd d <rd>`.
//
// `handleSnapshotCreate` (pkg/rest/snapshots.go) hydrated the
// per-snapshot fields from the parent RD via `Get(rd)`, then
// persisted the Snapshot CRD. The Get and the Snapshots().Create()
// were not atomic with the symmetrical rd-delete: `handleRDDelete`'s
// own pre-walk lists `Snapshots().ListByDefinition(rd)` and refuses
// with FAIL_EXISTS_SNAPSHOT_DFN on a non-empty set, then cascades
// Resources, then drops the RD. A snap-create whose Snapshots().Create()
// landed BETWEEN rd-delete's list-snapshots probe and the final
// ResourceDefinitions().Delete() survived as an orphan Snapshot CRD
// pointing at an RD that no longer existed. linstor-csi's subsequent
// `list snapshots by RD` returned empty (parent gone), but
// `view snapshots` still surfaced the row; the satellite reconciler
// could not address the snapshot (no parent RD to walk back to) and
// the orphan persisted until manual cleanup.
//
// Fix shape mirrors Bug 174 in reverse: instead of capture-then-
// re-walk-dependents on the delete side, the snapshot-create side
// gets a pre-Persist RD-existence + DELETE-flag guard PLUS a
// post-Persist re-Get of the parent RD. On a miss (or DELETE flag
// stamped during the window), the just-staged Snapshot is rolled
// back via Snapshots().Delete() and a 4xx envelope cites the race.
// The symmetric pre-walk on the rd-delete side (existing
// FAIL_EXISTS_SNAPSHOT_DFN refusal) closes the opposite ordering;
// together either ordering yields a clean error envelope and zero
// orphan Snapshot CRDs.

// TestBug180SnapshotCreateRollsBackOnRDDeletedRace fires 50 pairs
// of (snap-create X, rd-delete X). Either ordering is acceptable:
// rd-d wins → snap-create's pre-Persist hydrate hits ErrNotFound and
// 4xx's; or snap-c wins → rd-delete's pre-walk sees the snapshot row
// and 409's with FAIL_EXISTS_SNAPSHOT_DFN. The bad interleaving the
// fix closes: rd-delete's pre-walk lists snapshots (empty), then
// snap-create persists, then rd-delete drops the RD — leaving the
// just-created snapshot row pointing at a deleted RD. The post-write
// re-Get + rollback in handleSnapshotCreate catches that ordering.
//
// Assertion: every persisted Snapshot MUST have a live RD row
// matching ResourceName. An orphan is the Bug 180 symptom.
func TestBug180SnapshotCreateRollsBackOnRDDeletedRace(t *testing.T) {
	t.Parallel()

	const pairs = 50

	st := store.NewInMemory()
	ctx := t.Context()

	// Seed per-pair RD so each goroutine pair races against its own
	// key. Independent keys keep the test stable under `-race` and
	// isolate the assertion to "no orphan per pair".
	for i := range pairs {
		rdName := fmt.Sprintf("rd180-%d", i)

		if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rdName}); err != nil {
			t.Fatalf("seed RD %s: %v", rdName, err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	var wg sync.WaitGroup

	wg.Add(pairs * 2)

	for i := range pairs {
		rdName := fmt.Sprintf("rd180-%d", i)
		snapName := fmt.Sprintf("snap180-%d", i)

		// Goroutine A: snap c — try to persist a Snapshot under the RD.
		// Either succeeds (rd d hasn't started yet, or the post-write
		// re-Get sees the RD still live) or 4xx's on the pre/post RD-
		// existence guard (rd d won the race).
		go func() {
			defer wg.Done()

			body, _ := json.Marshal(apiv1.Snapshot{Name: snapName})

			resp := httpPost(t, base+"/v1/resource-definitions/"+rdName+"/snapshots", body)
			_ = resp.Body.Close()
		}()

		// Goroutine B: rd d — try to drop the RD. Either succeeds
		// (no racing snap persisted at pre-walk time) or 409's on the
		// FAIL_EXISTS_SNAPSHOT_DFN pre-walk (snap c won the race).
		go func() {
			defer wg.Done()

			resp := httpDelete(t, base+"/v1/resource-definitions/"+rdName)
			_ = resp.Body.Close()
		}()
	}

	wg.Wait()

	// Walk every persisted Snapshot — each MUST have a live RD row
	// matching its ResourceName. An orphan is the bug.
	snaps, err := st.Snapshots().List(ctx)
	if err != nil {
		t.Fatalf("list snapshots: %v", err)
	}

	for _, snap := range snaps {
		_, err := st.ResourceDefinitions().Get(ctx, snap.ResourceName)
		if errors.Is(err, store.ErrNotFound) {
			t.Errorf("orphan Snapshot %s/%s references deleted RD %q (Bug 180)",
				snap.ResourceName, snap.Name, snap.ResourceName)

			continue
		}

		if err != nil {
			t.Errorf("lookup RD %s for snapshot %s: %v",
				snap.ResourceName, snap.Name, err)
		}
	}
}

// TestBug180SnapshotCreateRefusesOnAlreadyDeletedRD pins the pre-write
// guard: an RD whose Flags slice carries "DELETE" (upstream LINSTOR's
// `Spec.Flags` analog of a CRD DeletionTimestamp — see
// pkg/rest/flags_validation.go::rdFlagDelete) is in tear-down. A
// snap-create against such an RD MUST refuse — letting the create
// land would re-trigger the Bug 180 orphan window since the rd-d
// path that stamped DELETE will eventually drop the RD anyway.
//
// Without the guard the handler would happily hydrate from the
// mid-delete RD and persist the Snapshot, then the rd-d cascade
// would proceed and the snapshot would survive as an orphan.
func TestBug180SnapshotCreateRefusesOnAlreadyDeletedRD(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	// Seed an RD with the upstream DELETE flag stamped — the
	// in-memory analog of `metadata.DeletionTimestamp != nil` on a
	// real CRD. handleRDDelete on the k8s store would have stamped
	// this on its way to the final cascade; in the in-memory store
	// we plant it directly to pin the refusal envelope shape.
	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:  "rd180-deleting",
		Flags: []string{rdFlagDelete},
	}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.Snapshot{Name: "snap180-refuse"})

	resp := httpPost(t, base+"/v1/resource-definitions/rd180-deleting/snapshots", body)
	defer func() { _ = resp.Body.Close() }()

	// 4xx — the exact code is implementation-detail (404 / 409 both
	// fit the "race observed, retry won't help" semantic). Pinning
	// to the "not 2xx, not 5xx" band keeps the test from being
	// brittle on the envelope-mask choice.
	if resp.StatusCode < 400 || resp.StatusCode >= 500 {
		t.Fatalf("status: got %d, want 4xx", resp.StatusCode)
	}

	// Envelope: upstream-shaped []ApiCallRc with an error-mask
	// ret_code. The CLI parses replies[0].ret_code unconditionally;
	// a bare `{"error": "..."}` body would have crashed the
	// python-linstor decoder (Bug 66 sibling).
	var rc []apiv1.APICallRc
	if jErr := json.NewDecoder(resp.Body).Decode(&rc); jErr != nil {
		t.Fatalf("decode envelope: %v", jErr)
	}

	if len(rc) == 0 {
		t.Fatalf("empty envelope")
	}

	if rc[0].RetCode >= 0 {
		t.Errorf("ret_code: got %d, want error-masked (negative)", rc[0].RetCode)
	}

	// The snapshot MUST NOT have been persisted — a pre-write
	// refusal is observable as "snapshot still absent in the store".
	_, err := st.Snapshots().Get(ctx, "rd180-deleting", "snap180-refuse")
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("snapshot persisted despite refusal: %v", err)
	}
}

// TestBug180SnapshotCreateHappyPath pins the no-race case: a snap-create
// against a live RD with no concurrent rd-delete succeeds with 201 +
// maskInfo envelope. Guards against the pre/post RD-existence guard
// accidentally refusing on the happy path.
func TestBug180SnapshotCreateHappyPath(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "rd180-happy"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.Snapshot{Name: "snap180-happy"})

	resp := httpPost(t, base+"/v1/resource-definitions/rd180-happy/snapshots", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201", resp.StatusCode)
	}

	_, err := st.Snapshots().Get(ctx, "rd180-happy", "snap180-happy")
	if err != nil {
		t.Errorf("snapshot not persisted on happy path: %v", err)
	}
}
