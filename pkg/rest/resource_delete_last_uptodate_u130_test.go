// SPDX-License-Identifier: Apache-2.0

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

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// U130 — `r d` of the last UpToDate diskful while a peer is mid-sync
// must be REJECTED with a structured 409 envelope. These L1 tests pin
// both the refusal (the bug) and the must-NOT-regress cases:
//   - no peer mid-sync → last diskful delete proceeds (bug-hunt #6)
//   - another UpToDate diskful survives → delete proceeds
//   - diskless/tiebreaker target → never guarded
//   - ?force=true → override bypasses the guard

func seedU130RD(t *testing.T, st store.Store, rd string) {
	t.Helper()

	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: rd}); err != nil {
		t.Fatalf("seed RD %s: %v", rd, err)
	}
}

func diskfulReplica(rd, node, diskState string) *apiv1.Resource {
	return &apiv1.Resource{
		Name:     rd,
		NodeName: node,
		Volumes: []apiv1.Volume{{
			VolumeNumber: 0,
			State:        apiv1.VolumeState{DiskState: diskState},
		}},
	}
}

func disklessReplica(rd, node string, flag string) *apiv1.Resource {
	return &apiv1.Resource{
		Name:     rd,
		NodeName: node,
		Flags:    []string{flag},
	}
}

// syncSourceUnstampedReplica is the BUG-045 shape: a diskful replica
// that is actively feeding a peer's resync (SyncSource replication-state
// toward `peer`) but whose local diskState projection is blank — the
// satellite observer's device-frame `disk:UpToDate` was not re-stamped
// on the Secondary SyncSource. The kernel ground truth (SyncSource) says
// it IS an UpToDate source; the guard must trust that, not the empty
// diskState.
func syncSourceUnstampedReplica(rd, node, peer string) *apiv1.Resource {
	return &apiv1.Resource{
		Name:     rd,
		NodeName: node,
		Volumes: []apiv1.Volume{{
			VolumeNumber: 0,
			State: apiv1.VolumeState{
				DiskState: "", // BUG-045: projection left it blank
				ReplicationStates: map[string]apiv1.ReplicationState{
					peer: {ReplicationState: drbdReplStateSyncSource},
				},
			},
		}},
	}
}

// TestU130RejectsDeleteOfLastUpToDateWhilePeerSyncing is the core
// repro: 1 diskful UpToDate source + 1 diskful SyncTarget (mid-sync) +
// 1 diskless Primary. Deleting the UpToDate source must be refused.
func TestU130RejectsDeleteOfLastUpToDateWhilePeerSyncing(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()
	rd := "pvc-u130"

	seedU130RD(t, st, rd)

	// n1: original diskful, the only UpToDate source.
	if err := st.Resources().Create(ctx, diskfulReplica(rd, "n1", drbdDiskStateUpToDate)); err != nil {
		t.Fatalf("seed n1: %v", err)
	}
	// n2: freshly-added second diskful, still catching up.
	if err := st.Resources().Create(ctx, diskfulReplica(rd, "n2", drbdDiskStateSyncTarget)); err != nil {
		t.Fatalf("seed n2: %v", err)
	}
	// n3: diskless Primary/InUse holding the resource open.
	if err := st.Resources().Create(ctx, disklessReplica(rd, "n3", apiv1.ResourceFlagDiskless)); err != nil {
		t.Fatalf("seed n3: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t, base+"/v1/resource-definitions/"+rd+"/resources/n1")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status: got %d, want 409 (U130 refusal)", resp.StatusCode)
	}

	var rc []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rc); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}

	if len(rc) == 0 || rc[0].RetCode&apiCallRcError == 0 {
		t.Fatalf("expected MASK_ERROR envelope, got %+v", rc)
	}

	if !strings.Contains(rc[0].Message, "last UpToDate diskful") {
		t.Errorf("message %q missing 'last UpToDate diskful' marker", rc[0].Message)
	}

	// The replica MUST still exist — the refusal blocked the delete.
	if _, err := st.Resources().Get(ctx, rd, "n1"); err != nil {
		t.Errorf("n1 must survive the refused delete; got err=%v", err)
	}
}

// TestU130RejectsViaPeerReplicationStateSyncTarget pins that the guard
// also fires when the mid-sync signal is the per-peer replication
// state (SyncTarget toward a peer) rather than the local disk_state.
func TestU130RejectsViaPeerReplicationStateSyncTarget(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()
	rd := "pvc-u130-repl"

	seedU130RD(t, st, rd)

	if err := st.Resources().Create(ctx, diskfulReplica(rd, "n1", drbdDiskStateUpToDate)); err != nil {
		t.Fatalf("seed n1: %v", err)
	}

	// n2 reports its local disk as Outdated but a SyncTarget
	// replication-state toward n1 — still mid-sync, still stranded.
	n2 := &apiv1.Resource{
		Name:     rd,
		NodeName: "n2",
		Volumes: []apiv1.Volume{{
			VolumeNumber: 0,
			State: apiv1.VolumeState{
				DiskState: "Outdated",
				ReplicationStates: map[string]apiv1.ReplicationState{
					"n1": {ReplicationState: drbdReplStateSyncTarget},
				},
			},
		}},
	}
	if err := st.Resources().Create(ctx, n2); err != nil {
		t.Fatalf("seed n2: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t, base+"/v1/resource-definitions/"+rd+"/resources/n1")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status: got %d, want 409 (U130 refusal via repl-state)", resp.StatusCode)
	}
}

// TestU130RejectsSyncSourceTargetWithBlankDiskState is the BUG-045
// repro: the deleted target is the SOURCE replica (a Secondary feeding
// the resync) whose CRD diskState projection is BLANK because the
// satellite observer never re-stamped `disk:UpToDate` on the Secondary
// SyncSource. Its SyncSource replication-state toward the mid-sync peer
// is kernel ground truth that it holds an UpToDate copy. The guard must
// NOT trust the blank diskState and conclude "not a source" — it must
// refuse the delete that would strand the SyncTarget.
func TestU130RejectsSyncSourceTargetWithBlankDiskState(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()
	rd := "pvc-u130-bug045"

	seedU130RD(t, st, rd)

	// n1: the source being deleted — Secondary SyncSource, blank
	// diskState (the BUG-045 unstamped-projection window).
	if err := st.Resources().Create(ctx, syncSourceUnstampedReplica(rd, "n1", "n2")); err != nil {
		t.Fatalf("seed n1: %v", err)
	}
	// n2: freshly-added second diskful, still catching up.
	if err := st.Resources().Create(ctx, diskfulReplica(rd, "n2", drbdDiskStateSyncTarget)); err != nil {
		t.Fatalf("seed n2: %v", err)
	}
	// n3: diskless Primary/InUse holding the resource open.
	if err := st.Resources().Create(ctx, disklessReplica(rd, "n3", apiv1.ResourceFlagDiskless)); err != nil {
		t.Fatalf("seed n3: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t, base+"/v1/resource-definitions/"+rd+"/resources/n1")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status: got %d, want 409 (BUG-045: blank-diskState SyncSource must still be refused)", resp.StatusCode)
	}

	if _, err := st.Resources().Get(ctx, rd, "n1"); err != nil {
		t.Errorf("n1 must survive the refused delete; got err=%v", err)
	}
}

// TestU130RejectsBlankDiskStateTargetMidSync pins the fail-safe widening
// independent of any replication-state signal on the target: a diskful
// target with a completely UNKNOWN diskState (no SyncSource token either
// — e.g. the projection has not landed any field yet) while a diskful
// peer is mid-sync must be refused. Refusing in doubt is the load-bearing
// data-safety property; a false allow strands the SyncTarget.
func TestU130RejectsBlankDiskStateTargetMidSync(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()
	rd := "pvc-u130-blank"

	seedU130RD(t, st, rd)

	// n1: diskful target with an entirely blank state surface.
	if err := st.Resources().Create(ctx, diskfulReplica(rd, "n1", "")); err != nil {
		t.Fatalf("seed n1: %v", err)
	}
	if err := st.Resources().Create(ctx, diskfulReplica(rd, "n2", drbdDiskStateSyncTarget)); err != nil {
		t.Fatalf("seed n2: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t, base+"/v1/resource-definitions/"+rd+"/resources/n1")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status: got %d, want 409 (fail-safe: blank-diskState target mid-sync must be refused)", resp.StatusCode)
	}
}

// TestU130AllowsDeleteWhenSyncSourceSiblingSurvives pins that a SyncSource
// SIBLING with a blank diskState counts as a surviving UpToDate source:
// deleting one UpToDate diskful is allowed because the SyncSource peer
// keeps feeding the SyncTarget. Without crediting the SyncSource sibling
// the guard would over-refuse a legitimate delete (must-not-regress).
func TestU130AllowsDeleteWhenSyncSourceSiblingSurvives(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()
	rd := "pvc-u130-srcsib"

	seedU130RD(t, st, rd)

	// n1: the delete target — plainly UpToDate.
	if err := st.Resources().Create(ctx, diskfulReplica(rd, "n1", drbdDiskStateUpToDate)); err != nil {
		t.Fatalf("seed n1: %v", err)
	}
	// n2: a SyncSource sibling (blank diskState) — kernel-confirmed
	// surviving source that keeps feeding n3.
	if err := st.Resources().Create(ctx, syncSourceUnstampedReplica(rd, "n2", "n3")); err != nil {
		t.Fatalf("seed n2: %v", err)
	}
	// n3: the SyncTarget being fed by n2.
	if err := st.Resources().Create(ctx, diskfulReplica(rd, "n3", drbdDiskStateSyncTarget)); err != nil {
		t.Fatalf("seed n3: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t, base+"/v1/resource-definitions/"+rd+"/resources/n1")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200 (SyncSource sibling survives as source)", resp.StatusCode)
	}
}

// TestU130AllowsDeleteWhenNoPeerSyncing pins bug-hunt #6: when NO peer
// is mid-sync, deleting the last diskful is allowed (matches upstream).
// Here n1 is the only diskful; n2 is a plain diskless (not syncing).
func TestU130AllowsDeleteWhenNoPeerSyncing(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()
	rd := "pvc-u130-nosync"

	seedU130RD(t, st, rd)

	if err := st.Resources().Create(ctx, diskfulReplica(rd, "n1", drbdDiskStateUpToDate)); err != nil {
		t.Fatalf("seed n1: %v", err)
	}
	if err := st.Resources().Create(ctx, disklessReplica(rd, "n2", apiv1.ResourceFlagDiskless)); err != nil {
		t.Fatalf("seed n2: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t, base+"/v1/resource-definitions/"+rd+"/resources/n1")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200 (no-sync last-diskful delete allowed)", resp.StatusCode)
	}

	if _, err := st.Resources().Get(ctx, rd, "n1"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("n1 should be physically removed; got err=%v (want ErrNotFound)", err)
	}
}

// TestU130AllowsDeleteWhenAnotherUpToDateDiskfulSurvives pins that a
// 2-UpToDate + 1-SyncTarget shape still permits dropping one UpToDate
// source — the SyncTarget keeps a valid source after the delete.
func TestU130AllowsDeleteWhenAnotherUpToDateDiskfulSurvives(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()
	rd := "pvc-u130-twoutd"

	seedU130RD(t, st, rd)

	for _, n := range []string{"n1", "n2"} {
		if err := st.Resources().Create(ctx, diskfulReplica(rd, n, drbdDiskStateUpToDate)); err != nil {
			t.Fatalf("seed %s: %v", n, err)
		}
	}
	if err := st.Resources().Create(ctx, diskfulReplica(rd, "n3", drbdDiskStateSyncTarget)); err != nil {
		t.Fatalf("seed n3: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t, base+"/v1/resource-definitions/"+rd+"/resources/n1")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200 (another UpToDate diskful survives)", resp.StatusCode)
	}
}

// TestU130AllowsDeleteOfDisklessTargetEvenMidSync pins that the guard
// is diskful-only: deleting a DISKLESS or TIE_BREAKER replica is never
// blocked, even with a SyncTarget peer in flight (it holds no source).
func TestU130AllowsDeleteOfDisklessTargetEvenMidSync(t *testing.T) {
	for _, flag := range []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker} {
		t.Run(flag, func(t *testing.T) {
			st := store.NewInMemory()
			ctx := t.Context()
			rd := "pvc-u130-dl"

			seedU130RD(t, st, rd)

			if err := st.Resources().Create(ctx, diskfulReplica(rd, "n1", drbdDiskStateUpToDate)); err != nil {
				t.Fatalf("seed n1: %v", err)
			}
			if err := st.Resources().Create(ctx, diskfulReplica(rd, "n2", drbdDiskStateSyncTarget)); err != nil {
				t.Fatalf("seed n2: %v", err)
			}
			if err := st.Resources().Create(ctx, disklessReplica(rd, "n3", flag)); err != nil {
				t.Fatalf("seed n3: %v", err)
			}

			base, stop := startServerWithStore(t, st)
			defer stop()

			resp := httpDelete(t, base+"/v1/resource-definitions/"+rd+"/resources/n3")
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != http.StatusOK {
				t.Fatalf("%s target: got %d, want 200 (diskless delete never guarded)", flag, resp.StatusCode)
			}
		})
	}
}

// TestU130ForceOverridesGuard pins the escape hatch: `?force=true`
// bypasses the refusal so an operator can acknowledge the risk.
func TestU130ForceOverridesGuard(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()
	rd := "pvc-u130-force"

	seedU130RD(t, st, rd)

	if err := st.Resources().Create(ctx, diskfulReplica(rd, "n1", drbdDiskStateUpToDate)); err != nil {
		t.Fatalf("seed n1: %v", err)
	}
	if err := st.Resources().Create(ctx, diskfulReplica(rd, "n2", drbdDiskStateSyncTarget)); err != nil {
		t.Fatalf("seed n2: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t, base+"/v1/resource-definitions/"+rd+"/resources/n1?force=true")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200 (force overrides U130 guard)", resp.StatusCode)
	}

	if _, err := st.Resources().Get(ctx, rd, "n1"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("n1 should be removed under force; got err=%v (want ErrNotFound)", err)
	}
}

// TestU130PredicateUnit exercises the pure refusal predicate directly
// so the decision matrix is pinned independent of the HTTP wiring.
func TestU130PredicateUnit(t *testing.T) {
	rd := "r"

	cases := []struct {
		name     string
		target   *apiv1.Resource
		siblings []*apiv1.Resource
		refuse   bool
	}{
		{
			name:   "last-uptodate-with-synctarget-peer",
			target: diskfulReplica(rd, "n1", drbdDiskStateUpToDate),
			siblings: []*apiv1.Resource{
				diskfulReplica(rd, "n1", drbdDiskStateUpToDate),
				diskfulReplica(rd, "n2", drbdDiskStateSyncTarget),
			},
			refuse: true,
		},
		{
			name:   "last-uptodate-with-inconsistent-peer",
			target: diskfulReplica(rd, "n1", drbdDiskStateUpToDate),
			siblings: []*apiv1.Resource{
				diskfulReplica(rd, "n1", drbdDiskStateUpToDate),
				diskfulReplica(rd, "n2", drbdDiskStateInconsistent),
			},
			refuse: true,
		},
		{
			name:   "no-sync-in-flight",
			target: diskfulReplica(rd, "n1", drbdDiskStateUpToDate),
			siblings: []*apiv1.Resource{
				diskfulReplica(rd, "n1", drbdDiskStateUpToDate),
				disklessReplica(rd, "n2", apiv1.ResourceFlagDiskless),
			},
			refuse: false,
		},
		{
			name:   "another-uptodate-survives",
			target: diskfulReplica(rd, "n1", drbdDiskStateUpToDate),
			siblings: []*apiv1.Resource{
				diskfulReplica(rd, "n1", drbdDiskStateUpToDate),
				diskfulReplica(rd, "n2", drbdDiskStateUpToDate),
				diskfulReplica(rd, "n3", drbdDiskStateSyncTarget),
			},
			refuse: false,
		},
		{
			name:   "target-not-uptodate",
			target: diskfulReplica(rd, "n1", drbdDiskStateInconsistent),
			siblings: []*apiv1.Resource{
				diskfulReplica(rd, "n1", drbdDiskStateInconsistent),
				diskfulReplica(rd, "n2", drbdDiskStateSyncTarget),
			},
			refuse: false,
		},
		{
			// BUG-045: the source's diskState is blank but it is a
			// SyncSource feeding the mid-sync peer — must refuse.
			name:   "syncsource-target-blank-diskstate",
			target: syncSourceUnstampedReplica(rd, "n1", "n2"),
			siblings: []*apiv1.Resource{
				syncSourceUnstampedReplica(rd, "n1", "n2"),
				diskfulReplica(rd, "n2", drbdDiskStateSyncTarget),
			},
			refuse: true,
		},
		{
			// Fail-safe: an entirely blank-state diskful target while a
			// peer is mid-sync — refuse in doubt.
			name:   "blank-diskstate-target-midsync",
			target: diskfulReplica(rd, "n1", ""),
			siblings: []*apiv1.Resource{
				diskfulReplica(rd, "n1", ""),
				diskfulReplica(rd, "n2", drbdDiskStateSyncTarget),
			},
			refuse: true,
		},
		{
			// A blank-state diskful target with NO peer mid-sync is the
			// genuine last-copy case — still allowed (no regression).
			name:   "blank-diskstate-target-no-sync",
			target: diskfulReplica(rd, "n1", ""),
			siblings: []*apiv1.Resource{
				diskfulReplica(rd, "n1", ""),
				disklessReplica(rd, "n2", apiv1.ResourceFlagDiskless),
			},
			refuse: false,
		},
		{
			// A SyncSource sibling (blank diskState) is a confirmed
			// surviving source — dropping one UpToDate copy is allowed.
			name:   "syncsource-sibling-survives",
			target: diskfulReplica(rd, "n1", drbdDiskStateUpToDate),
			siblings: []*apiv1.Resource{
				diskfulReplica(rd, "n1", drbdDiskStateUpToDate),
				syncSourceUnstampedReplica(rd, "n2", "n3"),
				diskfulReplica(rd, "n3", drbdDiskStateSyncTarget),
			},
			refuse: false,
		},
		{
			name:   "diskless-target",
			target: disklessReplica(rd, "n3", apiv1.ResourceFlagDiskless),
			siblings: []*apiv1.Resource{
				diskfulReplica(rd, "n1", drbdDiskStateUpToDate),
				diskfulReplica(rd, "n2", drbdDiskStateSyncTarget),
				disklessReplica(rd, "n3", apiv1.ResourceFlagDiskless),
			},
			refuse: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sibs := make([]apiv1.Resource, len(tc.siblings))
			for i := range tc.siblings {
				sibs[i] = *tc.siblings[i]
			}

			got := resourceMidSyncDeleteRefusal(tc.target, sibs)
			if got != tc.refuse {
				t.Errorf("resourceMidSyncDeleteRefusal = %v, want %v", got, tc.refuse)
			}
		})
	}
}
