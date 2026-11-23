// SPDX-License-Identifier: Apache-2.0

package rest

import (
	"net/http"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Deleting the snapshot once the restore finished is ordinary cleanup, and a
// retry after it answered 404: the replay was reached only through a snapshot
// read. A finished target is judged against the volumes it recorded.
func TestSnapshotRestoreReplayOverAFinishedTargetSurvivesTheSnapshot(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-gone")
	seedRestoreSnapshot(t, st, "src-gone", "snap-gone", []string{"node-a"})

	base, stop := startServerWithStore(t, st)
	defer stop()

	body := map[string]any{"to_resource": "dst-gone", "node_names": []string{"node-a"}}
	if code := restoreOnce(t, base, "src-gone", "snap-gone", body); code != http.StatusCreated {
		t.Fatalf("first restore = %d, want 201", code)
	}

	if err := st.Snapshots().Delete(ctx, "src-gone", "snap-gone"); err != nil {
		t.Fatalf("delete the snapshot: %v", err)
	}

	before, _ := st.Resources().ListByDefinition(ctx, "dst-gone")

	code, rc := restoreAnswer(t, base, "src-gone", "snap-gone", body)
	if code != http.StatusCreated {
		t.Fatalf("replay with the snapshot gone = %d %q, want 201", code, rc.Message)
	}

	after, _ := st.Resources().ListByDefinition(ctx, "dst-gone")
	if len(after) != len(before) {
		t.Errorf("the replay placed replicas: %d before, %d after", len(before), len(after))
	}
}

// An unfinished target cannot be finished from a snapshot that is gone, and
// the refusal names the way out.
func TestSnapshotRestoreRefusesAnUnfinishedTargetWhoseSnapshotIsGone(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-half")

	snap := apiv1.Snapshot{
		Name: "snap-half", ResourceName: "src-half",
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{{VolumeNumber: 0, SizeKib: 64 * 1024}},
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: "dst-half", Props: store.WithRestoreMarker(nil, &snap),
	}); err != nil {
		t.Fatalf("seed the unfinished target: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	code, rc := restoreAnswer(t, base, "src-half", "snap-half", map[string]any{"to_resource": "dst-half"})
	if code != http.StatusNotFound {
		t.Fatalf("restore over an unfinished target with the snapshot gone = %d %q, want 404", code, rc.Message)
	}

	if !strings.Contains(rc.Correc, "restore it from another snapshot") {
		t.Errorf("correction %q does not name the way out", rc.Correc)
	}

	if vds, _ := st.VolumeDefinitions().List(ctx, "dst-half"); len(vds) != 0 {
		t.Errorf("the refused restore hydrated %d volume(s)", len(vds))
	}
}

// A target restored before the volumes were recorded keeps the 404 it got:
// there is nothing to judge it against.
func TestSnapshotRestoreWithoutTheSnapshotKeepsALegacyTargetA404(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-legacy")
	seedRestoreLeftover(t, st, "src-legacy", "snap-legacy", "dst-legacy", nil)

	// Whole by its own volumes: judged without a record it would read as a
	// finished restore and answer 201 over volumes nothing vouches for.
	if err := st.VolumeDefinitions().Create(ctx, "dst-legacy",
		&apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 64 * 1024}); err != nil {
		t.Fatalf("seed the legacy volume: %v", err)
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{Name: "dst-legacy", NodeName: "node-a"}); err != nil {
		t.Fatalf("seed the legacy replica: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	if code, rc := restoreAnswer(t, base, "src-legacy", "snap-legacy",
		map[string]any{"to_resource": "dst-legacy"}); code != http.StatusNotFound {
		t.Errorf("restore over a legacy target with the snapshot gone = %d %q, want 404", code, rc.Message)
	}
}

// seedFinishedTargetWithoutSnapshot seeds a target restored from snapName,
// finished (its volumes and a live replica), with the snapshot already gone.
func seedFinishedTargetWithoutSnapshot(t *testing.T, st store.Store, src, snapName, target string, flags []string) {
	t.Helper()

	ctx := t.Context()
	snap := apiv1.Snapshot{
		Name: snapName, ResourceName: src,
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{{VolumeNumber: 0, SizeKib: 64 * 1024}},
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: target, Props: store.WithRestoreMarker(nil, &snap), Flags: flags,
	}); err != nil {
		t.Fatalf("seed the target: %v", err)
	}

	if err := st.VolumeDefinitions().Create(ctx, target,
		&apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 64 * 1024}); err != nil {
		t.Fatalf("seed the target's volume: %v", err)
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{Name: target, NodeName: "node-a"}); err != nil {
		t.Fatalf("seed the target's replica: %v", err)
	}
}

// A finished target being deleted is not a replay to report done, snapshot or
// no snapshot.
func TestSnapshotRestoreWithoutSnapshotRefusesATargetBeingDeleted(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedDeployedCloneSource(t, st, "src-del")
	seedFinishedTargetWithoutSnapshot(t, st, "src-del", "snap-del", "dst-del", []string{rdFlagDelete})

	base, stop := startServerWithStore(t, st)
	defer stop()

	code, rc := restoreAnswer(t, base, "src-del", "snap-del", map[string]any{"to_resource": "dst-del"})
	if code != http.StatusConflict {
		t.Fatalf("replay over a target being deleted = %d %q, want 409", code, rc.Message)
	}

	if rc.RetCode&apiCallRcFailExistsRscDfn != apiCallRcFailExistsRscDfn {
		t.Errorf("refusal ret_code %#x carries no FAIL_EXISTS_RSC_DFN band", rc.RetCode)
	}
}

// A target restored from another snapshot is not this restore's replay: it
// would report a restore that never happened as done.
func TestSnapshotRestoreWithoutSnapshotIgnoresATargetOfAnotherSnapshot(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedDeployedCloneSource(t, st, "src-other")
	seedFinishedTargetWithoutSnapshot(t, st, "src-other", "snap-b", "dst-other", nil)

	base, stop := startServerWithStore(t, st)
	defer stop()

	code, rc := restoreAnswer(t, base, "src-other", "snap-a", map[string]any{"to_resource": "dst-other"})
	if code != http.StatusNotFound {
		t.Errorf("restore of a gone snapshot over another snapshot's target = %d %q, want 404", code, rc.Message)
	}
}

// Whether the target is a finished replay cannot be told when it cannot be
// read, and the snapshot's 404 would read as permanent.
func TestSnapshotRestoreWithoutSnapshotAnswersAFailedTargetReadAsRetryable(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedDeployedCloneSource(t, backend, "src-rd")
	seedFinishedTargetWithoutSnapshot(t, backend, "src-rd", "snap-rd", "dst-rd", nil)

	base, stop := startServerWithStore(t, faultyStore{backend, storeFaults{
		target: "dst-rd", rdGetUncached: errStoreFault,
	}})
	defer stop()

	code, rc := restoreAnswer(t, base, "src-rd", "snap-rd", map[string]any{"to_resource": "dst-rd"})
	if code != http.StatusInternalServerError {
		t.Fatalf("replay whose target cannot be read = %d %q, want 500", code, rc.Message)
	}

	if !strings.Contains(rc.Correc, "retry") {
		t.Errorf("correction %q does not say to retry", rc.Correc)
	}
}

// A bare restore is finished without a replica only while the snapshot its
// placement would restore from exists. Once that is gone a shell with no
// replica holds its data nowhere, and the replay answered 201 over it while
// the CLI, judging the same definition, refused it. One placed since keeps its
// 201.
func TestSnapshotRestoreReplayOfABareRestoreNeedsAReplicaOnceTheSnapshotIsGone(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		replica bool
	}{
		{name: "no-replica"},
		{name: "placed-since", replica: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			st := store.NewInMemory()
			ctx := t.Context()
			seedDeployedCloneSource(t, st, "src-bare")
			seedRestoreSnapshot(t, st, "src-bare", "snap-bare", []string{"node-a"})

			base, stop := startServerWithStore(t, st)
			defer stop()

			body := map[string]any{"to_resource": "dst-bare"}
			if code := restoreOnce(t, base, "src-bare", "snap-bare", body); code != http.StatusCreated {
				t.Fatalf("bare restore = %d, want 201", code)
			}

			if tc.replica {
				if err := st.Resources().Create(ctx, &apiv1.Resource{Name: "dst-bare", NodeName: "node-a"}); err != nil {
					t.Fatalf("place a replica: %v", err)
				}
			}

			if err := st.Snapshots().Delete(ctx, "src-bare", "snap-bare"); err != nil {
				t.Fatalf("delete the snapshot: %v", err)
			}

			code, rc := restoreAnswer(t, base, "src-bare", "snap-bare", body)

			if tc.replica {
				if code != http.StatusCreated {
					t.Errorf("replay of a bare restore placed since = %d %q, want 201", code, rc.Message)
				}

				return
			}

			if code != http.StatusNotFound || !strings.Contains(rc.Correc, "restore it from another snapshot") {
				t.Errorf("replay of a bare restore with no replica and no snapshot = %d %q / %q, want 404 naming the way out",
					code, rc.Message, rc.Correc)
			}
		})
	}
}

// A restore being torn down once its snapshot is gone cannot be restored
// again after the tear-down: there is nothing left to restore it from, so the
// refusal says to restore it from another snapshot rather than to wait and
// retry into the snapshot-gone refusal.
func TestSnapshotRestoreTearDownWithTheSnapshotGoneNamesAnotherSnapshot(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-tdown")
	seedRestoreSnapshot(t, st, "src-tdown", "snap-tdown", []string{"node-a"})

	base, stop := startServerWithStore(t, st)
	defer stop()

	body := map[string]any{"to_resource": "dst-tdown"}
	if code := restoreOnce(t, base, "src-tdown", "snap-tdown", body); code != http.StatusCreated {
		t.Fatalf("bare restore = %d, want 201", code)
	}

	seedTerminatingReplica(t, st, "dst-tdown", "node-a")

	if err := st.Snapshots().Delete(ctx, "src-tdown", "snap-tdown"); err != nil {
		t.Fatalf("delete the snapshot: %v", err)
	}

	code, rc := restoreAnswer(t, base, "src-tdown", "snap-tdown", body)
	if code != http.StatusConflict || !strings.Contains(rc.Correc, "restore it from another snapshot") {
		t.Errorf("replay over a tear-down with the snapshot gone = %d %q / %q, want 409 naming another snapshot",
			code, rc.Message, rc.Correc)
	}
}

// A leftover holding a volume the snapshot never recorded is refused, and once
// the snapshot is gone the refusal does not say to delete it and "restore
// again": that restore would meet nothing to restore from.
func TestSnapshotRestoreForeignLeftoverWithTheSnapshotGoneNamesAnotherSnapshot(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-fgone")
	seedRestoreSnapshot(t, st, "src-fgone", "snap-fgone", []string{"node-a"})

	base, stop := startServerWithStore(t, st)
	defer stop()

	body := map[string]any{"to_resource": "dst-fgone"}
	if code := restoreOnce(t, base, "src-fgone", "snap-fgone", body); code != http.StatusCreated {
		t.Fatalf("bare restore = %d, want 201", code)
	}

	if err := st.VolumeDefinitions().Create(ctx, "dst-fgone",
		&apiv1.VolumeDefinition{VolumeNumber: 1, SizeKib: 64 * 1024}); err != nil {
		t.Fatalf("add a volume the snapshot never recorded: %v", err)
	}

	if err := st.Snapshots().Delete(ctx, "src-fgone", "snap-fgone"); err != nil {
		t.Fatalf("delete the snapshot: %v", err)
	}

	code, rc := restoreAnswer(t, base, "src-fgone", "snap-fgone", body)
	if code != http.StatusConflict || !strings.Contains(rc.Correc, "another snapshot") ||
		strings.Contains(rc.Correc, "restore again") {
		t.Errorf("replay over a foreign leftover with the snapshot gone = %d %q / %q, want 409 naming another snapshot",
			code, rc.Message, rc.Correc)
	}
}
