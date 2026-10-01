// SPDX-License-Identifier: Apache-2.0

package rest

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// A restore writes the same marker a clone does, and an operator can name a
// snapshot `clone-<target>` and restore it under that target. Ownership read
// off the name destroyed that snapshot with the definition.
func TestRDDeleteKeepsAnOperatorSnapshotNamedLikeTheClonesOwn(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-op9")
	seedRestoreSnapshot(t, st, "src-op9", cloneSnapshotName("dst-op9"), []string{"node-a"})

	base, stop := startServerWithStore(t, st)
	defer stop()

	if code := restoreOnce(t, base, "src-op9", cloneSnapshotName("dst-op9"),
		map[string]any{"to_resource": "dst-op9"}); code != http.StatusCreated {
		t.Fatalf("restore = %d, want 201", code)
	}

	if code := deleteRD(t, base, "dst-op9"); code != http.StatusOK {
		t.Fatalf("delete = %d, want 200", code)
	}

	if _, err := st.Snapshots().Get(ctx, "src-op9", cloneSnapshotName("dst-op9")); err != nil {
		t.Errorf("the operator's snapshot was reaped because of its name: %v", err)
	}
}

// The internal snapshot is visible in `s l`, so a third definition can be
// restored from it, and that definition keeps reading it through its marker.
func TestRDDeleteKeepsTheCloneSnapshotAnotherDefinitionWasRestoredFrom(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-dep9")

	base, stop := startServerWithStore(t, st)
	defer stop()

	if code := cloneOnce(t, base, "src-dep9", "dst-dep9", nil); code != http.StatusCreated {
		t.Fatalf("clone = %d, want 201", code)
	}

	if code := restoreOnce(t, base, "src-dep9", cloneSnapshotName("dst-dep9"),
		map[string]any{"to_resource": "third-dep9"}); code != http.StatusCreated {
		t.Fatalf("restore from the internal snapshot = %d, want 201", code)
	}

	if code := deleteRD(t, base, "dst-dep9"); code != http.StatusOK {
		t.Fatalf("delete of the clone = %d, want 200", code)
	}

	if _, err := st.Snapshots().Get(ctx, "src-dep9", cloneSnapshotName("dst-dep9")); err != nil {
		t.Errorf("the snapshot third-dep9 was restored from was reaped with the clone: %v", err)
	}
}

// A reap that was skipped or failed cannot be re-run by deleting the clone
// again, since it is gone. The refusal a delete of the source meets is the one
// place that still sees the snapshot, so it names it.
func TestRDDeleteRefusalNamesAnInternalSnapshotItsCloneLeftBehind(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-orph9")

	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name:              cloneSnapshotName("gone-orph9"),
		ResourceName:      "src-orph9",
		Nodes:             []string{"node-a"},
		Props:             map[string]string{store.CloneSnapshotOwnerProp: "gone-orph9"},
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{{VolumeNumber: 0, SizeKib: 64 * 1024}},
	}); err != nil {
		t.Fatalf("seed the orphaned snapshot: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t, base+"/v1/resource-definitions/src-orph9")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("delete of the source = %d, want 409", resp.StatusCode)
	}

	var rcs []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rcs); err != nil || len(rcs) == 0 {
		t.Fatalf("decode the refusal: %v", err)
	}

	if !strings.Contains(rcs[0].Cause, cloneSnapshotName("gone-orph9")) {
		t.Errorf("refusal cause %q does not name the snapshot the clone left", rcs[0].Cause)
	}
}

// A volume-less clone takes no snapshot, so the derived name's ceiling does not
// apply to it, and a name `rd create` accepts has to stay possible.
func TestRDCloneAppliesTheSnapshotNameCeilingOnlyWhereASnapshotIsTaken(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedDeployedCloneSource(t, st, "src-ceil9")

	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "shell-ceil9"}); err != nil {
		t.Fatalf("seed the volume-less source: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	long := "d" + strings.Repeat("x", 42)

	if code := cloneOnce(t, base, "shell-ceil9", long, nil); code != http.StatusCreated {
		t.Errorf("volume-less clone into a %d-char name = %d, want 201", len(long), code)
	}

	if code := cloneOnce(t, base, "src-ceil9", long+"y", nil); code != http.StatusBadRequest {
		t.Errorf("data-path clone whose snapshot name passes the ceiling = %d, want 400", code)
	}
}

// Every existing fixture for the DELETE refusal on the restore door seeds a
// leftover with no volumes, so a deeper check gives the same 409 and the term
// never decides. With volumes and a replica, the replay would answer 201 over
// a definition being deleted.
func TestSnapshotRestoreReplayRefusesAFinishedLeftoverBeingDeleted(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedDeployedCloneSource(t, st, "src-del9")
	seedRestoreSnapshot(t, st, "src-del9", "snap-del9", []string{"node-a"})

	base, stop := startServerWithStore(t, st)
	defer stop()

	body := map[string]any{"to_resource": "dst-del9", "node_names": []string{"node-a"}}

	if code := restoreOnce(t, base, "src-del9", "snap-del9", body); code != http.StatusCreated {
		t.Fatalf("first restore = %d, want 201", code)
	}

	updateSource(t, st, "dst-del9", func(rd *apiv1.ResourceDefinition) {
		rd.Flags = append(rd.Flags, rdFlagDelete)
	})

	if code := restoreOnce(t, base, "src-del9", "snap-del9", body); code != http.StatusConflict {
		t.Errorf("replay over a finished restore being deleted = %d, want 409", code)
	}
}
