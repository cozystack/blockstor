// SPDX-License-Identifier: Apache-2.0

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

func deleteRD(t *testing.T, base, name string) int {
	t.Helper()

	resp := httpDelete(t, base+"/v1/resource-definitions/"+name)
	_ = resp.Body.Close()

	return resp.StatusCode
}

// The clone takes an internal snapshot on the SOURCE, and nothing reaped it:
// the target could be deleted and the snapshot stayed, so the source could
// never be deleted again through the API, which refuses a definition that has
// snapshots. Every clone from CSI goes through this path.
func TestRDDeleteReapsTheInternalCloneSnapshotFromTheSource(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-reap")

	base, stop := startServerWithStore(t, st)
	defer stop()

	if code := cloneOnce(t, base, "src-reap", "dst-reap", nil); code != http.StatusCreated {
		t.Fatalf("clone = %d, want 201", code)
	}

	if _, err := st.Snapshots().Get(ctx, "src-reap", cloneSnapshotName("dst-reap")); err != nil {
		t.Fatalf("fixture: the clone took no internal snapshot: %v", err)
	}

	if clone, err := st.ResourceDefinitions().Get(ctx, "dst-reap"); err != nil {
		t.Fatalf("read the clone: %v", err)
	} else if owner, ok := clone.Props[store.CloneSnapshotOwnerProp]; ok {
		t.Errorf("the clone carries the snapshot's owner prop %q", owner)
	}

	if code := deleteRD(t, base, "dst-reap"); code != http.StatusOK {
		t.Fatalf("delete of the clone = %d, want 200", code)
	}

	if _, err := st.Snapshots().Get(ctx, "src-reap", cloneSnapshotName("dst-reap")); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("the internal snapshot outlived the clone it backed: %v", err)
	}

	if code := deleteRD(t, base, "src-reap"); code != http.StatusOK {
		t.Errorf("delete of the source = %d, want 200: the source is undeletable while that snapshot stands", code)
	}
}

// A restore's marker names an operator's snapshot, which is somebody's data.
// Only the snapshot a clone derived from its own target name is reaped.
func TestRDDeleteLeavesTheSnapshotARestoreCameFrom(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-keep")
	seedRestoreSnapshot(t, st, "src-keep", "snap-keep", []string{"node-a"})

	base, stop := startServerWithStore(t, st)
	defer stop()

	if code := restoreOnce(t, base, "src-keep", "snap-keep",
		map[string]any{"to_resource": "dst-keep"}); code != http.StatusCreated {
		t.Fatalf("restore = %d, want 201", code)
	}

	if code := deleteRD(t, base, "dst-keep"); code != http.StatusOK {
		t.Fatalf("delete of the restored definition = %d, want 200", code)
	}

	if _, err := st.Snapshots().Get(ctx, "src-keep", "snap-keep"); err != nil {
		t.Errorf("the operator's snapshot was reaped with the restore that used it: %v", err)
	}
}

// A restore writes the same marker a clone does, and an operator can name a
// snapshot `clone-<target>` and restore it under that target. Ownership read
// off the name destroyed that snapshot with the definition.
func TestRDDeleteKeepsAnOperatorSnapshotNamedLikeTheClonesOwn(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-op")
	seedRestoreSnapshot(t, st, "src-op", cloneSnapshotName("dst-op"), []string{"node-a"})

	base, stop := startServerWithStore(t, st)
	defer stop()

	if code := restoreOnce(t, base, "src-op", cloneSnapshotName("dst-op"),
		map[string]any{"to_resource": "dst-op"}); code != http.StatusCreated {
		t.Fatalf("restore = %d, want 201", code)
	}

	if code := deleteRD(t, base, "dst-op"); code != http.StatusOK {
		t.Fatalf("delete = %d, want 200", code)
	}

	if _, err := st.Snapshots().Get(ctx, "src-op", cloneSnapshotName("dst-op")); err != nil {
		t.Errorf("the operator's snapshot was reaped because of its name: %v", err)
	}
}

// The internal snapshot is visible in `s l`, so a third definition can be
// restored from it, and that definition keeps reading it through its marker.
func TestRDDeleteKeepsTheCloneSnapshotAnotherDefinitionWasRestoredFrom(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-dep")

	base, stop := startServerWithStore(t, st)
	defer stop()

	if code := cloneOnce(t, base, "src-dep", "dst-dep", nil); code != http.StatusCreated {
		t.Fatalf("clone = %d, want 201", code)
	}

	if code := restoreOnce(t, base, "src-dep", cloneSnapshotName("dst-dep"),
		map[string]any{"to_resource": "third-dep"}); code != http.StatusCreated {
		t.Fatalf("restore from the internal snapshot = %d, want 201", code)
	}

	if code := deleteRD(t, base, "dst-dep"); code != http.StatusOK {
		t.Fatalf("delete of the clone = %d, want 200", code)
	}

	if _, err := st.Snapshots().Get(ctx, "src-dep", cloneSnapshotName("dst-dep")); err != nil {
		t.Errorf("the snapshot third-dep was restored from was reaped with the clone: %v", err)
	}
}

// A reap that was skipped or failed cannot be re-run by deleting the clone
// again, since it is gone. The refusal a delete of the source meets is the one
// place that still sees the snapshot, so it names it.
func TestRDDeleteRefusalNamesAnInternalSnapshotItsCloneLeftBehind(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-orph")

	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name:              cloneSnapshotName("gone-orph"),
		ResourceName:      "src-orph",
		Nodes:             []string{"node-a"},
		Props:             map[string]string{store.CloneSnapshotOwnerProp: "gone-orph"},
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{{VolumeNumber: 0, SizeKib: 64 * 1024}},
	}); err != nil {
		t.Fatalf("seed the orphaned snapshot: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t, base+"/v1/resource-definitions/src-orph")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("delete of the source = %d, want 409", resp.StatusCode)
	}

	var rcs []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rcs); err != nil || len(rcs) == 0 {
		t.Fatalf("decode the refusal: %v", err)
	}

	if !strings.Contains(rcs[0].Cause, cloneSnapshotName("gone-orph")) {
		t.Errorf("refusal cause %q does not name the snapshot the clone left", rcs[0].Cause)
	}
}

// A snapshot the reap kept because another definition was restored from it is
// not deletable. The refusal on the source used to call it an orphan and tell
// the operator to delete it, which takes the point-in-time from under the
// definition still restoring from it.
func TestRDDeleteRefusalNamesTheDefinitionThatKeepsACloneSnapshot(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedDeployedCloneSource(t, st, "src-use")

	base, stop := startServerWithStore(t, st)
	defer stop()

	if code := cloneOnce(t, base, "src-use", "dst-use", nil); code != http.StatusCreated {
		t.Fatalf("clone = %d, want 201", code)
	}

	if code := restoreOnce(t, base, "src-use", cloneSnapshotName("dst-use"),
		map[string]any{"to_resource": "third-use"}); code != http.StatusCreated {
		t.Fatalf("restore from the internal snapshot = %d, want 201", code)
	}

	if code := deleteRD(t, base, "dst-use"); code != http.StatusOK {
		t.Fatalf("delete of the clone = %d, want 200", code)
	}

	resp := httpDelete(t, base+"/v1/resource-definitions/src-use")
	defer func() { _ = resp.Body.Close() }()

	var rcs []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rcs); err != nil || len(rcs) == 0 {
		t.Fatalf("decode the refusal (status %d): %v", resp.StatusCode, err)
	}

	rc := rcs[0]

	if !strings.Contains(rc.Correc, "keep "+cloneSnapshotName("dst-use")+" while third-use exists") {
		t.Errorf("correction %q does not name the definition restoring from the snapshot", rc.Correc)
	}

	// The snapshot delete door does not check for dependents, so a command
	// next to "kept because" is one an operator runs.
	if strings.Contains(rc.Correc, "s d src-use "+cloneSnapshotName("dst-use")) {
		t.Errorf("correction %q tells the operator to delete a snapshot still in use", rc.Correc)
	}
}

// A snapshot a version before the owner prop took is never reaped, and the
// operator was never told which one keeps the source refused.
func TestRDDeleteRefusalNamesAnUnownedCloneSnapshot(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedDeployedCloneSource(t, st, "src-old")
	seedRestoreSnapshot(t, st, "src-old", cloneSnapshotName("gone-old"), []string{"node-a"})

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t, base+"/v1/resource-definitions/src-old")
	defer func() { _ = resp.Body.Close() }()

	var rcs []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rcs); err != nil || len(rcs) == 0 {
		t.Fatalf("decode the refusal (status %d): %v", resp.StatusCode, err)
	}

	if !strings.Contains(rcs[0].Cause, cloneSnapshotName("gone-old")) {
		t.Errorf("cause %q does not name the unowned clone snapshot", rcs[0].Cause)
	}
}

func refusalOf(t *testing.T, base, rdName string) apiv1.APICallRc {
	t.Helper()

	resp := httpDelete(t, base+"/v1/resource-definitions/"+rdName)
	defer func() { _ = resp.Body.Close() }()

	var rcs []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rcs); err != nil || len(rcs) == 0 {
		t.Fatalf("decode the refusal (status %d): %v", resp.StatusCode, err)
	}

	return rcs[0]
}

// The bucket decides what the operator is told to run: a stamped orphan gets
// the command outright, one never stamped gets it behind a warning. Both carry
// the snapshot's own name, since a placeholder pasted into a shell is a
// redirect and `s d` of a name that does not exist exits 0.
func TestRDDeleteRefusalWordsEachLeftSnapshotByItsBucket(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-bkt")

	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name: "clone-owned", ResourceName: "src-bkt",
		Props: map[string]string{store.CloneSnapshotOwnerProp: "owned"},
	}); err != nil {
		t.Fatalf("seed the stamped orphan: %v", err)
	}

	seedRestoreSnapshot(t, st, "src-bkt", "clone-legacy", []string{"node-a"})

	base, stop := startServerWithStore(t, st)
	defer stop()

	rc := refusalOf(t, base, "src-bkt")

	if !strings.Contains(rc.Correc, "`linstor s d src-bkt clone-owned`; ") ||
		strings.Contains(rc.Correc, "depends on clone-owned") {
		t.Errorf("correction %q does not give the stamped orphan its command outright", rc.Correc)
	}

	if !strings.Contains(rc.Correc,
		"if nothing of yours depends on clone-legacy, `linstor s d src-bkt clone-legacy`") {
		t.Errorf("correction %q does not put the unstamped snapshot behind a warning", rc.Correc)
	}

	if !strings.HasSuffix(rc.Correc, "then delete src-bkt again") {
		t.Errorf("correction %q does not end on the source delete", rc.Correc)
	}
}
