package rest

import (
	"errors"
	"net/http"
	"testing"

	"github.com/cozystack/blockstor/pkg/store"
)

// A restored definition reads from its snapshot until a replica is up from
// it. `s d` under a restore still running went straight through, and the
// replicas the restore had not brought up yet came up blank.
func TestSnapshotDeleteIsRefusedWhileARestoreReadsFromIt(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedDeployedCloneSource(t, st, "src-busy")

	base, stop := startServerWithStore(t, st)
	defer stop()

	if code := cloneOnce(t, base, "src-busy", "dst-busy", nil); code != http.StatusCreated {
		t.Fatalf("clone = %d, want 201", code)
	}

	snap := cloneSnapshotName("dst-busy")

	resp := httpDelete(t, base+"/v1/resource-definitions/src-busy/snapshots/"+snap)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("snapshot delete under an unfinished restore = %d, want 409", resp.StatusCode)
	}

	if _, err := st.Snapshots().Get(t.Context(), "src-busy", snap); err != nil {
		t.Fatalf("the refused delete took the snapshot: %v", err)
	}
}

// Once the restore is done the dependency ends: the delete takes the marker
// off the restored definition first, so its later replicas sync from their
// peers instead of looking for a snapshot that is gone.
func TestSnapshotDeleteReleasesAFinishedRestore(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedDeployedCloneSource(t, st, "src-done")

	base, stop := startServerWithStore(t, st)
	defer stop()

	if code := cloneOnce(t, base, "src-done", "dst-done", nil); code != http.StatusCreated {
		t.Fatalf("clone = %d, want 201", code)
	}

	reportVolumesUpToDate(t, st, "dst-done")

	snap := cloneSnapshotName("dst-done")

	resp := httpDelete(t, base+"/v1/resource-definitions/src-done/snapshots/"+snap)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("snapshot delete after the restore finished = %d, want 200", resp.StatusCode)
	}

	rd, err := st.ResourceDefinitions().Get(t.Context(), "dst-done")
	if err != nil {
		t.Fatalf("read back: %v", err)
	}

	if mark, ok := rd.Props[store.RestoreFromSnapshotProp]; ok {
		t.Errorf("the restored definition still names the deleted snapshot: %q", mark)
	}

	if _, err := st.Snapshots().Get(t.Context(), "src-done", snap); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("the snapshot is still there: err=%v", err)
	}
}
