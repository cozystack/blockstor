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

// A snapshot the reap kept because another definition was restored from it is
// not deletable. The refusal on the source used to call it an orphan and tell
// the operator to delete it, which takes the point-in-time from under the
// definition still restoring from it.
func TestRDDeleteRefusalNamesTheDefinitionThatKeepsACloneSnapshot(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedDeployedCloneSource(t, st, "src-use10")

	base, stop := startServerWithStore(t, st)
	defer stop()

	if code := cloneOnce(t, base, "src-use10", "dst-use10", nil); code != http.StatusCreated {
		t.Fatalf("clone = %d, want 201", code)
	}

	if code := restoreOnce(t, base, "src-use10", cloneSnapshotName("dst-use10"),
		map[string]any{"to_resource": "third-use10"}); code != http.StatusCreated {
		t.Fatalf("restore from the internal snapshot = %d, want 201", code)
	}

	if code := deleteRD(t, base, "dst-use10"); code != http.StatusOK {
		t.Fatalf("delete of the clone = %d, want 200", code)
	}

	resp := httpDelete(t, base+"/v1/resource-definitions/src-use10")
	defer func() { _ = resp.Body.Close() }()

	var rcs []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rcs); err != nil || len(rcs) == 0 {
		t.Fatalf("decode the refusal (status %d): %v", resp.StatusCode, err)
	}

	rc := rcs[0]

	want := "delete third-use10 if it is no longer needed, then `linstor s d src-use10 " +
		cloneSnapshotName("dst-use10") + "`"
	if !strings.Contains(rc.Correc, want) {
		t.Errorf("correction %q does not put the definition restoring from the snapshot first", rc.Correc)
	}
}

// A snapshot a version before the owner prop took is never reaped, and the
// operator was never told which one keeps the source refused.
func TestRDDeleteRefusalNamesAnUnownedCloneSnapshot(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedDeployedCloneSource(t, st, "src-old10")
	seedRestoreSnapshot(t, st, "src-old10", cloneSnapshotName("gone-old10"), []string{"node-a"})

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t, base+"/v1/resource-definitions/src-old10")
	defer func() { _ = resp.Body.Close() }()

	var rcs []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rcs); err != nil || len(rcs) == 0 {
		t.Fatalf("decode the refusal (status %d): %v", resp.StatusCode, err)
	}

	if !strings.Contains(rcs[0].Cause, cloneSnapshotName("gone-old10")) {
		t.Errorf("cause %q does not name the unowned clone snapshot", rcs[0].Cause)
	}
}

// Only a missing passphrase is the caller's to fix. A Secret that could not be
// read was answered 400, which linstor-csi takes as permanent.
func TestRDCloneAnswersAnUnreadableLUKSPrerequisiteAsAServerError(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedCloneSourceWithStack(t, st, "src-luks10", []string{"DRBD", "LUKS", "STORAGE"})

	base, stop := startServerCustom(t, &Server{
		Addr:      pickFreeAddr(t),
		Store:     st,
		Client:    secretsForbiddenClient(t),
		Namespace: testRESTNamespace,
	})
	defer stop()

	code := cloneOnce(t, base, "src-luks10", "dst-luks10",
		map[string]any{"layer_list": []string{"DRBD", "LUKS", "STORAGE"}})
	if code != http.StatusInternalServerError {
		t.Errorf("clone with an unreadable passphrase Secret = %d, want 500", code)
	}
}
