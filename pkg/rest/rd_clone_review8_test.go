// SPDX-License-Identifier: Apache-2.0

package rest

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

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
	seedDeployedCloneSource(t, st, "src-reap8")

	base, stop := startServerWithStore(t, st)
	defer stop()

	if code := cloneOnce(t, base, "src-reap8", "dst-reap8", nil); code != http.StatusCreated {
		t.Fatalf("clone = %d, want 201", code)
	}

	if _, err := st.Snapshots().Get(ctx, "src-reap8", cloneSnapshotName("dst-reap8")); err != nil {
		t.Fatalf("fixture: the clone took no internal snapshot: %v", err)
	}

	if clone, err := st.ResourceDefinitions().Get(ctx, "dst-reap8"); err != nil {
		t.Fatalf("read the clone: %v", err)
	} else if owner, ok := clone.Props[store.CloneSnapshotOwnerProp]; ok {
		t.Errorf("the clone carries the snapshot's owner prop %q", owner)
	}

	if code := deleteRD(t, base, "dst-reap8"); code != http.StatusOK {
		t.Fatalf("delete of the clone = %d, want 200", code)
	}

	if _, err := st.Snapshots().Get(ctx, "src-reap8", cloneSnapshotName("dst-reap8")); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("the internal snapshot outlived the clone it backed: %v", err)
	}

	if code := deleteRD(t, base, "src-reap8"); code != http.StatusOK {
		t.Errorf("delete of the source = %d, want 200: the source is undeletable while that snapshot stands", code)
	}
}

// A restore's marker names an operator's snapshot, which is somebody's data.
// Only the snapshot a clone derived from its own target name is reaped.
func TestRDDeleteLeavesTheSnapshotARestoreCameFrom(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-keep8")
	seedRestoreSnapshot(t, st, "src-keep8", "snap-keep8", []string{"node-a"})

	base, stop := startServerWithStore(t, st)
	defer stop()

	if code := restoreOnce(t, base, "src-keep8", "snap-keep8",
		map[string]any{"to_resource": "dst-keep8"}); code != http.StatusCreated {
		t.Fatalf("restore = %d, want 201", code)
	}

	if code := deleteRD(t, base, "dst-keep8"); code != http.StatusOK {
		t.Fatalf("delete of the restored definition = %d, want 200", code)
	}

	if _, err := st.Snapshots().Get(ctx, "src-keep8", "snap-keep8"); err != nil {
		t.Errorf("the operator's snapshot was reaped with the restore that used it: %v", err)
	}
}

// With the replica requirement gone, an explicit-node restore whose first
// attempt hydrated the volumes and died before placing anything reads as
// finished, and the replay reports success over a target that exists on no
// node.
func TestSnapshotRestoreReplayPlacesWhenTheFirstAttemptPlacedNothing(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-np8")
	seedRestoreSnapshot(t, st, "src-np8", "snap-np8", []string{"node-a"})

	// The leftover a first attempt left: the marker, the snapshot's volumes,
	// and no Resource at all.
	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: "dst-np8",
		Props: map[string]string{
			restoreFromSnapshotKey: restoreMarker("src-np8", "snap-np8"),
		},
	}); err != nil {
		t.Fatalf("seed the leftover: %v", err)
	}

	if err := st.VolumeDefinitions().Create(ctx, "dst-np8",
		&apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 64 * 1024}); err != nil {
		t.Fatalf("seed the hydrated volume: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	if code := restoreOnce(t, base, "src-np8", "snap-np8", map[string]any{
		"to_resource": "dst-np8", "node_names": []string{"node-a"},
	}); code != http.StatusCreated {
		t.Fatalf("retry = %d, want 201", code)
	}

	placed, err := st.Resources().ListByDefinition(ctx, "dst-np8")
	if err != nil {
		t.Fatalf("list the restored replicas: %v", err)
	}

	if len(placed) == 0 {
		t.Error("the retry reported the restore done over a definition with no replica")
	}
}

// Every other door that creates a definition validates the name it is handed.
// The clone door checked only that it was non-empty, so it could create a
// definition `rd create` answers 400 for.
func TestRDCloneRefusesANameRDCreateWouldRefuse(t *testing.T) {
	st := store.NewInMemory()
	seedDeployedCloneSource(t, st, "src-name8")

	base, stop := startServerWithStore(t, st)
	defer stop()

	for _, tc := range []struct{ name, target string }{
		{name: "dots", target: "dst.name.8"},
		{name: "leading-digit", target: "8dst"},
		{name: "space", target: "dst name"},
		{name: "past-the-ceiling-once-prefixed", target: strings.Repeat("d", 48)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code := cloneOnce(t, base, "src-name8", tc.target, nil)
			if code != http.StatusBadRequest {
				t.Errorf("clone into %q = %d, want 400", tc.target, code)
			}
		})
	}
}

// The body decode is the last refusal on this endpoint that answered outside
// the envelope, and python-linstor reads `messages` off whatever comes back.
func TestRDCloneBodyDecodeRefusalsKeepTheEnvelope(t *testing.T) {
	st := store.NewInMemory()
	seedDeployedCloneSource(t, st, "src-dec8")

	base, stop := startServerWithStore(t, st)
	defer stop()

	for _, tc := range []struct{ name, body string }{
		{name: "malformed", body: `{"name":`},
		{name: "unknown-field", body: `{"name":"dst-dec8","no_such_field":1}`},
		{name: "wrong-type", body: `{"name":42}`},
		{name: "empty", body: ``},
		{name: "trailing-data", body: `{"name":"dst-dec8"}garbage`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := httpPost(t, base+"/v1/resource-definitions/src-dec8/clone", []byte(tc.body))
			defer func() { _ = resp.Body.Close() }()

			raw, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("read the body: %v", err)
			}

			if resp.StatusCode < http.StatusBadRequest {
				t.Fatalf("status = %d, want a refusal", resp.StatusCode)
			}

			var envelope cloneStartedResponse
			if err := json.Unmarshal(raw, &envelope); err != nil ||
				envelope.Messages == nil || len(*envelope.Messages) == 0 {
				t.Errorf("decode refusal is not a CloneStarted object with messages: %s", raw)
			}
		})
	}
}
