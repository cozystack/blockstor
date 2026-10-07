// SPDX-License-Identifier: Apache-2.0

package rest

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// snapshotGoesOnCreate deletes one snapshot just before a definition is
// created: the reap of a clone landing between the restore's snapshot read
// and its definition create.
type snapshotGoesOnCreate struct {
	store.Store

	source, snapshot string
}

type snapshotGoesOnCreateRDs struct {
	store.ResourceDefinitionStore

	outer snapshotGoesOnCreate
}

func (s snapshotGoesOnCreate) ResourceDefinitions() store.ResourceDefinitionStore {
	return snapshotGoesOnCreateRDs{ResourceDefinitionStore: s.Store.ResourceDefinitions(), outer: s}
}

func (r snapshotGoesOnCreateRDs) Create(ctx context.Context, rd *apiv1.ResourceDefinition) error {
	_ = r.outer.Store.Snapshots().Delete(ctx, r.outer.source, r.outer.snapshot)

	return r.ResourceDefinitionStore.Create(ctx, rd) //nolint:wrapcheck // pass-through test double
}

// A reap whose dependent list was taken before the restore created its
// definition deleted the snapshot from under it, and the restore answered 201
// over a definition the satellite can only bring up blank.
func TestSnapshotRestoreWithdrawsWhenItsSnapshotGoesBeforeTheDefinitionLands(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, backend, "src-gone")
	seedRestoreSnapshot(t, backend, "src-gone", "snap-gone", []string{"node-a"})

	base, stop := startServerWithStore(t, snapshotGoesOnCreate{
		Store: backend, source: "src-gone", snapshot: "snap-gone",
	})
	defer stop()

	code := restoreOnce(t, base, "src-gone", "snap-gone", map[string]any{"to_resource": "dst-gone"})
	if code != http.StatusNotFound {
		t.Errorf("restore over a snapshot deleted under it = %d, want 404", code)
	}

	if _, err := backend.ResourceDefinitions().Get(ctx, "dst-gone"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("the definition restored from a deleted snapshot was left behind: %v", err)
	}
}

// The reap marks before it lists, so a restore whose create landed after the
// list reads the mark back and withdraws.
func TestSnapshotRestoreWithdrawsFromASnapshotBeingReaped(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-reap")

	seedRestoreSnapshot(t, st, "src-reap", "clone-dst-reap", []string{"node-a"})

	snap, err := st.Snapshots().Get(ctx, "src-reap", "clone-dst-reap")
	if err != nil {
		t.Fatalf("read the seeded snapshot: %v", err)
	}

	snap.Props = map[string]string{
		store.CloneSnapshotOwnerProp:   "dst-reap",
		store.CloneSnapshotReapingProp: "dst-reap@" + time.Now().UTC().Format(time.RFC3339),
	}

	if err := st.Snapshots().Update(ctx, &snap); err != nil {
		t.Fatalf("mark the snapshot as being reaped: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	code := restoreOnce(t, base, "src-reap", "clone-dst-reap", map[string]any{"to_resource": "third-reap"})
	if code != http.StatusNotFound {
		t.Errorf("restore from a snapshot being reaped = %d, want 404", code)
	}

	if _, err := st.ResourceDefinitions().Get(ctx, "third-reap"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("the definition restored from a snapshot being reaped was left behind: %v", err)
	}
}

// A mark no live reap owns is not withdrawn on, and it is the snapshot's: a
// definition restored from the snapshot does not carry it onward to every
// snapshot later taken of it.
func TestSnapshotRestoreDropsAnExpiredReapingMark(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-stale")
	seedRestoreSnapshot(t, st, "src-stale", "snap-stale", []string{"node-a"})

	snap, err := st.Snapshots().Get(ctx, "src-stale", "snap-stale")
	if err != nil {
		t.Fatalf("read the seeded snapshot: %v", err)
	}

	snap.Props = map[string]string{
		store.CloneSnapshotReapingProp: "gone@" + time.Now().Add(-time.Hour).UTC().Format(time.RFC3339),
	}

	if err := st.Snapshots().Update(ctx, &snap); err != nil {
		t.Fatalf("leave an expired mark: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	if code := restoreOnce(t, base, "src-stale", "snap-stale",
		map[string]any{"to_resource": "dst-stale"}); code != http.StatusCreated {
		t.Fatalf("restore over an expired mark = %d, want 201", code)
	}

	rd, err := st.ResourceDefinitions().Get(ctx, "dst-stale")
	if err != nil {
		t.Fatalf("read the restored definition: %v", err)
	}

	if mark, ok := rd.Props[store.CloneSnapshotReapingProp]; ok {
		t.Errorf("the restored definition carries the snapshot's reaping mark %q", mark)
	}
}

// A leftover an earlier attempt left is not this request's to delete, and the
// answer names it: without that every retry repeats a 404 about the snapshot
// with nothing pointing at the definition that can no longer be finished.
func TestSnapshotRestoreKeepsAnEarlierLeftoverWhenItsSnapshotGoes(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, backend, "src-left")
	seedRestoreSnapshot(t, backend, "src-left", "snap-left", []string{"node-a"})

	if err := backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:  "dst-left",
		Props: map[string]string{restoreFromSnapshotKey: restoreMarker("src-left", "snap-left")},
	}); err != nil {
		t.Fatalf("seed the leftover: %v", err)
	}

	base, stop := startServerWithStore(t, snapshotGoesOnCreate{
		Store: backend, source: "src-left", snapshot: "snap-left",
	})
	defer stop()

	raw, err := json.Marshal(map[string]any{"to_resource": "dst-left"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-definitions/src-left/snapshot-restore-resource/snap-left", raw)
	defer func() { _ = resp.Body.Close() }()

	var rcs []apiv1.APICallRc
	if err := json.NewDecoder(resp.Body).Decode(&rcs); err != nil || len(rcs) == 0 {
		t.Fatalf("decode the answer (status %d): %v", resp.StatusCode, err)
	}

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("restore over a leftover whose snapshot went = %d, want 404", resp.StatusCode)
	}

	if !strings.Contains(rcs[0].Message, "linstor rd d dst-left") {
		t.Errorf("answer %q does not name the leftover", rcs[0].Message)
	}

	if _, err := backend.ResourceDefinitions().Get(ctx, "dst-left"); err != nil {
		t.Errorf("the earlier attempt's leftover was deleted: %v", err)
	}
}
