// SPDX-License-Identifier: Apache-2.0

package rest

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

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
	seedDeployedCloneSource(t, st, "src-bkt11")

	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name: "clone-owned11", ResourceName: "src-bkt11",
		Props: map[string]string{store.CloneSnapshotOwnerProp: "owned11"},
	}); err != nil {
		t.Fatalf("seed the stamped orphan: %v", err)
	}

	seedRestoreSnapshot(t, st, "src-bkt11", "clone-legacy11", []string{"node-a"})

	base, stop := startServerWithStore(t, st)
	defer stop()

	rc := refusalOf(t, base, "src-bkt11")

	if !strings.Contains(rc.Correc, "`linstor s d src-bkt11 clone-owned11`; ") ||
		strings.Contains(rc.Correc, "depends on clone-owned11") {
		t.Errorf("correction %q does not give the stamped orphan its command outright", rc.Correc)
	}

	if !strings.Contains(rc.Correc,
		"if nothing of yours depends on clone-legacy11, `linstor s d src-bkt11 clone-legacy11`") {
		t.Errorf("correction %q does not put the unstamped snapshot behind a warning", rc.Correc)
	}

	if !strings.HasSuffix(rc.Correc, "then delete src-bkt11 again") {
		t.Errorf("correction %q does not end on the source delete", rc.Correc)
	}
}

// A cluster without a passphrase is the caller's to fix, and stays a 400.
func TestRDCloneAnswersAMissingLUKSPassphraseAsAClientError(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedCloneSourceWithStack(t, st, "src-nopass11", []string{"DRBD", "LUKS", "STORAGE"})

	base, stop := startServerCustom(t, &Server{
		Addr:      pickFreeAddr(t),
		Store:     st,
		Client:    newFakeRESTClient(t),
		Namespace: testRESTNamespace,
	})
	defer stop()

	code := cloneOnce(t, base, "src-nopass11", "dst-nopass11",
		map[string]any{"layer_list": []string{"DRBD", "LUKS", "STORAGE"}})
	if code != http.StatusBadRequest {
		t.Errorf("clone on a cluster with no passphrase = %d, want 400", code)
	}
}

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
	seedDeployedCloneSource(t, backend, "src-gone11")
	seedRestoreSnapshot(t, backend, "src-gone11", "snap-gone11", []string{"node-a"})

	base, stop := startServerWithStore(t, snapshotGoesOnCreate{
		Store: backend, source: "src-gone11", snapshot: "snap-gone11",
	})
	defer stop()

	code := restoreOnce(t, base, "src-gone11", "snap-gone11", map[string]any{"to_resource": "dst-gone11"})
	if code != http.StatusNotFound {
		t.Errorf("restore over a snapshot deleted under it = %d, want 404", code)
	}

	if _, err := backend.ResourceDefinitions().Get(ctx, "dst-gone11"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("the definition restored from a deleted snapshot was left behind: %v", err)
	}
}

// The reap marks before it lists, so a restore whose create landed after the
// list reads the mark back and withdraws.
func TestSnapshotRestoreWithdrawsFromASnapshotBeingReaped(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-reap11")

	seedRestoreSnapshot(t, st, "src-reap11", "clone-dst-reap11", []string{"node-a"})

	snap, err := st.Snapshots().Get(ctx, "src-reap11", "clone-dst-reap11")
	if err != nil {
		t.Fatalf("read the seeded snapshot: %v", err)
	}

	snap.Props = map[string]string{
		store.CloneSnapshotOwnerProp:   "dst-reap11",
		store.CloneSnapshotReapingProp: "dst-reap11",
	}

	if err := st.Snapshots().Update(ctx, &snap); err != nil {
		t.Fatalf("mark the snapshot as being reaped: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	code := restoreOnce(t, base, "src-reap11", "clone-dst-reap11", map[string]any{"to_resource": "third-reap11"})
	if code != http.StatusNotFound {
		t.Errorf("restore from a snapshot being reaped = %d, want 404", code)
	}

	if _, err := st.ResourceDefinitions().Get(ctx, "third-reap11"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("the definition restored from a snapshot being reaped was left behind: %v", err)
	}
}
