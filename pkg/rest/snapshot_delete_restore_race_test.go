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
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// deleteOnMarkedCreate deletes the snapshot through the running server just
// before the first definition carrying a restore marker is created: a delete
// that lands between the restore's read of its snapshot and its create.
type deleteOnMarkedCreate struct {
	store.Store

	t        *testing.T
	base     *string
	snapPath string
	once     *sync.Once
	status   *int
}

type deleteOnMarkedCreateRDs struct {
	store.ResourceDefinitionStore

	outer deleteOnMarkedCreate
}

func (d deleteOnMarkedCreate) ResourceDefinitions() store.ResourceDefinitionStore {
	return deleteOnMarkedCreateRDs{ResourceDefinitionStore: d.Store.ResourceDefinitions(), outer: d}
}

func (r deleteOnMarkedCreateRDs) Create(ctx context.Context, rd *apiv1.ResourceDefinition) error {
	if rd.Props[store.RestoreFromSnapshotProp] != "" {
		r.outer.once.Do(func() { //nolint:contextcheck // the helper sends on t's own context
			resp := httpDelete(r.outer.t, *r.outer.base+r.outer.snapPath)
			_ = resp.Body.Close()
			*r.outer.status = resp.StatusCode
		})
	}

	return r.ResourceDefinitionStore.Create(ctx, rd) //nolint:wrapcheck // pass-through test double
}

// A snapshot delete that lands between a restore's read of the snapshot and
// its create of the marked definition found no dependent, deleted the
// snapshot, and the restore then answered 201 over a definition whose marker
// named a snapshot that no longer existed: every later replica came up blank.
// The restore now reads the snapshot back once its definition stands, and
// takes the definition back when the snapshot is gone.
func TestSnapshotDeleteRacingARestoreLeavesNoMarkerOnAGoneSnapshot(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedTwoNodeCloneSource(t, backend, "src-race")
	seedRestoreSnapshot(t, backend, "src-race", "snap-race", []string{"node-a", "node-b"})

	var (
		base         string
		deleteStatus int
	)

	st := deleteOnMarkedCreate{
		Store: backend, t: t, base: &base, snapPath: "/v1/resource-definitions/src-race/snapshots/snap-race",
		once: &sync.Once{}, status: &deleteStatus,
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	code := restoreOnce(t, base, "src-race", "snap-race", map[string]any{"to_resource": "dst-race"})

	if deleteStatus != http.StatusOK {
		t.Fatalf("the racing delete = %d, want 200", deleteStatus)
	}

	if code != http.StatusNotFound {
		t.Errorf("restore whose snapshot was deleted under it = %d, want 404", code)
	}

	if _, err := backend.Snapshots().Get(t.Context(), "src-race", "snap-race"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("fixture: the snapshot is still there: %v", err)
	}

	rd, err := backend.ResourceDefinitions().Get(t.Context(), "dst-race")
	if err == nil && rd.Props[store.RestoreFromSnapshotProp] != "" {
		t.Fatalf("dst-race names the deleted snapshot: %q", rd.Props[store.RestoreFromSnapshotProp])
	}
}

// A snapshot that is not there is the idempotent 200 it always was, even
// with a definition whose marker still names it and whose restore can never
// finish: answered 409, linstor-csi retried DeleteSnapshot forever.
func TestSnapshotDeleteOfAMissingSnapshotIgnoresAHangingMarker(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedTwoNodeCloneSource(t, st, "src-hang")

	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{
		Name: "dst-hang", Props: map[string]string{store.RestoreFromSnapshotProp: "src-hang:snap-gone"},
	}); err != nil {
		t.Fatalf("seed the definition: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t, base+"/v1/resource-definitions/src-hang/snapshots/snap-gone")
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("delete of a missing snapshot under a hanging marker = %d, want 200", resp.StatusCode)
	}
}

// A restore whose snapshot carries a live delete mark withdraws with a
// retryable 409: the delete may still keep the snapshot.
func TestSnapshotRestoreFromASnapshotBeingDeletedIsRetryable(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedTwoNodeCloneSource(t, st, "src-mark")
	seedRestoreSnapshot(t, st, "src-mark", "snap-mark", []string{"node-a", "node-b"})

	snap, err := st.Snapshots().Get(t.Context(), "src-mark", "snap-mark")
	if err != nil {
		t.Fatalf("read the snapshot: %v", err)
	}

	snap.Props = map[string]string{store.SnapshotDeletingProp: "tok@" + time.Now().UTC().Format(time.RFC3339)}
	if err := st.Snapshots().Update(t.Context(), &snap); err != nil {
		t.Fatalf("mark the snapshot: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	if code := restoreOnce(t, base, "src-mark", "snap-mark", map[string]any{"to_resource": "dst-mark"}); code != http.StatusConflict {
		t.Errorf("restore from a snapshot being deleted = %d, want 409", code)
	}

	if _, err := st.ResourceDefinitions().Get(t.Context(), "dst-mark"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("the withdrawn restore left its definition behind: %v", err)
	}
}
