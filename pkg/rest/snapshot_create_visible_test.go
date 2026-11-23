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
	"encoding/json"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// trailingSnapshotStore hides a created snapshot from Get and List for
// `lag` after its Create, the way the informer cache trails a write to
// the apiserver.
type trailingSnapshotStore struct {
	store.SnapshotStore

	lag time.Duration

	mu      sync.Mutex
	visible map[string]time.Time
}

func (f *trailingSnapshotStore) Create(ctx context.Context, snap *apiv1.Snapshot) error {
	err := f.SnapshotStore.Create(ctx, snap)
	if err != nil {
		return err //nolint:wrapcheck // pass-through
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.visible == nil {
		f.visible = map[string]time.Time{}
	}

	f.visible[snap.ResourceName+"/"+snap.Name] = time.Now().Add(f.lag)

	return nil
}

func (f *trailingSnapshotStore) Get(ctx context.Context, rdName, snapName string) (apiv1.Snapshot, error) {
	if f.hidden(rdName, snapName) {
		return apiv1.Snapshot{}, errors.Wrapf(store.ErrNotFound, "snapshot %q on RD %q", snapName, rdName)
	}

	return f.SnapshotStore.Get(ctx, rdName, snapName) //nolint:wrapcheck // pass-through
}

func (f *trailingSnapshotStore) List(ctx context.Context) ([]apiv1.Snapshot, error) {
	all, err := f.SnapshotStore.List(ctx)
	if err != nil {
		return nil, err //nolint:wrapcheck // pass-through
	}

	out := make([]apiv1.Snapshot, 0, len(all))

	for i := range all {
		if !f.hidden(all[i].ResourceName, all[i].Name) {
			out = append(out, all[i])
		}
	}

	return out, nil
}

func (f *trailingSnapshotStore) hidden(rdName, snapName string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	at, ok := f.visible[rdName+"/"+snapName]

	return ok && time.Now().Before(at)
}

// trailingSnapshotsStore serves snapshots through trailingSnapshotStore.
type trailingSnapshotsStore struct {
	store.Store

	snaps *trailingSnapshotStore
}

func (f *trailingSnapshotsStore) Snapshots() store.SnapshotStore { return f.snaps }

// TestSnapshotCreate_ListedRightAfterCreate pins that `s l` lists a
// snapshot as soon as its create has answered. csi-snapshotter pages
// through ListSnapshots right after CreateSnapshot; with the list served
// from a cache that has not seen the create yet, a page came back short
// (TestGroupJ/CSIListSnapshotsPagination: "page2: got 1 entries, want 2").
func TestSnapshotCreate_ListedRightAfterCreate(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()

	err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-list-src"})
	if err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	lagging := &trailingSnapshotsStore{
		Store: st,
		snaps: &trailingSnapshotStore{SnapshotStore: st.Snapshots(), lag: 300 * time.Millisecond},
	}

	base, stop := startServerWithStore(t, lagging)
	defer stop()

	body, err := json.Marshal(apiv1.Snapshot{Name: "snap-1"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	createResp := httpPost(t, base+"/v1/resource-definitions/pvc-list-src/snapshots", body)
	_ = createResp.Body.Close()

	if createResp.StatusCode != http.StatusCreated {
		t.Fatalf("create status: got %d, want 201", createResp.StatusCode)
	}

	listResp := httpGet(t, base+"/v1/view/snapshots?resources=pvc-list-src")
	defer func() { _ = listResp.Body.Close() }()

	if listResp.StatusCode != http.StatusOK {
		t.Fatalf("list status: got %d, want 200", listResp.StatusCode)
	}

	var got []apiv1.Snapshot

	err = json.NewDecoder(listResp.Body).Decode(&got)
	if err != nil {
		t.Fatalf("decode list: %v", err)
	}

	if len(got) != 1 || got[0].Name != "snap-1" {
		t.Fatalf("list right after create: got %+v, want snap-1", got)
	}
}
