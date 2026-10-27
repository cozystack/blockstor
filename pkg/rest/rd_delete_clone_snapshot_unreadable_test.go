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
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

var errSnapshotsUnreadable = errors.New("apiserver unavailable")

type unreadableSnapshots struct{ store.SnapshotStore }

// ListByDefinitionUncached fails only for the clone's source, so the
// delete's own snapshot gate over the clone still answers and the refusal
// comes from the owner lookup.
func (u unreadableSnapshots) ListByDefinitionUncached(ctx context.Context, rdName string) ([]apiv1.Snapshot, error) {
	if rdName == "src" {
		return nil, errSnapshotsUnreadable
	}

	return u.SnapshotStore.ListByDefinitionUncached(ctx, rdName) //nolint:wrapcheck // pass-through test double
}

type unreadableSnapshotsStore struct{ store.Store }

func (s unreadableSnapshotsStore) Snapshots() store.SnapshotStore {
	return unreadableSnapshots{SnapshotStore: s.Store.Snapshots()}
}

// A clone whose snapshot cannot be read is not deleted: its props are the
// only record of that snapshot, and the delete would lose it to nobody.
func TestRDDeleteStopsWhenTheCloneSnapshotCannotBeRead(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	if err := backend.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{
		Name:  "dst-unread",
		Props: map[string]string{store.RestoreFromSnapshotProp: "src:clone-dst-unread"},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, unreadableSnapshotsStore{Store: backend})
	defer stop()

	resp := httpDelete(t, base+"/v1/resource-definitions/dst-unread")
	_ = resp.Body.Close()

	if resp.StatusCode < http.StatusBadRequest {
		t.Errorf("delete = %d, want a refusal", resp.StatusCode)
	}

	if _, err := backend.ResourceDefinitions().Get(t.Context(), "dst-unread"); err != nil {
		t.Errorf("the clone was deleted although its snapshot could not be read: %v", err)
	}
}
