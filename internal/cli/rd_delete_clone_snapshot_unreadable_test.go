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

package cli_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/cozystack/blockstor/internal/cli"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

var errSourceSnapshotsUnreadable = errors.New("apiserver unavailable")

type sourceSnapshotsUnreadable struct{ store.SnapshotStore }

func (u sourceSnapshotsUnreadable) ListByDefinitionUncached(ctx context.Context, rdName string) ([]apiv1.Snapshot, error) {
	if rdName == "src" {
		return nil, errSourceSnapshotsUnreadable
	}

	return u.SnapshotStore.ListByDefinitionUncached(ctx, rdName) //nolint:wrapcheck // pass-through test double
}

type sourceSnapshotsUnreadableStore struct{ store.Store }

func (s sourceSnapshotsUnreadableStore) Snapshots() store.SnapshotStore {
	return sourceSnapshotsUnreadable{SnapshotStore: s.Store.Snapshots()}
}

// The CLI door stops the same way the REST one does: a clone whose
// snapshot cannot be read is left in place for a retry to reap.
func TestRDDeleteCLIStopsWhenTheCloneSnapshotCannotBeRead(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	if err := backend.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{
		Name:  "dst-unread",
		Props: map[string]string{store.RestoreFromSnapshotProp: "src:clone-dst-unread"},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	var out, errBuf bytes.Buffer

	app := &cli.App{
		Out: &out,
		Err: &errBuf,
		StoreFor: func(context.Context) (store.Store, error) {
			return sourceSnapshotsUnreadableStore{Store: backend}, nil
		},
	}

	if code := app.Run(t.Context(), []string{"resource-definition", "delete", "dst-unread"}); code == 0 {
		t.Errorf("rd d exited 0 over an unreadable clone snapshot (stderr %q)", errBuf.String())
	}

	if _, err := backend.ResourceDefinitions().Get(t.Context(), "dst-unread"); err != nil {
		t.Errorf("the clone was deleted although its snapshot could not be read: %v", err)
	}
}
