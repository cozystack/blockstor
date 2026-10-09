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

package cli

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

var errPlacementFailed = errors.New("placement failed")

// markFindsNothing answers the gone-th definition patch NotFound, while the
// backend holds a definition under the name: the one the rollback meant went,
// and another writer put a new one there.
type markFindsNothing struct {
	store.ResourceDefinitionStore

	patches *atomic.Int32
	gone    int32
}

func (r markFindsNothing) PatchResourceDefinitionSpec(
	ctx context.Context, name string, mutate func(*apiv1.ResourceDefinition) error,
) error {
	if r.patches.Add(1) == r.gone {
		return store.ErrNotFound
	}

	return r.ResourceDefinitionStore.PatchResourceDefinitionSpec(ctx, name, mutate) //nolint:wrapcheck // test double
}

type markFindsNothingStore struct {
	store.Store

	patches *atomic.Int32
	gone    int32
}

func (s markFindsNothingStore) ResourceDefinitions() store.ResourceDefinitionStore {
	return markFindsNothing{ResourceDefinitionStore: s.Store.ResourceDefinitions(), patches: s.patches, gone: s.gone}
}

// A restore rollback whose definition is gone deletes nothing by name, and
// answers with the restore's own failure alone, since there was nothing left
// to roll back: whether it finds the definition gone on the mark that starts
// the rollback or on the one that takes it apart.
func TestRestoreRollbackWhoseDefinitionGoesBeforeItsDeleteDeletesNothing(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		gone int32
	}{
		{name: "gone when the rollback starts", gone: 1},
		{name: "gone before the first delete", gone: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := store.NewInMemory()
			ctx := t.Context()

			if err := backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-regone"}); err != nil {
				t.Fatalf("seed: %v", err)
			}

			run := &runContext{Store: markFindsNothingStore{Store: backend, patches: new(atomic.Int32), gone: tc.gone}}

			err := rollbackRestore(ctx, run, "pvc-regone", nil, errPlacementFailed)
			if err == nil || err.Error() != errPlacementFailed.Error() {
				t.Errorf("rollback = %v, want the restore's own failure and nothing else", err)
			}

			if _, err := backend.ResourceDefinitions().Get(ctx, "pvc-regone"); err != nil {
				t.Errorf("the definition now under the name was deleted: %v", err)
			}
		})
	}
}
