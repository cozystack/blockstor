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
	"strings"
	"sync/atomic"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

var errMarkPatchRefused = errors.New("patch refused")

// A rollback that yields to an adopter and cannot take its mark back off
// leaves a definition every re-run is refused over. The answer says so and
// names the command that clears the mark, rather than only the re-run.
func TestRestoreRollbackThatYieldsWithAStuckMarkNamesTheClear(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	backend := store.NewInMemory()

	if err := backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: "pvc-yield", Props: map[string]string{store.RestoreAdoptedProp: "token"},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// The first patch, the in-progress mark, lands; the one that takes it
	// back off fails.
	run := &runContext{
		Store: patchesFailAfterStore{Store: backend, patches: new(atomic.Int32), after: 1},
		Flags: &flagSet{Values: map[string]string{
			"from-resource": "src", "from-snapshot": "snap", "to-resource": "pvc-yield",
		}},
	}

	err := rollbackRestore(ctx, run, "pvc-yield", nil, errPlacementFailed)
	if !errors.Is(err, errPlacementFailed) {
		t.Fatalf("rollback = %v, want the restore's own failure first", err)
	}

	if !strings.Contains(err.Error(), "delete-property pvc-yield "+store.RollbackAbandonedProp) {
		t.Errorf("answer %q does not name the command that clears the stuck mark", err)
	}
}

// patchesFailAfterStore lets the first after definition patches through and
// fails every one after them.
type patchesFailAfterStore struct {
	store.Store

	patches *atomic.Int32
	after   int32
}

type patchesFailAfter struct {
	store.ResourceDefinitionStore

	patches *atomic.Int32
	after   int32
}

func (s patchesFailAfterStore) ResourceDefinitions() store.ResourceDefinitionStore {
	return patchesFailAfter{ResourceDefinitionStore: s.Store.ResourceDefinitions(), patches: s.patches, after: s.after}
}

func (r patchesFailAfter) PatchResourceDefinitionSpec(
	ctx context.Context, name string, mutate func(*apiv1.ResourceDefinition) error,
) error {
	if r.patches.Add(1) > r.after {
		return errMarkPatchRefused
	}

	return r.ResourceDefinitionStore.PatchResourceDefinitionSpec(ctx, name, mutate) //nolint:wrapcheck // test double
}

// A replica the plan put on a pool of its own is named with that pool: without
// it `resource create` takes the pool of the first diskful sibling.
func TestPlaceCommandsNameThePoolThePlanChose(t *testing.T) {
	t.Parallel()

	planned := []apiv1.Resource{
		{Name: "pvc-x", NodeName: "node-a", Props: map[string]string{storPoolNameProp: "fast"}},
		{Name: "pvc-x", NodeName: "node-b", Props: map[string]string{storPoolNameProp: "slow"}},
		{Name: "pvc-x", NodeName: "node-c"},
	}

	got := placeCommands("pvc-x", planned, []string{"node-b", "node-c"})

	want := "run `blockstor resource create node-b pvc-x --storage-pool slow`, `blockstor resource create node-c pvc-x`"
	if got != want {
		t.Errorf("commands = %q, want %q", got, want)
	}
}
