// SPDX-License-Identifier: Apache-2.0

package store_test

import (
	"context"
	"errors"
	"maps"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// cacheBehindOnce hands the first definition patch a view without the
// rollback mark, the way an informer cache that has not seen the mark's own
// write yet reads to the patch's closure.
type cacheBehindOnce struct {
	store.Store

	lagged *bool
}

type cacheBehindOnceRDs struct {
	store.ResourceDefinitionStore

	lagged *bool
}

func (c cacheBehindOnce) ResourceDefinitions() store.ResourceDefinitionStore {
	return cacheBehindOnceRDs{ResourceDefinitionStore: c.Store.ResourceDefinitions(), lagged: c.lagged}
}

func (r cacheBehindOnceRDs) PatchResourceDefinitionSpec(
	ctx context.Context, name string, mutate func(*apiv1.ResourceDefinition) error,
) error {
	if *r.lagged {
		return r.ResourceDefinitionStore.PatchResourceDefinitionSpec(ctx, name, mutate) //nolint:wrapcheck // pass-through test double
	}

	*r.lagged = true

	rd, err := r.GetUncached(ctx, name)
	if err != nil {
		return err //nolint:wrapcheck // pass-through test double
	}

	// A copy: the store hands out its own map, and the stale view must not
	// write through to it.
	rd.Props = maps.Clone(rd.Props)
	delete(rd.Props, store.RollbackAbandonedProp)

	return mutate(&rd)
}

// The CLI door's rollback meets the same stale read: the definition still
// carries its in-progress mark, so it stands down and takes the mark off
// rather than leave one every re-run is refused over.
func TestEnterDestructiveRollbackStandsDownOverACacheBehindItsMark(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	backend := store.NewInMemory()

	if err := backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: "dst-lag", Props: map[string]string{store.RollbackAbandonedProp: store.RollbackInProgress},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	lagged := false

	err := store.EnterDestructiveRollback(ctx, cacheBehindOnce{Store: backend, lagged: &lagged}, "dst-lag",
		store.RollbackStepDeleteDefinition)
	if err == nil || errors.Is(err, store.ErrRollbackGone) {
		t.Fatalf("enter = %v, want a refusal that deletes nothing", err)
	}

	left, getErr := backend.ResourceDefinitions().Get(ctx, "dst-lag")
	if getErr != nil {
		t.Fatalf("the definition is gone: %v", getErr)
	}

	if mark, ok := left.Props[store.RollbackAbandonedProp]; ok {
		t.Errorf("the definition still carries rollback mark %q", mark)
	}
}
