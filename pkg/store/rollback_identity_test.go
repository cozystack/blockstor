// SPDX-License-Identifier: Apache-2.0

package store_test

import (
	"context"
	"errors"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// A rollback whose definition is already gone has nothing of it to remove,
// and says so, so the door does not go on to delete whatever stands under the
// name by then.
func TestHoldRollbackReportsADefinitionAlreadyGone(t *testing.T) {
	t.Parallel()

	err := store.HoldRollback(t.Context(), store.NewInMemory(), "pvc-gone", true)
	if !errors.Is(err, store.ErrRollbackGone) {
		t.Errorf("HoldRollback over a missing definition = %v, want ErrRollbackGone", err)
	}
}

// The step mark goes on only over the rollback's own in-progress mark: a
// definition that no longer carries it is not the one the rollback holds.
func TestEnterDestructiveRollback(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		mark     string
		missing  bool
		want     error
		wantMark string
	}{
		{name: "its own mark", mark: store.RollbackInProgress, wantMark: store.RollbackStepDeleteDefinition},
		{name: "the mark was cleared", mark: "", wantMark: ""},
		{name: "the definition is gone", missing: true, want: store.ErrRollbackGone},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			backend := store.NewInMemory()

			if !tc.missing {
				props := map[string]string{}
				if tc.mark != "" {
					props[store.RollbackAbandonedProp] = tc.mark
				}

				if err := backend.ResourceDefinitions().Create(ctx,
					&apiv1.ResourceDefinition{Name: "pvc-new", Props: props}); err != nil {
					t.Fatalf("seed: %v", err)
				}
			}

			err := store.EnterDestructiveRollback(ctx, backend, "pvc-new", store.RollbackStepDeleteDefinition)

			switch {
			case tc.want != nil:
				if !errors.Is(err, tc.want) {
					t.Fatalf("EnterDestructiveRollback = %v, want %v", err, tc.want)
				}

				return
			case tc.mark == store.RollbackInProgress:
				if err != nil {
					t.Fatalf("EnterDestructiveRollback = %v, want nil", err)
				}
			default:
				if err == nil {
					t.Fatal("EnterDestructiveRollback went ahead over a mark that is not its own")
				}
			}

			rd, err := backend.ResourceDefinitions().Get(ctx, "pvc-new")
			if err != nil {
				t.Fatalf("read back: %v", err)
			}

			if got := rd.Props[store.RollbackAbandonedProp]; got != tc.wantMark {
				t.Errorf("rollback mark = %q, want %q", got, tc.wantMark)
			}
		})
	}
}

// A claim refused after its mark went on takes the mark back off, or the
// creator's rollback reads it as somebody answered for the definition and
// leaves it for good. A mark another request wrote before stays.
func TestARefusedAdoptionTakesItsMarkBackOff(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		prior string
		store func(store.Store) store.Store
		mark  string
	}{
		{name: "the read-back fails", store: func(s store.Store) store.Store { return readRefusingStore{s} }},
		{name: "the creator is rolling back", mark: store.RollbackInProgress},
		{name: "the creator is taking it apart", mark: store.RollbackStepReapReplicas},
		{name: "an earlier adoption stays", prior: "2026-10-07T00:00:00Z", mark: store.RollbackInProgress},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			backend := store.NewInMemory()

			props := map[string]string{}
			if tc.prior != "" {
				props[store.RestoreAdoptedProp] = tc.prior
			}

			if tc.mark != "" {
				props[store.RollbackAbandonedProp] = tc.mark
			}

			if err := backend.ResourceDefinitions().Create(ctx,
				&apiv1.ResourceDefinition{Name: "pvc-claimed", Props: props}); err != nil {
				t.Fatalf("seed: %v", err)
			}

			var st store.Store = backend
			if tc.store != nil {
				st = tc.store(backend)
			}

			if err := store.ClaimAdoptedLeftover(ctx, st, "pvc-claimed", nil); err == nil {
				t.Fatal("the claim was not refused")
			}

			rd, err := backend.ResourceDefinitions().Get(ctx, "pvc-claimed")
			if err != nil {
				t.Fatalf("read back: %v", err)
			}

			if got := rd.Props[store.RestoreAdoptedProp]; got != tc.prior {
				t.Errorf("adoption mark after a refused claim = %q, want %q", got, tc.prior)
			}
		})
	}
}

// A patch that failed may still have landed; the claim takes it back off.
func TestAnAdoptionWhoseMarkWriteFailsLeavesNoMark(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()

	if err := backend.ResourceDefinitions().Create(t.Context(),
		&apiv1.ResourceDefinition{Name: "pvc-landed"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// The claim's context runs out with its write, as a request's does: the
	// release has to go out on a context of its own.
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	if err := store.ClaimAdoptedLeftover(ctx, landedThenFailedStore{Store: backend, cancel: cancel},
		"pvc-landed", nil); err == nil {
		t.Fatal("a claim whose write reported failure went ahead")
	}

	rd, err := backend.ResourceDefinitions().Get(t.Context(), "pvc-landed")
	if err != nil {
		t.Fatalf("read back: %v", err)
	}

	if got, ok := rd.Props[store.RestoreAdoptedProp]; ok {
		t.Errorf("a failed claim left adoption mark %q", got)
	}
}

// landedThenFailedRDs applies the first spec patch, ends the caller's context
// and reports the deadline, the way a write that lands at its deadline does.
// Like a real client it sends nothing on a context that is already done.
type landedThenFailedRDs struct {
	store.ResourceDefinitionStore

	once   *bool
	cancel context.CancelFunc
}

func (r landedThenFailedRDs) PatchResourceDefinitionSpec(
	ctx context.Context, name string, mutate func(*apiv1.ResourceDefinition) error,
) error {
	if err := ctx.Err(); err != nil {
		return err //nolint:wrapcheck // test double
	}

	err := r.ResourceDefinitionStore.PatchResourceDefinitionSpec(ctx, name, mutate)
	if err == nil && !*r.once {
		*r.once = true
		r.cancel()

		return context.DeadlineExceeded
	}

	return err //nolint:wrapcheck // test double
}

type landedThenFailedStore struct {
	store.Store

	cancel context.CancelFunc
}

func (s landedThenFailedStore) ResourceDefinitions() store.ResourceDefinitionStore {
	return landedThenFailedRDs{
		ResourceDefinitionStore: s.Store.ResourceDefinitions(), once: new(bool), cancel: s.cancel,
	}
}
