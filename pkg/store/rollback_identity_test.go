// SPDX-License-Identifier: Apache-2.0

package store_test

import (
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
