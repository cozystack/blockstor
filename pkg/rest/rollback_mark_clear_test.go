// SPDX-License-Identifier: Apache-2.0

package rest

import (
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// A stand-down takes off the in-progress mark and nothing else. A step mark
// on the definition was written by another rollback that stopped at that step,
// and the replay gate reads it to refuse a replay over a half-reaped
// definition, so taking it off with the in-progress one would let a retry
// answer 201 over it.
func TestClearingTheRollbackMarkLeavesAnotherRollbacksStep(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name, seeded string
		keep         bool
	}{
		{name: "in-progress", seeded: rollbackInProgress, keep: false},
		{name: "step", seeded: store.RollbackStepSnapshots, keep: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			st := store.NewInMemory()
			ctx := t.Context()

			if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
				Name: "dst-mark", Props: map[string]string{rollbackAbandonedKey: tc.seeded},
			}); err != nil {
				t.Fatalf("seed the definition: %v", err)
			}

			if err := (&Server{Store: st}).patchRollbackMarkAway(ctx, "dst-mark"); err != nil {
				t.Fatalf("patchRollbackMarkAway: %v", err)
			}

			rd, err := st.ResourceDefinitions().Get(ctx, "dst-mark")
			if err != nil {
				t.Fatalf("read the definition back: %v", err)
			}

			got, present := rd.Props[rollbackAbandonedKey]
			if present != tc.keep || (tc.keep && got != tc.seeded) {
				t.Errorf("mark after the stand-down = %q (present %v), want present %v", got, present, tc.keep)
			}
		})
	}
}
