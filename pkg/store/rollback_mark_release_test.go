// SPDX-License-Identifier: Apache-2.0

package store

import (
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// The CLI door's stand-down takes off the in-progress mark and leaves a step
// mark another rollback wrote, for the same reason as the REST door's.
func TestReleasingTheInProgressMarkLeavesAnotherRollbacksStep(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name, seeded string
		keep         bool
	}{
		{name: "in-progress", seeded: RollbackInProgress, keep: false},
		{name: "step", seeded: RollbackStepSnapshots, keep: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			st := NewInMemory()
			ctx := t.Context()

			if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
				Name: "dst-mark", Props: map[string]string{RollbackAbandonedProp: tc.seeded},
			}); err != nil {
				t.Fatalf("seed the definition: %v", err)
			}

			if err := releaseInProgress(ctx, st, "dst-mark"); err != nil {
				t.Fatalf("releaseInProgress: %v", err)
			}

			rd, err := st.ResourceDefinitions().Get(ctx, "dst-mark")
			if err != nil {
				t.Fatalf("read the definition back: %v", err)
			}

			got, present := rd.Props[RollbackAbandonedProp]
			if present != tc.keep || (tc.keep && got != tc.seeded) {
				t.Errorf("mark after the release = %q (present %v), want present %v", got, present, tc.keep)
			}
		})
	}
}
