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
	"maps"
	"sync"
	"testing"
	"time"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// recordMarks keeps the props every successful definition patch left.
type recordMarks struct {
	store.Store

	mu    *sync.Mutex
	props *[]map[string]string
}

type recordMarksRDs struct {
	store.ResourceDefinitionStore

	outer recordMarks
}

func (r recordMarks) ResourceDefinitions() store.ResourceDefinitionStore {
	return recordMarksRDs{ResourceDefinitionStore: r.Store.ResourceDefinitions(), outer: r}
}

func (r recordMarksRDs) PatchResourceDefinitionSpec(
	ctx context.Context, name string, mutate func(*apiv1.ResourceDefinition) error,
) error {
	err := r.ResourceDefinitionStore.PatchResourceDefinitionSpec(ctx, name, mutate)
	if err != nil {
		return err //nolint:wrapcheck // pass-through test double
	}

	rd, getErr := r.GetUncached(ctx, name)
	if getErr == nil {
		r.outer.mu.Lock()
		*r.outer.props = append(*r.outer.props, maps.Clone(rd.Props))
		r.outer.mu.Unlock()
	}

	return nil
}

// The takeover of a stale mark reads the time on it, so the writer has to put
// it there: a REST rollback's in-progress mark carries the time it was
// written, and the step mark that replaces it carries none.
func TestTheRESTRollbackStampsItsInProgressMarkWithTheTime(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		snapshot bool
		adopted  bool
		wantStep string
	}{
		{name: "completes", wantStep: rollbackStepNames[rollbackStepReapReplicas]},
		{name: "stops-at-snapshots", snapshot: true, wantStep: rollbackStepNames[rollbackStepSnapshots]},
		// A rollback that yields takes its mark off, and the time with it.
		{name: "yields", adopted: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := store.NewInMemory()
			rdName := "dst-since-" + tc.name
			seedRolledBackDefinition(t, backend, rdName)

			if tc.snapshot {
				if err := backend.Snapshots().Create(t.Context(), &apiv1.Snapshot{Name: "snap", ResourceName: rdName}); err != nil {
					t.Fatalf("seed snapshot: %v", err)
				}
			}

			if tc.adopted {
				if err := backend.ResourceDefinitions().PatchResourceDefinitionSpec(t.Context(), rdName,
					func(rd *apiv1.ResourceDefinition) error {
						rd.Props = map[string]string{store.RestoreAdoptedProp: "claim"}

						return nil
					}); err != nil {
					t.Fatalf("seed the adoption: %v", err)
				}
			}

			var recorded []map[string]string

			srv := &Server{Store: recordMarks{Store: backend, mu: &sync.Mutex{}, props: &recorded}}
			before := time.Now().Add(-time.Second)

			_ = srv.rollBackCompensating(t.Context(), rdName, []string{"node-a"}, rollbackEvenIfFinished)

			assertMarksCarryTheirTime(t, recorded, before, tc.wantStep)
		})
	}
}

// assertMarksCarryTheirTime checks that every in-progress mark among props
// carries a time no earlier than before, that one was written at all, and
// that the step mark wantStep was written without one.
func assertMarksCarryTheirTime(t *testing.T, recorded []map[string]string, before time.Time, wantStep string) {
	t.Helper()

	sawInProgress, sawStep := false, false

	for _, props := range recorded {
		switch props[store.RollbackAbandonedProp] {
		case store.RollbackInProgress:
			sawInProgress = true

			since, err := time.Parse(time.RFC3339, props[store.RollbackSinceProp])
			if err != nil || since.Before(before.Truncate(time.Second)) || since.After(time.Now()) {
				t.Errorf("in-progress mark carries time %q (%v), want one written now", props[store.RollbackSinceProp], err)
			}
		case wantStep:
			sawStep = true

			if v, ok := props[store.RollbackSinceProp]; ok {
				t.Errorf("step mark %q carries a time %q: a step stands until an operator acts", wantStep, v)
			}
		case "":
			if v, ok := props[store.RollbackSinceProp]; ok {
				t.Errorf("the mark was taken off, but its time %q stayed", v)
			}
		}
	}

	if !sawInProgress {
		t.Errorf("no in-progress mark was written: %v", recorded)
	}

	if wantStep != "" && !sawStep {
		t.Errorf("no %q step mark was written: %v", wantStep, recorded)
	}

	if wantStep == "" && (len(recorded) == 0 || recorded[len(recorded)-1][store.RollbackAbandonedProp] != "") {
		t.Errorf("the mark was not taken off: %v", recorded)
	}
}
