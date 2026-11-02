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

package store

import (
	"testing"
	"time"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// The CLI's rollback writes its marks through HoldRollback,
// EnterDestructiveRollback and the stand-downs. The takeover of a stale mark
// reads the time on it, so the in-progress mark has to carry it, and every
// write that replaces or removes that mark has to take the time with it.
func TestTheCLIRollbackWritersKeepTheTimeOnTheInProgressMarkOnly(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	st := NewInMemory()

	for _, name := range []string{"dst-hold", "dst-yield"} {
		if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: name}); err != nil {
			t.Fatalf("seed %s: %v", name, err)
		}
	}

	props := func(name string) map[string]string {
		rd, err := st.ResourceDefinitions().GetUncached(ctx, name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}

		return rd.Props
	}

	before := time.Now().Add(-time.Second).Truncate(time.Second)

	if err := HoldRollback(ctx, st, "dst-hold", true); err != nil {
		t.Fatalf("HoldRollback: %v", err)
	}

	held := props("dst-hold")

	since, err := time.Parse(time.RFC3339, held[RollbackSinceProp])
	if held[RollbackAbandonedProp] != RollbackInProgress || err != nil || since.Before(before) {
		t.Fatalf("held mark = %v (%v), want in-progress with the time it was written", held, err)
	}

	if err := EnterDestructiveRollback(ctx, st, "dst-hold", RollbackStepReapReplicas); err != nil {
		t.Fatalf("EnterDestructiveRollback: %v", err)
	}

	if step := props("dst-hold"); step[RollbackAbandonedProp] != RollbackStepReapReplicas || step[RollbackSinceProp] != "" {
		t.Errorf("step mark = %v, want the step with no time", step)
	}

	// A hold that yields stands down: the mark goes, and its time with it.
	if err := st.ResourceDefinitions().PatchResourceDefinitionSpec(ctx, "dst-yield",
		func(rd *apiv1.ResourceDefinition) error {
			rd.Props = map[string]string{RestoreAdoptedProp: "claim"}

			return nil
		}); err != nil {
		t.Fatalf("seed the adoption: %v", err)
	}

	if err := HoldRollback(ctx, st, "dst-yield", true); err == nil {
		t.Fatal("HoldRollback over an adopted definition did not yield")
	}

	if left := props("dst-yield"); left[RollbackAbandonedProp] != "" || left[RollbackSinceProp] != "" {
		t.Errorf("after the stand-down = %v, want no mark and no time", left)
	}
}

func TestMarkAndUnmarkRollbackMoveTheTimeWithTheMark(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC)

	props := MarkRollback(nil, RollbackInProgress, now)
	if props[RollbackSinceProp] != now.Format(time.RFC3339) {
		t.Fatalf("in-progress mark = %v, want the time on it", props)
	}

	props = MarkRollback(props, RollbackStepReapReplicas, now)
	if _, ok := props[RollbackSinceProp]; ok {
		t.Errorf("step mark = %v, want no time", props)
	}

	props = MarkRollback(props, RollbackInProgress, now)
	UnmarkRollback(props)

	if len(props) != 0 {
		t.Errorf("after UnmarkRollback = %v, want nothing", props)
	}
}
