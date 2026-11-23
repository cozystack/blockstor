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

// The CLI door asks liveRollbackMark which mark a retry answers for: a stale
// in-progress mark is taken over and none is left, a fresh one stands.
func TestLiveRollbackMarkTakesOverOnlyAStaleMark(t *testing.T) {
	t.Parallel()

	for name, age := range map[string]time.Duration{"fresh": 10 * time.Second, "stale": StaleRollbackAfter + time.Minute} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			st := NewInMemory()
			rd := apiv1.ResourceDefinition{
				Name:  "dst-" + name,
				Props: MarkRollback(map[string]string{}, RollbackInProgress, time.Now().Add(-age)),
			}

			if err := st.ResourceDefinitions().Create(t.Context(), &rd); err != nil {
				t.Fatalf("seed: %v", err)
			}

			mark, err := liveRollbackMark(t.Context(), st, &rd)
			if err != nil {
				t.Fatalf("liveRollbackMark: %v", err)
			}

			want := RollbackInProgress
			if name == "stale" {
				want = ""
			}

			if mark != want {
				t.Fatalf("mark = %q, want %q", mark, want)
			}
		})
	}
}

// A mark another retry already took off is no rollback to refuse over: the
// second of two retries racing over a stale mark proceeds too.
func TestTakeOverStaleRollbackCountsAMarkAlreadyTakenOff(t *testing.T) {
	t.Parallel()

	st := NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "dst-gone-mark"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	took, err := TakeOverStaleRollback(t.Context(), st, "dst-gone-mark", time.Now())
	if err != nil || !took {
		t.Fatalf("took=%v err=%v over a definition with no mark, want took", took, err)
	}
}

// The takeover rests on every rollback being over before a mark turns stale.
func TestCLIRollbackBudgetEndsBeforeAMarkTurnsStale(t *testing.T) {
	t.Parallel()

	if CLIRollbackBudget >= StaleRollbackAfter {
		t.Fatalf("CLIRollbackBudget %s is not shorter than StaleRollbackAfter %s", CLIRollbackBudget, StaleRollbackAfter)
	}
}
