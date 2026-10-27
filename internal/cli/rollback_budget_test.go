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
	"testing"
	"time"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

type deadlineRecordingRDs struct {
	store.ResourceDefinitionStore

	deadlines *[]time.Duration
}

func (d deadlineRecordingRDs) PatchResourceDefinitionSpec(
	ctx context.Context, name string, mutate func(*apiv1.ResourceDefinition) error,
) error {
	if deadline, ok := ctx.Deadline(); ok {
		*d.deadlines = append(*d.deadlines, time.Until(deadline))
	} else {
		*d.deadlines = append(*d.deadlines, -1)
	}

	return d.ResourceDefinitionStore.PatchResourceDefinitionSpec(ctx, name, mutate) //nolint:wrapcheck // test double
}

type deadlineRecordingStore struct {
	store.Store

	deadlines *[]time.Duration
}

func (s deadlineRecordingStore) ResourceDefinitions() store.ResourceDefinitionStore {
	return deadlineRecordingRDs{ResourceDefinitionStore: s.Store.ResourceDefinitions(), deadlines: s.deadlines}
}

var errRestorePlacement = errors.New("placement failed")

// A CLI rollback writes the same in-progress mark a REST one does, and a
// retry takes a mark over once it is stale on the grounds that no rollback
// can still be running. The CLI command has no deadline of its own, so the
// rollback must carry one, shorter than the staleness bound.
func TestCLIRollbackRunsOnABudget(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	backend := store.NewInMemory()

	if err := backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-budget"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	var deadlines []time.Duration

	run := &runContext{
		Store: deadlineRecordingStore{Store: backend, deadlines: &deadlines},
		Flags: &flagSet{Values: map[string]string{}},
	}

	_ = rollbackRestore(context.Background(), run, "pvc-budget", nil, errRestorePlacement)

	if len(deadlines) == 0 {
		t.Fatal("the rollback wrote no mark")
	}

	for _, left := range deadlines {
		if left < 0 || left > store.CLIRollbackBudget {
			t.Fatalf("a rollback write ran with %s left, want a deadline within %s", left, store.CLIRollbackBudget)
		}
	}
}
