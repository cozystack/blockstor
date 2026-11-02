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
	"errors"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// The CLI deletes snapshots through the store directly, so it runs the same
// release the REST door does: a snapshot an unfinished restore reads from
// stays, with the reason.
func TestSnapshotDeleteRefusesWhileARestoreReadsFromIt(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	st := store.NewInMemory()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "src-cli"}); err != nil {
		t.Fatalf("seed source: %v", err)
	}

	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{Name: "snap-cli", ResourceName: "src-cli"}); err != nil {
		t.Fatalf("seed snapshot: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:  "dst-cli",
		Props: map[string]string{store.RestoreFromSnapshotProp: "src-cli:snap-cli"},
	}); err != nil {
		t.Fatalf("seed restored definition: %v", err)
	}

	run := &runContext{
		Store: st,
		Flags: &flagSet{Values: map[string]string{}, Positionals: []string{"src-cli", "snap-cli"}},
	}

	err := snapshotDelete(ctx, run)
	if !errors.Is(err, store.ErrRestoreSourceInUse) {
		t.Fatalf("snapshot delete under an unfinished restore = %v, want ErrRestoreSourceInUse", err)
	}

	if _, err := st.Snapshots().Get(ctx, "src-cli", "snap-cli"); err != nil {
		t.Fatalf("the refused delete took the snapshot: %v", err)
	}
}
