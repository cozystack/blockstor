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
	"errors"
	"net/http"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// Replicas under a name with no definition over them are not this request's
// work. Created over them, the restore adopted them, and its rollback after a
// failed placement cascaded over them too.
func TestCloneRefusesATargetNameWithReplicasAndNoDefinition(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-orph")

	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name: "dst-orph", NodeName: "node-x", Flags: []string{apiv1.ResourceFlagDiskless},
	}); err != nil {
		t.Fatalf("seed the replica: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	if code := cloneOnce(t, base, "src-orph", "dst-orph", nil); code != http.StatusConflict {
		t.Fatalf("clone over replicas with no definition = %d, want 409", code)
	}

	if _, err := st.Resources().Get(ctx, "dst-orph", "node-x"); err != nil {
		t.Fatalf("the refusal took the replica that was there: %v", err)
	}

	if _, err := st.ResourceDefinitions().Get(ctx, "dst-orph"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("the refusal left a definition behind: err=%v", err)
	}
}

// The controller's tie-breaker witness is not refused: the stamp promotes it
// into the replica the restore asked for, as it did before.
func TestRestoreStillPromotesAnOrphanWitness(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, backend, "src-tbo")
	seedRestoreSnapshot(t, backend, "src-tbo", "snap-tbo", []string{"node-a"})

	if err := backend.Resources().Create(ctx, &apiv1.Resource{
		Name: "dst-tbo", NodeName: "node-a",
		Flags: []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker},
	}); err != nil {
		t.Fatalf("seed the witness: %v", err)
	}

	base, stop := startServerWithStore(t, backend)
	defer stop()

	if code := restoreOnce(t, base, "src-tbo", "snap-tbo", map[string]any{
		"to_resource": "dst-tbo", "node_names": []string{"node-a"},
	}); code != http.StatusCreated {
		t.Fatalf("restore onto a node holding an orphan witness = %d, want 201", code)
	}
}
