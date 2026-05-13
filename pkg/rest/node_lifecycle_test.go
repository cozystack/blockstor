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
	"net/http"
	"slices"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestNodeEvacuateMarksFlag: POST /v1/nodes/{node}/evacuate adds the
// EVICTED flag to the Node. Replica migration is the reconciler's job;
// the REST endpoint only marks intent.
func TestNodeEvacuateMarksFlag(t *testing.T) {
	st := store.NewInMemory()
	if err := st.Nodes().Create(t.Context(), &apiv1.Node{Name: "n1"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/nodes/n1/evacuate", nil)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.Nodes().Get(t.Context(), "n1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if !slices.Contains(got.Flags, "EVICTED") {
		t.Errorf("expected EVICTED flag; got %v", got.Flags)
	}
}

// TestNodeRestoreClearsFlag: POST /v1/nodes/{node}/restore removes
// the EVICTED flag.
func TestNodeRestoreClearsFlag(t *testing.T) {
	st := store.NewInMemory()
	if err := st.Nodes().Create(t.Context(), &apiv1.Node{
		Name:  "n1",
		Flags: []string{"EVICTED"},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/nodes/n1/restore", nil)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.Nodes().Get(t.Context(), "n1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if slices.Contains(got.Flags, "EVICTED") {
		t.Errorf("EVICTED still present: %v", got.Flags)
	}
}

// TestNodeLostDeletesNode pins the upstream-LINSTOR semantic:
// `controller drop-node` removes the Node entry entirely (not
// "mark with LOST/EVICTED flags"). Orphan Resources are re-placed
// by the reconciler on the next cycle (Phase 6 work). The
// recorder-corpus comparison against the oracle relied on this —
// keeping the old "mark flags" behaviour made the post-Lost
// /v1/nodes list diverge from upstream.
func TestNodeLostDeletesNode(t *testing.T) {
	st := store.NewInMemory()
	if err := st.Nodes().Create(t.Context(), &apiv1.Node{Name: "n1"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/nodes/n1/lost", nil)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	_, err := st.Nodes().Get(t.Context(), "n1")
	if err == nil {
		t.Errorf("expected node to be deleted, but Get succeeded")
	}
}

// TestNodeEvacuateUnknown: 404 if the node doesn't exist.
func TestNodeEvacuateUnknown(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpPost(t, base+"/v1/nodes/ghost/evacuate", nil)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", resp.StatusCode)
	}
}

// TestNodeEvacuateIdempotent: a second POST to /evacuate must not
// duplicate the EVICTED flag — addFlag's slices.Contains branch
// short-circuits on already-set flags. Pinned because operators
// often re-fire evacuate after a controller restart; without
// idempotency the flag list would grow unbounded.
func TestNodeEvacuateIdempotent(t *testing.T) {
	st := store.NewInMemory()
	if err := st.Nodes().Create(t.Context(), &apiv1.Node{
		Name:  "n1",
		Flags: []string{"EVICTED"},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/nodes/n1/evacuate", nil)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	got, _ := st.Nodes().Get(t.Context(), "n1")

	count := 0

	for _, f := range got.Flags {
		if f == "EVICTED" {
			count++
		}
	}

	if count != 1 {
		t.Errorf("EVICTED count after re-evacuate: got %d, want 1; flags=%v", count, got.Flags)
	}
}

// TestNodeRestoreUnknown: 404 if the node doesn't exist on restore.
// Pinned alongside Evacuate's 404 so the entire lifecycle matrix
// has consistent error surfaces — operator scripts that loop
// restore + evacuate calls don't have to special-case which
// endpoint returns what.
func TestNodeRestoreUnknown(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpPost(t, base+"/v1/nodes/ghost/restore", nil)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", resp.StatusCode)
	}
}

// TestNodeLostUnknownIsIdempotent: `lost` on a non-existent node is
// folded into success — matches upstream LINSTOR's "drop-node is
// idempotent" semantic so re-running an operator teardown script
// doesn't fail on an already-cleaned node.
func TestNodeLostUnknownIsIdempotent(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpPost(t, base+"/v1/nodes/ghost/lost", nil)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status: got %d, want 200", resp.StatusCode)
	}
}
