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
	"io"
	"net/http"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestNodeDeleteRefusedWhenEvicted pins corner-case F6: a plain
// `node delete` on an EVICTED (draining) node is refused with 409 so
// the operator chooses explicitly between `node restore` and
// `node lost`. The node carries NO resources / pools — the refusal is
// driven purely by the EVICTED latched state, not by the Bug 92 / 179
// reference gate. Without this gate a bare `n d` would race the
// in-flight eviction migration and silently discard the restore-or-lost
// decision.
func TestNodeDeleteRefusedWhenEvicted(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{
		Name:  "drain1",
		Flags: []string{apiv1.NodeFlagEvicted},
	}); err != nil {
		t.Fatalf("seed evicted node: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t, base+"/v1/nodes/drain1")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status: got %d, want 409", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	low := strings.ToLower(string(body))

	if !strings.Contains(low, "evicted") {
		t.Errorf("body: missing 'EVICTED' explanation; got %q", string(body))
	}

	// Both sanctioned exits must be named so the operator knows the path.
	if !strings.Contains(low, "node restore") {
		t.Errorf("body: missing 'node restore' guidance; got %q", string(body))
	}

	if !strings.Contains(low, "node lost") {
		t.Errorf("body: missing 'node lost' guidance; got %q", string(body))
	}

	// The node must still exist — a refused delete does not half-apply.
	if _, err := st.Nodes().Get(ctx, "drain1"); err != nil {
		t.Errorf("node deleted despite refusal: %v", err)
	}
}

// TestNodeDeleteEvictedForcedSucceeds pins the F6 escape hatch:
// ?force=true overrides the EVICTED latch (it already cascades orphans),
// matching the Bug 92 / Bug 179 disaster-recovery precedent on this same
// handler. The node is actually removed.
func TestNodeDeleteEvictedForcedSucceeds(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{
		Name:  "drain2",
		Flags: []string{apiv1.NodeFlagEvicted},
	}); err != nil {
		t.Fatalf("seed evicted node: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t, base+"/v1/nodes/drain2?force=true")
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	if _, err := st.Nodes().Get(ctx, "drain2"); err == nil {
		t.Errorf("node still present after force-delete of EVICTED node")
	}
}

// TestNodeDeleteUnevictedStillSucceeds guards against the F6 gate
// over-reaching: a plain, NON-EVICTED node with no references must
// still delete cleanly. A regression that rejected every `n d` (e.g.
// matching on the wrong flag) would brick the normal node-teardown
// path.
func TestNodeDeleteUnevictedStillSucceeds(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "idle1"}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t, base+"/v1/nodes/idle1")
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	if _, err := st.Nodes().Get(ctx, "idle1"); err == nil {
		t.Errorf("non-evicted node not deleted")
	}
}
