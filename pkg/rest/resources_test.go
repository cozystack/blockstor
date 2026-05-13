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
	"encoding/json"
	"net/http"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestResourcesViewEmpty: empty list, never nil — golinstor iterates blindly.
func TestResourcesViewEmpty(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	c := newClient(t, base)

	got, err := c.Resources.GetResourceView(t.Context())
	if err != nil {
		t.Fatalf("GetResourceView: %v", err)
	}

	if got == nil || len(got) != 0 {
		t.Errorf("got %v, want empty", got)
	}
}

// TestResourcesViewAcrossNodes: replicas on different nodes are all returned.
func TestResourcesViewAcrossNodes(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	for _, r := range []apiv1.Resource{
		{Name: "pvc-1", NodeName: "n1"},
		{Name: "pvc-1", NodeName: "n2"},
		{Name: "pvc-2", NodeName: "n2"},
	} {
		if err := st.Resources().Create(ctx, &r); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	c := newClient(t, base)

	got, err := c.Resources.GetResourceView(t.Context())
	if err != nil {
		t.Fatalf("GetResourceView: %v", err)
	}

	if len(got) != 3 {
		t.Errorf("len: got %d, want 3", len(got))
	}
}

// TestResourcesViewNodeFilter: ?nodes=n1 returns only the n1 replicas.
// Case-insensitive matching mirrors Java LINSTOR behaviour so a client
// sending mixed-case node names still gets results.
func TestResourcesViewNodeFilter(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	for _, r := range []apiv1.Resource{
		{Name: "pvc-1", NodeName: "n1"},
		{Name: "pvc-1", NodeName: "n2"},
		{Name: "pvc-2", NodeName: "n2"},
	} {
		if err := st.Resources().Create(ctx, &r); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/view/resources?nodes=N1")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got []apiv1.ResourceWithVolumes

	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("len: got %d, want 1; entries=%v", len(got), got)
	}

	if got[0].NodeName != "n1" {
		t.Errorf("filter leaked: got node %q, want n1", got[0].NodeName)
	}
}

// TestResourcesViewRDFilter: ?resources=pvc-1 returns only that RD's replicas.
func TestResourcesViewRDFilter(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	for _, r := range []apiv1.Resource{
		{Name: "pvc-1", NodeName: "n1"},
		{Name: "pvc-2", NodeName: "n2"},
	} {
		if err := st.Resources().Create(ctx, &r); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/view/resources?resources=pvc-1")
	defer func() { _ = resp.Body.Close() }()

	var got []apiv1.ResourceWithVolumes

	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(got) != 1 || got[0].Name != "pvc-1" {
		t.Errorf("got %v, want one entry for pvc-1", got)
	}
}

// TestFaultyFilterPrioritizesZeroUpToDate exercises the recovery-
// copilot ranking: `?faulty=true` excludes fully-healthy RDs and
// orders the remainder so the RDs with ZERO UpToDate copies come
// first (operators have to intervene there — DRBD has no good
// replica to seed from), followed by RDs that still have at least
// one UpToDate replica.
//
// Seed:
//   - rd-1: 3 replicas, all Inconsistent  → 0 UpToDate, faulty (FIRST)
//   - rd-2: 1 UpToDate + 1 StandAlone     → 1 UpToDate, faulty (SECOND)
//   - rd-3: 3 replicas, all UpToDate      → 3 UpToDate, healthy (EXCLUDED)
func TestFaultyFilterPrioritizesZeroUpToDate(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	seed := []apiv1.Resource{
		// rd-1 — all replicas Inconsistent: 0 UpToDate, must come first.
		{Name: "rd-1", NodeName: "n1", Volumes: []apiv1.Volume{{
			VolumeNumber: 0,
			State:        apiv1.VolumeState{DiskState: "Inconsistent"},
		}}},
		{Name: "rd-1", NodeName: "n2", Volumes: []apiv1.Volume{{
			VolumeNumber: 0,
			State:        apiv1.VolumeState{DiskState: "Inconsistent"},
		}}},
		{Name: "rd-1", NodeName: "n3", Volumes: []apiv1.Volume{{
			VolumeNumber: 0,
			State:        apiv1.VolumeState{DiskState: "Inconsistent"},
		}}},
		// rd-2 — one UpToDate replica + one StandAlone peer: 1 UpToDate.
		{Name: "rd-2", NodeName: "n1", Volumes: []apiv1.Volume{{
			VolumeNumber: 0,
			State:        apiv1.VolumeState{DiskState: "UpToDate"},
		}}},
		{Name: "rd-2", NodeName: "n2", Volumes: []apiv1.Volume{{
			VolumeNumber: 0,
			State:        apiv1.VolumeState{DiskState: "StandAlone"},
		}}},
		// rd-3 — fully healthy: must be filtered out entirely.
		{Name: "rd-3", NodeName: "n1", Volumes: []apiv1.Volume{{
			VolumeNumber: 0,
			State:        apiv1.VolumeState{DiskState: "UpToDate"},
		}}},
		{Name: "rd-3", NodeName: "n2", Volumes: []apiv1.Volume{{
			VolumeNumber: 0,
			State:        apiv1.VolumeState{DiskState: "UpToDate"},
		}}},
		{Name: "rd-3", NodeName: "n3", Volumes: []apiv1.Volume{{
			VolumeNumber: 0,
			State:        apiv1.VolumeState{DiskState: "UpToDate"},
		}}},
	}

	for i := range seed {
		if err := st.Resources().Create(ctx, &seed[i]); err != nil {
			t.Fatalf("seed %s/%s: %v", seed[i].Name, seed[i].NodeName, err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/view/resources?faulty=true")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got []apiv1.ResourceWithVolumes

	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	// rd-3 (3 replicas, all UpToDate) must be excluded; only rd-1
	// (3 replicas) + rd-2 (2 replicas) survive = 5 entries.
	if len(got) != 5 {
		t.Fatalf("len: got %d, want 5; entries=%v", len(got), got)
	}

	// rd-3 must not appear anywhere — defence in depth against
	// regressions where the filter weakens to "any non-empty
	// disk_state" or similar.
	for i := range got {
		if got[i].Name == "rd-3" {
			t.Errorf("rd-3 (all UpToDate) leaked into faulty view at index %d", i)
		}
	}

	// First three entries belong to rd-1 (0 UpToDate); next two to
	// rd-2 (1 UpToDate). Bucket boundary is the load-bearing
	// invariant — within each bucket the deterministic Name+NodeName
	// tiebreak keeps order stable for pagination.
	for i := range 3 {
		if got[i].Name != "rd-1" {
			t.Errorf("position %d: got %q, want rd-1 (0 UpToDate first)", i, got[i].Name)
		}
	}

	for i := 3; i < 5; i++ {
		if got[i].Name != "rd-2" {
			t.Errorf("position %d: got %q, want rd-2 (1 UpToDate second)", i, got[i].Name)
		}
	}
}

// TestResourcesViewWithoutStore: 503 when store is nil.
func TestResourcesViewWithoutStore(t *testing.T) {
	base, stop := startServerCustom(t, &Server{Addr: pickFreeAddr(t), Store: nil})
	defer stop()

	resp := httpGet(t, base+"/v1/view/resources")
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status: got %d, want 503", resp.StatusCode)
	}
}

// TestResourceShapeIncludesReversibilityHint is a cross-project contract test
// for the ccp drbd-recovery copilot SKILL §Core-Principles #8 "ASK before
// destructive op" prompt. The copilot needs four fields to render an
// actionable approval prompt for a target resource:
//
//  1. Resource name + volume(s)             — already exposed via Resource/Volume.
//  2. All replica states (UpToDate / Inconsistent / Diskless flags)
//     — already exposed via Resource.Flags + VolumeState.DiskState
//     (`?faulty=true` filter, Bug 16 fix).
//  3. The exact LINSTOR/drbdadm command the copilot wants to run
//     — MISSING. The copilot currently has to hand-roll the command
//     string from heuristics, which makes the approval prompt fragile
//     (e.g. is it `drbdadm primary --force` or `drbdadm -- --discard-my-data
//     connect`?).
//  4. The reversibility classification of that command
//     — MISSING. Without it the prompt cannot distinguish
//     read-only (safe to retry) from interrupts-I/O (causes brief
//     outage) from destroys-data (irreversible).
//
// Wire shape expected (proposal, pending review):
//
//	{
//	  "name": "pvc-1",
//	  "node_name": "n1",
//	  ...
//	  "recovery_metadata": {
//	    "actionable_commands": [
//	      {
//	        "cmd": "drbdadm -- --discard-my-data connect pvc-1",
//	        "reversibility_class": "destroys-data"
//	      },
//	      {
//	        "cmd": "linstor resource toggle-disk n1 pvc-1 --diskless",
//	        "reversibility_class": "interrupts-io"
//	      }
//	    ]
//	  }
//	}
//
// `reversibility_class` is one of: `read-only`, `interrupts-io`,
// `destroys-data` — mirrors the SKILL's destructive-op taxonomy.
//
// Today the field is absent on every code path: there is no
// `RecoveryMetadata` type in pkg/api/v1, no producer in pkg/rest, and no
// consumer in the copilot beyond the SKILL spec itself. This test stands
// as the contract: when the hint engine lands, drop the t.Skip and the
// assertions below become the green-bar definition of "done".
func TestResourceShapeIncludesReversibilityHint(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	// One faulty replica is enough to make the hint engine
	// non-trivial: a single Inconsistent replica + an UpToDate
	// peer is the canonical "ASK before forcing primary" case.
	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name:     "pvc-1",
		NodeName: "n1",
		Flags:    []string{},
		Volumes: []apiv1.Volume{{
			VolumeNumber: 0,
			State:        apiv1.VolumeState{DiskState: "Inconsistent"},
		}},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/view/resources")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	// Decode loosely so the test does not depend on the as-yet
	// unwritten Go type. When the field lands the loose decode
	// will still succeed; only the Skip-vs-assert branch flips.
	var raw []map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(raw) != 1 {
		t.Fatalf("len: got %d, want 1", len(raw))
	}

	meta, ok := raw[0]["recovery_metadata"].(map[string]any)
	if !ok {
		t.Skip("spec gap: Resource wire shape has no `recovery_metadata` " +
			"block yet — the ccp drbd-recovery copilot SKILL §Core " +
			"Principles #8 approval prompt currently has to hand-roll " +
			"the destructive-op command + reversibility class from " +
			"heuristics. Follow-up: add api/v1 RecoveryMetadata type " +
			"with ActionableCommands []{Cmd, ReversibilityClass} and a " +
			"producer in pkg/rest/resources.go that classifies the " +
			"recommended next step from replica flags + disk_state. " +
			"reversibility_class taxonomy: read-only | interrupts-io | " +
			"destroys-data.")
	}

	cmds, ok := meta["actionable_commands"].([]any)
	if !ok || len(cmds) == 0 {
		t.Fatalf("recovery_metadata.actionable_commands: got %v, want non-empty list", meta["actionable_commands"])
	}

	for i, c := range cmds {
		entry, ok := c.(map[string]any)
		if !ok {
			t.Errorf("actionable_commands[%d]: not an object: %T", i, c)

			continue
		}

		cmd, _ := entry["cmd"].(string)
		if cmd == "" {
			t.Errorf("actionable_commands[%d].cmd: empty", i)
		}

		rev, _ := entry["reversibility_class"].(string)
		switch rev {
		case "read-only", "interrupts-io", "destroys-data":
			// ok
		default:
			t.Errorf("actionable_commands[%d].reversibility_class: got %q, want one of read-only|interrupts-io|destroys-data", i, rev)
		}
	}
}
