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

// Bug 112: `n describe <node>` crashed in the python CLI because the
// apiserver's volume-bearing endpoints emitted `allocated_size_kib`
// under `,omitempty` — a Go zero collapsed to a missing JSON key,
// the python CLI's `vlm.allocated_size` property returned None, and
// `SizeCalc.approximate_size_string(None)` raised TypeError. Fix:
// always emit `allocated_size_kib` as an int (default 0). The wire
// contract pinned here is "the JSON key is present and the value
// parses as an int, never `null` and never absent".

// TestBug112AllocatedSizeNeverNull pins the `/v1/view/resources` wire
// contract: every volume in every replica MUST surface
// `allocated_size_kib` as a non-null int (>= 0). A Go zero must
// serialise as `0`, not be omitted or rendered as `null` — without
// this, the python CLI's `n describe` crashes on `SizeCalc.
// approximate_size_string(None)`.
func TestBug112AllocatedSizeNeverNull(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n1"}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "rdthin"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	// Resource with a volume that has zero allocated size — mimics
	// freshly-provisioned FILE_THIN volume on a satellite that hasn't
	// reported usage yet. Before the fix the zero AllocatedKib was
	// dropped by `omitempty` and the wire view omitted the key.
	if err := st.Resources().Create(ctx, &apiv1.Resource{
		Name:     "rdthin",
		NodeName: "n1",
		Volumes: []apiv1.Volume{{
			VolumeNumber: 0,
			StoragePool:  "stand",
			AllocatedKib: 0, // satellite hasn't reported yet
		}},
	}); err != nil {
		t.Fatalf("seed resource: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/view/resources")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	// Decode into a loose map so we can introspect what the wire
	// actually emitted — a strongly-typed decode would silently
	// swallow `null` into the Go zero, hiding the regression.
	var wire []map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&wire); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(wire) == 0 {
		t.Fatalf("empty view; expected 1 replica")
	}

	for _, rsc := range wire {
		volsRaw, ok := rsc["volumes"]
		if !ok {
			t.Errorf("replica %v: missing volumes key", rsc["name"])

			continue
		}

		vols, ok := volsRaw.([]any)
		if !ok {
			t.Errorf("replica %v: volumes is not a list", rsc["name"])

			continue
		}

		for i, vRaw := range vols {
			vmap, ok := vRaw.(map[string]any)
			if !ok {
				t.Errorf("replica %v vol %d: not an object", rsc["name"], i)

				continue
			}

			val, present := vmap["allocated_size_kib"]
			if !present {
				t.Errorf("replica %v vol %d: allocated_size_kib MISSING from wire — python CLI's n describe will crash on None",
					rsc["name"], i)

				continue
			}

			if val == nil {
				t.Errorf("replica %v vol %d: allocated_size_kib is null — python CLI's n describe will crash on None",
					rsc["name"], i)

				continue
			}

			// JSON numbers decode to float64; the type check rules out
			// a future regression emitting a string.
			if _, isNum := val.(float64); !isNum {
				t.Errorf("replica %v vol %d: allocated_size_kib is not a number: %T %v",
					rsc["name"], i, val, val)
			}
		}
	}
}

// TestBug112NodeListAllocatedSizeNeverNull is the per-node-list
// surface mirror: any wire response that embeds Volume objects must
// keep the same `allocated_size_kib`-always-present contract.
// `/v1/nodes` itself doesn't carry Volumes, but `/v1/view/resources`
// is what `linstor n describe` calls under the hood (via
// `_linstor.resource_list()` per linstor_client/commands/node_cmds.py
// 766). This test pins the contract from a second angle — multiple
// replicas, mixed zero and non-zero allocated sizes — so a future
// refactor that re-introduces `omitempty` is caught even when a
// non-zero replica masks the regression on the first call.
func TestBug112NodeListAllocatedSizeNeverNull(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	for _, n := range []string{"n1", "n2"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{Name: n}); err != nil {
			t.Fatalf("seed node %s: %v", n, err)
		}
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "rdmix"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	seed := []apiv1.Resource{
		{Name: "rdmix", NodeName: "n1", Volumes: []apiv1.Volume{{
			VolumeNumber: 0,
			AllocatedKib: 0, // zero on this replica
		}}},
		{Name: "rdmix", NodeName: "n2", Volumes: []apiv1.Volume{{
			VolumeNumber: 0,
			AllocatedKib: 4096, // non-zero on the other replica
		}}},
	}

	for i := range seed {
		if err := st.Resources().Create(ctx, &seed[i]); err != nil {
			t.Fatalf("seed resource %s/%s: %v", seed[i].Name, seed[i].NodeName, err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/view/resources")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var wire []map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&wire); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(wire) != 2 {
		t.Fatalf("len: got %d replicas, want 2", len(wire))
	}

	for _, rsc := range wire {
		vols := rsc["volumes"].([]any)
		for i, vRaw := range vols {
			vmap := vRaw.(map[string]any)
			val, present := vmap["allocated_size_kib"]

			if !present || val == nil {
				t.Errorf("replica %v vol %d: allocated_size_kib missing or null (present=%v val=%v) — Bug 112 regression",
					rsc["node_name"], i, present, val)
			}
		}
	}
}
