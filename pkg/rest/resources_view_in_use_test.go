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
	"encoding/json"
	"net/http"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// A replica whose stack is topped by STORAGE has nothing that observes
// whether its volume is open, and upstream LINSTOR leaves its in_use
// unset, which `linstor r l` renders as a blank Usage cell. The store
// projects every replica's unobserved InUse as false, so the view used
// to send `in_use: false` and the CLI printed `Unused` for a volume a
// pod may be writing to. Stacks with a layer that reports usage keep
// what the store holds.
func TestResourcesViewInUseFollowsTheTopLayer(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	stacks := map[string][]string{
		"pvc-local":     {"STORAGE"},
		"pvc-encrypted": {"LUKS", "STORAGE"},
		"pvc-drbd":      {"DRBD", "STORAGE"},
	}

	for name, stack := range stacks {
		if err := st.ResourceDefinitions().Create(ctx,
			&apiv1.ResourceDefinition{Name: name, LayerStack: stack}); err != nil {
			t.Fatalf("seed RD: %v", err)
		}

		unused := false

		if err := st.Resources().Create(ctx, &apiv1.Resource{
			Name:     name,
			NodeName: "worker-1",
			State:    apiv1.ResourceState{InUse: &unused},
			Volumes:  []apiv1.Volume{{VolumeNumber: 0}},
		}); err != nil {
			t.Fatalf("seed Resource: %v", err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpGet(t, base+"/v1/view/resources")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var got []struct {
		Name  string         `json:"name"`
		State map[string]any `json:"state"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(got) != len(stacks) {
		t.Fatalf("len: got %d, want %d", len(got), len(stacks))
	}

	for _, res := range got {
		inUse, present := res.State["in_use"]

		switch res.Name {
		case "pvc-local":
			if present {
				t.Errorf("%s: in_use = %v, want it absent", res.Name, inUse)
			}
		default:
			if inUse != false {
				t.Errorf("%s: in_use = %v (present %v), want false", res.Name, inUse, present)
			}
		}
	}
}
