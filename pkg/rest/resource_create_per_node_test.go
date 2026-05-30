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
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestResourceCreateOnNodeFromURL pins the linstor-csi v1.10.1 wire
// shape for a single-node resource create: POST to
// /v1/resource-definitions/{rd}/resources/{node} with an empty body
// (the URL path carries the complete intent). Pre-fix this route was
// only registered for PUT (modify-props) and the CSI driver looped
// forever on "method not allowed" → CreateVolume failed.
func TestResourceCreateOnNodeFromURL(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-local-1"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Nodes().Create(t.Context(), &apiv1.Node{Name: "worker-1"}); err != nil {
		t.Fatalf("seed Node: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-local-1/resources/worker-1", nil)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201", resp.StatusCode)
	}

	got, err := st.Resources().Get(t.Context(), "pvc-local-1", "worker-1")
	if err != nil {
		t.Fatalf("Resources().Get: %v", err)
	}

	if got.NodeName != "worker-1" {
		t.Errorf("NodeName: got %q, want %q", got.NodeName, "worker-1")
	}
}

// TestResourceCreateOnNodeBodyConflict pins the strict refusal when
// the URL path and the body disagree on which node the Resource lives
// on. Silently honouring one over the other would let a typo create
// a replica on the wrong node and the operator would never see it.
func TestResourceCreateOnNodeBodyConflict(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-conf-1"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	for _, n := range []string{"worker-1", "worker-2"} {
		if err := st.Nodes().Create(t.Context(), &apiv1.Node{Name: n}); err != nil {
			t.Fatalf("seed Node %s: %v", n, err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.ResourceCreate{
		Resource: apiv1.Resource{NodeName: "worker-2"},
	})

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-conf-1/resources/worker-1", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("conflict status: got %d, want 400", resp.StatusCode)
	}

	// Sanity: no Resource was created on EITHER node.
	for _, n := range []string{"worker-1", "worker-2"} {
		_, err := st.Resources().Get(t.Context(), "pvc-conf-1", n)
		if err == nil {
			t.Errorf("Resource leaked on %s after rejected conflict create", n)
		}
	}
}

// TestResourceCreateOnNodeBodyMatchesURL verifies the happy path
// where the body explicitly states the same NodeName as the URL —
// CSI clients that DO populate NodeName in the body must work too.
func TestResourceCreateOnNodeBodyMatchesURL(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-match-1"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Nodes().Create(t.Context(), &apiv1.Node{Name: "worker-3"}); err != nil {
		t.Fatalf("seed Node: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.ResourceCreate{
		Resource: apiv1.Resource{NodeName: "worker-3"},
	})

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-match-1/resources/worker-3", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		body := make([]byte, 4096)
		n, _ := resp.Body.Read(body)
		t.Fatalf("status: got %d, want 201; body=%s", resp.StatusCode, strings.TrimSpace(string(body[:n])))
	}
}
