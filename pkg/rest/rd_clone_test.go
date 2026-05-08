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

// TestRDCloneCreatesCopy: POST .../clone duplicates a ResourceDefinition.
// Maps to `linstor resource-definition clone`.
func TestRDCloneCreatesCopy(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:  "pvc-1",
		Props: map[string]string{"k": "v"},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(map[string]string{"name": "pvc-2"})

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/clone", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201", resp.StatusCode)
	}

	got, err := st.ResourceDefinitions().Get(ctx, "pvc-2")
	if err != nil {
		t.Fatalf("expected pvc-2 to exist: %v", err)
	}

	if got.Props["k"] != "v" {
		t.Errorf("Props not copied: %v", got.Props)
	}
}

// TestRDCloneSourceMissing: 404 if the source RD doesn't exist.
func TestRDCloneSourceMissing(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	body, _ := json.Marshal(map[string]string{"name": "pvc-2"})

	resp := httpPost(t, base+"/v1/resource-definitions/ghost/clone", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", resp.StatusCode)
	}
}

// TestRDCloneTargetExists: 409 if the target name is taken.
func TestRDCloneTargetExists(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-2"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(map[string]string{"name": "pvc-2"})

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/clone", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusConflict {
		t.Errorf("status: got %d, want 409", resp.StatusCode)
	}
}
