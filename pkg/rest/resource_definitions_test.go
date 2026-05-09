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

	lapi "github.com/LINBIT/golinstor/client"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestResourceDefinitionsListEmpty: empty list, never nil.
func TestResourceDefinitionsListEmpty(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	c := newClient(t, base)

	got, err := c.ResourceDefinitions.GetAll(t.Context(), lapi.RDGetAllRequest{})
	if err != nil {
		t.Fatalf("GetAll: %v", err)
	}

	if got == nil || len(got) != 0 {
		t.Errorf("got %v, want empty", got)
	}
}

// TestResourceDefinitionsCreateRoundTrip: create via golinstor, get it back.
func TestResourceDefinitionsCreateRoundTrip(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	c := newClient(t, base)

	if err := c.ResourceDefinitions.Create(t.Context(), lapi.ResourceDefinitionCreate{
		ResourceDefinition: lapi.ResourceDefinition{
			Name:              "pvc-1",
			ExternalName:      "pvc-1",
			ResourceGroupName: "rg-1",
		},
	}); err != nil {
		t.Fatalf("Create: %v", err)
	}

	got, err := c.ResourceDefinitions.Get(t.Context(), "pvc-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.Name != "pvc-1" || got.ResourceGroupName != "rg-1" {
		t.Errorf("got %+v", got)
	}
}

// TestResourceDefinitionsCreateConflict: 409 on duplicate.
func TestResourceDefinitionsCreateConflict(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.ResourceDefinitionCreate{
		ResourceDefinition: apiv1.ResourceDefinition{Name: "pvc-1"},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-definitions", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusConflict {
		t.Errorf("status: got %d, want 409", resp.StatusCode)
	}
}

// TestResourceDefinitionsGetMissing: 404 on missing rd.
func TestResourceDefinitionsGetMissing(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpGet(t, base+"/v1/resource-definitions/ghost")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", resp.StatusCode)
	}
}

// TestResourceDefinitionsDelete: round-trip via golinstor.
func TestResourceDefinitionsDelete(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	c := newClient(t, base)
	if err := c.ResourceDefinitions.Delete(t.Context(), "pvc-1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	all, err := c.ResourceDefinitions.GetAll(t.Context(), lapi.RDGetAllRequest{})
	if err != nil {
		t.Fatalf("GetAll: %v", err)
	}

	if len(all) != 0 {
		t.Errorf("after Delete: got %d, want 0", len(all))
	}
}

// TestResourceDefinitionsWithoutStore: 503 without store.
func TestResourceDefinitionsWithoutStore(t *testing.T) {
	base, stop := startServerCustom(t, &Server{Addr: pickFreeAddr(t), Store: nil})
	defer stop()

	resp := httpGet(t, base+"/v1/resource-definitions")
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status: got %d, want 503", resp.StatusCode)
	}
}

// TestResourceDefinitionUpdateChangesRG: PUT /v1/resource-definitions/{rd}
// with a new resource_group_name persists the change. Subsequent
// reads return the new parent. Ensures `linstor rd m --resource-group=X`
// works mechanically — the DRBD-options resolver picks up the new
// RG's props on the next satellite reconcile via the option hierarchy.
func TestResourceDefinitionUpdateChangesRG(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	for _, rg := range []string{"rg-old", "rg-new"} {
		if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{Name: rg}); err != nil {
			t.Fatalf("seed RG %s: %v", rg, err)
		}
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:              "pvc-1",
		ResourceGroupName: "rg-old",
	}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.ResourceDefinition{
		Name:              "pvc-1",
		ResourceGroupName: "rg-new",
	})

	resp := httpPut(t, base+"/v1/resource-definitions/pvc-1", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.ResourceDefinitions().Get(ctx, "pvc-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if got.ResourceGroupName != "rg-new" {
		t.Errorf("RG: got %q, want rg-new", got.ResourceGroupName)
	}
}
