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

// TestResourceGroupsListEmpty: golinstor sees an empty list, not nil.
func TestResourceGroupsListEmpty(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	c := newClient(t, base)

	got, err := c.ResourceGroups.GetAll(t.Context())
	if err != nil {
		t.Fatalf("GetAll: %v", err)
	}

	if got == nil || len(got) != 0 {
		t.Errorf("got %v, want empty", got)
	}
}

// TestResourceGroupsCreateRoundTrip: create via golinstor, fetch it back.
func TestResourceGroupsCreateRoundTrip(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	c := newClient(t, base)

	if err := c.ResourceGroups.Create(t.Context(), lapi.ResourceGroup{
		Name:        "rg-1",
		Description: "test",
		SelectFilter: lapi.AutoSelectFilter{
			PlaceCount:  3,
			StoragePool: "pool",
		},
	}); err != nil {
		t.Fatalf("Create: %v", err)
	}

	got, err := c.ResourceGroups.Get(t.Context(), "rg-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.Name != "rg-1" || got.Description != "test" {
		t.Errorf("got %+v", got)
	}

	if got.SelectFilter.PlaceCount != 3 || got.SelectFilter.StoragePool != "pool" {
		t.Errorf("SelectFilter: got %+v", got.SelectFilter)
	}
}

// TestResourceGroupsCreateConflict: 409 on duplicate name.
func TestResourceGroupsCreateConflict(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceGroups().Create(t.Context(), &apiv1.ResourceGroup{Name: "rg-1"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.ResourceGroup{Name: "rg-1"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-groups", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusConflict {
		t.Errorf("status: got %d, want 409", resp.StatusCode)
	}
}

// TestResourceGroupsGetMissing: 404 on missing rg.
func TestResourceGroupsGetMissing(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpGet(t, base+"/v1/resource-groups/ghost")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", resp.StatusCode)
	}
}

// TestResourceGroupsDelete: round-trip via golinstor.
func TestResourceGroupsDelete(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceGroups().Create(t.Context(), &apiv1.ResourceGroup{Name: "rg-1"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	c := newClient(t, base)
	if err := c.ResourceGroups.Delete(t.Context(), "rg-1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	all, err := c.ResourceGroups.GetAll(t.Context())
	if err != nil {
		t.Fatalf("GetAll: %v", err)
	}

	if len(all) != 0 {
		t.Errorf("after Delete, len=%d, want 0", len(all))
	}
}

// TestResourceGroupsWithoutStore: 503 when no store wired in.
func TestResourceGroupsWithoutStore(t *testing.T) {
	base, stop := startServerCustom(t, &Server{Addr: pickFreeAddr(t), Store: nil})
	defer stop()

	for _, path := range []string{
		"/v1/resource-groups",
		"/v1/resource-groups/rg-1",
	} {
		resp := httpGet(t, base+path)
		_ = resp.Body.Close()

		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Errorf("%s: got %d, want 503", path, resp.StatusCode)
		}
	}
}
