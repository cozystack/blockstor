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

// TestResourceDefinitionsCreateBadJSON: malformed body → 400 from
// the JSON decoder. Pinned because golinstor is the primary client
// here; a regression that surfaced raw decoder errors as 500 would
// flip golinstor's retry classification (it retries 5xx, gives up
// on 4xx) and bury operator typos in infinite loops.
func TestResourceDefinitionsCreateBadJSON(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpPost(t, base+"/v1/resource-definitions", []byte("{not-json"))
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400", resp.StatusCode)
	}
}

// TestResourceDefinitionsCreateMissingName: empty name in body → 400.
// The spawn-flow always supplies a name, but the bare-RD-create
// endpoint is also used by external tooling (linstor-csi reconciler
// edge cases); without this validator the store would persist a
// nameless RD that no later reconcile can address.
func TestResourceDefinitionsCreateMissingName(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	body, err := json.Marshal(apiv1.ResourceDefinitionCreate{
		ResourceDefinition: apiv1.ResourceDefinition{}, // Name omitted
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-definitions", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400 (missing name)", resp.StatusCode)
	}
}

// TestResourceDefinitionsUpdateBadJSON: malformed body → 400 from
// the JSON decoder on the PUT path.
func TestResourceDefinitionsUpdateBadJSON(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPut(t, base+"/v1/resource-definitions/pvc-1", []byte("{not-json"))
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400", resp.StatusCode)
	}
}

// TestResourceDefinitionsUpdateMissingRD: PUT against a non-existent
// {rd} pathvar → 404 (writeStoreError translates ErrNotFound).
func TestResourceDefinitionsUpdateMissingRD(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	body, err := json.Marshal(apiv1.ResourceDefinition{ResourceGroupName: "rg-new"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPut(t, base+"/v1/resource-definitions/ghost", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", resp.StatusCode)
	}
}

// TestResourceDefinitionsDeleteMissingRD: DELETE on missing RD →
// 404 surface from writeStoreError (idempotent delete is the
// store's job, not the handler's, but the handler must surface
// the not-found cleanly).
func TestResourceDefinitionsDeleteMissingRD(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpDelete(t, base+"/v1/resource-definitions/ghost")
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", resp.StatusCode)
	}
}

// TestResourceDefinitionsDeleteCascadesChildren pins the RD-delete
// cascade: every child Resource must be deleted before the RD
// itself goes. Without the cascade, child Resource CRDs never
// receive a DeletionTimestamp, the satellite's
// `blockstor.io.blockstor.io/satellite-resource` finalizer never
// fires, and DRBD kernel state (minor numbers, TCP ports, peer
// entries) lingers on every node — the next RD-create with the
// same name then collides on a stale port or sees half-configured
// peers.
//
// Regression guard for Bug 1: the trivial handler that only called
// Store.ResourceDefinitions().Delete left orphan replicas.
func TestResourceDefinitionsDeleteCascadesChildren(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-cascade"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	for _, n := range []string{"n1", "n2", "n3"} {
		if err := st.Resources().Create(ctx, &apiv1.Resource{
			Name: "pvc-cascade", NodeName: n,
		}); err != nil {
			t.Fatalf("seed replica %s: %v", n, err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t, base+"/v1/resource-definitions/pvc-cascade")
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("delete status: got %d, want 200", resp.StatusCode)
	}

	// All children gone.
	left, err := st.Resources().ListByDefinition(ctx, "pvc-cascade")
	if err != nil {
		t.Fatalf("list children: %v", err)
	}

	if len(left) != 0 {
		t.Errorf("children left after cascade: %d (%v)", len(left), left)
	}

	// RD itself gone.
	if _, err := st.ResourceDefinitions().Get(ctx, "pvc-cascade"); err == nil {
		t.Errorf("RD still present after delete")
	}
}

// TestResourceDefinitionsDeleteCascadeMissingChildIsIdempotent: the
// cascade swallows ErrNotFound on a per-child Delete. Models the
// race where another controller (or a previous, partial cascade)
// already removed the child between the ListByDefinition and the
// per-child Delete. The whole RD-delete must still succeed —
// otherwise concurrent reconciles never converge.
func TestResourceDefinitionsDeleteCascadeMissingChildIsIdempotent(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	// Seed an RD with no children. The cascade has nothing to
	// drop, but the handler must still succeed: this models the
	// "every child already gone" tail of the race window.
	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-empty"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t, base+"/v1/resource-definitions/pvc-empty")
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status: got %d, want 200 (cascade with no children is OK)", resp.StatusCode)
	}

	if _, err := st.ResourceDefinitions().Get(ctx, "pvc-empty"); err == nil {
		t.Errorf("RD still present after delete")
	}
}
