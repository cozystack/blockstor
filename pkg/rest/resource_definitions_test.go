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
	"slices"
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
// 200 + warn-mask `resource definition already absent` envelope, NOT
// 404. CSI § DeleteVolume is idempotent, so a re-issued delete on an
// RD that the previous request already cleared must succeed —
// otherwise linstor-csi loops on its retry path. Mirrors upstream
// LINSTOR's `linstor rd d` on a missing RD (200 + WARNING exit 0)
// and the Bug 56 fix for the per-resource DELETE.
func TestResourceDefinitionsDeleteMissingRD(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpDelete(t, base+"/v1/resource-definitions/ghost")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status: got %d, want 200", resp.StatusCode)
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

// TestRDCreateInheritsLayerStackFromRG pins Bug 54: when the caller POSTs
// an RD with empty layer_stack and a resource_group_name that points at an
// RG whose SelectFilter pins a LayerStack, the stored RD must carry that
// stack. Without this stamp, the dispatcher reads rd.Spec.LayerStack == nil
// and the satellite's legacy needsDRBD default re-stacks DRBD onto an
// STORAGE-only RG (reproducer: `linstor rg c test`, set LayerStack=STORAGE,
// `linstor rd c test --resource-group test` → Resources show DRBD,STORAGE).
func TestRDCreateInheritsLayerStackFromRG(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "test-rg",
		SelectFilter: apiv1.AutoSelectFilter{
			LayerStack: []string{apiv1.LayerKindStorage},
		},
	}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.ResourceDefinitionCreate{
		ResourceDefinition: apiv1.ResourceDefinition{
			Name:              "rd-inherit",
			ResourceGroupName: "test-rg",
			// LayerStack intentionally empty — exercise the inheritance path.
		},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-definitions", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201", resp.StatusCode)
	}

	got, err := st.ResourceDefinitions().Get(ctx, "rd-inherit")
	if err != nil {
		t.Fatalf("get rd: %v", err)
	}

	want := []string{apiv1.LayerKindStorage}
	if !slices.Equal(got.LayerStack, want) {
		t.Errorf("LayerStack: got %v, want %v", got.LayerStack, want)
	}
}

// TestRDCreateExplicitLayerStackWinsOverRG pins the precedence rule:
// when the caller supplies a non-empty layer_stack on the RD, the parent
// RG's SelectFilter.LayerStack does NOT override it. Caller > RG, mirrors
// v1.ResolveLayerStack's RD-wins ordering at the dispatch read-side and
// keeps operator-supplied compositions sticky against a later RG retune.
func TestRDCreateExplicitLayerStackWinsOverRG(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "test-rg",
		SelectFilter: apiv1.AutoSelectFilter{
			LayerStack: []string{apiv1.LayerKindStorage},
		},
	}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	explicit := []string{apiv1.LayerKindDRBD, apiv1.LayerKindStorage}

	body, err := json.Marshal(apiv1.ResourceDefinitionCreate{
		ResourceDefinition: apiv1.ResourceDefinition{
			Name:              "rd-explicit",
			ResourceGroupName: "test-rg",
			LayerStack:        explicit,
		},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-definitions", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201", resp.StatusCode)
	}

	got, err := st.ResourceDefinitions().Get(ctx, "rd-explicit")
	if err != nil {
		t.Fatalf("get rd: %v", err)
	}

	if !slices.Equal(got.LayerStack, explicit) {
		t.Errorf("LayerStack: got %v, want explicit %v", got.LayerStack, explicit)
	}
}

// TestRDCreateNoInheritWhenRGHasNoLayerStack regression-guards the new
// inheritance code: when the parent RG has no SelectFilter LayerStack,
// the RD's LayerStack must stay empty (the legacy "empty == default
// DRBD+STORAGE" downstream path takes over via dispatcher / needsDRBD).
// Otherwise the inheritance pass would stamp a hard default that
// future SelectFilter-mediated overrides could no longer relax.
func TestRDCreateNoInheritWhenRGHasNoLayerStack(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "silent-rg",
		// SelectFilter intentionally zero — no LayerStack pin.
	}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.ResourceDefinitionCreate{
		ResourceDefinition: apiv1.ResourceDefinition{
			Name:              "rd-silent",
			ResourceGroupName: "silent-rg",
		},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-definitions", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201", resp.StatusCode)
	}

	got, err := st.ResourceDefinitions().Get(ctx, "rd-silent")
	if err != nil {
		t.Fatalf("get rd: %v", err)
	}

	if len(got.LayerStack) != 0 {
		t.Errorf("LayerStack: got %v, want empty (defer to legacy default)", got.LayerStack)
	}
}

// TestRDCreateScenario4W09BareRDDefaultsAndInherits closes wave2-04
// scenario 4.W09: `linstor rd create <name>` with no further flags
// reserves the RD by name only — storage allocation is deferred to
// `r c` / `rg spawn`. The bare-create wire shape (just `name` set)
// must:
//
//  1. Persist the RD under its given name (201 Created, GET returns it).
//  2. Default `ResourceGroupName` to the canonical `DfltRscGrp` literal
//     when the caller didn't pin one — and lazily create that RG.
//  3. Leave `LayerStack` empty so the downstream legacy default
//     (DRBD+STORAGE via dispatcher / needsDRBD) governs the eventual
//     replicas without the apiserver locking in a stack at RD-create.
//  4. NOT allocate a DRBD TCP port at this stage — port allocation is
//     per-replica (internal/controller/resource_controller's
//     `allocateDRBDFields` picks from `pkg/drbd.LowestFreePort` on the
//     controller's per-node range) and happens on `r c`, not on
//     RD-create. Bare RDs carry no `tcp_ports` on the wire.
//
// Sibling tests pin the explicit-RG branch (TestResourceDefinitionsCreateRoundTrip),
// the canonical CamelCase contract (TestEnsureDefaultRGUsesCanonicalCamelCase),
// and the Bug 54 LayerStack inheritance (TestRDCreateInheritsLayerStackFromRG).
// This test is the scenario-anchored end-to-end pin so wave2-04 4.W09 has
// one named assertion that fails together with any regression in the bare
// create shape — operators reading the scenario can jump straight here.
func TestRDCreateScenario4W09BareRDDefaultsAndInherits(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	base, stop := startServerWithStore(t, st)
	defer stop()

	// Bare create: only Name set. Mirrors `linstor rd create rd-bare`
	// with no `--resource-group` / `--layer-list` flags.
	body, err := json.Marshal(apiv1.ResourceDefinitionCreate{
		ResourceDefinition: apiv1.ResourceDefinition{Name: "rd-bare"},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-definitions", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201", resp.StatusCode)
	}

	rd, err := st.ResourceDefinitions().Get(ctx, "rd-bare")
	if err != nil {
		t.Fatalf("get rd: %v", err)
	}

	if rd.Name != "rd-bare" {
		t.Errorf("Name: got %q, want %q", rd.Name, "rd-bare")
	}

	if rd.ResourceGroupName != DefaultResourceGroupName {
		t.Errorf("ResourceGroupName: got %q, want %q (default RG)",
			rd.ResourceGroupName, DefaultResourceGroupName)
	}

	if len(rd.LayerStack) != 0 {
		t.Errorf("LayerStack: got %v, want empty (defer to legacy default)",
			rd.LayerStack)
	}

	// Bare RD must NOT carry a per-replica DRBD layer entry — port
	// allocation is deferred to `r c` / `rg spawn` at the controller
	// (internal/controller/resource_controller.allocateDRBDFields).
	for _, layer := range rd.LayerData {
		if layer.Drbd != nil && len(layer.Drbd.TCPPorts) > 0 {
			t.Errorf("bare RD carries DRBD TCPPorts %v — port allocation must defer to `r c`",
				layer.Drbd.TCPPorts)
		}
	}

	// Default RG was lazily materialised under the canonical literal.
	rg, err := st.ResourceGroups().Get(ctx, DefaultResourceGroupName)
	if err != nil {
		t.Fatalf("default RG not created on bare RD-create: %v", err)
	}

	if rg.Name != DefaultResourceGroupName {
		t.Errorf("default RG Name: got %q, want %q", rg.Name, DefaultResourceGroupName)
	}
}

// TestRDCreateScenario4W09ExplicitRGPreserved closes the
// `--resource-group <rg>` branch of scenario 4.W09: an operator-supplied
// RG must be persisted verbatim (no rewrite to `DfltRscGrp`) and the
// lazily-created default RG must NOT appear as a side effect — that
// would pollute `linstor rg l` against clusters where the operator
// curates their own RG set.
func TestRDCreateScenario4W09ExplicitRGPreserved(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "ops-rg",
	}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.ResourceDefinitionCreate{
		ResourceDefinition: apiv1.ResourceDefinition{
			Name:              "rd-explicit-rg",
			ResourceGroupName: "ops-rg",
		},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-definitions", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201", resp.StatusCode)
	}

	rd, err := st.ResourceDefinitions().Get(ctx, "rd-explicit-rg")
	if err != nil {
		t.Fatalf("get rd: %v", err)
	}

	if rd.ResourceGroupName != "ops-rg" {
		t.Errorf("ResourceGroupName: got %q, want %q (explicit caller value)",
			rd.ResourceGroupName, "ops-rg")
	}

	// No side-effect creation of the default RG.
	rgs, err := st.ResourceGroups().List(ctx)
	if err != nil {
		t.Fatalf("list RGs: %v", err)
	}

	for _, rg := range rgs {
		if rg.Name == DefaultResourceGroupName {
			t.Errorf("explicit RG path created %q as a side effect (RG list: %v)",
				DefaultResourceGroupName, rgNames(rgs))
		}
	}
}

// seedRDs is a small helper for the filter-test family: inserts the
// named RDs into the in-memory store and returns the wired server.
func seedRDsForFilterTests(t *testing.T, names ...string) (string, func()) {
	t.Helper()

	st := store.NewInMemory()
	ctx := t.Context()

	for _, name := range names {
		if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: name}); err != nil {
			t.Fatalf("seed %q: %v", name, err)
		}
	}

	return startServerWithStore(t, st)
}

// getRDListWith fires GET /v1/resource-definitions{query} and decodes
// the response into the canonical wire shape so filter tests stay
// close to a real client. The body is closed before returning so the
// caller doesn't have to (and bodyclose stays happy with a single
// helper covering both the request and the resource cleanup).
func getRDListWith(t *testing.T, base, query string) []apiv1.ResourceDefinition {
	t.Helper()

	resp := httpGet(t, base+"/v1/resource-definitions"+query)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	var rds []apiv1.ResourceDefinition

	if err := json.NewDecoder(resp.Body).Decode(&rds); err != nil {
		t.Fatalf("decode: %v", err)
	}

	return rds
}

func rdNames(rds []apiv1.ResourceDefinition) []string {
	out := make([]string, 0, len(rds))
	for i := range rds {
		out = append(out, rds[i].Name)
	}

	return out
}

// TestRDListFiltersByNameQuery pins Bug 61: the upstream LINSTOR CLI
// sends `?resource_definitions=<name>` on `linstor rd l
// --resource-definitions <name>`. Before the fix, the param was
// ignored and ALL RDs came back.
func TestRDListFiltersByNameQuery(t *testing.T) {
	base, stop := seedRDsForFilterTests(t, "alpha", "beta", "gamma")
	defer stop()

	rds := getRDListWith(t, base, "?resource_definitions=beta")

	if got := rdNames(rds); len(got) != 1 || got[0] != "beta" {
		t.Errorf("filter=beta: got %v, want [beta]", got)
	}
}

// TestRDListFiltersAcceptsMultipleNames pins the multi-value wire
// shape python-linstor's urlencode(doseq=True) sends:
// `?resource_definitions=a&resource_definitions=b`.
func TestRDListFiltersAcceptsMultipleNames(t *testing.T) {
	base, stop := seedRDsForFilterTests(t, "alpha", "beta", "gamma")
	defer stop()

	rds := getRDListWith(t, base, "?resource_definitions=alpha&resource_definitions=gamma")

	got := rdNames(rds)
	slices.Sort(got)

	want := []string{"alpha", "gamma"}
	if !slices.Equal(got, want) {
		t.Errorf("multi-name filter: got %v, want %v", got, want)
	}
}

// TestRDListFiltersCaseInsensitive pins upstream LINSTOR's
// case-insensitive RD-name matching: the controller normalises names
// when filtering, so `?resource_definitions=ALPHA` must match the
// stored `alpha`.
func TestRDListFiltersCaseInsensitive(t *testing.T) {
	base, stop := seedRDsForFilterTests(t, "alpha", "beta", "gamma")
	defer stop()

	rds := getRDListWith(t, base, "?resource_definitions=ALPHA")

	if got := rdNames(rds); len(got) != 1 || got[0] != "alpha" {
		t.Errorf("case-insensitive filter: got %v, want [alpha]", got)
	}
}

// TestRDListFilterUnknownNameReturnsEmpty pins the upstream
// "filter — not lookup" semantic: an unknown RD name in the filter
// yields an empty 200 body, NOT a 404. (404 is reserved for the
// per-RD GET /v1/resource-definitions/{rd} path.)
func TestRDListFilterUnknownNameReturnsEmpty(t *testing.T) {
	base, stop := seedRDsForFilterTests(t, "alpha", "beta", "gamma")
	defer stop()

	rds := getRDListWith(t, base, "?resource_definitions=ghost")

	if len(rds) != 0 {
		t.Errorf("filter=ghost: got %v, want []", rdNames(rds))
	}
}

// TestRDListNoFilterReturnsAll pins backward compat: callers that
// don't send the `resource_definitions` query param see the full
// list (golinstor's plain `ResourceDefinitions.GetAll`, csi-linstor's
// existing call sites).
func TestRDListNoFilterReturnsAll(t *testing.T) {
	base, stop := seedRDsForFilterTests(t, "alpha", "beta", "gamma")
	defer stop()

	rds := getRDListWith(t, base, "")

	got := rdNames(rds)
	slices.Sort(got)

	want := []string{"alpha", "beta", "gamma"}
	if !slices.Equal(got, want) {
		t.Errorf("no-filter: got %v, want %v", got, want)
	}
}
