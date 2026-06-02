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
	"io"
	"net/http"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestBug375SPDfnModifyRefusesZPoolOverride pins the Bug 375 (Round 9)
// guard: `PUT /v1/storage-pool-definitions/{name}` MUST refuse any
// `override_props` that touches the backing-driver identity keys
// (StorDriver/ZPool[Thin], LvmVg, ThinPool, FileDir, StorPoolName)
// when the named SPD already exists.
//
// SPD-level echo of Bug 373: the StoragePoolDefinition is the catalog
// row every future per-node StoragePool inherits its default backing
// driver identity from. Pre-fix, `linstor sp-d set-property zfs-thin
// StorDriver/ZPoolThin bogus-pool` returned 200 + MASK_INFO and the
// catalog row flipped to "bogus-pool", silently desyncing the SPD
// default from any per-node SP that hadn't materialised yet. Post-fix:
// 400 + apiCallRcFailInvldStorPoolName (552), live row stays byte-
// identical, operator gets actionable "drop + recreate the definition"
// guidance.
func TestBug375SPDfnModifyRefusesZPoolOverride(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.StoragePoolDefinitions().Create(ctx, &store.StoragePoolDefinition{
		Name:  "zfs-thin",
		Props: map[string]string{"StorDriver/ZPoolThin": "blockstor-zfs"},
	}); err != nil {
		t.Fatalf("seed SPD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.GenericPropsModify{
		OverrideProps: map[string]string{"StorDriver/ZPoolThin": "bogus-catalog-pool"},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPut(t, base+"/v1/storage-pool-definitions/zfs-thin", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status: got %d, want 400", resp.StatusCode)
	}

	raw, _ := io.ReadAll(resp.Body)

	var envelope []apiv1.APICallRc
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatalf("decode envelope: %v (body: %s)", err, string(raw))
	}

	if len(envelope) == 0 {
		t.Fatalf("envelope empty, want one ApiCallRc")
	}

	if envelope[0].RetCode != apiCallRcError|apiCallRcFailInvldStorPoolName {
		t.Errorf("ret_code = %d, want %d (apiCallRcError | apiCallRcFailInvldStorPoolName)",
			envelope[0].RetCode, apiCallRcError|apiCallRcFailInvldStorPoolName)
	}

	if !strings.Contains(envelope[0].Message, "StorDriver/ZPoolThin") {
		t.Errorf("message missing offending key: %q", envelope[0].Message)
	}

	if envelope[0].ObjRefs["StorPoolDfn"] != "zfs-thin" {
		t.Errorf("obj_refs[StorPoolDfn] = %q, want zfs-thin", envelope[0].ObjRefs["StorPoolDfn"])
	}

	// Catalog row's Props must stay byte-identical to pre-call — the
	// whole point of the fix is "PUT must not flip the backing key on
	// an existing definition".
	got, err := st.StoragePoolDefinitions().Get(ctx, "zfs-thin")
	if err != nil {
		t.Fatalf("Get after refused PUT: %v", err)
	}

	if got.Props["StorDriver/ZPoolThin"] != "blockstor-zfs" {
		t.Errorf("Props[StorDriver/ZPoolThin] = %q, want blockstor-zfs (refused PUT must not mutate the row)",
			got.Props["StorDriver/ZPoolThin"])
	}
}

// TestBug375SPDfnModifyRefusesDeletePropsOnDriverKey pins the
// `delete_props`-flank guard. Same as the SP-level Bug 373 case: a
// `delete_props` of `StorDriver/LvmVg` is arguably worse than override
// because it leaves the catalog with no default backing key, so every
// per-node SP that auto-inherits from the SPD breaks on creation with
// a misleading "requires StorDriver/<key> in props" error.
func TestBug375SPDfnModifyRefusesDeletePropsOnDriverKey(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.StoragePoolDefinitions().Create(ctx, &store.StoragePoolDefinition{
		Name: "lvm-thin",
		Props: map[string]string{
			"StorDriver/LvmVg":    "vg1",
			"StorDriver/ThinPool": "thin",
		},
	}); err != nil {
		t.Fatalf("seed SPD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.GenericPropsModify{
		DeleteProps: []string{"StorDriver/LvmVg"},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPut(t, base+"/v1/storage-pool-definitions/lvm-thin", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status: got %d, want 400", resp.StatusCode)
	}

	got, err := st.StoragePoolDefinitions().Get(ctx, "lvm-thin")
	if err != nil {
		t.Fatalf("Get after refused PUT: %v", err)
	}

	if got.Props["StorDriver/LvmVg"] != "vg1" {
		t.Errorf("Props[StorDriver/LvmVg] = %q, want vg1 (refused delete_props must not drop the key)",
			got.Props["StorDriver/LvmVg"])
	}
}

// TestBug375SPDfnModifyRefusesDeleteNamespaceOnStorDriver pins the
// `delete_namespaces`-flank guard. `linstor sp-d delete-namespace
// <name> StorDriver` would otherwise wipe the SPD's entire default
// backing identity in one round-trip.
func TestBug375SPDfnModifyRefusesDeleteNamespaceOnStorDriver(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.StoragePoolDefinitions().Create(ctx, &store.StoragePoolDefinition{
		Name:  "zfs-thin",
		Props: map[string]string{"StorDriver/ZPoolThin": "blockstor-zfs"},
	}); err != nil {
		t.Fatalf("seed SPD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.GenericPropsModify{
		DeleteNamespace: []string{"StorDriver"},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPut(t, base+"/v1/storage-pool-definitions/zfs-thin", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status: got %d, want 400", resp.StatusCode)
	}

	raw, _ := io.ReadAll(resp.Body)

	var envelope []apiv1.APICallRc
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatalf("decode envelope: %v (body: %s)", err, string(raw))
	}

	if !strings.Contains(envelope[0].Message, "delete_namespaces:StorDriver") {
		t.Errorf("message missing namespace hit: %q", envelope[0].Message)
	}

	got, err := st.StoragePoolDefinitions().Get(ctx, "zfs-thin")
	if err != nil {
		t.Fatalf("Get after refused PUT: %v", err)
	}

	if got.Props["StorDriver/ZPoolThin"] != "blockstor-zfs" {
		t.Errorf("Props[StorDriver/ZPoolThin] = %q, want blockstor-zfs (refused delete_namespaces must not drop keys)",
			got.Props["StorDriver/ZPoolThin"])
	}
}

// TestBug375SPDfnModifyAcceptsBenignOverride is the positive flank: a
// PUT that only touches non-driver props (Aux/*, MaxOversubscription
// Ratio, etc.) MUST keep landing exactly as it did before the fix.
// Regression guard against an overly-broad refuseSPDriverPropMutation
// match.
func TestBug375SPDfnModifyAcceptsBenignOverride(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.StoragePoolDefinitions().Create(ctx, &store.StoragePoolDefinition{
		Name:  "zfs-thin",
		Props: map[string]string{"StorDriver/ZPoolThin": "blockstor-zfs"},
	}); err != nil {
		t.Fatalf("seed SPD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.GenericPropsModify{
		OverrideProps: map[string]string{
			"Aux/rack-id":              "r1",
			"MaxOversubscriptionRatio": "2.0",
		},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPut(t, base+"/v1/storage-pool-definitions/zfs-thin", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200 (benign override must land)", resp.StatusCode)
	}

	got, err := st.StoragePoolDefinitions().Get(ctx, "zfs-thin")
	if err != nil {
		t.Fatalf("Get after PUT: %v", err)
	}

	if got.Props["Aux/rack-id"] != "r1" {
		t.Errorf("Props[Aux/rack-id] = %q, want r1", got.Props["Aux/rack-id"])
	}

	if got.Props["MaxOversubscriptionRatio"] != "2.0" {
		t.Errorf("Props[MaxOversubscriptionRatio] = %q, want 2.0", got.Props["MaxOversubscriptionRatio"])
	}

	if got.Props["StorDriver/ZPoolThin"] != "blockstor-zfs" {
		t.Errorf("Props[StorDriver/ZPoolThin] = %q, want blockstor-zfs (benign PUT must preserve backing key)",
			got.Props["StorDriver/ZPoolThin"])
	}
}

// TestBug375SPDfnPUTCreateAllowsDriverKey is the PUT-create flank.
// When the named SPD does NOT yet exist, the handler's auto-create
// branch must still accept StorDriver/* seed props — that's the
// `linstor sp-d set-property new-def StorDriver/LvmVg vg-x` one-round-
// trip provisioning flow operators rely on. The refusal is gated on
// "the row exists already"; brand-new names are a seed path, not a
// mutation path.
func TestBug375SPDfnPUTCreateAllowsDriverKey(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.GenericPropsModify{
		OverrideProps: map[string]string{
			"StorDriver/LvmVg":    "vg-new-1",
			"StorDriver/ThinPool": "thin",
		},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPut(t, base+"/v1/storage-pool-definitions/brand-new-spd", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200 (PUT-create with seed StorDriver/* must succeed)",
			resp.StatusCode)
	}

	got, err := st.StoragePoolDefinitions().Get(ctx, "brand-new-spd")
	if err != nil {
		t.Fatalf("Get after PUT-create: %v", err)
	}

	if got.Props["StorDriver/LvmVg"] != "vg-new-1" {
		t.Errorf("Props[StorDriver/LvmVg] = %q, want vg-new-1 (PUT-create must seed the backing key)",
			got.Props["StorDriver/LvmVg"])
	}

	if got.Props["StorDriver/ThinPool"] != "thin" {
		t.Errorf("Props[StorDriver/ThinPool] = %q, want thin", got.Props["StorDriver/ThinPool"])
	}
}
