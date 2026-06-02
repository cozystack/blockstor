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

// TestBug373SPModifyRefusesZPoolOverride pins the Bug 373 (Round 8 P1)
// guard: `PUT /v1/nodes/{node}/storage-pools/{pool}` MUST refuse any
// `override_props` that touches the backing-driver identity keys
// (StorDriver/ZPool[Thin], LvmVg, ThinPool, FileDir, StorPoolName).
//
// Pre-fix, `linstor sp set-property dev-worker-1 zfs-thin
// StorDriver/ZPoolThin bogus-pool` returned 200 + MASK_INFO, the
// store row's StorDriver/ZPoolThin flipped to "bogus-pool", and the
// satellite's NewProviderFromKind started failing every subsequent
// placement / autoplace / resize against the pool. All active
// replicas still reported UpToDate (the kernel-DRBD device was
// already open against the original backend), so the cluster gave
// no operator-visible signal until the next provisioning call.
//
// Post-fix: 400 + apiCallRcFailInvldStorPoolName (552), live row's
// Props stay byte-identical to pre-call, operator gets actionable
// "drop + recreate the pool with the new backing key" guidance.
func TestBug373SPModifyRefusesZPoolOverride(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n1", Type: apiv1.NodeTypeSatellite}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		NodeName:        "n1",
		StoragePoolName: "zfs-thin",
		ProviderKind:    apiv1.StoragePoolKindZFSThin,
		Props:           map[string]string{"StorDriver/ZPoolThin": "blockstor-zfs"},
	}); err != nil {
		t.Fatalf("seed pool: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.GenericPropsModify{
		OverrideProps: map[string]string{"StorDriver/ZPoolThin": "bogus-live-pool"},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPut(t, base+"/v1/nodes/n1/storage-pools/zfs-thin", body)
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

	// Live row's Props must stay byte-identical to pre-call — the
	// whole point of the fix is "PUT must not flip the backing key".
	got, err := st.StoragePools().Get(ctx, "n1", "zfs-thin")
	if err != nil {
		t.Fatalf("Get after refused PUT: %v", err)
	}

	if got.Props["StorDriver/ZPoolThin"] != "blockstor-zfs" {
		t.Errorf("Props[StorDriver/ZPoolThin] = %q, want blockstor-zfs (refused PUT must not mutate the row)",
			got.Props["StorDriver/ZPoolThin"])
	}
}

// TestBug373SPModifyRefusesDeletePropsOnDriverKey pins the
// `delete_props`-flank guard. The same backing-identity keys must be
// refused via `delete_props` too — dropping the key wholesale is
// arguably worse than overriding it (the satellite-side provider
// loader will surface "requires StorDriver/<key> in props" instead
// of silently using a wrong backend).
func TestBug373SPModifyRefusesDeletePropsOnDriverKey(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n1", Type: apiv1.NodeTypeSatellite}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		NodeName:        "n1",
		StoragePoolName: "lvm-thin",
		ProviderKind:    apiv1.StoragePoolKindLVMThin,
		Props: map[string]string{
			"StorDriver/LvmVg":    "vg1",
			"StorDriver/ThinPool": "thin",
		},
	}); err != nil {
		t.Fatalf("seed pool: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.GenericPropsModify{
		DeleteProps: []string{"StorDriver/LvmVg"},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPut(t, base+"/v1/nodes/n1/storage-pools/lvm-thin", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status: got %d, want 400", resp.StatusCode)
	}

	got, err := st.StoragePools().Get(ctx, "n1", "lvm-thin")
	if err != nil {
		t.Fatalf("Get after refused PUT: %v", err)
	}

	if got.Props["StorDriver/LvmVg"] != "vg1" {
		t.Errorf("Props[StorDriver/LvmVg] = %q, want vg1 (refused delete_props must not drop the key)",
			got.Props["StorDriver/LvmVg"])
	}
}

// TestBug373SPModifyRefusesDeleteNamespaceOnStorDriver pins the
// `delete_namespaces`-flank guard. `linstor sp delete-namespace
// <n> <p> StorDriver` would otherwise wipe the pool's entire backing
// identity in one round-trip.
func TestBug373SPModifyRefusesDeleteNamespaceOnStorDriver(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n1", Type: apiv1.NodeTypeSatellite}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		NodeName:        "n1",
		StoragePoolName: "zfs-thin",
		ProviderKind:    apiv1.StoragePoolKindZFSThin,
		Props:           map[string]string{"StorDriver/ZPoolThin": "blockstor-zfs"},
	}); err != nil {
		t.Fatalf("seed pool: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.GenericPropsModify{
		DeleteNamespace: []string{"StorDriver"},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPut(t, base+"/v1/nodes/n1/storage-pools/zfs-thin", body)
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

	got, err := st.StoragePools().Get(ctx, "n1", "zfs-thin")
	if err != nil {
		t.Fatalf("Get after refused PUT: %v", err)
	}

	if got.Props["StorDriver/ZPoolThin"] != "blockstor-zfs" {
		t.Errorf("Props[StorDriver/ZPoolThin] = %q, want blockstor-zfs (refused delete_namespaces must not drop keys)",
			got.Props["StorDriver/ZPoolThin"])
	}
}

// TestBug373SPModifyAcceptsBenignOverride is the positive flank: a
// PUT that only touches non-driver props (PrefNic, Aux/*, etc.) MUST
// keep landing exactly as it did before the fix. Regression guard
// against an overly-broad refuseSPDriverPropMutation match.
func TestBug373SPModifyAcceptsBenignOverride(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n1", Type: apiv1.NodeTypeSatellite}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		NodeName:        "n1",
		StoragePoolName: "zfs-thin",
		ProviderKind:    apiv1.StoragePoolKindZFSThin,
		Props:           map[string]string{"StorDriver/ZPoolThin": "blockstor-zfs"},
	}); err != nil {
		t.Fatalf("seed pool: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.GenericPropsModify{
		OverrideProps: map[string]string{
			"PrefNic":     "default",
			"Aux/rack-id": "r1",
		},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPut(t, base+"/v1/nodes/n1/storage-pools/zfs-thin", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200 (benign override must land)", resp.StatusCode)
	}

	got, err := st.StoragePools().Get(ctx, "n1", "zfs-thin")
	if err != nil {
		t.Fatalf("Get after PUT: %v", err)
	}

	if got.Props["PrefNic"] != "default" {
		t.Errorf("Props[PrefNic] = %q, want default", got.Props["PrefNic"])
	}

	if got.Props["Aux/rack-id"] != "r1" {
		t.Errorf("Props[Aux/rack-id] = %q, want r1", got.Props["Aux/rack-id"])
	}

	if got.Props["StorDriver/ZPoolThin"] != "blockstor-zfs" {
		t.Errorf("Props[StorDriver/ZPoolThin] = %q, want blockstor-zfs (benign PUT must preserve backing key)",
			got.Props["StorDriver/ZPoolThin"])
	}
}
