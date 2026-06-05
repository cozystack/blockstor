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

// Corner-case I1: empty set-property value = key deletion at the REST
// level, on EVERY CLI-reachable GenericPropsModify surface.
//
// PR #97 introduced the shared applyPropsModify core (override entry
// with an empty value deletes the key) and routed the resource-
// definition and resource-group modify handlers through it. The other
// ~10 GenericPropsModify sites kept the local `maps.Copy(props,
// override)` pattern, which STORED an empty string instead of deleting
// the key. This is the follow-up consolidation flagged in #97's commit
// body: every CLI-reachable set-property handler is now routed through
// applyPropsModify, so `linstor <obj> set-property KEY ""` converges to
// key-absence regardless of which wire shape the client sends.
//
// Upstream NOTE (UG9 §"Auto-quorum policies" ~4277): "Setting
// `DrbdOptions/Resource/on-no-quorum` to an empty value … deletes the
// property from the object entirely."
//
// Each sub-test seeds a target key plus a sibling key, sends an
// override_props with the target key mapped to "", and asserts the
// target key is GONE while the sibling SURVIVES (the merge must be a
// surgical delete, not a wipe).

// TestI1NodeEmptyOverrideDeletes pins `node set-property n KEY ""`.
func TestI1NodeEmptyOverrideDeletes(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	if err := st.Nodes().Create(t.Context(), &apiv1.Node{
		Name: "n1",
		Type: apiv1.NodeTypeSatellite,
		Props: map[string]string{
			"PrefNic":     "nic_10G",
			"Aux/keep-me": "stay",
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.NodeModify{
		GenericPropsModify: apiv1.GenericPropsModify{
			OverrideProps: map[string]string{"PrefNic": ""},
		},
	})

	resp := httpPut(t, base+"/v1/nodes/n1", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.Nodes().Get(t.Context(), "n1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if _, present := got.Props["PrefNic"]; present {
		t.Errorf("empty override must DELETE PrefNic, got Props=%v", got.Props)
	}

	if got.Props["Aux/keep-me"] != "stay" {
		t.Errorf("sibling clobbered: got Props=%v", got.Props)
	}
}

// TestI1StoragePoolEmptyOverrideDeletes pins `sp set-property n p KEY ""`.
func TestI1StoragePoolEmptyOverrideDeletes(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n1", Type: apiv1.NodeTypeSatellite}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
		NodeName:        "n1",
		StoragePoolName: "p1",
		ProviderKind:    apiv1.StoragePoolKindLVMThin,
		Props: map[string]string{
			"PrefNic":          "nic_10G",
			"StorDriver/LvmVg": "vg1",
		},
	}); err != nil {
		t.Fatalf("seed pool: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.GenericPropsModify{
		OverrideProps: map[string]string{"PrefNic": ""},
	})

	resp := httpPut(t, base+"/v1/nodes/n1/storage-pools/p1", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.StoragePools().Get(ctx, "n1", "p1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if _, present := got.Props["PrefNic"]; present {
		t.Errorf("empty override must DELETE PrefNic, got Props=%v", got.Props)
	}

	if got.Props["StorDriver/LvmVg"] != "vg1" {
		t.Errorf("sibling clobbered: got Props=%v", got.Props)
	}
}

// TestI1ControllerEmptyOverrideDeletes pins `controller set-property KEY ""`.
func TestI1ControllerEmptyOverrideDeletes(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	seed, _ := json.Marshal(apiv1.GenericPropsModify{
		OverrideProps: map[string]string{"KeepMe": "1", "RemoveMe": "2"},
	})
	resp := httpPost(t, base+"/v1/controller/properties", seed)
	_ = resp.Body.Close()

	empty, _ := json.Marshal(apiv1.GenericPropsModify{
		OverrideProps: map[string]string{"RemoveMe": ""},
	})
	resp2 := httpPost(t, base+"/v1/controller/properties", empty)
	_ = resp2.Body.Close()

	if resp2.StatusCode != http.StatusCreated {
		t.Fatalf("empty-override status: got %d", resp2.StatusCode)
	}

	getResp := httpGet(t, base+"/v1/controller/properties")
	defer func() { _ = getResp.Body.Close() }()

	var got map[string]string
	_ = json.NewDecoder(getResp.Body).Decode(&got)

	if _, present := got["RemoveMe"]; present {
		t.Errorf("empty override must DELETE RemoveMe, got %v", got)
	}

	if got["KeepMe"] != "1" {
		t.Errorf("sibling clobbered: got %v", got)
	}
}

// TestI1ResourceEmptyOverrideDeletes pins `r set-property n rd KEY ""`.
func TestI1ResourceEmptyOverrideDeletes(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	if err := st.Resources().Create(t.Context(), &apiv1.Resource{
		Name:     "pvc-1",
		NodeName: "n1",
		Props: map[string]string{
			"DrbdOptions/SkipDisk": "True",
			"keep-me":              "stay",
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.GenericPropsModify{
		OverrideProps: map[string]string{"DrbdOptions/SkipDisk": ""},
	})

	resp := httpPut(t, base+"/v1/resource-definitions/pvc-1/resources/n1", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.Resources().Get(t.Context(), "pvc-1", "n1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if _, present := got.Props["DrbdOptions/SkipDisk"]; present {
		t.Errorf("empty override must DELETE DrbdOptions/SkipDisk, got Props=%v", got.Props)
	}

	if got.Props["keep-me"] != "stay" {
		t.Errorf("sibling clobbered: got Props=%v", got.Props)
	}
}

// TestI1VolumeDefinitionEmptyOverrideDeletes pins `vd set-property rd vn KEY ""`.
func TestI1VolumeDefinitionEmptyOverrideDeletes(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.VolumeDefinitions().Create(ctx, "pvc-1", &apiv1.VolumeDefinition{
		VolumeNumber: 0,
		SizeKib:      1024 * 1024,
		Props: map[string]string{
			"sys/fs/blkio_throttle_write": "1048576",
			"keep-me":                     "stay",
		},
	}); err != nil {
		t.Fatalf("seed VD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.GenericPropsModify{
		OverrideProps: map[string]string{"sys/fs/blkio_throttle_write": ""},
	})

	resp := httpPut(t, base+"/v1/resource-definitions/pvc-1/volume-definitions/0", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.VolumeDefinitions().Get(ctx, "pvc-1", 0)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if _, present := got.Props["sys/fs/blkio_throttle_write"]; present {
		t.Errorf("empty override must DELETE the key, got Props=%v", got.Props)
	}

	if got.Props["keep-me"] != "stay" {
		t.Errorf("sibling clobbered: got Props=%v", got.Props)
	}
}

// TestI1VolumeGroupEmptyOverrideDeletes pins `vg set-property rg vn KEY ""`.
func TestI1VolumeGroupEmptyOverrideDeletes(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "rg-1",
		VolumeGroups: []apiv1.VolumeGroup{
			{
				VolumeNumber: 0,
				Props: map[string]string{
					"sys/fs/blkio_throttle_write": "1048576",
					"keep-me":                     "stay",
				},
			},
		},
	}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(map[string]any{
		"override_props": map[string]string{"sys/fs/blkio_throttle_write": ""},
	})

	resp := httpPut(t, base+"/v1/resource-groups/rg-1/volume-groups/0", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.ResourceGroups().Get(ctx, "rg-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if len(got.VolumeGroups) == 0 {
		t.Fatalf("VolumeGroups dropped: %+v", got)
	}

	vgProps := got.VolumeGroups[0].Props
	if _, present := vgProps["sys/fs/blkio_throttle_write"]; present {
		t.Errorf("empty override must DELETE the key, got Props=%v", vgProps)
	}

	if vgProps["keep-me"] != "stay" {
		t.Errorf("sibling clobbered: got Props=%v", vgProps)
	}
}

// TestI1StoragePoolDefinitionEmptyOverrideDeletes pins
// `sp-d set-property name KEY ""`.
func TestI1StoragePoolDefinitionEmptyOverrideDeletes(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.StoragePoolDefinitions().Create(ctx, &store.StoragePoolDefinition{
		Name: "spd-1",
		Props: map[string]string{
			"Aux/note": "drop-me",
			"keep-me":  "stay",
		},
	}); err != nil {
		t.Fatalf("seed SPD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(apiv1.GenericPropsModify{
		OverrideProps: map[string]string{"Aux/note": ""},
	})

	resp := httpPut(t, base+"/v1/storage-pool-definitions/spd-1", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.StoragePoolDefinitions().Get(ctx, "spd-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if _, present := got.Props["Aux/note"]; present {
		t.Errorf("empty override must DELETE Aux/note, got Props=%v", got.Props)
	}

	if got.Props["keep-me"] != "stay" {
		t.Errorf("sibling clobbered: got Props=%v", got.Props)
	}
}
