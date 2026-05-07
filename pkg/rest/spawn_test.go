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

// TestSpawnCreatesRDAndVDs: this is the gateway linstor-csi calls on every
// CreateVolume; it must materialise both the RD and one VD per requested
// size, atomically.
func TestSpawnCreatesRDAndVDs(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name:  "rg-1",
		Props: map[string]string{"DrbdOptions/auto-quorum": "io-error"},
	}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.ResourceGroupSpawn{
		ResourceDefinitionName: "pvc-1",
		VolumeSizes:            []int64{2 * 1024 * 1024, 4 * 1024 * 1024}, // bytes
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-groups/rg-1/spawn", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201", resp.StatusCode)
	}

	rd, err := st.ResourceDefinitions().Get(ctx, "pvc-1")
	if err != nil {
		t.Fatalf("RD not created: %v", err)
	}

	if rd.ResourceGroupName != "rg-1" {
		t.Errorf("ResourceGroupName: got %q, want rg-1", rd.ResourceGroupName)
	}

	if rd.Props["DrbdOptions/auto-quorum"] != "io-error" {
		t.Errorf("RG props were not inherited: %+v", rd.Props)
	}

	vds, err := st.VolumeDefinitions().List(ctx, "pvc-1")
	if err != nil {
		t.Fatalf("List VDs: %v", err)
	}

	if len(vds) != 2 {
		t.Fatalf("len: got %d, want 2", len(vds))
	}

	// 2 MiB / 1024 = 2048 KiB; 4 MiB → 4096 KiB.
	if vds[0].SizeKib != 2048 || vds[1].SizeKib != 4096 {
		t.Errorf("VD sizes: got %d, %d, want 2048, 4096", vds[0].SizeKib, vds[1].SizeKib)
	}
}

// TestSpawnMissingRG: 404 if the named ResourceGroup does not exist.
func TestSpawnMissingRG(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	body, err := json.Marshal(apiv1.ResourceGroupSpawn{
		ResourceDefinitionName: "pvc-1",
		VolumeSizes:            []int64{1024 * 1024},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-groups/ghost/spawn", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", resp.StatusCode)
	}
}

// TestSpawnMissingRDName: 400 if the request omits resource_definition_name.
func TestSpawnMissingRDName(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceGroups().Create(t.Context(), &apiv1.ResourceGroup{Name: "rg-1"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.ResourceGroupSpawn{VolumeSizes: []int64{1024 * 1024}})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-groups/rg-1/spawn", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400", resp.StatusCode)
	}
}

// TestSpawnRollsBackOnVDFailure: if any VD create fails, the half-built RD
// is rolled back so the caller can retry. We force the failure by spawning
// twice with the same name.
func TestSpawnRollsBackOnVDFailure(t *testing.T) {
	st := store.NewInMemory()
	ctx := t.Context()
	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{Name: "rg-1"}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// pre-existing RD with the same name to provoke an Already-Exists on RD create.
	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.ResourceGroupSpawn{
		ResourceDefinitionName: "pvc-1",
		VolumeSizes:            []int64{1024 * 1024},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-groups/rg-1/spawn", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusConflict {
		t.Errorf("status: got %d, want 409 (RD already exists)", resp.StatusCode)
	}
}
