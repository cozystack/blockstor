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

// TestVolumeDefinitionsCreateRoundTrip: create via REST envelope, list+get see it.
func TestVolumeDefinitionsCreateRoundTrip(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.VolumeDefinitionCreate{
		VolumeDefinition: apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 2048},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/volume-definitions", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201", resp.StatusCode)
	}

	listResp := httpGet(t, base+"/v1/resource-definitions/pvc-1/volume-definitions")
	defer func() { _ = listResp.Body.Close() }()

	var vds []apiv1.VolumeDefinition
	if jErr := json.NewDecoder(listResp.Body).Decode(&vds); jErr != nil {
		t.Fatalf("decode: %v", jErr)
	}

	if len(vds) != 1 || vds[0].SizeKib != 2048 {
		t.Errorf("got %+v, want one VD with SizeKib=2048", vds)
	}
}

// TestVolumeDefinitionsGetMissingRD: 404 when RD itself does not exist.
func TestVolumeDefinitionsGetMissingRD(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpGet(t, base+"/v1/resource-definitions/ghost/volume-definitions")
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", resp.StatusCode)
	}
}

// TestVolumeDefinitionsBadVolumeNumber: non-numeric `vn` → 400.
func TestVolumeDefinitionsBadVolumeNumber(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpGet(t, base+"/v1/resource-definitions/x/volume-definitions/notanum")
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400", resp.StatusCode)
	}
}

// TestVolumeDefinitionsWithoutStore: 503 across all VD paths.
func TestVolumeDefinitionsWithoutStore(t *testing.T) {
	base, stop := startServerCustom(t, &Server{Addr: pickFreeAddr(t), Store: nil})
	defer stop()

	resp := httpGet(t, base+"/v1/resource-definitions/x/volume-definitions")
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status: got %d, want 503", resp.StatusCode)
	}
}
