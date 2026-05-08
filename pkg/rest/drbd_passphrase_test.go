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

// TestDRBDPassphraseSet writes the per-RD shared secret onto the
// ResourceDefinition's props. The satellite consumes it via the same
// props pipeline that already powers ApplyResources.drbd_options.
func TestDRBDPassphraseSet(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(map[string]string{"passphrase": "supersecret"})

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/encryption-passphrase", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.ResourceDefinitions().Get(t.Context(), "pvc-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if got.Props["DrbdOptions/Net/shared-secret"] != "supersecret" {
		t.Errorf("expected shared-secret stored; got %v", got.Props)
	}
}

// TestDRBDPassphraseEmpty: empty passphrase → 400.
func TestDRBDPassphraseEmpty(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(map[string]string{})

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-1/encryption-passphrase", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400", resp.StatusCode)
	}
}

// TestDRBDPassphraseUnknownRD: 404 for missing RD.
func TestDRBDPassphraseUnknownRD(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	body, _ := json.Marshal(map[string]string{"passphrase": "supersecret"})

	resp := httpPost(t, base+"/v1/resource-definitions/ghost/encryption-passphrase", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", resp.StatusCode)
	}
}
