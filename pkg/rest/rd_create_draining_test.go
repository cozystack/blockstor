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
	"net/http"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestRDCreateWaitsForTheDeletedDefinitionsReplicas is linstor-csi's retry of
// a CreateVolume whose first attempt it rolled back while a node was down:
// the rollback deleted the definition, the down node's replica of it stays
// until its satellite tears it down, and the retry creates the definition
// again under the same name. LINSTOR still holds the old definition then and
// answers the create with FAIL_EXISTS_RSC_DFN until the node is back, so
// linstor-csi keeps retrying. Created over the old replica, the new volume
// took it for one of its own and came up Inconsistent on every replica.
func TestRDCreateWaitsForTheDeletedDefinitionsReplicas(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	draining := apiv1.Resource{Name: "pvc-retry", NodeName: "node-down", Flags: []string{apiv1.ResourceFlagDelete}}
	if err := st.Resources().Create(ctx, &draining); err != nil {
		t.Fatalf("seed draining replica: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body := []byte(`{"resource_definition":{"name":"pvc-retry"}}`)

	resp := httpPost(t, base+"/v1/resource-definitions", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("create over a draining replica: status %d, want 409", resp.StatusCode)
	}

	if _, err := st.ResourceDefinitions().Get(ctx, "pvc-retry"); err == nil {
		t.Fatal("the refused create left a definition behind")
	}

	if err := st.Resources().Delete(ctx, "pvc-retry", "node-down"); err != nil {
		t.Fatalf("drain the replica: %v", err)
	}

	resp = httpPost(t, base+"/v1/resource-definitions", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create once the replica is gone: status %d, want 201", resp.StatusCode)
	}
}
