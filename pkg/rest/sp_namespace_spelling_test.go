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

// Every spelling of the StorDriver namespace that the deleter honours is one
// the backing-identity guard refuses, on both pool doors: a spelling the guard
// read another way would strip a live pool's identity, and the same guard then
// refuses to put it back.
func TestPoolDoorsRefuseEverySpellingOfTheDriverNamespace(t *testing.T) {
	t.Parallel()

	for _, ns := range []string{"StorDriver", "StorDriver/", "StorDriver//"} {
		t.Run("pool "+ns, func(t *testing.T) {
			t.Parallel()

			st := store.NewInMemory()
			ctx := t.Context()

			if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n1", Type: apiv1.NodeTypeSatellite}); err != nil {
				t.Fatalf("seed node: %v", err)
			}

			if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
				NodeName: "n1", StoragePoolName: "zfs-thin", ProviderKind: apiv1.StoragePoolKindZFSThin,
				Props: map[string]string{"StorDriver/ZPoolThin": "blockstor-zfs"},
			}); err != nil {
				t.Fatalf("seed pool: %v", err)
			}

			code := putNamespaceDelete(t, st, "/v1/nodes/n1/storage-pools/zfs-thin", ns)
			if code != http.StatusBadRequest {
				t.Errorf("delete_namespaces=[%q] on a pool = %d, want 400", ns, code)
			}

			sp, err := st.StoragePools().Get(ctx, "n1", "zfs-thin")
			if err != nil {
				t.Fatalf("read the pool: %v", err)
			}

			if sp.Props["StorDriver/ZPoolThin"] != "blockstor-zfs" {
				t.Errorf("delete_namespaces=[%q] stripped the pool's backing identity", ns)
			}
		})

		t.Run("definition "+ns, func(t *testing.T) {
			t.Parallel()

			st := store.NewInMemory()
			ctx := t.Context()

			if err := st.StoragePoolDefinitions().Create(ctx, &store.StoragePoolDefinition{
				Name: "zfs-thin", Props: map[string]string{"StorDriver/ZPoolThin": "blockstor-zfs"},
			}); err != nil {
				t.Fatalf("seed definition: %v", err)
			}

			code := putNamespaceDelete(t, st, "/v1/storage-pool-definitions/zfs-thin", ns)
			if code != http.StatusBadRequest {
				t.Errorf("delete_namespaces=[%q] on a pool definition = %d, want 400", ns, code)
			}

			def, err := st.StoragePoolDefinitions().Get(ctx, "zfs-thin")
			if err != nil {
				t.Fatalf("read the definition: %v", err)
			}

			if def.Props["StorDriver/ZPoolThin"] != "blockstor-zfs" {
				t.Errorf("delete_namespaces=[%q] stripped the definition's backing identity", ns)
			}
		})
	}
}

func putNamespaceDelete(t *testing.T, st store.Store, path, ns string) int {
	t.Helper()

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, err := json.Marshal(apiv1.GenericPropsModify{DeleteNamespace: []string{ns}})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	resp := httpPut(t, base+path, body)
	_ = resp.Body.Close()

	return resp.StatusCode
}
