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

// seedSnapshotOnNode is a node with one diskful pool and a snapshot whose copy
// is on it, the replica that took it already deleted.
func seedSnapshotOnNode(t *testing.T, withPool bool) store.Store {
	t.Helper()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "n1", Type: apiv1.NodeTypeSatellite}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	if withPool {
		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			NodeName:        "n1",
			StoragePoolName: "zfs1",
			ProviderKind:    apiv1.StoragePoolKindZFS,
			Props:           map[string]string{"StorDriver/ZPool": "tank"},
		}); err != nil {
			t.Fatalf("seed pool: %v", err)
		}
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-x"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{Name: "s1", ResourceName: "pvc-x", Nodes: []string{"n1"}}); err != nil {
		t.Fatalf("seed snapshot: %v", err)
	}

	return st
}

// A pool that may hold a snapshot's copy is not deleted: the copy can only be
// deleted through it, and the node's hold on the Snapshot would never be
// released. LINSTOR refuses the same over the pool's snapshot-volumes.
func TestStoragePoolDeleteRefusedWhileASnapshotIsOnTheNode(t *testing.T) {
	t.Parallel()

	st := seedSnapshotOnNode(t, true)

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t, base+"/v1/nodes/n1/storage-pools/zfs1")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusConflict {
		got, _ := readAllBody(resp)
		t.Fatalf("status %d, want 409 while pvc-x/s1 has a copy on n1. Body: %s", resp.StatusCode, got)
	}

	if _, err := st.StoragePools().Get(t.Context(), "n1", "zfs1"); err != nil {
		t.Errorf("pool removed despite the refusal: %v", err)
	}
}

// A node that still holds a snapshot's copy is not deleted: once it is gone
// nothing deletes the copy and `node lost` can no longer be asked for it.
func TestNodeDeleteRefusedWhileASnapshotIsOnTheNode(t *testing.T) {
	t.Parallel()

	st := seedSnapshotOnNode(t, false)

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t, base+"/v1/nodes/n1")
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusConflict {
		got, _ := readAllBody(resp)
		t.Fatalf("status %d, want 409 while pvc-x/s1 has a copy on n1. Body: %s", resp.StatusCode, got)
	}

	if _, err := st.Nodes().Get(t.Context(), "n1"); err != nil {
		t.Errorf("node removed despite the refusal: %v", err)
	}
}
