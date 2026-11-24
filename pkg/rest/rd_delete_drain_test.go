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
	"context"
	"net/http"
	"slices"
	"testing"
	"time"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// finalizerHeldResources deletes a replica the way the Kubernetes store does
// while its satellite's finalizer holds it: the replica stays, flagged DELETE,
// until the satellite has torn it down (here, until the test removes it).
type finalizerHeldResources struct {
	store.ResourceStore
}

func (f finalizerHeldResources) Delete(ctx context.Context, rdName, node string) error {
	return f.PatchResourceSpec(ctx, rdName, node, func(r *apiv1.Resource) error { //nolint:wrapcheck // test helper
		if !slices.Contains(r.Flags, apiv1.ResourceFlagDelete) {
			r.Flags = append(r.Flags, apiv1.ResourceFlagDelete)
		}

		return nil
	})
}

type finalizerHeldStore struct {
	store.Store

	resources finalizerHeldResources
}

func (f finalizerHeldStore) Resources() store.ResourceStore { return f.resources }

func newFinalizerHeldStore() (finalizerHeldStore, *store.InMemory) {
	inner := store.NewInMemory()

	return finalizerHeldStore{Store: inner, resources: finalizerHeldResources{inner.Resources()}}, inner
}

// LINSTOR answers `rd d` once the online satellites have torn their replicas
// down, so a create of the same name right after it goes through. The delete
// answered as soon as the objects were marked, and the create that followed
// was refused over replicas still tearing down on nodes that were up.
//
// The teardown outlasts the cache wait's own budget, which already held the
// reply for a teardown shorter than it.
func TestRDDeleteWaitsForReplicasOnOnlineNodes(t *testing.T) {
	t.Parallel()

	teardown := cacheConvergeBudget + time.Second

	st, inner := newFinalizerHeldStore()
	ctx := t.Context()

	seedDrainFixture(t, inner, "pvc-online", apiv1.NodeTypeOnline)

	base, stop := startServerWithStore(t, st)
	defer stop()

	go func() {
		time.Sleep(teardown)

		_ = inner.Resources().Delete(context.WithoutCancel(ctx), "pvc-online", "node-1")
	}()

	start := time.Now()

	resp := httpDelete(t, base+"/v1/resource-definitions/pvc-online")
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("rd d: status %d, want 200", resp.StatusCode)
	}

	if elapsed := time.Since(start); elapsed < teardown {
		t.Fatalf("rd d answered after %v, before the online replica's %v teardown finished", elapsed, teardown)
	}

	resp = httpPost(t, base+"/v1/resource-definitions", []byte(`{"resource_definition":{"name":"pvc-online"}}`))
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("rd c right after rd d: status %d, want 201", resp.StatusCode)
	}
}

// A replica on an offline node drains only when the node is back. The delete
// does not wait for it, and the create of the same name is refused until then,
// as LINSTOR refuses it.
func TestRDDeleteDoesNotWaitForAnOfflineNode(t *testing.T) {
	t.Parallel()

	st, inner := newFinalizerHeldStore()

	seedDrainFixture(t, inner, "pvc-offline", apiv1.NodeTypeOffline)

	base, stop := startServerWithStore(t, st)
	defer stop()

	start := time.Now()

	resp := httpDelete(t, base+"/v1/resource-definitions/pvc-offline")
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("rd d: status %d, want 200", resp.StatusCode)
	}

	// The cache wait still runs its own 5s budget over the replica that
	// stays; the drain wait adds nothing on top of it for an offline node.
	if elapsed := time.Since(start); elapsed > cacheConvergeBudget+2*time.Second {
		t.Fatalf("rd d waited %v for a replica on an offline node", elapsed)
	}

	resp = httpPost(t, base+"/v1/resource-definitions", []byte(`{"resource_definition":{"name":"pvc-offline"}}`))
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("rd c over the offline node's replica: status %d, want 409", resp.StatusCode)
	}
}

func seedDrainFixture(t *testing.T, st *store.InMemory, rdName, connection string) {
	t.Helper()

	ctx := t.Context()

	if err := st.Nodes().Create(ctx, &apiv1.Node{Name: "node-1", ConnectionStatus: connection}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rdName}); err != nil {
		t.Fatalf("seed definition: %v", err)
	}

	if err := st.Resources().Create(ctx, &apiv1.Resource{Name: rdName, NodeName: "node-1"}); err != nil {
		t.Fatalf("seed replica: %v", err)
	}
}
