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
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// cacheKeepsDeletedRDs lists, from its cache, definitions a delete has
// already removed from the API server; ListUncached answers from the
// API server.
type cacheKeepsDeletedRDs struct {
	store.ResourceDefinitionStore

	gone []apiv1.ResourceDefinition
}

func (c cacheKeepsDeletedRDs) List(ctx context.Context) ([]apiv1.ResourceDefinition, error) {
	live, err := c.ResourceDefinitionStore.List(ctx)
	if err != nil {
		return nil, err //nolint:wrapcheck // pass-through test double
	}

	return append(live, c.gone...), nil
}

type staleRDCacheStore struct {
	store.Store

	rds cacheKeepsDeletedRDs
}

func (s staleRDCacheStore) ResourceDefinitions() store.ResourceDefinitionStore { return s.rds }

// linstor-csi deletes a storage class's group right after the last volume
// of the class, and gives up when that is refused. A cache still holding
// the definition the volume delete just removed must not refuse it: the
// group would stay behind for good. LINSTOR answers from state that no
// longer has the definition.
func TestRGDeleteCountsChildRDsPastTheCache(t *testing.T) {
	t.Parallel()

	mem := store.NewInMemory()
	ctx := t.Context()

	if err := mem.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{Name: "sc-group"}); err != nil {
		t.Fatalf("seed rg: %v", err)
	}

	st := staleRDCacheStore{Store: mem, rds: cacheKeepsDeletedRDs{
		ResourceDefinitionStore: mem.ResourceDefinitions(),
		gone:                    []apiv1.ResourceDefinition{{Name: "pvc-last", ResourceGroupName: "sc-group"}},
	}}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpDelete(t, base+"/v1/resource-groups/sc-group")
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("delete of a group whose last definition is gone: %d, want 200", resp.StatusCode)
	}

	if _, err := mem.ResourceGroups().Get(ctx, "sc-group"); err == nil {
		t.Error("the group is still there")
	}
}
