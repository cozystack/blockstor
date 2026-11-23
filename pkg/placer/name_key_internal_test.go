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

package placer

import (
	"context"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// The min-rsc-count weight reads a pool's node in the spelling the pool was
// created with, NODE-1, and counts the replicas the store shows on Node-1:
// they are one node, so the loaded node scores lower than an empty one.
func TestMinRscCountCountsReplicasInAnySpellingOfThePoolsNode(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewInMemory()

	for _, rd := range []string{"pvc-a", "pvc-b"} {
		if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: rd}); err != nil {
			t.Fatalf("seed RD: %v", err)
		}

		if err := st.Resources().Create(ctx, &apiv1.Resource{Name: rd, NodeName: "Node-1"}); err != nil {
			t.Fatalf("seed replica: %v", err)
		}
	}

	perNode, err := New(st).resourcesPerNode(ctx)
	if err != nil {
		t.Fatalf("resourcesPerNode: %v", err)
	}

	w := weights{minRscCount: 1}
	loaded := composite(&apiv1.StoragePool{StoragePoolName: "pool", NodeName: "NODE-1"}, w, perNode, 0)
	empty := composite(&apiv1.StoragePool{StoragePoolName: "pool", NodeName: "node-2"}, w, perNode, 0)

	if loaded >= empty {
		t.Errorf("NODE-1 with two replicas scores %v, an empty node %v; want the loaded node lower", loaded, empty)
	}
}

// An existing replica shows its node as Node-1 while the pool listing has
// it as node-1: the replica's pool is still found, so its shared LUN keeps a
// new replica off the pool that shares it.
func TestAnExistingReplicasPoolIsFoundInAnotherNodeSpelling(t *testing.T) {
	t.Parallel()

	existing := []apiv1.Resource{{
		Name: "pvc-x", NodeName: "Node-1", Props: map[string]string{storPoolNameProp: "san"},
	}}
	pools := []apiv1.StoragePool{
		{StoragePoolName: "san", NodeName: "node-1", ProviderKind: "LVM", SharedSpaceID: "lun-7"},
		{StoragePoolName: "san", NodeName: "node-2", ProviderKind: "LVM", SharedSpaceID: "lun-7"},
	}

	s := newState(&apiv1.AutoSelectFilter{}, existing, map[string]map[string]string{}, pools, false)

	if _, seen := s.sharedSeen["lun-7"]; !seen || s.firstKind != "LVM" {
		t.Errorf("shared LUNs seen %v, first kind %q; want lun-7 and LVM from the replica on Node-1",
			s.sharedSeen, s.firstKind)
	}
}
