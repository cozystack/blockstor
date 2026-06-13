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

// Bug 038 (release gate): `rd clone` of a FILE_THIN-backed source
// produced a target whose replica landed on a ZFS pool. The clone
// handler's placement branch deferred to the parent ResourceGroup's
// SelectFilter.PlaceCount — the stand-default DfltRscGrp has an EMPTY
// spec, so the clone POST placed NOTHING, and the controller-side RG
// reconciler later ran the placer with the raw RG filter (place_count
// defaulted to 1, no provider pin): the capacity-weighted ranking
// picked the roomier ZFS pool, and the target satellite piped the
// source's FILE_THIN snapshot stream into `zfs recv` — looping
// forever on `cannot receive: invalid stream (bad magic number)`
// (cli-matrix/rd-clone-vd-data-plane on stands big + dev).
//
// Upstream-faithful contract (verified against the live
// linstor-oracle, LINSTOR 1.33.2): a snapshot restore — blockstor's
// clone data plane — lands on EXACTLY the snapshot's nodes, in the
// snapshot's own storage pool, regardless of the parent RG's
// SelectFilter. Upstream `rd clone` likewise operates on the source's
// own pools (it refuses sources on pools it cannot clone:
// "Clone source contains unsupported storage pools").
//
// This test drives the exact stand shape through the REST clone
// endpoint and pins: the clone POST itself stamps one replica per
// snapshot node, in the SOURCE pool — same backend by construction,
// no dependency on the RG's place_count.
func TestBug038ClonePlacesReplicasOnSnapshotNodesInSourcePool(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	// Stand-default parent RG: empty SelectFilter (no place_count).
	if err := st.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
		Name: "DfltRscGrp",
	}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	// Two online workers; each carries the FILE_THIN pool `stand`
	// (snapshot-capable) and a much roomier ZFS_THIN decoy.
	for _, n := range []string{"w1", "w2"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{
			Name:             n,
			ConnectionStatus: "ONLINE",
		}); err != nil {
			t.Fatalf("seed node %s: %v", n, err)
		}

		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			StoragePoolName:  "stand",
			NodeName:         n,
			ProviderKind:     apiv1.StoragePoolKindFileThin,
			SupportsSnapshot: true,
			FreeCapacity:     1000,
			TotalCapacity:    10000,
		}); err != nil {
			t.Fatalf("seed FILE_THIN pool on %s: %v", n, err)
		}

		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			StoragePoolName:  "zfs-thin",
			NodeName:         n,
			ProviderKind:     apiv1.StoragePoolKindZFSThin,
			SupportsSnapshot: true,
			FreeCapacity:     100000,
			TotalCapacity:    1000000,
		}); err != nil {
			t.Fatalf("seed ZFS decoy pool on %s: %v", n, err)
		}
	}

	// Deployed VD-bearing source: 2 diskful replicas on `stand`.
	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:              "src038",
		ResourceGroupName: "DfltRscGrp",
	}); err != nil {
		t.Fatalf("seed source RD: %v", err)
	}

	if err := st.VolumeDefinitions().Create(ctx, "src038", &apiv1.VolumeDefinition{
		VolumeNumber: 0,
		SizeKib:      64 * 1024,
	}); err != nil {
		t.Fatalf("seed source VD: %v", err)
	}

	for _, n := range []string{"w1", "w2"} {
		if err := st.Resources().Create(ctx, &apiv1.Resource{
			Name:     "src038",
			NodeName: n,
			Props:    map[string]string{"StorPoolName": "stand"},
		}); err != nil {
			t.Fatalf("seed source replica on %s: %v", n, err)
		}
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := postClone(t, base, "src038", map[string]any{
		"name":          "dst038",
		"use_zfs_clone": true,
	})
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("clone status: got %d, want 201", resp.StatusCode)
	}

	got, err := st.Resources().ListByDefinition(ctx, "dst038")
	if err != nil {
		t.Fatalf("list clone replicas: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("clone replicas stamped by the clone POST: got %d, want 2 "+
			"(one per snapshot node; pre-fix the empty-spec RG placed zero "+
			"and the RG reconciler later mis-placed onto ZFS)", len(got))
	}

	wantNodes := map[string]bool{"w1": false, "w2": false}

	for i := range got {
		res := &got[i]

		if _, ok := wantNodes[res.NodeName]; !ok {
			t.Errorf("clone replica off the snapshot node set: %q", res.NodeName)

			continue
		}

		wantNodes[res.NodeName] = true

		if pool := res.Props["StorPoolName"]; pool != "stand" {
			t.Errorf("clone replica on %s landed on pool %q, want the source "+
				"pool %q (a ZFS placement feeds the FILE_THIN stream into "+
				"`zfs recv` → bad-magic loop)", res.NodeName, pool, "stand")
		}
	}

	for n, placed := range wantNodes {
		if !placed {
			t.Errorf("no clone replica on snapshot node %q", n)
		}
	}
}
