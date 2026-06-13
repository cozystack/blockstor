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

package placer_test

import (
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/placer"
	"github.com/cozystack/blockstor/pkg/store"
)

// Bug 038 (release gate): the controller-side Place callers
// (ResourceGroup apply, RG rebalance, node replacement) feed the
// placer the RAW RG SelectFilter — no snapshot-node constraint, no
// provider pin. On stand `big` that landed a FILE_THIN-sourced clone
// replica on a ZFS pool; the satellite then piped the source's
// FILE_THIN snapshot stream into `zfs recv`, which looped forever on
// `cannot receive: invalid stream (bad magic number)` and the clone
// never reached UpToDate.
//
// These tests pin the placer-internal restore-source constraint:
// Place() on an RD carrying the BlockstorRestoreFromSnapshot marker
// must (a) only consider the snapshot's nodes when the caller didn't
// pin any, and (b) only consider pools whose ProviderKind matches the
// source replica's — even when a different-backend pool is roomier
// and would win the capacity-weighted ranking.

// seedRestoreMarkedRD builds the Bug 038 repro shape: a deployed
// FILE_THIN source RD (replicas on n1/n2 in pool "stand"), its
// internal clone snapshot on those nodes, a restore-marked target RD,
// and a roomier ZFS_THIN decoy pool on every node.
func seedRestoreMarkedRD(t *testing.T, st store.Store) {
	t.Helper()

	ctx := t.Context()

	for _, n := range []string{"n1", "n2", "n3"} {
		if err := st.Nodes().Create(ctx, &apiv1.Node{Name: n, Type: apiv1.NodeTypeSatellite}); err != nil {
			t.Fatalf("seed node %s: %v", n, err)
		}

		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			NodeName:        n,
			StoragePoolName: "stand",
			ProviderKind:    apiv1.StoragePoolKindFileThin,
			FreeCapacity:    1000,
			TotalCapacity:   10000,
		}); err != nil {
			t.Fatalf("seed FILE_THIN pool on %s: %v", n, err)
		}

		// The decoy: same nodes, far more free space AND a far
		// better Free/Total ratio (0.9 vs 0.1) — the capacity-
		// weighted ranking picks it whenever the provider pin is
		// absent (see TestPlaceUnmarkedRDKeepsFreePlacement).
		if err := st.StoragePools().Create(ctx, &apiv1.StoragePool{
			NodeName:        n,
			StoragePoolName: "zfs-thin",
			ProviderKind:    apiv1.StoragePoolKindZFSThin,
			FreeCapacity:    900000,
			TotalCapacity:   1000000,
		}); err != nil {
			t.Fatalf("seed ZFS decoy pool on %s: %v", n, err)
		}
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "src"}); err != nil {
		t.Fatalf("seed source RD: %v", err)
	}

	for _, n := range []string{"n1", "n2"} {
		if err := st.Resources().Create(ctx, &apiv1.Resource{
			Name: "src", NodeName: n,
			Props: map[string]string{"StorPoolName": "stand"},
		}); err != nil {
			t.Fatalf("seed source replica on %s: %v", n, err)
		}
	}

	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name:         "clone-target",
		ResourceName: "src",
		Nodes:        []string{"n1", "n2"},
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{
			{VolumeNumber: 0, SizeKib: 64},
		},
	}); err != nil {
		t.Fatalf("seed snapshot: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:  "target",
		Props: map[string]string{"BlockstorRestoreFromSnapshot": "src:clone-target"},
	}); err != nil {
		t.Fatalf("seed restore-marked target RD: %v", err)
	}

	if err := st.VolumeDefinitions().Create(ctx, "target", &apiv1.VolumeDefinition{
		VolumeNumber: 0, SizeKib: 64,
	}); err != nil {
		t.Fatalf("seed target VD: %v", err)
	}
}

// TestPlaceConstrainsRestoreMarkedRDToSourceBackend drives Place with
// the exact filter shape the RG reconcilers use (bare PlaceCount, no
// pins) and asserts the replica lands on the source's FILE_THIN pool
// on a snapshot node — not on the roomier ZFS decoy.
func TestPlaceConstrainsRestoreMarkedRDToSourceBackend(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedRestoreMarkedRD(t, st)

	placed, want, err := placer.New(st).Place(t.Context(), "target", &apiv1.AutoSelectFilter{
		PlaceCount: 1,
	})
	if err != nil {
		t.Fatalf("Place: %v", err)
	}

	if placed != 1 || want != 1 {
		t.Fatalf("placed/want: got %d/%d, want 1/1", placed, want)
	}

	resList, err := st.Resources().ListByDefinition(t.Context(), "target")
	if err != nil {
		t.Fatalf("list target replicas: %v", err)
	}

	if len(resList) != 1 {
		t.Fatalf("target replicas: got %d, want 1", len(resList))
	}

	res := resList[0]
	if res.NodeName != "n1" && res.NodeName != "n2" {
		t.Errorf("replica node: got %q, want a snapshot node (n1/n2)", res.NodeName)
	}

	if pool := res.Props["StorPoolName"]; pool != "stand" {
		t.Errorf("replica pool: got %q, want the source's FILE_THIN pool %q "+
			"(a ZFS placement feeds the FILE_THIN snapshot stream into "+
			"`zfs recv` → bad magic loop)", pool, "stand")
	}
}

// TestPlaceRestoreConstraintDoesNotLeakIntoCallerFilter pins that the
// constraint operates on a copy: RG reconcilers reuse one filter
// value across sibling RDs, so a restore-marked RD must not pollute
// the next sibling's candidate set.
func TestPlaceRestoreConstraintDoesNotLeakIntoCallerFilter(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedRestoreMarkedRD(t, st)

	filter := apiv1.AutoSelectFilter{PlaceCount: 1}

	_, _, err := placer.New(st).Place(t.Context(), "target", &filter)
	if err != nil {
		t.Fatalf("Place: %v", err)
	}

	if len(filter.NodeNameList) != 0 || len(filter.ProviderList) != 0 {
		t.Errorf("caller filter mutated: NodeNameList=%v ProviderList=%v, want both empty",
			filter.NodeNameList, filter.ProviderList)
	}
}

// TestPlaceUnmarkedRDKeepsFreePlacement is the negative control: an
// RD without the restore marker keeps the unconstrained behaviour
// (the roomier ZFS decoy wins the ranking).
func TestPlaceUnmarkedRDKeepsFreePlacement(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedRestoreMarkedRD(t, st)

	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "plain"}); err != nil {
		t.Fatalf("seed plain RD: %v", err)
	}

	placed, _, err := placer.New(st).Place(t.Context(), "plain", &apiv1.AutoSelectFilter{
		PlaceCount: 1,
	})
	if err != nil {
		t.Fatalf("Place: %v", err)
	}

	if placed != 1 {
		t.Fatalf("placed: got %d, want 1", placed)
	}

	resList, err := st.Resources().ListByDefinition(t.Context(), "plain")
	if err != nil {
		t.Fatalf("list plain replicas: %v", err)
	}

	if len(resList) != 1 {
		t.Fatalf("plain replicas: got %d, want 1", len(resList))
	}

	if pool := resList[0].Props["StorPoolName"]; pool != "zfs-thin" {
		t.Errorf("unmarked RD pool: got %q, want the roomiest pool %q "+
			"(free placement must stay unconstrained)", pool, "zfs-thin")
	}
}
