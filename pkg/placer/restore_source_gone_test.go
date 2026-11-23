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

// recordCopies records, on the snapshot seedRestoreMarkedRD seeds, the pool
// each node's copy is in.
func recordCopies(t *testing.T, st store.Store, pools map[string]string) {
	t.Helper()

	snap, err := st.Snapshots().Get(t.Context(), "src", "clone-target")
	if err != nil {
		t.Fatalf("get snapshot: %v", err)
	}

	snap.Snapshots = nil
	for _, node := range snap.Nodes {
		snap.Snapshots = append(snap.Snapshots, apiv1.SnapshotPerNode{NodeName: node, StoragePool: pools[node]})
	}

	if err := st.Snapshots().Update(t.Context(), &snap); err != nil {
		t.Fatalf("record the snapshot's pools: %v", err)
	}
}

// placeTarget places one replica of the restore-marked target and returns
// the pool it landed in.
func placeTarget(t *testing.T, st store.Store, filter *apiv1.AutoSelectFilter) string {
	t.Helper()

	placed, _, err := placer.New(st).Place(t.Context(), "target", filter)
	if err != nil || placed != 1 {
		t.Fatalf("Place = %d, %v; want one replica", placed, err)
	}

	resList, err := st.Resources().ListByDefinition(t.Context(), "target")
	if err != nil || len(resList) != 1 {
		t.Fatalf("target replicas = %+v, %v; want one", resList, err)
	}

	return resList[0].Props["StorPoolName"]
}

// Once the source has no replica left, the backend pin comes from the pool
// the snapshot records its copies in. Before, it came only from the
// source's live replicas, so it vanished in exactly the restore-after-delete
// case and a clone could land on a backend whose stream it cannot receive.
func TestPlaceRestoreAfterSourceGoneKeepsTheBackendPin(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedRestoreMarkedRD(t, st)
	recordCopies(t, st, map[string]string{"n1": "stand", "n2": "stand"})

	for _, n := range []string{"n1", "n2"} {
		if err := st.Resources().Delete(t.Context(), "src", n); err != nil {
			t.Fatalf("delete source replica on %s: %v", n, err)
		}
	}

	// n3 holds no copy, so only the backend pin keeps the replica off the
	// roomier ZFS pool there.
	pool := placeTarget(t, st, &apiv1.AutoSelectFilter{PlaceCount: 1, NodeNameList: []string{"n3"}})
	if pool != "stand" {
		t.Errorf("replica pool = %q, want a FILE_THIN pool like the one the snapshot records", pool)
	}
}

// On a node whose copy the snapshot records, a replica lands in that pool
// and not in a roomier pool of the same backend, which holds no snapshot
// to clone from.
func TestPlaceRestorePinsTheRecordedPoolOnItsNode(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedRestoreMarkedRD(t, st)

	if err := st.StoragePools().Create(t.Context(), &apiv1.StoragePool{
		NodeName:        "n1",
		StoragePoolName: "stand-roomy",
		ProviderKind:    apiv1.StoragePoolKindFileThin,
		FreeCapacity:    900000,
		TotalCapacity:   1000000,
	}); err != nil {
		t.Fatalf("seed second FILE_THIN pool on n1: %v", err)
	}

	recordCopies(t, st, map[string]string{"n1": "stand", "n2": "stand"})

	pool := placeTarget(t, st, &apiv1.AutoSelectFilter{PlaceCount: 1, NodeNameList: []string{"n1"}})
	if pool != "stand" {
		t.Errorf("replica pool on n1 = %q, want stand, the pool that holds n1's copy", pool)
	}
}
