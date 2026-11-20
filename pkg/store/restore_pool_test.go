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

package store_test

import (
	"errors"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/naming"
	"github.com/cozystack/blockstor/pkg/store"
)

// restorePoolStore holds the source pvc-src with a replica on each of
// replicas, in pool "live-<node>".
func restorePoolStore(t *testing.T, replicas ...string) store.Store {
	t.Helper()

	st := store.NewInMemory()

	for _, node := range replicas {
		if err := st.Resources().Create(t.Context(), &apiv1.Resource{
			Name: "pvc-src", NodeName: node, Props: map[string]string{"StorPoolName": "live-" + node},
		}); err != nil {
			t.Fatalf("seed replica on %s: %v", node, err)
		}
	}

	return st
}

func restorePoolSnapshot(recorded map[string]string) *apiv1.Snapshot {
	snap := &apiv1.Snapshot{Name: "snap", ResourceName: "pvc-src", Nodes: []string{"n1", "n2"}}

	for _, node := range snap.Nodes {
		snap.Snapshots = append(snap.Snapshots, apiv1.SnapshotPerNode{NodeName: node, StoragePool: recorded[node]})
	}

	return snap
}

// The pool the snapshot recorded on a node is the one restored into, even
// while the source still has a replica there in another pool: the snapshot
// lives where it was taken, as LINSTOR reads it off the snapshot volume.
func TestRestorePoolsTakeThePoolTheSnapshotRecorded(t *testing.T) {
	t.Parallel()

	st := restorePoolStore(t, "n1")

	pools, err := store.RestorePools(t.Context(), st, restorePoolSnapshot(map[string]string{"n1": "taken-in"}), []string{"n1"})
	if err != nil || pools[naming.Key("n1")] != "taken-in" {
		t.Errorf("pools = %v, %v; want n1 in the recorded taken-in", pools, err)
	}
}

// A snapshot from before the record restores into the pool of the source's
// replica on that node.
func TestRestorePoolsFallBackToTheLiveReplicaOnTheNode(t *testing.T) {
	t.Parallel()

	st := restorePoolStore(t, "n1", "n2")

	pools, err := store.RestorePools(t.Context(), st, restorePoolSnapshot(nil), []string{"n2"})
	if err != nil || pools[naming.Key("n2")] != "live-n2" {
		t.Errorf("pools = %v, %v; want n2 in its own replica's live-n2", pools, err)
	}
}

// seedPool registers pool on node with the given backend.
func seedPool(t *testing.T, st store.Store, node, pool, kind string) {
	t.Helper()

	if err := st.StoragePools().Create(t.Context(), &apiv1.StoragePool{
		NodeName: node, StoragePoolName: pool, ProviderKind: kind,
	}); err != nil {
		t.Fatalf("seed pool %s on %s: %v", pool, node, err)
	}
}

// A node neither the record nor a live replica knows takes the pool recorded
// on another of the snapshot's nodes, when it has a pool of that name and
// backend.
func TestRestorePoolsFallBackToAnotherNodesRecord(t *testing.T) {
	t.Parallel()

	st := restorePoolStore(t)
	seedPool(t, st, "n1", "data", apiv1.StoragePoolKindZFSThin)
	seedPool(t, st, "n2", "data", apiv1.StoragePoolKindZFSThin)

	pools, err := store.RestorePools(t.Context(), st, restorePoolSnapshot(map[string]string{"n1": "data"}), []string{"n2"})
	if err != nil || pools[naming.Key("n2")] != "data" {
		t.Errorf("pools = %v, %v; want n2 in the pool recorded on n1", pools, err)
	}
}

// The fallback is refused for a node without a pool of that name, or with
// one of another backend: its satellite could not build the replica, the
// same failure as a replica with no pool at all.
func TestRestorePoolsRefuseAFallbackTheNodeLacks(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct{ source, target string }{
		"no such pool":                 {source: apiv1.StoragePoolKindZFSThin},
		"no such pool, source unknown": {},
		"another backend":              {source: apiv1.StoragePoolKindZFSThin, target: apiv1.StoragePoolKindLVMThin},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			st := restorePoolStore(t)
			if tc.source != "" {
				seedPool(t, st, "n1", "data", tc.source)
			}

			kind := tc.target

			if kind != "" {
				seedPool(t, st, "n2", "data", kind)
			}

			_, err := store.RestorePools(t.Context(), st,
				restorePoolSnapshot(map[string]string{"n1": "data"}), []string{"n2"})
			if !errors.Is(err, store.ErrNoRestorePool) {
				t.Errorf("err = %v, want ErrNoRestorePool", err)
			}
		})
	}
}

// With the source gone and no record, nothing names the pool, and the restore
// is refused rather than given a replica its satellite cannot build.
func TestRestorePoolsRefuseWhenNothingNamesThePool(t *testing.T) {
	t.Parallel()

	st := restorePoolStore(t)

	_, err := store.RestorePools(t.Context(), st, restorePoolSnapshot(nil), []string{"n1"})
	if !errors.Is(err, store.ErrNoRestorePool) {
		t.Errorf("err = %v, want ErrNoRestorePool", err)
	}
}

// A diskless replica of the source holds no copy, and its pool is no
// evidence of where the snapshot is.
func TestRestorePoolsIgnoreADisklessSourceReplica(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	if err := st.Resources().Create(t.Context(), &apiv1.Resource{
		Name: "pvc-src", NodeName: "n1", Flags: []string{apiv1.ResourceFlagDiskless},
		Props: map[string]string{"StorPoolName": "historic"},
	}); err != nil {
		t.Fatalf("seed diskless replica: %v", err)
	}

	_, err := store.RestorePools(t.Context(), st, restorePoolSnapshot(nil), []string{"n1"})
	if !errors.Is(err, store.ErrNoRestorePool) {
		t.Errorf("err = %v, want ErrNoRestorePool", err)
	}
}

// The node a request names in another spelling is the node the record was
// written for.
func TestRestorePoolsReadTheRecordInAnotherNodeSpelling(t *testing.T) {
	t.Parallel()

	st := restorePoolStore(t)
	snap := &apiv1.Snapshot{
		Name: "snap", ResourceName: "pvc-src", Nodes: []string{"Node-1"},
		Snapshots: []apiv1.SnapshotPerNode{{NodeName: "Node-1", StoragePool: "data"}},
	}

	pools, err := store.RestorePools(t.Context(), st, snap, []string{"node-1"})
	if err != nil || pools[naming.Key("node-1")] != "data" {
		t.Errorf("pools = %v, %v; want node-1 in data", pools, err)
	}
}
