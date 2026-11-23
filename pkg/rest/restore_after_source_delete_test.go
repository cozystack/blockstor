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
	"errors"
	"net/http"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// sourceGoneStore holds the definition pvc-gone with no replica left and its
// snapshot snap on n1, n2 and n3, recording pool for each node when pool is
// not empty. That is what a volume delete leaves behind once LINSTOR's delete
// semantics keep the snapshot.
func sourceGoneStore(t *testing.T, pool string) store.Store {
	t.Helper()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-gone"}); err != nil {
		t.Fatalf("seed source definition: %v", err)
	}

	snap := apiv1.Snapshot{
		Name:              "snap",
		ResourceName:      "pvc-gone",
		Nodes:             []string{"n1", "n2", "n3"},
		Flags:             []string{apiv1.SnapshotFlagSuccessful},
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{{VolumeNumber: 0, SizeKib: 1024 * 1024}},
	}

	for _, node := range snap.Nodes {
		snap.Snapshots = append(snap.Snapshots, apiv1.SnapshotPerNode{
			SnapshotName: "snap", NodeName: node, CreateTimestamp: 1, StoragePool: pool,
		})
	}

	if err := st.Snapshots().Create(ctx, &snap); err != nil {
		t.Fatalf("seed snapshot: %v", err)
	}

	return st
}

// linstor-csi restores onto one node after the source PVC is gone: the
// replica lands in the pool the snapshot recorded there, the only place
// that still names it.
func TestSnapshotRestoreAfterSourceDeleteTakesThePoolFromTheSnapshot(t *testing.T) {
	t.Parallel()

	st := sourceGoneStore(t, "data")

	base, stop := startServerWithStore(t, st)
	defer stop()

	code, rc := restoreAnswer(t, base, "pvc-gone", "snap", map[string]any{
		"to_resource": "pvc-restored", "nodes": []string{"n2"},
	})
	if code != http.StatusCreated {
		t.Fatalf("restore after the source went = %d %q, want 201", code, rc.Message)
	}

	res, err := st.Resources().Get(t.Context(), "pvc-restored", "n2")
	if err != nil {
		t.Fatalf("read the restored replica: %v", err)
	}

	if pool := res.Props[storPoolPropKey]; pool != "data" {
		t.Errorf("restored replica StorPoolName = %q, want the snapshot's data", pool)
	}
}

// Nothing names the pool any more: the snapshot predates the record and the
// source has no replica left. The restore is refused before it writes
// anything, instead of creating a replica that loops on `unknown storage
// pool ""` on its satellite while its peers wait on it for good.
func TestSnapshotRestoreAfterSourceDeleteRefusesWithoutAPool(t *testing.T) {
	t.Parallel()

	st := sourceGoneStore(t, "")

	base, stop := startServerWithStore(t, st)
	defer stop()

	code, rc := restoreAnswer(t, base, "pvc-gone", "snap", map[string]any{
		"to_resource": "pvc-restored", "nodes": []string{"n2"},
	})
	if code != http.StatusBadRequest || rc.RetCode&apiCallRcFailNotFoundStorPool != apiCallRcFailNotFoundStorPool {
		t.Fatalf("restore with no pool to restore into = %d rc %#x %q, want 400 with FAIL_NOT_FOUND_STOR_POOL",
			code, rc.RetCode, rc.Message)
	}

	if _, err := st.ResourceDefinitions().Get(t.Context(), "pvc-restored"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("the refused restore left its definition behind: %v", err)
	}

	replicas, err := st.Resources().ListByDefinition(t.Context(), "pvc-restored")
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("list replicas: %v", err)
	}

	if len(replicas) != 0 {
		t.Errorf("the refused restore placed replicas: %+v", replicas)
	}
}

// linstor-csi prepares the target first: the definition, then its volumes
// restored from the snapshot, then the replica. The refusal over that
// prepared target places nothing on it either.
func TestSnapshotRestoreIntoAPreparedTargetRefusesWithoutAPool(t *testing.T) {
	t.Parallel()

	st := sourceGoneStore(t, "")
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-prepared"}); err != nil {
		t.Fatalf("seed the prepared target: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPost(t, base+"/v1/resource-definitions/pvc-gone/snapshot-restore-volume-definition/snap",
		[]byte(`{"to_resource":"pvc-prepared"}`))
	_ = resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		t.Fatalf("volume definition restore = %d, want success", resp.StatusCode)
	}

	code, rc := restoreAnswer(t, base, "pvc-gone", "snap", map[string]any{
		"to_resource": "pvc-prepared", "nodes": []string{"n1"},
	})
	if code != http.StatusBadRequest || rc.RetCode&apiCallRcFailNotFoundStorPool != apiCallRcFailNotFoundStorPool {
		t.Fatalf("restore into the prepared target = %d rc %#x %q, want 400 with FAIL_NOT_FOUND_STOR_POOL",
			code, rc.RetCode, rc.Message)
	}

	replicas, err := st.Resources().ListByDefinition(ctx, "pvc-prepared")
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("list replicas: %v", err)
	}

	if len(replicas) != 0 {
		t.Errorf("the refused restore placed replicas on the prepared target: %+v", replicas)
	}
}

// The snapshot's pool wins over the source's live replica: the copy is where
// it was taken, and a replica moved to another pool since did not take its
// snapshots along.
func TestSnapshotRestorePrefersTheSnapshotsPoolToTheSourceReplicas(t *testing.T) {
	t.Parallel()

	st := sourceGoneStore(t, "taken-in")

	if err := st.Resources().Create(t.Context(), &apiv1.Resource{
		Name: "pvc-gone", NodeName: "n1", Props: map[string]string{storPoolPropKey: "moved-to"},
	}); err != nil {
		t.Fatalf("seed the moved source replica: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	code, rc := restoreAnswer(t, base, "pvc-gone", "snap", map[string]any{
		"to_resource": "pvc-restored", "nodes": []string{"n1"},
	})
	if code != http.StatusCreated {
		t.Fatalf("restore = %d %q, want 201", code, rc.Message)
	}

	res, err := st.Resources().Get(t.Context(), "pvc-restored", "n1")
	if err != nil {
		t.Fatalf("read the restored replica: %v", err)
	}

	if pool := res.Props[storPoolPropKey]; pool != "taken-in" {
		t.Errorf("restored replica StorPoolName = %q, want the snapshot's taken-in", pool)
	}
}
