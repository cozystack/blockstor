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

package cli_test

import (
	"context"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// seedSourceGone is a definition whose replicas are all gone, with a
// snapshot of it on both nodes that records pool per node when pool is set.
func seedSourceGone(pool string) func(context.Context, store.Store) {
	return func(ctx context.Context, backend store.Store) {
		_ = backend.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{Name: "grp"})
		_ = backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
			Name: "pvc-x", ResourceGroupName: "grp", LayerStack: []string{"DRBD", "STORAGE"},
		})

		snap := apiv1.Snapshot{
			Name: "snap-1", ResourceName: "pvc-x",
			Nodes:             []string{"node-1", "node-2"},
			VolumeDefinitions: []apiv1.SnapshotVolumeDef{{VolumeNumber: 0, SizeKib: 1 << 20}},
		}

		for _, node := range snap.Nodes {
			snap.Snapshots = append(snap.Snapshots, apiv1.SnapshotPerNode{NodeName: node, StoragePool: pool})
		}

		_ = backend.Snapshots().Create(ctx, &snap)
	}
}

// With the source's replicas gone, the restore still lands every replica in
// the pool the snapshot recorded on its node.
func TestSnapshotRestoreAfterTheSourceWentTakesTheSnapshotsPool(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, seedSourceGone("data"))

	argv := []string{
		"s", "resource", "restore",
		"--from-resource", "pvc-x", "--from-snapshot", "snap-1", "--to-resource", "pvc-y",
	}

	if got := app.Run(t.Context(), argv); got != 0 {
		t.Fatalf("restore exit = %d (stderr: %s)", got, errBuf.String())
	}

	replicas, err := appStore(t, app).Resources().ListByDefinition(t.Context(), "pvc-y")
	if err != nil || len(replicas) != 2 {
		t.Fatalf("restored replicas = %+v, %v; want one per snapshot node", replicas, err)
	}

	for i := range replicas {
		if pool := replicas[i].Props["StorPoolName"]; pool != "data" {
			t.Errorf("replica on %s pinned to %q, want the snapshot's data", replicas[i].NodeName, pool)
		}
	}
}

// With nothing left to name the pool, the restore is refused and says how to
// name one.
func TestSnapshotRestoreAfterTheSourceWentRefusesWithoutAPool(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, seedSourceGone(""))

	argv := []string{
		"s", "resource", "restore",
		"--from-resource", "pvc-x", "--from-snapshot", "snap-1", "--to-resource", "pvc-y",
	}

	if got := app.Run(t.Context(), argv); got == 0 {
		t.Fatal("restore with no pool to restore into succeeded")
	}

	if !strings.Contains(errBuf.String(), "--storage-pool") {
		t.Errorf("stderr %q does not say how to name a pool", errBuf.String())
	}

	if replicas, _ := appStore(t, app).Resources().ListByDefinition(t.Context(), "pvc-y"); len(replicas) != 0 {
		t.Errorf("the refused restore placed replicas: %+v", replicas)
	}
}
