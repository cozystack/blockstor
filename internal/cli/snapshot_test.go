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

// seedSnapshotSource is a two-replica definition with one 1 GiB
// volume and a snapshot of it on both nodes.
func seedSnapshotSource(ctx context.Context, backend store.Store) {
	_ = backend.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{Name: "grp"})
	_ = backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: "pvc-x", ResourceGroupName: "grp", LayerStack: []string{"DRBD", "STORAGE"},
	})
	_ = backend.VolumeDefinitions().Create(ctx, "pvc-x", &apiv1.VolumeDefinition{
		VolumeNumber: 0, SizeKib: 1 << 20,
	})

	for _, node := range []string{"node-1", "node-2"} {
		_ = backend.Resources().Create(ctx, &apiv1.Resource{
			Name: "pvc-x", NodeName: node,
			Props: map[string]string{"StorPoolName": "data"},
		})
	}

	_ = backend.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name: "snap-1", ResourceName: "pvc-x",
		Nodes:             []string{"node-1", "node-2"},
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{{VolumeNumber: 0, SizeKib: 1 << 20}},
	})
}

// Restoring produces a new definition whose volumes match the
// snapshot, and replicas on the nodes that HOLD the snapshot — pinned
// to the pool the source uses there. A restored replica in a different
// backend makes the satellite pipe the snapshot stream into the wrong
// receiver, which never converges.
func TestSnapshotResourceRestore(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, seedSnapshotSource)

	argv := []string{
		"s", "resource", "restore",
		"--from-resource", "pvc-x", "--from-snapshot", "snap-1", "--to-resource", "pvc-y",
	}

	if got := app.Run(t.Context(), argv); got != 0 {
		t.Fatalf("restore exit = %d (stderr: %s)", got, errBuf.String())
	}

	backend := appStore(t, app)

	def, err := backend.ResourceDefinitions().Get(t.Context(), "pvc-y")
	if err != nil {
		t.Fatalf("get restored definition: %v", err)
	}

	if def.ResourceGroupName != "grp" {
		t.Errorf("restored definition group = %q, want the source's grp", def.ResourceGroupName)
	}

	if def.Props["BlockstorRestoreFromSnapshot"] != "pvc-x:snap-1" {
		t.Errorf("restore marker = %q, want pvc-x:snap-1", def.Props["BlockstorRestoreFromSnapshot"])
	}

	vds, err := backend.VolumeDefinitions().List(t.Context(), "pvc-y")
	if err != nil {
		t.Fatalf("list volume definitions: %v", err)
	}

	if len(vds) != 1 || vds[0].SizeKib != 1<<20 {
		t.Fatalf("restored volumes = %+v, want one of 1 GiB", vds)
	}

	replicas, err := backend.Resources().ListByDefinition(t.Context(), "pvc-y")
	if err != nil {
		t.Fatalf("list replicas: %v", err)
	}

	if len(replicas) != 2 {
		t.Fatalf("restored %d replicas, want one per snapshot-holding node", len(replicas))
	}

	for i := range replicas {
		if replicas[i].Props["StorPoolName"] != "data" {
			t.Errorf("replica on %s pinned to %q, want the source's data pool",
				replicas[i].NodeName, replicas[i].Props["StorPoolName"])
		}
	}
}

// The volume-definition variant hydrates the layout onto an EXISTING
// definition and creates no replicas.
func TestSnapshotVolumeDefinitionRestore(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
		seedSnapshotSource(ctx, backend)
		_ = backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-y"})
	})

	argv := []string{
		"s", "vd", "restore",
		"--from-resource", "pvc-x", "--from-snapshot", "snap-1", "--to-resource", "pvc-y",
	}

	if got := app.Run(t.Context(), argv); got != 0 {
		t.Fatalf("restore exit = %d (stderr: %s)", got, errBuf.String())
	}

	backend := appStore(t, app)

	vds, err := backend.VolumeDefinitions().List(t.Context(), "pvc-y")
	if err != nil {
		t.Fatalf("list volume definitions: %v", err)
	}

	if len(vds) != 1 {
		t.Fatalf("hydrated %d volume definitions, want 1", len(vds))
	}

	replicas, err := backend.Resources().ListByDefinition(t.Context(), "pvc-y")
	if err != nil {
		t.Fatalf("list replicas: %v", err)
	}

	if len(replicas) != 0 {
		t.Errorf("the volume-definition variant placed replicas: %+v", replicas)
	}
}

// A volume-number collision is refused BEFORE anything is written:
// hydrating volume by volume would land the non-colliding ones first
// and leave the target half-restored.
func TestSnapshotVolumeDefinitionRestoreRefusesCollisionBeforeWriting(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
		_ = backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-x"})
		_ = backend.Snapshots().Create(ctx, &apiv1.Snapshot{
			Name: "snap-1", ResourceName: "pvc-x",
			VolumeDefinitions: []apiv1.SnapshotVolumeDef{
				{VolumeNumber: 1, SizeKib: 1024},
				{VolumeNumber: 0, SizeKib: 1024},
			},
		})
		_ = backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-y"})
		_ = backend.VolumeDefinitions().Create(ctx, "pvc-y", &apiv1.VolumeDefinition{
			VolumeNumber: 0, SizeKib: 4096,
		})
	})

	argv := []string{
		"s", "vd", "restore",
		"--from-resource", "pvc-x", "--from-snapshot", "snap-1", "--to-resource", "pvc-y",
	}

	if got := app.Run(t.Context(), argv); got == 0 {
		t.Fatalf("a colliding restore succeeded (stderr: %s)", errBuf.String())
	}

	vds, err := appStore(t, app).VolumeDefinitions().List(t.Context(), "pvc-y")
	if err != nil {
		t.Fatalf("list volume definitions: %v", err)
	}

	// The snapshot's volume 1 does not collide and would have been
	// written first by a per-volume loop.
	if len(vds) != 1 {
		t.Errorf("the refused restore still mutated the target: %+v", vds)
	}
}

// In-place rollback is deliberately absent: it destroys every snapshot
// newer than the target. The refusal names the recoverable
// alternative rather than just failing.
func TestSnapshotRollbackIsRefused(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, seedSnapshotSource)

	if got := app.Run(t.Context(), []string{"s", "rollback", "pvc-x", "snap-1"}); got == 0 {
		t.Fatal("rollback reported success")
	}

	if !strings.Contains(errBuf.String(), "restore") {
		t.Errorf("the refusal does not point at the alternative:\n%s", errBuf.String())
	}
}

// create-multiple stamps ONE group id across the batch, which is what
// makes the controller take them under a single suspend-io barrier.
// Separate barriers would give snapshots that are individually
// consistent but not consistent with each other.
func TestSnapshotCreateMultipleSharesOneGroup(t *testing.T) {
	t.Parallel()

	for _, argv := range [][]string{
		{"s", "create-multiple", "pvc-a:snap", "pvc-b:snap"},
		{"s", "create-multiple", "snap", "-r", "pvc-a", "pvc-b"},
	} {
		app, _, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
			_ = backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-a"})
			_ = backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-b"})
		})

		if got := app.Run(t.Context(), argv); got != 0 {
			t.Fatalf("%v: exit = %d (stderr: %s)", argv, got, errBuf.String())
		}

		snaps, err := appStore(t, app).Snapshots().List(t.Context())
		if err != nil {
			t.Fatalf("list snapshots: %v", err)
		}

		if len(snaps) != 2 {
			t.Fatalf("%v: created %d snapshots, want 2", argv, len(snaps))
		}

		if snaps[0].GroupID == "" || snaps[0].GroupID != snaps[1].GroupID {
			t.Errorf("%v: group ids %q and %q are not one batch",
				argv, snaps[0].GroupID, snaps[1].GroupID)
		}

		if snaps[0].GroupSize != 2 {
			t.Errorf("%v: group size = %d, want 2", argv, snaps[0].GroupSize)
		}
	}
}

// A malformed pair is a client-side rejection, not a snapshot named
// after half of it.
func TestSnapshotCreateMultipleRejectsBadPair(t *testing.T) {
	t.Parallel()

	app, _, _ := newApp(t, nil)

	if got := app.Run(t.Context(), []string{"s", "create-multiple", "pvc-a:"}); got != 2 {
		t.Errorf("exit = %d, want 2", got)
	}
}
