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
			for _, name := range []string{"pvc-a", "pvc-b"} {
				_ = backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: name})
				_ = backend.VolumeDefinitions().Create(ctx, name, &apiv1.VolumeDefinition{SizeKib: 1 << 20})
				_ = backend.Resources().Create(ctx, &apiv1.Resource{Name: name, NodeName: "node-1"})
			}
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

// A snapshot with no nodes behind it captures NOTHING: the snapshot
// controller treats an empty Spec.Nodes as degenerate and returns
// without capturing, while the command exits 0 and the listing renders
// the snapshot as healthy. An empty VolumeDefinitions slice does the
// same on the way back out — restoring it hydrates zero volumes, also
// with exit 0. This is a phantom backup, so the hydration the
// apiserver does before persisting has to happen here too.
func TestSnapshotCreateHydratesNodesAndVolumes(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, seedSnapshotSource)

	if got := app.Run(t.Context(), []string{"s", "c", "pvc-x", "snap-2"}); got != 0 {
		t.Fatalf("create exit = %d (stderr: %s)", got, errBuf.String())
	}

	snap, err := appStore(t, app).Snapshots().Get(t.Context(), "pvc-x", "snap-2")
	if err != nil {
		t.Fatalf("get snapshot: %v", err)
	}

	if len(snap.Nodes) == 0 {
		t.Error("the snapshot names no nodes, so nothing would be captured")
	}

	if len(snap.VolumeDefinitions) == 0 {
		t.Error("the snapshot records no volumes, so a restore would hydrate nothing")
	}
}

// A diskless replica has no backing volume to capture. Including its
// node fails that node, and a failed node fails the whole snapshot —
// which is how a clone of a resource with a tiebreaker aborted.
func TestSnapshotSkipsDisklessReplicas(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
		seedSnapshotSource(ctx, backend)
		_ = backend.Resources().Create(ctx, &apiv1.Resource{
			Name: "pvc-x", NodeName: "node-witness",
			Flags: []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker},
		})
	})

	if got := app.Run(t.Context(), []string{"s", "c", "pvc-x", "snap-2"}); got != 0 {
		t.Fatalf("create exit = %d (stderr: %s)", got, errBuf.String())
	}

	snap, err := appStore(t, app).Snapshots().Get(t.Context(), "pvc-x", "snap-2")
	if err != nil {
		t.Fatalf("get snapshot: %v", err)
	}

	for _, node := range snap.Nodes {
		if node == "node-witness" {
			t.Errorf("the snapshot targets a diskless witness: %v", snap.Nodes)
		}
	}

	// The clone path builds its internal snapshot the same way.
	if got := app.Run(t.Context(), []string{"rd", "clone", "pvc-x", "pvc-clone"}); got != 0 {
		t.Fatalf("clone exit = %d (stderr: %s)", got, errBuf.String())
	}

	clone, err := appStore(t, app).Snapshots().Get(t.Context(), "pvc-x", "clone-pvc-clone")
	if err != nil {
		t.Fatalf("get clone snapshot: %v", err)
	}

	for _, node := range clone.Nodes {
		if node == "node-witness" {
			t.Errorf("the clone snapshot targets a diskless witness: %v", clone.Nodes)
		}
	}
}

// A definition with no diskful replica cannot be snapshotted at all,
// so the attempt is refused rather than recorded as a success with
// nothing behind it.
func TestSnapshotRefusesWithNoDiskfulReplica(t *testing.T) {
	t.Parallel()

	app, _, _ := newApp(t, seedDefinition)

	if got := app.Run(t.Context(), []string{"s", "c", "pvc-x", "snap-1"}); got == 0 {
		t.Error("a snapshot of a definition with no diskful replica reported success")
	}
}

// `-l` is the short form of --layer-list. It used to consume its value
// and drop it, so a pinned layer stack — a LUKS layer, say — was
// discarded and the volume came up without it, with exit 0.
func TestShortLayerListFlagReachesTheSameField(t *testing.T) {
	t.Parallel()

	for _, flag := range []string{"-l", "--layer-list"} {
		app, _, errBuf := newApp(t, nil)

		if got := app.Run(t.Context(), []string{"rd", "c", "pvc-x", flag, "drbd,luks,storage"}); got != 0 {
			t.Fatalf("%s: exit = %d (stderr: %s)", flag, got, errBuf.String())
		}

		def, err := appStore(t, app).ResourceDefinitions().Get(t.Context(), "pvc-x")
		if err != nil {
			t.Fatalf("%s: get definition: %v", flag, err)
		}

		if len(def.LayerStack) != 3 {
			t.Errorf("%s: layer stack = %v, want three layers", flag, def.LayerStack)
		}
	}
}

// seedPoollessSnapshotSource is the disaster-recovery shape restore
// exists for: the snapshot outlived its source's diskful replicas, so
// there is no pool left to infer one from.
func seedPoollessSnapshotSource(ctx context.Context, backend store.Store) {
	_ = backend.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{Name: "grp"})
	_ = backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: "pvc-x", ResourceGroupName: "grp", LayerStack: []string{"DRBD", "STORAGE"},
	})
	_ = backend.VolumeDefinitions().Create(ctx, "pvc-x", &apiv1.VolumeDefinition{
		VolumeNumber: 0, SizeKib: 1 << 20,
	})

	// Diskless survivors: present, but carrying no StorPoolName.
	for _, node := range []string{"node-1", "node-2"} {
		_ = backend.Resources().Create(ctx, &apiv1.Resource{Name: "pvc-x", NodeName: node})
	}

	_ = backend.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name: "snap-1", ResourceName: "pvc-x",
		Nodes:             []string{"node-1", "node-2"},
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{{VolumeNumber: 0, SizeKib: 1 << 20}},
	})
}

// TestSnapshotRestoreRefusesWithoutAPool: a restore that cannot work out
// which storage pool the new replica belongs in has to say so, not
// create a diskful replica with an empty one.
//
// Nothing rejects that replica: the CRD does not require the field, the
// empty-value stamp is a documented no-op, and Create succeeds — so the
// verb reports success and the satellite then fails every reconcile with
// `unknown storage pool ""`. This repository has been bitten by that end
// state once already, from a different cause.
func TestSnapshotRestoreRefusesWithoutAPool(t *testing.T) {
	t.Parallel()

	app, _, _ := newApp(t, seedPoollessSnapshotSource)

	argv := []string{
		"s", "resource", "restore",
		"--from-resource", "pvc-x", "--from-snapshot", "snap-1", "--to-resource", "pvc-y",
	}

	if got := app.Run(t.Context(), argv); got == 0 {
		t.Fatal("restore with no pool to infer exited 0; want a refusal")
	}

	replicas, err := appStore(t, app).Resources().ListByDefinition(t.Context(), "pvc-y")
	if err == nil && len(replicas) > 0 {
		t.Errorf("a replica was created with no storage pool: %+v", replicas[0].Props)
	}

	// The refusal lands after the definition was created, so it also
	// has to unwind. Left behind, it turns the obvious response —
	// running the corrected command again — into "already exists",
	// and the operator has to delete by hand first.
	if _, err := appStore(t, app).ResourceDefinitions().Get(t.Context(), "pvc-y"); err == nil {
		t.Error("the failed restore left its resource definition behind")
	}
}

// TestSnapshotRestoreTakesTheNamedPool: --storage-pool is the operator's
// answer when the source cannot supply one.
func TestSnapshotRestoreTakesTheNamedPool(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, seedPoollessSnapshotSource)

	argv := []string{
		"s", "resource", "restore",
		"--from-resource", "pvc-x", "--from-snapshot", "snap-1", "--to-resource", "pvc-y",
		"--storage-pool", "rescue",
	}

	if got := app.Run(t.Context(), argv); got != 0 {
		t.Fatalf("restore with --storage-pool exit = %d (stderr: %s)", got, errBuf.String())
	}

	replicas, err := appStore(t, app).Resources().ListByDefinition(t.Context(), "pvc-y")
	if err != nil {
		t.Fatalf("list restored replicas: %v", err)
	}

	if len(replicas) == 0 {
		t.Fatal("no replica was restored")
	}

	for i := range replicas {
		if replicas[i].Props["StorPoolName"] != "rescue" {
			t.Errorf("replica on %s got pool %q, want rescue",
				replicas[i].NodeName, replicas[i].Props["StorPoolName"])
		}
	}
}
