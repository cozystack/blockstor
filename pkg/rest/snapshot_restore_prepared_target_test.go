// SPDX-License-Identifier: Apache-2.0

package rest

import (
	"net/http"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

func assertTargetUntouched(t *testing.T, st store.Store, target string) {
	t.Helper()

	rd, err := st.ResourceDefinitions().Get(t.Context(), target)
	if err != nil {
		t.Fatalf("read the target: %v", err)
	}

	if marker := rd.Props[restoreFromSnapshotKey]; marker != "" {
		t.Errorf("the refused target was marked %q", marker)
	}

	replicas, err := st.Resources().ListByDefinition(t.Context(), target)
	if err != nil {
		t.Fatalf("list the target's replicas: %v", err)
	}

	for i := range replicas {
		if !replicaAcceptedForDeletion(&replicas[i]) {
			t.Errorf("a replica was placed on the refused target on %s", replicas[i].NodeName)
		}
	}
}

// A restore puts the source's bytes under the target's stack. A layer the
// source did not have writes its own metadata across them, and one it had is
// missing on read; through linstor-csi this is a VolumeSnapshot restored into
// a PVC of another StorageClass.
func TestSnapshotRestoreRefusesAPreparedTargetOfAnotherLayerStack(t *testing.T) {
	t.Parallel()

	luks := []string{"DRBD", "LUKS", "STORAGE"}

	for _, tc := range []struct {
		name          string
		source, class []string
	}{
		{name: "plaintext-source-into-luks", class: luks},
		{name: "luks-source-into-plaintext", source: luks},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			st := store.NewInMemory()
			seedCloneSourceWithStack(t, st, "src-layers", tc.source)
			seedRestoreSnapshot(t, st, "src-layers", "snap-layers", []string{"node-a"})

			// A LUKS definition is created only on a cluster with a passphrase.
			base, stop := startServerWithPassphrase(t, st)
			defer stop()

			csiPrepareRestoreTargetWithLayers(t, base, "src-layers", "snap-layers", "pvc-layers", tc.class)

			if code := restoreOnce(t, base, "src-layers", "snap-layers", map[string]any{
				"to_resource": "pvc-layers", "nodes": []string{"node-a"},
			}); code != http.StatusConflict {
				t.Errorf("restore into a target of another layer stack = %d, want 409", code)
			}

			assertTargetUntouched(t, st, "pvc-layers")
		})
	}
}

// Placing over replicas still being deleted races the tear-down, and the
// refusal comes before the marker goes on a definition that is the caller's.
func TestSnapshotRestoreRefusesAPreparedTargetBeingTornDown(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedDeployedCloneSource(t, st, "src-down")
	seedRestoreSnapshot(t, st, "src-down", "snap-down", []string{"node-a"})

	base, stop := startServerWithStore(t, st)
	defer stop()

	csiPrepareRestoreTarget(t, base, "src-down", "snap-down", "pvc-down")
	seedTerminatingReplica(t, st, "pvc-down", "node-a")

	if code := restoreOnce(t, base, "src-down", "snap-down", map[string]any{
		"to_resource": "pvc-down", "nodes": []string{"node-a"},
	}); code != http.StatusConflict {
		t.Errorf("restore into a target being torn down = %d, want 409", code)
	}

	assertTargetUntouched(t, st, "pvc-down")
}

// The post-write group guard refused a prepared target parented to a group
// that is gone only after the marker was on it.
func TestSnapshotRestoreLeavesAPreparedTargetUnmarkedWhenItsGroupIsGone(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-nogrp")
	seedRestoreSnapshot(t, st, "src-nogrp", "snap-nogrp", []string{"node-a"})

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: "pvc-nogrp", ResourceGroupName: "grp-gone",
	}); err != nil {
		t.Fatalf("seed the target: %v", err)
	}

	if err := st.VolumeDefinitions().Create(ctx, "pvc-nogrp",
		&apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 64 * 1024}); err != nil {
		t.Fatalf("seed the target's volume: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	if code := restoreOnce(t, base, "src-nogrp", "snap-nogrp", map[string]any{
		"to_resource": "pvc-nogrp", "nodes": []string{"node-a"},
	}); code != http.StatusConflict {
		t.Errorf("restore into a target whose group is gone = %d, want 409", code)
	}

	assertTargetUntouched(t, st, "pvc-nogrp")
}

// The same stack is no difference, whatever the order or spelling.
func TestSnapshotRestoreTakesAPreparedTargetOfTheSourcesLayerStack(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedCloneSourceWithStack(t, st, "src-same", []string{"DRBD", "LUKS", "STORAGE"})
	seedRestoreSnapshot(t, st, "src-same", "snap-same", []string{"node-a"})

	base, stop := startServerWithPassphrase(t, st)
	defer stop()

	csiPrepareRestoreTargetWithLayers(t, base, "src-same", "snap-same", "pvc-same", []string{"drbd", "luks", "storage"})

	if code := restoreOnce(t, base, "src-same", "snap-same", map[string]any{
		"to_resource": "pvc-same", "nodes": []string{"node-a"},
	}); code != http.StatusCreated {
		t.Errorf("restore into a target of the source's own stack = %d, want 201", code)
	}
}

// A source stored without a stack has the default, not no layers, so a target
// that names the default explicitly is the same stack.
func TestSnapshotRestoreTakesAnExplicitDefaultStackOverAnImplicitOne(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedCloneSourceWithStack(t, st, "src-implicit", nil)
	seedRestoreSnapshot(t, st, "src-implicit", "snap-implicit", []string{"node-a"})

	base, stop := startServerWithStore(t, st)
	defer stop()

	csiPrepareRestoreTargetWithLayers(t, base, "src-implicit", "snap-implicit", "pvc-explicit",
		apiv1.DefaultLayerStack())

	if code := restoreOnce(t, base, "src-implicit", "snap-implicit", map[string]any{
		"to_resource": "pvc-explicit", "nodes": []string{"node-a"},
	}); code != http.StatusCreated {
		t.Errorf("restore into an explicit default stack over an implicit one = %d, want 201", code)
	}
}
