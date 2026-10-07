// SPDX-License-Identifier: Apache-2.0

package rest

import (
	"encoding/json"
	"net/http"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// csiPrepareRestoreTarget runs the first two calls linstor-csi v1.11.2's
// VolFromSnap makes before it asks for the resources: it creates the
// definition itself (reconcileResourceDefinition), then restores the volume
// definitions onto it when it has none (reconcileSnapshotVolumeDefinitions).
func csiPrepareRestoreTarget(t *testing.T, base, src, snap, target string) {
	t.Helper()

	csiPrepareRestoreTargetWithLayers(t, base, src, snap, target, nil)
}

// csiPrepareRestoreTargetWithLayers is csiPrepareRestoreTarget for a
// StorageClass that names a layer list, which linstor-csi sends on the
// definition it creates.
func csiPrepareRestoreTargetWithLayers(t *testing.T, base, src, snap, target string, layers []string) {
	t.Helper()

	create := map[string]any{"resource_definition": map[string]any{"name": target}}
	if len(layers) > 0 {
		create["layer_list"] = layers
	}

	body, err := json.Marshal(create)
	if err != nil {
		t.Fatalf("marshal rd create: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-definitions", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("rd create (reconcileResourceDefinition) = %d, want 201", resp.StatusCode)
	}

	raw, err := json.Marshal(map[string]any{"to_resource": target})
	if err != nil {
		t.Fatalf("marshal vd restore: %v", err)
	}

	resp = httpPost(t, base+"/v1/resource-definitions/"+src+"/snapshot-restore-volume-definition/"+snap, raw)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("vd restore (reconcileSnapshotVolumeDefinitions) = %d, want 200", resp.StatusCode)
	}
}

// linstor-csi creates the target definition itself and restores its volume
// definitions before it asks for the resources, so the definition never
// carried this restore's marker and the resource restore refused it as
// somebody else's: every volume restored from a snapshot through CSI failed on
// its first call.
func TestSnapshotRestoreFollowsLinstorCSIsOwnSequence(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-csi")
	seedRestoreSnapshot(t, st, "src-csi", "snap-csi", []string{"node-a"})

	base, stop := startServerWithStore(t, st)
	defer stop()

	csiPrepareRestoreTarget(t, base, "src-csi", "snap-csi", "pvc-csi")

	// reconcileSnapshotResources: one node that holds the snapshot.
	if code := restoreOnce(t, base, "src-csi", "snap-csi", map[string]any{
		"to_resource": "pvc-csi", "nodes": []string{"node-a"},
	}); code != http.StatusCreated {
		t.Fatalf("resource restore over the definition CSI prepared = %d, want 201", code)
	}

	rd, err := st.ResourceDefinitions().Get(ctx, "pvc-csi")
	if err != nil {
		t.Fatalf("read the restored definition: %v", err)
	}

	if got, want := rd.Props[restoreFromSnapshotKey], restoreMarker("src-csi", "snap-csi"); got != want {
		t.Errorf("marker = %q, want %q: the satellite restores a replica from the snapshot only through it", got, want)
	}

	replicas, err := st.Resources().ListByDefinition(ctx, "pvc-csi")
	if err != nil || len(replicas) != 1 || replicas[0].NodeName != "node-a" {
		t.Errorf("replicas after the restore = %v (%v), want one on node-a", replicas, err)
	}
}

// CSI retries the whole chain when a step fails. Its own checks skip the
// definition and volume steps once they are done; the resource restore it
// re-issues lands on a target it already marked, and finishes it.
func TestSnapshotRestoreFinishesLinstorCSIsRetriedChain(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-csr")
	seedRestoreSnapshot(t, st, "src-csr", "snap-csr", []string{"node-a"})

	base, stop := startServerWithStore(t, st)
	defer stop()

	csiPrepareRestoreTarget(t, base, "src-csr", "snap-csr", "pvc-csr")

	body := map[string]any{"to_resource": "pvc-csr", "nodes": []string{"node-a"}}

	if code := restoreOnce(t, base, "src-csr", "snap-csr", body); code != http.StatusCreated {
		t.Fatalf("first resource restore = %d, want 201", code)
	}

	// The first attempt's replica did not survive; CSI's next pass finds
	// the definition and its volumes and asks for the resources again.
	if err := st.Resources().Delete(ctx, "pvc-csr", "node-a"); err != nil {
		t.Fatalf("drop the replica: %v", err)
	}

	if code := restoreOnce(t, base, "src-csr", "snap-csr", body); code != http.StatusCreated {
		t.Fatalf("retried resource restore = %d, want 201", code)
	}

	if replicas, _ := st.Resources().ListByDefinition(ctx, "pvc-csr"); len(replicas) != 1 {
		t.Errorf("replicas after the retried chain = %d, want 1", len(replicas))
	}
}

// What the caller prepared is taken only in the state CSI leaves. A definition
// with a live replica may hold data, and one whose volumes are not exactly the
// snapshot's was not prepared from it.
func TestSnapshotRestoreRefusesATargetNotPreparedFromTheSnapshot(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		seed func(t *testing.T, st store.Store, target string)
	}{
		{name: "live-replica", seed: func(t *testing.T, st store.Store, target string) {
			t.Helper()

			if err := st.VolumeDefinitions().Create(t.Context(), target,
				&apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 64 * 1024}); err != nil {
				t.Fatalf("seed the volume: %v", err)
			}

			if err := st.Resources().Create(t.Context(), &apiv1.Resource{Name: target, NodeName: "node-a"}); err != nil {
				t.Fatalf("seed the live replica: %v", err)
			}
		}},
		{name: "other-size", seed: func(t *testing.T, st store.Store, target string) {
			t.Helper()

			if err := st.VolumeDefinitions().Create(t.Context(), target,
				&apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 128 * 1024}); err != nil {
				t.Fatalf("seed the volume: %v", err)
			}
		}},
		{name: "no-volumes", seed: func(*testing.T, store.Store, string) {}},
		{name: "being-deleted", seed: func(t *testing.T, st store.Store, target string) {
			t.Helper()

			seedSnapshotShapedVolume(t, st, target)

			rd, err := st.ResourceDefinitions().Get(t.Context(), target)
			if err != nil {
				t.Fatalf("read the target: %v", err)
			}

			rd.Flags = append(rd.Flags, apiv1.ResourceFlagDelete)
			if err := st.ResourceDefinitions().Update(t.Context(), &rd); err != nil {
				t.Fatalf("flag the target for deletion: %v", err)
			}
		}},
		{name: "extra-volume", seed: func(t *testing.T, st store.Store, target string) {
			t.Helper()

			seedSnapshotShapedVolume(t, st, target)

			if err := st.VolumeDefinitions().Create(t.Context(), target,
				&apiv1.VolumeDefinition{VolumeNumber: 1, SizeKib: 64 * 1024}); err != nil {
				t.Fatalf("seed the volume the snapshot never held: %v", err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			st := store.NewInMemory()
			ctx := t.Context()
			target := "pvc-np-" + tc.name
			seedDeployedCloneSource(t, st, "src-np")
			seedRestoreSnapshot(t, st, "src-np", "snap-np", []string{"node-a"})

			if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: target}); err != nil {
				t.Fatalf("seed the target: %v", err)
			}

			tc.seed(t, st, target)

			base, stop := startServerWithStore(t, st)
			defer stop()

			if code := restoreOnce(t, base, "src-np", "snap-np", map[string]any{
				"to_resource": target, "nodes": []string{"node-a"},
			}); code != http.StatusConflict {
				t.Errorf("restore into a target not prepared from the snapshot = %d, want 409", code)
			}

			rd, err := st.ResourceDefinitions().Get(ctx, target)
			if err != nil {
				t.Fatalf("read the target: %v", err)
			}

			if marker := rd.Props[restoreFromSnapshotKey]; marker != "" {
				t.Errorf("the refused target was marked %q", marker)
			}
		})
	}
}

// seedSnapshotShapedVolume gives a prepared target the one volume the
// snapshot holds, at its size, so a case differs from a usable target only in
// what it adds.
func seedSnapshotShapedVolume(t *testing.T, st store.Store, target string) {
	t.Helper()

	if err := st.VolumeDefinitions().Create(t.Context(), target,
		&apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 64 * 1024}); err != nil {
		t.Fatalf("seed the volume: %v", err)
	}
}
