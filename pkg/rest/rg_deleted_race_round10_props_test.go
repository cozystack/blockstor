// SPDX-License-Identifier: Apache-2.0

package rest

import (
	"encoding/json"
	"net/http"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

func markDefinition(t *testing.T, st store.Store, rdName, step string) {
	t.Helper()

	rd, err := st.ResourceDefinitions().Get(t.Context(), rdName)
	if err != nil {
		t.Fatalf("read %s: %v", rdName, err)
	}

	if rd.Props == nil {
		rd.Props = map[string]string{}
	}

	rd.Props[rollbackAbandonedKey] = step

	if err := st.ResourceDefinitions().Update(t.Context(), &rd); err != nil {
		t.Fatalf("mark %s: %v", rdName, err)
	}
}

func assertUnmarked(t *testing.T, props map[string]string, what string) {
	t.Helper()

	if step, ok := props[rollbackAbandonedKey]; ok {
		t.Errorf("%s carries the abandoned-rollback mark %q it was never given", what, step)
	}
}

// A definition's props travel onto every clone and restore taken from it, and
// the abandoned-rollback mark travelled with them: a healthy clone of a marked
// source was refused on the replay linstor-csi sends.
func TestRDCloneReplayOfACloneOfAMarkedSourceIsStillAReplay(t *testing.T) {
	t.Parallel()

	backend := store.NewInMemory()
	seedGroupedCloneSource(t, backend, "src-marked10", "grp-marked10", true)
	markDefinition(t, backend, "src-marked10", "snapshots")

	base, stop := startServerWithStore(t, backend)
	defer stop()

	for attempt := 1; attempt <= 2; attempt++ {
		resp := postClone(t, base, "src-marked10", map[string]any{"name": "dst-marked10", "use_zfs_clone": true})
		_ = resp.Body.Close()

		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("attempt %d = %d, want 201: the clone is whole and its group is live", attempt, resp.StatusCode)
		}
	}

	clone, err := backend.ResourceDefinitions().Get(t.Context(), "dst-marked10")
	if err != nil {
		t.Fatalf("read the clone: %v", err)
	}

	assertUnmarked(t, clone.Props, "the clone")
}

// Each place a definition's props are copied onward strips the mark itself.
// The clone path passes through two of them, so each gets a case where it is
// the only one on the way.
func TestTheAbandonedRollbackMarkDoesNotTravel(t *testing.T) {
	t.Parallel()

	t.Run("snapshot-of-a-marked-definition", func(t *testing.T) {
		t.Parallel()

		backend := store.NewInMemory()
		seedDeployedCloneSource(t, backend, "src-snap10")
		markDefinition(t, backend, "src-snap10", "snapshots")

		base, stop := startServerWithStore(t, backend)
		defer stop()

		body, _ := json.Marshal(map[string]any{"name": "snap10", "resource_name": "src-snap10"})

		resp := httpPost(t, base+"/v1/resource-definitions/src-snap10/snapshots", body)
		_ = resp.Body.Close()

		snap, err := backend.Snapshots().Get(t.Context(), "src-snap10", "snap10")
		if err != nil {
			t.Fatalf("snapshot create = %d, read back: %v", resp.StatusCode, err)
		}

		assertUnmarked(t, snap.Props, "the snapshot")
	})

	t.Run("restore-from-a-snapshot-carrying-it", func(t *testing.T) {
		t.Parallel()

		backend := store.NewInMemory()
		seedDeployedCloneSource(t, backend, "src-rs10")

		// A snapshot taken before the mark stopped travelling.
		if err := backend.Snapshots().Create(t.Context(), &apiv1.Snapshot{
			Name: "snap-rs10", ResourceName: "src-rs10", Nodes: []string{"node-a"},
			Props:             map[string]string{rollbackAbandonedKey: "snapshots"},
			VolumeDefinitions: []apiv1.SnapshotVolumeDef{{VolumeNumber: 0, SizeKib: 64 * 1024}},
		}); err != nil {
			t.Fatalf("seed the snapshot: %v", err)
		}

		restoreInto(t, backend, "src-rs10", "snap-rs10", "dst-rs10")
	})

	t.Run("restore-falling-back-to-the-source-props", func(t *testing.T) {
		t.Parallel()

		backend := store.NewInMemory()
		seedDeployedCloneSource(t, backend, "src-rf10")
		markDefinition(t, backend, "src-rf10", "snapshots")

		if err := backend.Snapshots().Create(t.Context(), &apiv1.Snapshot{
			Name: "snap-rf10", ResourceName: "src-rf10", Nodes: []string{"node-a"},
			VolumeDefinitions: []apiv1.SnapshotVolumeDef{{VolumeNumber: 0, SizeKib: 64 * 1024}},
		}); err != nil {
			t.Fatalf("seed the snapshot: %v", err)
		}

		restoreInto(t, backend, "src-rf10", "snap-rf10", "dst-rf10")
	})

	t.Run("volume-less-clone", func(t *testing.T) {
		t.Parallel()

		backend := store.NewInMemory()

		if err := backend.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{
			Name: "shell-src10", Props: map[string]string{rollbackAbandonedKey: "snapshots", "Aux/keep": "1"},
		}); err != nil {
			t.Fatalf("seed the volume-less source: %v", err)
		}

		base, stop := startServerWithStore(t, backend)
		defer stop()

		resp := postClone(t, base, "shell-src10", map[string]any{"name": "shell-dst10"})
		_ = resp.Body.Close()

		clone, err := backend.ResourceDefinitions().Get(t.Context(), "shell-dst10")
		if err != nil {
			t.Fatalf("clone = %d, read back: %v", resp.StatusCode, err)
		}

		assertUnmarked(t, clone.Props, "the volume-less clone")

		if clone.Props["Aux/keep"] != "1" {
			t.Errorf("the clone lost the source's ordinary props: %v", clone.Props)
		}
	})
}

func restoreInto(t *testing.T, backend store.Store, src, snap, dst string) {
	t.Helper()

	base, stop := startServerWithStore(t, backend)
	defer stop()

	body, _ := json.Marshal(map[string]any{"to_resource": dst})

	resp := httpPost(t, base+"/v1/resource-definitions/"+src+"/snapshot-restore-resource/"+snap, body)
	_ = resp.Body.Close()

	restored, err := backend.ResourceDefinitions().Get(t.Context(), dst)
	if err != nil {
		t.Fatalf("restore = %d, read back: %v", resp.StatusCode, err)
	}

	assertUnmarked(t, restored.Props, "the restored definition")
}
