// SPDX-License-Identifier: Apache-2.0

package rest

import (
	"encoding/json"
	"net/http"
	"slices"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

func seedRestoreSnapshot(t *testing.T, st store.Store, srcName, snapName string, nodes []string) {
	t.Helper()

	if err := st.Snapshots().Create(t.Context(), &apiv1.Snapshot{
		Name:              snapName,
		ResourceName:      srcName,
		Nodes:             nodes,
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{{VolumeNumber: 0, SizeKib: 64 * 1024}},
	}); err != nil {
		t.Fatalf("seed snapshot %s: %v", snapName, err)
	}
}

func restoreOnce(t *testing.T, base, src, snap string, body map[string]any) int {
	t.Helper()

	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal restore body: %v", err)
	}

	resp := httpPost(t, base+"/v1/resource-definitions/"+src+"/snapshot-restore-resource/"+snap, raw)
	_ = resp.Body.Close()

	return resp.StatusCode
}

// The restore replay resumed on the marker alone, and placement re-created a
// replica on every requested node that no longer had one, including a node the
// operator had emptied after the restore finished.
func TestSnapshotRestoreReplayLeavesAnEmptiedNodeAlone(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedTwoNodeCloneSource(t, st, "src-rr")
	seedRestoreSnapshot(t, st, "src-rr", "snap-rr", []string{"node-a", "node-b"})

	base, stop := startServerWithStore(t, st)
	defer stop()

	body := map[string]any{"to_resource": "dst-rr", "node_names": []string{"node-a", "node-b"}}

	if code := restoreOnce(t, base, "src-rr", "snap-rr", body); code != http.StatusCreated {
		t.Fatalf("first restore = %d, want 201", code)
	}

	if err := st.Resources().Delete(ctx, "dst-rr", "node-a"); err != nil {
		t.Fatalf("empty node-a: %v", err)
	}

	if code := restoreOnce(t, base, "src-rr", "snap-rr", body); code != http.StatusCreated {
		t.Fatalf("replay of the finished restore = %d, want 201", code)
	}

	after, err := st.Resources().ListByDefinition(ctx, "dst-rr")
	if err != nil {
		t.Fatalf("list the restored replicas: %v", err)
	}

	if names := nodeNamesOf(after); !slices.Equal(names, []string{"node-b"}) {
		t.Errorf("replicas after the replay = %v, want [node-b]: the replay re-stamped the emptied node", names)
	}
}

// A bare restore places nothing, so a replica is not what makes it finished. A
// volume added to it afterwards is its own, and the replay stays a replay.
func TestSnapshotRestoreReplayOfABareRestoreWithAnAddedVolume(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-rb")
	seedRestoreSnapshot(t, st, "src-rb", "snap-rb", []string{"node-a"})

	base, stop := startServerWithStore(t, st)
	defer stop()

	body := map[string]any{"to_resource": "dst-rb"}

	if code := restoreOnce(t, base, "src-rb", "snap-rb", body); code != http.StatusCreated {
		t.Fatalf("first restore = %d, want 201", code)
	}

	if err := st.VolumeDefinitions().Create(ctx, "dst-rb",
		&apiv1.VolumeDefinition{VolumeNumber: 1, SizeKib: 32 * 1024}); err != nil {
		t.Fatalf("add a volume to the restored definition: %v", err)
	}

	if code := restoreOnce(t, base, "src-rb", "snap-rb", body); code != http.StatusCreated {
		t.Errorf("replay of a bare restore that gained a volume = %d, want 201", code)
	}
}

// With the replica requirement gone, an explicit-node restore whose first
// attempt hydrated the volumes and died before placing anything reads as
// finished, and the replay reports success over a target that exists on no
// node.
func TestSnapshotRestoreReplayPlacesWhenTheFirstAttemptPlacedNothing(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-np")
	seedRestoreSnapshot(t, st, "src-np", "snap-np", []string{"node-a"})

	// The leftover a first attempt left: the marker, the snapshot's volumes,
	// and no Resource at all.
	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name: "dst-np",
		Props: map[string]string{
			restoreFromSnapshotKey: restoreMarker("src-np", "snap-np"),
		},
	}); err != nil {
		t.Fatalf("seed the leftover: %v", err)
	}

	if err := st.VolumeDefinitions().Create(ctx, "dst-np",
		&apiv1.VolumeDefinition{VolumeNumber: 0, SizeKib: 64 * 1024}); err != nil {
		t.Fatalf("seed the hydrated volume: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	if code := restoreOnce(t, base, "src-np", "snap-np", map[string]any{
		"to_resource": "dst-np", "node_names": []string{"node-a"},
	}); code != http.StatusCreated {
		t.Fatalf("retry = %d, want 201", code)
	}

	placed, err := st.Resources().ListByDefinition(ctx, "dst-np")
	if err != nil {
		t.Fatalf("list the restored replicas: %v", err)
	}

	if len(placed) == 0 {
		t.Error("the retry reported the restore done over a definition with no replica")
	}
}

// Every existing fixture for the DELETE refusal on the restore door seeds a
// leftover with no volumes, so a deeper check gives the same 409 and the term
// never decides. With volumes and a replica, the replay would answer 201 over
// a definition being deleted.
func TestSnapshotRestoreReplayRefusesAFinishedLeftoverBeingDeleted(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	seedDeployedCloneSource(t, st, "src-del")
	seedRestoreSnapshot(t, st, "src-del", "snap-del", []string{"node-a"})

	base, stop := startServerWithStore(t, st)
	defer stop()

	body := map[string]any{"to_resource": "dst-del", "node_names": []string{"node-a"}}

	if code := restoreOnce(t, base, "src-del", "snap-del", body); code != http.StatusCreated {
		t.Fatalf("first restore = %d, want 201", code)
	}

	updateSource(t, st, "dst-del", func(rd *apiv1.ResourceDefinition) {
		rd.Flags = append(rd.Flags, rdFlagDelete)
	})

	if code := restoreOnce(t, base, "src-del", "snap-del", body); code != http.StatusConflict {
		t.Errorf("replay over a finished restore being deleted = %d, want 409", code)
	}
}
