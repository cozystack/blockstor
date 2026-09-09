// SPDX-License-Identifier: Apache-2.0

package rest

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// "Found" is not "still right". The first attempt takes the internal snapshot
// and dies; the source is resized before the retry; the retry hydrates from
// the stale snapshot and answers 201, and the clone-status poll then reports
// COMPLETE because the volume counts agree. The caller gets a clone at the old
// size with nothing saying so — reachable only since the retry started
// resuming instead of reporting the leftover done.
func TestRDCloneRefusesToResumeOverAStaleSnapshot(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-stale")

	// What the first attempt left: the snapshot, taken at the old size.
	snap := apiv1.Snapshot{
		Name:              cloneSnapshotName("dst-stale"),
		ResourceName:      "src-stale",
		Nodes:             []string{"node-a"},
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{{VolumeNumber: 0, SizeKib: 64 * 1024}},
	}
	if err := st.Snapshots().Create(ctx, &snap); err != nil {
		t.Fatalf("seed the leftover snapshot: %v", err)
	}

	// The source grew between the attempts.
	vd, err := st.VolumeDefinitions().Get(ctx, "src-stale", 0)
	if err != nil {
		t.Fatalf("read the source volume: %v", err)
	}

	vd.SizeKib = 128 * 1024
	if err := st.VolumeDefinitions().Update(ctx, "src-stale", &vd); err != nil {
		t.Fatalf("grow the source: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := postClone(t, base, "src-stale", map[string]any{
		"name":          "dst-stale",
		"use_zfs_clone": true,
	})
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d, want 409 — the leftover snapshot is a volume size behind",
			resp.StatusCode)
	}

	vds, err := st.VolumeDefinitions().List(ctx, "dst-stale")
	if err != nil {
		t.Fatalf("list the target's volumes: %v", err)
	}

	if len(vds) != 0 {
		t.Errorf("the refused clone hydrated %d volume(s) from the stale snapshot", len(vds))
	}
}

// The positive control: the same resume, over a snapshot that still describes
// the source, still completes. Without it the refusal above could be coming
// from the resume path itself.
func TestRDCloneResumesOverACurrentSnapshot(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-current")

	snap := apiv1.Snapshot{
		Name:              cloneSnapshotName("dst-current"),
		ResourceName:      "src-current",
		Nodes:             []string{"node-a"},
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{{VolumeNumber: 0, SizeKib: 64 * 1024}},
	}
	if err := st.Snapshots().Create(ctx, &snap); err != nil {
		t.Fatalf("seed the leftover snapshot: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := postClone(t, base, "src-current", map[string]any{
		"name":          "dst-current",
		"use_zfs_clone": true,
	})
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want 201", resp.StatusCode)
	}

	vds, err := st.VolumeDefinitions().List(ctx, "dst-current")
	if err != nil {
		t.Fatalf("list the target's volumes: %v", err)
	}

	if len(vds) != 1 {
		t.Errorf("target has %d volume(s), want 1", len(vds))
	}
}

// The clone half of the case-fold. A retry spelling the target in another case
// addresses the same object — pkg/store/k8s/crdname.go lowercases every lookup
// key — so a marker comparison built from what the caller typed answered that
// somebody else owns the leftover, refused every retry, and left the target
// with no volumes forever. The restore side was fixed last round; this is its
// sibling.
func TestRDCloneResumesWhateverCaseTheRetryUses(t *testing.T) {
	t.Parallel()

	st := caseFoldingStore{store.NewInMemory()}
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-case")

	// What the first attempt left, stored lower-case.
	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name:              cloneSnapshotName("dst-case"),
		ResourceName:      "src-case",
		Nodes:             []string{"node-a"},
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{{VolumeNumber: 0, SizeKib: 64 * 1024}},
	}); err != nil {
		t.Fatalf("seed the leftover snapshot: %v", err)
	}

	seedCloneLeftover(t, st, "src-case", "dst-case")

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := postClone(t, base, "SRC-CASE", map[string]any{
		"name":          "DST-CASE",
		"use_zfs_clone": true,
	})
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("retry spelled DST-CASE over a leftover stored dst-case: got %d, want 201",
			resp.StatusCode)
	}

	vds, err := st.VolumeDefinitions().List(ctx, "dst-case")
	if err != nil {
		t.Fatalf("list the target's volumes: %v", err)
	}

	if len(vds) != 1 {
		t.Errorf("after the retry the target has %d volume(s), want 1", len(vds))
	}

	// The retry has to have RESUMED the leftover, not created a second
	// definition beside it. Everything above is satisfied either way, so
	// without this the test passes over a store whose Create does not fold —
	// which is what the real one does, and what the shim has to imitate for
	// the case-fold to be under test at all.
	rds, err := st.ResourceDefinitions().List(ctx)
	if err != nil {
		t.Fatalf("list definitions: %v", err)
	}

	targets := 0

	for i := range rds {
		if strings.EqualFold(rds[i].Name, "dst-case") {
			targets++
		}
	}

	if targets != 1 {
		t.Errorf("%d definitions under the target name; the retry created a second one "+
			"beside the leftover instead of resuming it", targets)
	}

	// And the marker the resume wrote is the stored spelling, not the one the
	// retry typed: the satellite splits this value to find the source, and a
	// clone made through the CLI has to be recognisable to a REST retry and
	// the other way round.
	got, err := st.ResourceDefinitions().Get(ctx, "dst-case")
	if err != nil {
		t.Fatalf("get the target: %v", err)
	}

	if want := restoreMarker("src-case", cloneSnapshotName("dst-case")); got.Props[restoreFromSnapshotKey] != want {
		t.Errorf("marker = %q, want %q — written from the stored objects, not from the request",
			got.Props[restoreFromSnapshotKey], want)
	}
}

// A resumed clone keeps the definition the first attempt created, so a retry
// naming a different shape gets that shape validated and then dropped while
// the answer says the clone completed. The parent group decides replica count
// and pool selection, so it is not cosmetic — and accepting a field and
// dropping it is what external_name and volume_passphrases are refused for.
func TestRDCloneRefusesARetryWithADifferentShape(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-shape2")
	seedCloneRG(t, st, "grp-a", "grp-b")

	// The leftover, parented to the group the first attempt asked for.
	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:              "dst-shape2",
		ResourceGroupName: "grp-a",
		Props: map[string]string{
			restoreFromSnapshotKey: restoreMarker("src-shape2", cloneSnapshotName("dst-shape2")),
		},
	}); err != nil {
		t.Fatalf("seed the leftover: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := postClone(t, base, "src-shape2", map[string]any{
		"name":           "dst-shape2",
		"resource_group": "grp-b",
		"use_zfs_clone":  true,
	})
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d, want 409 — the retry asks for a group the leftover was not started with",
			resp.StatusCode)
	}

	got, err := st.ResourceDefinitions().Get(ctx, "dst-shape2")
	if err != nil {
		t.Fatalf("get the leftover: %v", err)
	}

	if got.ResourceGroupName != "grp-a" {
		t.Errorf("resource group = %q; the refused retry re-parented the definition",
			got.ResourceGroupName)
	}
}

// delete_namespaces is honoured on the path CSI actually takes, not only on
// the volume-less shortcut the first test for it exercised.
func TestRDCloneHonoursDeleteNamespacesOnTheDataPath(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedDeployedCloneSource(t, st, "src-ns2")

	src, err := st.ResourceDefinitions().Get(ctx, "src-ns2")
	if err != nil {
		t.Fatalf("read the seeded source: %v", err)
	}

	src.Props = map[string]string{
		"DrbdOptions":              "bare",
		"DrbdOptions/Net/protocol": "C",
		"DrbdOptionsOther":         "keep",
	}

	if err := st.ResourceDefinitions().Update(ctx, &src); err != nil {
		t.Fatalf("put props on the source: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := postClone(t, base, "src-ns2", map[string]any{
		"name":              "dst-ns2",
		"delete_namespaces": []string{"DrbdOptions"},
		"use_zfs_clone":     true,
	})
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want 201", resp.StatusCode)
	}

	got, err := st.ResourceDefinitions().Get(ctx, "dst-ns2")
	if err != nil {
		t.Fatalf("get the clone: %v", err)
	}

	for _, key := range []string{"DrbdOptions", "DrbdOptions/Net/protocol"} {
		if _, present := got.Props[key]; present {
			t.Errorf("prop %q survived the namespace delete on the data path", key)
		}
	}

	if _, present := got.Props["DrbdOptionsOther"]; !present {
		t.Error("prop outside the named namespace was deleted")
	}
}

// The narrow race the pre-check cannot see: another request completes the
// restore and the target is deleted between the state check and this Create.
// The AlreadyExists fallback re-makes the decision on fresh state, and it has
// to re-make all of it — hydrating volumes into a dying definition is exactly
// what the 409 above prevents.
func TestMaterializeRefusesADyingLeftoverItRacedInto(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedRestoreSource(ctx, t, st)

	snap, err := st.Snapshots().Get(ctx, "pvc-src", "snap-1")
	if err != nil {
		t.Fatalf("read the seeded snapshot: %v", err)
	}

	// The state another request left behind mid-tear-down.
	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:  "pvc-raced",
		Flags: []string{rdFlagDelete},
		Props: map[string]string{
			restoreFromSnapshotKey: restoreMarker("pvc-src", "snap-1"),
		},
	}); err != nil {
		t.Fatalf("seed the dying leftover: %v", err)
	}

	srv := &Server{Store: st}
	req := &snapshotRestoreRequest{ToResource: "pvc-raced"}

	_, err = srv.materializeRestoredRD(ctx, "pvc-src", req, &snap, false, nil)
	if err == nil {
		t.Fatal("materialize accepted a target being deleted")
	}

	vds, listErr := st.VolumeDefinitions().List(ctx, "pvc-raced")
	if listErr != nil {
		t.Fatalf("list the leftover's volumes: %v", listErr)
	}

	if len(vds) != 0 {
		t.Errorf("hydrated %d volume(s) into a definition being deleted", len(vds))
	}
}

// The volume-definition restore fetched the target definition and threw it
// away. Its siblings refuse a target carrying DELETE because finishing one
// races the tear-down reaping what it writes, and volumes hydrated here are
// precisely that.
func TestSnapshotRestoreVolumeDefinitionRefusesADyingTarget(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedRestoreSource(ctx, t, st)

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:  "pvc-vd-dying",
		Flags: []string{rdFlagDelete},
	}); err != nil {
		t.Fatalf("seed the dying target: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(map[string]string{"to_resource": "pvc-vd-dying"})

	resp := httpPost(t,
		base+"/v1/resource-definitions/pvc-src/snapshot-restore-volume-definition/snap-1", body)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d, want 409 — that definition is being deleted", resp.StatusCode)
	}

	if msg := decodeRCs(t, resp)[0].Message; !strings.Contains(msg, "being deleted") {
		t.Errorf("message = %q, want it to name the deletion", msg)
	}

	vds, err := st.VolumeDefinitions().List(ctx, "pvc-vd-dying")
	if err != nil {
		t.Fatalf("list the target's volumes: %v", err)
	}

	if len(vds) != 0 {
		t.Errorf("hydrated %d volume(s) into a definition being deleted", len(vds))
	}
}
