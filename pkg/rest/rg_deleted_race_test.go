// SPDX-License-Identifier: Apache-2.0

package rest

import (
	"encoding/json"
	"net/http"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// seedGroupedCloneSource is seedDeployedCloneSource with the source parented
// to a resource group, which is the shape every definition linstor-csi creates
// has.
func seedGroupedCloneSource(t *testing.T, st store.Store, rdName, rgName string, createGroup bool) {
	t.Helper()

	seedDeployedCloneSource(t, st, rdName)

	src, err := st.ResourceDefinitions().Get(t.Context(), rdName)
	if err != nil {
		t.Fatalf("read the seeded source: %v", err)
	}

	src.ResourceGroupName = rgName

	if err := st.ResourceDefinitions().Update(t.Context(), &src); err != nil {
		t.Fatalf("parent the source: %v", err)
	}

	if createGroup {
		if err := st.ResourceGroups().Create(t.Context(),
			&apiv1.ResourceGroup{Name: rgName}); err != nil {
			t.Fatalf("seed RG: %v", err)
		}
	}
}

// `POST /v1/resource-definitions` checks the resource group twice — before the
// write and again after it, rolling the definition back when a concurrent
// `rg d` won the race. A definition left pointing at a group that is gone
// lists fine and places badly: the placer's Controller→RG→RD walk drops the RG
// tier without a word, taking auto-place, auto-diskful, place_count and
// rebalance with it.
//
// Clone creates a definition the same way and inherits the group the same way,
// and had neither half.
func TestRDCloneRollsBackWhenTheParentGroupIsGone(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedGroupedCloneSource(t, st, "src-rg-race", "grp-gone", false)

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := postClone(t, base, "src-rg-race", map[string]any{
		"name":          "dst-rg-race",
		"use_zfs_clone": true,
	})
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 — the parent group is gone", resp.StatusCode)
	}

	if _, err := st.ResourceDefinitions().Get(ctx, "dst-rg-race"); err == nil {
		t.Error("the definition survived, parented to a group that does not exist")
	}

	replicas, err := st.Resources().ListByDefinition(ctx, "dst-rg-race")
	if err != nil {
		t.Fatalf("list the target's replicas: %v", err)
	}

	if len(replicas) != 0 {
		t.Errorf("%d replica(s) left behind pointing at a definition that was rolled back",
			len(replicas))
	}

	// The source is untouched, and so is the snapshot the clone took: it may
	// be the only copy of something, and deleting one is the operator's call.
	if _, err := st.ResourceDefinitions().Get(ctx, "src-rg-race"); err != nil {
		t.Errorf("the rollback took the source with it: %v", err)
	}

	if _, err := st.Snapshots().Get(ctx, "src-rg-race",
		cloneSnapshotName("dst-rg-race")); err != nil {
		t.Errorf("the rollback deleted the internal snapshot: %v", err)
	}
}

// The positive control: the same clone with the group where it should be.
func TestRDCloneKeepsGoingWhenTheParentGroupIsThere(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()
	seedGroupedCloneSource(t, st, "src-rg-ok", "grp-there", true)

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := postClone(t, base, "src-rg-ok", map[string]any{
		"name":          "dst-rg-ok",
		"use_zfs_clone": true,
	})
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want 201", resp.StatusCode)
	}

	if _, err := st.ResourceDefinitions().Get(ctx, "dst-rg-ok"); err != nil {
		t.Errorf("target RD not persisted: %v", err)
	}
}

// The restore path inherits the same group the same way, and had the same gap.
func TestSnapshotRestoreRollsBackWhenTheParentGroupIsGone(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:              "restore-src",
		ResourceGroupName: "grp-gone-restore",
	}); err != nil {
		t.Fatalf("seed the source: %v", err)
	}

	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name:         "snap-rg",
		ResourceName: "restore-src",
		Nodes:        []string{"n1"},
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{
			{VolumeNumber: 0, SizeKib: 1024 * 1024},
		},
	}); err != nil {
		t.Fatalf("seed the snapshot: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(map[string]string{
		"to_resource":   "restore-dst",
		"from_snapshot": "snap-rg",
	})

	resp := httpPost(t, base+"/v1/resource-definitions/restore-src/snapshot-restore-resource", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 — the parent group is gone", resp.StatusCode)
	}

	if _, err := st.ResourceDefinitions().Get(ctx, "restore-dst"); err == nil {
		t.Error("the definition survived, parented to a group that does not exist")
	}
}

// The positive control for the restore path.
func TestSnapshotRestoreKeepsGoingWhenTheParentGroupIsThere(t *testing.T) {
	t.Parallel()

	st := store.NewInMemory()
	ctx := t.Context()

	if err := st.ResourceGroups().Create(ctx,
		&apiv1.ResourceGroup{Name: "grp-there-restore"}); err != nil {
		t.Fatalf("seed RG: %v", err)
	}

	if err := st.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
		Name:              "restore-src-ok",
		ResourceGroupName: "grp-there-restore",
	}); err != nil {
		t.Fatalf("seed the source: %v", err)
	}

	if err := st.Snapshots().Create(ctx, &apiv1.Snapshot{
		Name:         "snap-rg-ok",
		ResourceName: "restore-src-ok",
		Nodes:        []string{"n1"},
		VolumeDefinitions: []apiv1.SnapshotVolumeDef{
			{VolumeNumber: 0, SizeKib: 1024 * 1024},
		},
	}); err != nil {
		t.Fatalf("seed the snapshot: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	body, _ := json.Marshal(map[string]string{
		"to_resource":   "restore-dst-ok",
		"from_snapshot": "snap-rg-ok",
	})

	resp := httpPost(t, base+"/v1/resource-definitions/restore-src-ok/snapshot-restore-resource", body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want 201", resp.StatusCode)
	}

	if _, err := st.ResourceDefinitions().Get(ctx, "restore-dst-ok"); err != nil {
		t.Errorf("target RD not persisted: %v", err)
	}
}
