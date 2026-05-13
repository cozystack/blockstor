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
	"net/http"
	"slices"
	"testing"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// TestToggleDiskPromotesDiskless: PUT toggle-disk on a DISKLESS
// replica drops the DISKLESS flag — the satellite picks the rest
// of the work up from the auto-diskful path on its next reconcile.
func TestToggleDiskPromotesDiskless(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-1"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Resources().Create(t.Context(), &apiv1.Resource{
		Name:     "pvc-1",
		NodeName: "n1",
		Flags:    []string{apiv1.ResourceFlagDiskless},
	}); err != nil {
		t.Fatalf("seed Resource: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPut(t, base+"/v1/resource-definitions/pvc-1/resources/n1/toggle-disk", nil)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.Resources().Get(t.Context(), "pvc-1", "n1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if slices.Contains(got.Flags, apiv1.ResourceFlagDiskless) {
		t.Errorf("DISKLESS flag still present after toggle-disk: %v", got.Flags)
	}
}

// TestToggleDiskWithExplicitPool stamps the storage pool name on
// the typed Spec.Props["StorPoolName"] when promoting via the
// `/storage-pool/{pool}` path. Pins the upstream-LINSTOR-shaped
// URL and verifies the controller's auto-diskful path won't have
// to pick a pool.
func TestToggleDiskWithExplicitPool(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-pool"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Resources().Create(t.Context(), &apiv1.Resource{
		Name:     "pvc-pool",
		NodeName: "n2",
		Flags:    []string{apiv1.ResourceFlagDiskless},
	}); err != nil {
		t.Fatalf("seed Resource: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPut(t, base+"/v1/resource-definitions/pvc-pool/resources/n2/toggle-disk/storage-pool/zfs-thin", nil)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.Resources().Get(t.Context(), "pvc-pool", "n2")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.Props["StorPoolName"] != "zfs-thin" {
		t.Errorf("Props[StorPoolName]: got %q, want zfs-thin", got.Props["StorPoolName"])
	}

	if slices.Contains(got.Flags, apiv1.ResourceFlagDiskless) {
		t.Errorf("DISKLESS flag still present: %v", got.Flags)
	}
}

// TestToggleDiskDemotesDiskful: PUT on a diskful replica adds the
// DISKLESS flag — the satellite tears down the LV on its next
// reconcile via the existing detach hook.
func TestToggleDiskDemotesDiskful(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-d"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Resources().Create(t.Context(), &apiv1.Resource{
		Name:     "pvc-d",
		NodeName: "n3",
		// no DISKLESS flag — currently diskful
	}); err != nil {
		t.Fatalf("seed Resource: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPut(t, base+"/v1/resource-definitions/pvc-d/resources/n3/toggle-disk", nil)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.Resources().Get(t.Context(), "pvc-d", "n3")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if !slices.Contains(got.Flags, apiv1.ResourceFlagDiskless) {
		t.Errorf("DISKLESS flag missing after toggle-disk: %v", got.Flags)
	}
}

// TestToggleDiskUnknownReplica: 404 on a missing (rd, node) pair.
func TestToggleDiskUnknownReplica(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpPut(t, base+"/v1/resource-definitions/ghost/resources/n9/toggle-disk", nil)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", resp.StatusCode)
	}
}

// TestMigrateDiskBasicFlow: src has a diskful replica, dst has no
// replica yet. PUT migrate-disk under Option B (strict
// add-before-drop) should:
//   - create dst diskful with StorPoolName stamped
//   - stamp BlockstorMigratingFrom=<src-node> on dst
//   - LEAVE src in place — the ResourceMigrationReconciler will
//     prune it asynchronously once dst's Status.Volumes report
//     UpToDate
//
// Pins the redundancy invariant: at no point during this REST call
// does the diskful count drop below the original. The src lives
// until the destination is observed durable on the wire.
func TestMigrateDiskBasicFlow(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-mig"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Resources().Create(t.Context(), &apiv1.Resource{
		Name:     "pvc-mig",
		NodeName: "src-node",
		// No DISKLESS flag — diskful.
	}); err != nil {
		t.Fatalf("seed src Resource: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPut(t,
		base+"/v1/resource-definitions/pvc-mig/resources/dst-node/migrate-disk/src-node/zfs-thin",
		nil)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status: got %d, want 200", resp.StatusCode)
	}

	// dst exists, diskful, pool stamped, migrating-from prop set.
	got, err := st.Resources().Get(t.Context(), "pvc-mig", "dst-node")
	if err != nil {
		t.Fatalf("Get dst: %v", err)
	}

	if slices.Contains(got.Flags, apiv1.ResourceFlagDiskless) {
		t.Errorf("dst DISKLESS flag still set: %v", got.Flags)
	}

	if got.Props["StorPoolName"] != "zfs-thin" {
		t.Errorf("dst StorPoolName: got %q, want zfs-thin", got.Props["StorPoolName"])
	}

	if got.Props[MigratingFromProp] != "src-node" {
		t.Errorf("dst %s: got %q, want src-node",
			MigratingFromProp, got.Props[MigratingFromProp])
	}

	// src MUST still be present — Option B defers the prune to the
	// reconciler. Deleting it here would re-introduce the redundancy
	// regression Option B exists to fix.
	srcRes, err := st.Resources().Get(t.Context(), "pvc-mig", "src-node")
	if err != nil {
		t.Fatalf("src Resource pruned by REST handler (Option A regression): %v", err)
	}

	if slices.Contains(srcRes.Flags, apiv1.ResourceFlagDiskless) {
		t.Errorf("src unexpectedly flagged DISKLESS by migrate handler: %v", srcRes.Flags)
	}
}

// TestMigrateDiskWithExistingDiskless: dst already declared diskless
// (the typical two-step upstream flow: `linstor r c <dst> <rd>
// --drbd-diskless` then `linstor r td -s <pool> --migrate-from
// <src>`). Migrate flips dst diskful, stamps migrating-from, and
// leaves src untouched (the reconciler prunes it once dst is
// UpToDate — strict add-before-drop, Option B).
func TestMigrateDiskWithExistingDiskless(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-mig2"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Resources().Create(t.Context(), &apiv1.Resource{
		Name: "pvc-mig2", NodeName: "src-node",
	}); err != nil {
		t.Fatalf("seed src Resource: %v", err)
	}

	if err := st.Resources().Create(t.Context(), &apiv1.Resource{
		Name: "pvc-mig2", NodeName: "dst-node",
		Flags: []string{apiv1.ResourceFlagDiskless},
	}); err != nil {
		t.Fatalf("seed dst Resource: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPut(t,
		base+"/v1/resource-definitions/pvc-mig2/resources/dst-node/migrate-disk/src-node/zfs-thin",
		nil)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.Resources().Get(t.Context(), "pvc-mig2", "dst-node")
	if err != nil {
		t.Fatalf("Get dst: %v", err)
	}

	if slices.Contains(got.Flags, apiv1.ResourceFlagDiskless) {
		t.Errorf("dst DISKLESS not cleared: %v", got.Flags)
	}

	if got.Props["StorPoolName"] != "zfs-thin" {
		t.Errorf("dst StorPoolName: got %q, want zfs-thin", got.Props["StorPoolName"])
	}

	if got.Props[MigratingFromProp] != "src-node" {
		t.Errorf("dst %s: got %q, want src-node",
			MigratingFromProp, got.Props[MigratingFromProp])
	}

	// src MUST live until the reconciler observes dst UpToDate.
	if _, err := st.Resources().Get(t.Context(), "pvc-mig2", "src-node"); err != nil {
		t.Errorf("src Resource pruned synchronously by REST handler: %v", err)
	}
}

// TestMigrateDiskRefusesPrimaryInUse: an active Primary replica
// can't be migrated implicitly — UG9 requires the operator to
// demote the consumer first. 409 Conflict per writeError.
func TestMigrateDiskRefusesPrimaryInUse(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-busy"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	primary := true

	if err := st.Resources().Create(t.Context(), &apiv1.Resource{
		Name:     "pvc-busy",
		NodeName: "src-node",
		State:    apiv1.ResourceState{InUse: &primary},
	}); err != nil {
		t.Fatalf("seed src Resource: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPut(t,
		base+"/v1/resource-definitions/pvc-busy/resources/dst-node/migrate-disk/src-node/zfs-thin",
		nil)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusConflict {
		t.Errorf("status: got %d, want 409", resp.StatusCode)
	}

	// src must be untouched.
	if _, err := st.Resources().Get(t.Context(), "pvc-busy", "src-node"); err != nil {
		t.Errorf("src removed despite 409: %v", err)
	}
}

// TestToggleDiskCancelQuerySetsSpecField pins the Bug 40 cancel
// surface: PUT toggle-disk?cancel=true on a diskful (or in-flight
// converting) replica MUST set Spec.ToggleDiskCancel=true on the
// stored Resource without flipping the DISKLESS flag. The flag flip
// is the reconciler's job once it has torn down storage + DRBD —
// REST only writes the intent.
func TestToggleDiskCancelQuerySetsSpecField(t *testing.T) {
	st := store.NewInMemory()
	if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: "pvc-c"}); err != nil {
		t.Fatalf("seed RD: %v", err)
	}

	if err := st.Resources().Create(t.Context(), &apiv1.Resource{
		Name:     "pvc-c",
		NodeName: "n-cancel",
		// no DISKLESS flag — Resource is mid-conversion or already
		// diskful; either way, REST handler stamps the cancel
		// intent without touching Flags.
	}); err != nil {
		t.Fatalf("seed Resource: %v", err)
	}

	base, stop := startServerWithStore(t, st)
	defer stop()

	resp := httpPut(t, base+"/v1/resource-definitions/pvc-c/resources/n-cancel/toggle-disk?cancel=true", nil)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status: got %d, want 200", resp.StatusCode)
	}

	got, err := st.Resources().Get(t.Context(), "pvc-c", "n-cancel")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if !got.ToggleDiskCancel {
		t.Errorf("ToggleDiskCancel false after cancel=true: got %+v", got)
	}

	// Crucial: REST must NOT flip DISKLESS itself — the reconciler
	// is the one that re-stamps DISKLESS after the rollback runs.
	// A pre-emptive flag flip would make consumers see the Resource
	// as connection-mesh-only before the storage + drbdadm cleanup
	// finished, racing the satellite's tear-down path.
	if slices.Contains(got.Flags, apiv1.ResourceFlagDiskless) {
		t.Errorf("cancel handler unexpectedly flipped DISKLESS: %v", got.Flags)
	}
}

// TestMigrateDiskUnknownRD: missing ResourceDefinition surfaces as
// 404, not as a follow-on 500 from a later lookup.
func TestMigrateDiskUnknownRD(t *testing.T) {
	base, stop := startServerWithStore(t, store.NewInMemory())
	defer stop()

	resp := httpPut(t,
		base+"/v1/resource-definitions/ghost-rd/resources/dst/migrate-disk/src/zfs-thin",
		nil)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", resp.StatusCode)
	}
}
