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
// replica yet. PUT migrate-disk should:
//   - create dst diskful with StorPoolName stamped
//   - delete src
//
// Pins the Option-A wire (atomic REST semantics; satellite handles
// actual add-before-drop on the DRBD layer).
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

	// dst exists, diskful, pool stamped.
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

	// src removed.
	if _, err := st.Resources().Get(t.Context(), "pvc-mig", "src-node"); err == nil {
		t.Errorf("src Resource still present after migrate-disk")
	}
}

// TestMigrateDiskWithExistingDiskless: dst already declared diskless
// (the typical two-step upstream flow: `linstor r c <dst> <rd>
// --drbd-diskless` then `linstor r td -s <pool> --migrate-from
// <src>`). Migrate flips dst diskful and prunes src.
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

	if _, err := st.Resources().Get(t.Context(), "pvc-mig2", "src-node"); err == nil {
		t.Errorf("src Resource still present after migrate-disk")
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
