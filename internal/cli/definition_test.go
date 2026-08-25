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

// modify is a PARTIAL update: re-parenting a definition must not blank
// the layer stack the operator pinned at create time.
func TestResourceDefinitionModifyTouchesOnlyWhatWasNamed(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
		// Both groups have to exist: re-parenting onto a group that
		// does not is refused, which TestResourceDefinitionModifyRejectsUnknownGroup covers.
		_ = backend.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{Name: "old"})
		_ = backend.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{Name: "new"})
		_ = backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
			Name: "pvc-x", ResourceGroupName: "old", LayerStack: []string{"DRBD", "STORAGE"},
		})
	})

	if got := app.Run(t.Context(), []string{"rd", "modify", "pvc-x", "--resource-group", "new"}); got != 0 {
		t.Fatalf("modify exit = %d (stderr: %s)", got, errBuf.String())
	}

	def, err := appStore(t, app).ResourceDefinitions().Get(t.Context(), "pvc-x")
	if err != nil {
		t.Fatalf("get definition: %v", err)
	}

	if def.ResourceGroupName != "new" {
		t.Errorf("resource group = %q, want new", def.ResourceGroupName)
	}

	if len(def.LayerStack) != 2 {
		t.Errorf("modify blanked the layer stack: %v", def.LayerStack)
	}

	// A modify that names nothing is a usage error, not a no-op write.
	if got := app.Run(t.Context(), []string{"rd", "modify", "pvc-x"}); got != 2 {
		t.Errorf("bare modify exit = %d, want 2", got)
	}
}

// Cloning takes an internal snapshot and restores from it. The
// snapshot must SURVIVE the clone: a cloned volume stays dependent on
// its origin, so reaping it would break the clone.
func TestResourceDefinitionClone(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, seedSnapshotSource)

	if got := app.Run(t.Context(), []string{"rd", "clone", "pvc-x", "pvc-clone"}); got != 0 {
		t.Fatalf("clone exit = %d (stderr: %s)", got, errBuf.String())
	}

	backend := appStore(t, app)

	def, err := backend.ResourceDefinitions().Get(t.Context(), "pvc-clone")
	if err != nil {
		t.Fatalf("get clone: %v", err)
	}

	if def.Props["BlockstorRestoreFromSnapshot"] != "pvc-x:clone-pvc-clone" {
		t.Errorf("restore marker = %q, want pvc-x:clone-pvc-clone", def.Props["BlockstorRestoreFromSnapshot"])
	}

	if _, err = backend.Snapshots().Get(t.Context(), "pvc-x", "clone-pvc-clone"); err != nil {
		t.Errorf("the origin snapshot the clone depends on is gone: %v", err)
	}

	replicas, err := backend.Resources().ListByDefinition(t.Context(), "pvc-clone")
	if err != nil {
		t.Fatalf("list replicas: %v", err)
	}

	if len(replicas) != 2 {
		t.Errorf("clone placed %d replicas, want one per source node", len(replicas))
	}
}

// Cloning a definition that is being torn down is refused: the
// source's data is going away underneath the restore.
func TestResourceDefinitionCloneRefusesDeletingSource(t *testing.T) {
	t.Parallel()

	app, _, _ := newApp(t, func(ctx context.Context, backend store.Store) {
		_ = backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
			Name: "pvc-x", Flags: []string{apiv1.ResourceFlagDelete},
		})
	})

	if got := app.Run(t.Context(), []string{"rd", "clone", "pvc-x", "pvc-clone"}); got == 0 {
		t.Error("cloning a source being deleted succeeded")
	}
}

// The size query is bounded by the SMALLEST of the pools a replica set
// would occupy: every replica holds the whole volume, so the tightest
// pool sets the limit — reporting the widest would promise capacity
// that does not exist.
func TestResourceGroupQuerySizeInfo(t *testing.T) {
	t.Parallel()

	app, out, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
		_ = backend.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
			Name:         "grp",
			SelectFilter: apiv1.AutoSelectFilter{PlaceCount: 2, StoragePool: "data"},
		})

		for node, free := range map[string]int64{
			"node-1": 100 << 20, // 100 GiB
			"node-2": 10 << 20,  //  10 GiB — the binding constraint
			"node-3": 50 << 20,
		} {
			_ = backend.StoragePools().Create(ctx, &apiv1.StoragePool{
				NodeName: node, StoragePoolName: "data",
				ProviderKind: apiv1.StoragePoolKindLVMThin,
				FreeCapacity: free, TotalCapacity: free,
			})
		}
	})

	if got := app.Run(t.Context(), []string{"rg", "query-size-info", "grp"}); got != 0 {
		t.Fatalf("query exit = %d (stderr: %s)", got, errBuf.String())
	}

	// Two replicas fit in the 100 GiB and 50 GiB pools, so 50 GiB is
	// the bound — not 100, and not the 10 GiB pool that would not be
	// chosen.
	if !strings.Contains(out.String(), "| 50 GiB ") {
		t.Errorf("size info does not report the binding pool:\n%s", out.String())
	}
}

// Fewer candidate pools than the requested redundancy means nothing
// fits — reporting a size would promise a placement that cannot happen.
func TestResourceGroupQuerySizeInfoUnplaceable(t *testing.T) {
	t.Parallel()

	app, out, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
		_ = backend.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
			Name:         "grp",
			SelectFilter: apiv1.AutoSelectFilter{PlaceCount: 3, StoragePool: "data"},
		})
		_ = backend.StoragePools().Create(ctx, &apiv1.StoragePool{
			NodeName: "node-1", StoragePoolName: "data",
			ProviderKind: apiv1.StoragePoolKindLVMThin,
			FreeCapacity: 100 << 20,
		})
	})

	if got := app.Run(t.Context(), []string{"rg", "query-max-volume-size", "grp"}); got != 0 {
		t.Fatalf("query exit = %d (stderr: %s)", got, errBuf.String())
	}

	// The MaxVolumeSize column — the answer — must read 0, even though
	// the candidate pool it lists has room: one pool cannot hold three
	// replicas.
	if !strings.Contains(out.String(), "| grp           | 0 ") {
		t.Errorf("an unplaceable group was not reported as having no room:\n%s", out.String())
	}
}

// `physical-storage create-device-pool` records the attach request on
// the discovered device AND registers the pool. Registering the pool
// alone would leave the satellite with nothing to attach; stamping the
// device alone would leave the pool invisible.
func TestPhysicalStorageCreateDevicePool(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
		_ = backend.PhysicalDevices().Create(ctx, &apiv1.PhysicalDevice{
			Name: "node-1-sdb", NodeName: "node-1",
			DevicePath: "/dev/disk/by-id/wwn-0x1", CurrentDevPath: "/dev/sdb",
			SizeBytes: 1 << 40,
		})
	})

	argv := []string{"ps", "cdp", "zfs", "node-1", "/dev/sdb", "--pool-name", "tank"}
	if got := app.Run(t.Context(), argv); got != 0 {
		t.Fatalf("create-device-pool exit = %d (stderr: %s)", got, errBuf.String())
	}

	backend := appStore(t, app)

	device, err := backend.PhysicalDevices().Get(t.Context(), "node-1-sdb")
	if err != nil {
		t.Fatalf("get device: %v", err)
	}

	if device.AttachTo == nil || device.AttachTo.ZPoolName != "tank" {
		t.Fatalf("device attach request = %+v, want the tank zpool", device.AttachTo)
	}

	pool, err := backend.StoragePools().Get(t.Context(), "node-1", "tank")
	if err != nil {
		t.Fatalf("get pool: %v", err)
	}

	if pool.Props["StorDriver/ZPool"] != "tank" {
		t.Errorf("pool props = %v, want the ZFS backing key", pool.Props)
	}
}

// A device the satellites have not discovered is a refusal, not a pool
// registered against storage that may not exist.
func TestPhysicalStorageCreateDevicePoolUnknownDevice(t *testing.T) {
	t.Parallel()

	app, _, _ := newApp(t, nil)

	argv := []string{"ps", "cdp", "zfs", "node-1", "/dev/sdz", "--pool-name", "tank"}
	if got := app.Run(t.Context(), argv); got == 0 {
		t.Error("attaching an undiscovered device succeeded")
	}
}

// `physical-storage list` shows what the satellites discovered.
func TestPhysicalStorageList(t *testing.T) {
	t.Parallel()

	app, out, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
		_ = backend.PhysicalDevices().Create(ctx, &apiv1.PhysicalDevice{
			Name: "node-1-sdb", NodeName: "node-1",
			DevicePath: "/dev/disk/by-id/wwn-0x1", SizeBytes: 1 << 40,
		})
	})

	if got := app.Run(t.Context(), []string{"ps", "l"}); got != 0 {
		t.Fatalf("list exit = %d (stderr: %s)", got, errBuf.String())
	}

	for _, want := range []string{"node-1", "wwn-0x1"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("device listing is missing %q:\n%s", want, out.String())
		}
	}
}

// TestResourceDefinitionModifyRejectsUnknownGroup: re-parenting onto a
// group that does not exist must fail, and must leave the definition
// alone.
//
// Nothing downstream catches it. The controller treats an already
// materialised definition as self-sufficient, so a typo'd group is
// accepted, stored, and only surfaces later — when a spawn from that
// definition finds no placement policy to work from.
func TestResourceDefinitionModifyRejectsUnknownGroup(t *testing.T) {
	t.Parallel()

	app, _, _ := newApp(t, func(ctx context.Context, backend store.Store) {
		_ = backend.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{Name: "old"})
		_ = backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
			Name: "pvc-x", ResourceGroupName: "old", LayerStack: []string{"DRBD", "STORAGE"},
		})
	})

	if got := app.Run(t.Context(), []string{"rd", "modify", "pvc-x", "--resource-group", "nope"}); got == 0 {
		t.Fatal("modify onto an unknown group exited 0, want a failure")
	}

	def, err := appStore(t, app).ResourceDefinitions().Get(t.Context(), "pvc-x")
	if err != nil {
		t.Fatalf("get definition: %v", err)
	}

	if def.ResourceGroupName != "old" {
		t.Errorf("resource group = %q, want it left at old", def.ResourceGroupName)
	}
}
