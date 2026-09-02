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

// The StorDriver keys name where a pool's data physically lives. Rewriting
// one does not migrate anything: the pool keeps its name and its replicas
// keep reporting UpToDate while the driver is pointed somewhere else, or at
// something that does not exist.
func TestStoragePoolPropsAreImmutable(t *testing.T) {
	t.Parallel()

	seed := func(ctx context.Context, backend store.Store) {
		_ = backend.Nodes().Create(ctx, &apiv1.Node{Name: "node-1"})
		_ = backend.StoragePools().Create(ctx, &apiv1.StoragePool{
			NodeName:        "node-1",
			StoragePoolName: "data",
			ProviderKind:    apiv1.StoragePoolKindLVM,
			Props:           map[string]string{"StorDriver/LvmVg": "vg-real"},
		})
	}

	for _, argv := range [][]string{
		{"sp", "sp", "node-1", "data", "StorDriver/LvmVg", "hijacked"},
		{"sp", "sp", "node-1", "data", "StorDriver/LvmVg", ""},
		{"sp", "dp", "node-1", "data", "StorDriver/LvmVg"},
	} {
		app, _, _ := newApp(t, seed)

		if got := app.Run(t.Context(), argv); got == 0 {
			t.Errorf("%v was accepted; the backing identity must be immutable", argv)
		}

		pool, err := appStore(t, app).StoragePools().Get(t.Context(), "node-1", "data")
		if err != nil {
			t.Fatalf("get pool: %v", err)
		}

		if pool.Props["StorDriver/LvmVg"] != "vg-real" {
			t.Errorf("%v changed the backing store: %v", argv, pool.Props)
		}
	}
}

// A migration onto its own source deletes the replica it was asked to move:
// the reconciler resolves both ends to one object and prunes the source once
// the destination is UpToDate, which it already is.
func TestMigrateDiskRefusesSelfMigration(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
		_ = backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-x"})
		_ = backend.Resources().Create(ctx, &apiv1.Resource{Name: "pvc-x", NodeName: "node-1"})
	})

	argv := []string{"r", "td", "node-1", "pvc-x", "--migrate-from", "node-1"}
	if got := app.Run(t.Context(), argv); got == 0 {
		t.Fatalf("self-migration was accepted (stderr: %s)", errBuf.String())
	}

	res, err := appStore(t, app).Resources().Get(t.Context(), "pvc-x", "node-1")
	if err != nil {
		t.Fatalf("get resource: %v", err)
	}

	if res.Props["BlockstorMigratingFrom"] != "" {
		t.Errorf("the refused migration still stamped the source: %v", res.Props)
	}
}

// A restore onto a node that does not hold the snapshot creates a replica
// with nothing behind it and reports success.
func TestRestoreRefusesNodesWithoutTheSnapshot(t *testing.T) {
	t.Parallel()

	// The fixture has to be complete enough that the restore SUCCEEDS when
	// the named node holds the snapshot. Seeded any thinner, the command
	// fails for a missing storage pool whether the guard runs or not, and
	// the refusal proves nothing.
	restore := func(node string) (int, string) {
		app, _, errBuf := newApp(t, seedSnapshotSource)

		argv := []string{
			"s", "resource", "restore",
			"--from-resource", "pvc-x", "--from-snapshot", "snap-1",
			"--to-resource", "pvc-y", "--nodes", node,
		}

		return app.Run(t.Context(), argv), errBuf.String()
	}

	// Positive control: node-1 holds the snapshot, so everything else the
	// restore needs is in place.
	if got, stderr := restore("node-1"); got != 0 {
		t.Fatalf("restore onto a node that holds the snapshot exit = %d, so this fixture "+
			"cannot show what the guard refuses (stderr: %s)", got, stderr)
	}

	got, stderr := restore("node-9")
	if got == 0 {
		t.Fatal("a restore onto a node without the snapshot was accepted")
	}

	// And refused for the right reason: a replica placed there has no data
	// behind it, the satellite finds no snapshot to receive, and the
	// command otherwise reports success over an empty volume.
	if !strings.Contains(stderr, "does not hold this snapshot") {
		t.Errorf("refused for an unrelated reason: %s", stderr)
	}
}

// A value flag must not swallow the flag that follows it: the result reads as
// a filter that worked and a flag that silently never applied.
func TestValueFlagDoesNotSwallowTheNextFlag(t *testing.T) {
	t.Parallel()

	app, _, _ := newApp(t, nil)

	if got := app.Run(t.Context(), []string{"r", "l", "-n", "--faulty"}); got == 0 {
		t.Fatal("`-n --faulty` was accepted; --faulty was swallowed as the node name")
	}
}

// A pool on a node the cluster does not have persists looking real, with no
// capacity behind it and nothing to reconcile it.
func TestStoragePoolCreateRefusesUnknownNode(t *testing.T) {
	t.Parallel()

	app, _, _ := newApp(t, nil)

	if got := app.Run(t.Context(), []string{"sp", "c", "lvm", "ghost-node", "data", "vg0"}); got == 0 {
		t.Fatal("a pool on an unknown node was accepted")
	}
}

// The satellite publishes what it enumerated; a backing store it does not
// advertise is one the pool can never use.
func TestStoragePoolCreateRefusesUnadvertisedBacking(t *testing.T) {
	t.Parallel()

	app, _, _ := newApp(t, func(ctx context.Context, backend store.Store) {
		_ = backend.Nodes().Create(ctx, &apiv1.Node{
			Name:  "node-1",
			Props: map[string]string{"Aux/DiscoveredVGs": "vg-real"},
		})
	})

	argv := []string{"sp", "c", "lvm", "node-1", "data", "vg-nonexistent"}
	if got := app.Run(t.Context(), argv); got == 0 {
		t.Fatal("a pool naming an unadvertised VG was accepted")
	}
}

// Cloning onto an occupied name used to take the internal snapshot first and
// fail afterwards, leaving the snapshot behind.
func TestCloneRefusesOccupiedTargetBeforeWriting(t *testing.T) {
	t.Parallel()

	// A source with replicas, so a clone onto a free name actually gets as
	// far as taking the internal snapshot. Seeded without them the clone
	// fails earlier whether the guard runs or not, and the assertion that
	// no snapshot was left behind holds trivially.
	seed := func(ctx context.Context, backend store.Store) {
		_ = backend.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{Name: "grp"})
		_ = backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
			Name: "pvc-src", ResourceGroupName: "grp", LayerStack: []string{"DRBD", "STORAGE"},
		})
		_ = backend.VolumeDefinitions().Create(ctx, "pvc-src", &apiv1.VolumeDefinition{
			VolumeNumber: 0, SizeKib: 1 << 20,
		})

		for _, node := range []string{"node-1", "node-2"} {
			_ = backend.Resources().Create(ctx, &apiv1.Resource{
				Name: "pvc-src", NodeName: node,
				Props: map[string]string{"StorPoolName": "data"},
			})
		}

		_ = backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-dst"})
	}

	// Positive control: the same clone onto a free name succeeds, so
	// everything the refusal case needs is in place.
	control, _, controlErr := newApp(t, seed)
	if got := control.Run(t.Context(), []string{"rd", "clone", "pvc-src", "pvc-free"}); got != 0 {
		t.Fatalf("clone onto a free name exit = %d, so this fixture cannot show what the "+
			"guard refuses (stderr: %s)", got, controlErr.String())
	}

	app, _, _ := newApp(t, seed)

	if got := app.Run(t.Context(), []string{"rd", "clone", "pvc-src", "pvc-dst"}); got == 0 {
		t.Fatal("a clone onto an existing definition was accepted")
	}

	snaps, err := appStore(t, app).Snapshots().List(t.Context())
	if err != nil {
		t.Fatalf("list snapshots: %v", err)
	}

	if len(snaps) != 0 {
		t.Errorf("the refused clone left an internal snapshot behind: %d", len(snaps))
	}
}
