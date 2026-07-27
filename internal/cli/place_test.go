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

	"github.com/cozystack/blockstor/internal/cli"
)

// seedCluster registers `count` nodes, each with a `data` pool that
// has room, so the placer has somewhere to put replicas.
func seedCluster(count int) func(context.Context, store.Store) {
	return func(ctx context.Context, backend store.Store) {
		for i := 1; i <= count; i++ {
			name := "node-" + string(rune('0'+i))

			_ = backend.Nodes().Create(ctx, &apiv1.Node{Name: name, Type: "SATELLITE"})
			_ = backend.StoragePools().Create(ctx, &apiv1.StoragePool{
				NodeName:        name,
				StoragePoolName: "data",
				ProviderKind:    apiv1.StoragePoolKindLVMThin,
				FreeCapacity:    1 << 30,
				TotalCapacity:   1 << 30,
			})
		}
	}
}

func replicaCount(t *testing.T, app *cli.App, rdName string) int {
	t.Helper()

	resources, err := appStore(t, app).Resources().ListByDefinition(t.Context(), rdName)
	if err != nil {
		t.Fatalf("list replicas: %v", err)
	}

	diskful := 0

	for i := range resources {
		if !containsFlag(resources[i].Flags, apiv1.ResourceFlagDiskless) {
			diskful++
		}
	}

	return diskful
}

// `resource create --auto-place N <rd>` names no nodes: the placer
// picks them. It runs the controller's own selection code, so the CLI
// and the controller cannot disagree about where a replica belongs.
func TestResourceCreateAutoPlace(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
		seedCluster(3)(ctx, backend)
		_ = backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-x"})
		_ = backend.VolumeDefinitions().Create(ctx, "pvc-x", &apiv1.VolumeDefinition{SizeKib: 1024})
	})

	argv := []string{"r", "c", "--auto-place", "2", "--storage-pool", "data", "pvc-x"}
	if got := app.Run(t.Context(), argv); got != 0 {
		t.Fatalf("auto-place exit = %d (stderr: %s)", got, errBuf.String())
	}

	if got := replicaCount(t, app, "pvc-x"); got != 2 {
		t.Errorf("placed %d diskful replicas, want 2", got)
	}
}

// `--auto-place +1` is a delta on what already exists, not a target.
// Counting must skip diskless witnesses: on a resource with two
// diskful replicas and a tiebreaker, `+1` has to place a third diskful
// one, not conclude the target is already met.
func TestResourceCreateAutoPlaceDelta(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
		seedCluster(3)(ctx, backend)
		_ = backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-x"})
		_ = backend.VolumeDefinitions().Create(ctx, "pvc-x", &apiv1.VolumeDefinition{SizeKib: 1024})
		_ = backend.Resources().Create(ctx, &apiv1.Resource{Name: "pvc-x", NodeName: "node-1"})
		_ = backend.Resources().Create(ctx, &apiv1.Resource{
			Name: "pvc-x", NodeName: "node-3",
			Flags: []string{apiv1.ResourceFlagDiskless, apiv1.ResourceFlagTieBreaker},
		})
	})

	argv := []string{"r", "c", "--auto-place", "+1", "--storage-pool", "data", "pvc-x"}
	if got := app.Run(t.Context(), argv); got != 0 {
		t.Fatalf("auto-place +1 exit = %d (stderr: %s)", got, errBuf.String())
	}

	if got := replicaCount(t, app, "pvc-x"); got != 2 {
		t.Errorf("after +1 there are %d diskful replicas, want 2", got)
	}
}

// An over-committed request is deferred best-effort placement, not a
// failure: the rebalance reconciler tops the resource up when capacity
// appears, and failing here would break every runbook that provisions
// ahead of the hardware. The shortfall is reported on stderr so the
// operator still learns about it.
func TestAutoPlaceShortfallIsReportedNotFailed(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
		seedCluster(1)(ctx, backend)
		_ = backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{Name: "pvc-x"})
		_ = backend.VolumeDefinitions().Create(ctx, "pvc-x", &apiv1.VolumeDefinition{SizeKib: 1024})
	})

	argv := []string{"r", "c", "--auto-place", "3", "--storage-pool", "data", "pvc-x"}
	if got := app.Run(t.Context(), argv); got != 0 {
		t.Fatalf("over-committed auto-place exit = %d, want 0 (stderr: %s)", got, errBuf.String())
	}

	if !strings.Contains(errBuf.String(), "deferred") {
		t.Errorf("the shortfall was not reported:\n%s", errBuf.String())
	}
}

// `rg spawn-resources <rg> <rd> <size>` is create-plus-place: leaving
// the resource unplaced would make `resource list` empty until someone
// ran a separate placement.
func TestResourceGroupSpawn(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
		seedCluster(2)(ctx, backend)
		_ = backend.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
			Name:         "grp",
			SelectFilter: apiv1.AutoSelectFilter{PlaceCount: 2, StoragePool: "data"},
		})
	})

	if got := app.Run(t.Context(), []string{"rg", "spawn-resources", "grp", "pvc-x", "32M"}); got != 0 {
		t.Fatalf("spawn exit = %d (stderr: %s)", got, errBuf.String())
	}

	backend := appStore(t, app)

	def, err := backend.ResourceDefinitions().Get(t.Context(), "pvc-x")
	if err != nil {
		t.Fatalf("get definition: %v", err)
	}

	if def.ResourceGroupName != "grp" {
		t.Errorf("spawned definition group = %q, want grp", def.ResourceGroupName)
	}

	vds, err := backend.VolumeDefinitions().List(t.Context(), "pvc-x")
	if err != nil {
		t.Fatalf("list volume definitions: %v", err)
	}

	// `32M` must spawn a 32 MiB volume, not a 32 KiB one.
	if len(vds) != 1 || vds[0].SizeKib != 32768 {
		t.Fatalf("volume definitions = %+v, want one of 32768 KiB", vds)
	}

	if got := replicaCount(t, app, "pvc-x"); got != 2 {
		t.Errorf("spawn placed %d replicas, want the group's place count of 2", got)
	}
}

// Spawning from an over-committed group still succeeds — same
// deferred-placement contract as auto-place.
func TestResourceGroupSpawnOverCommitted(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
		seedCluster(1)(ctx, backend)
		_ = backend.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
			Name:         "grp",
			SelectFilter: apiv1.AutoSelectFilter{PlaceCount: 3, StoragePool: "data"},
		})
	})

	if got := app.Run(t.Context(), []string{"rg", "spawn", "grp", "pvc-x", "32M"}); got != 0 {
		t.Errorf("spawn on an over-committed group exit = %d, want 0 (stderr: %s)", got, errBuf.String())
	}
}

// `rg adjust` re-runs placement for the group's definitions, which is
// how a place-count change reaches resources that already exist.
func TestResourceGroupAdjust(t *testing.T) {
	t.Parallel()

	app, _, errBuf := newApp(t, func(ctx context.Context, backend store.Store) {
		seedCluster(3)(ctx, backend)
		_ = backend.ResourceGroups().Create(ctx, &apiv1.ResourceGroup{
			Name:         "grp",
			SelectFilter: apiv1.AutoSelectFilter{PlaceCount: 3, StoragePool: "data"},
		})
		_ = backend.ResourceDefinitions().Create(ctx, &apiv1.ResourceDefinition{
			Name: "pvc-x", ResourceGroupName: "grp",
		})
		_ = backend.VolumeDefinitions().Create(ctx, "pvc-x", &apiv1.VolumeDefinition{SizeKib: 1024})
		_ = backend.Resources().Create(ctx, &apiv1.Resource{Name: "pvc-x", NodeName: "node-1"})
	})

	if got := app.Run(t.Context(), []string{"rg", "adjust", "grp"}); got != 0 {
		t.Fatalf("adjust exit = %d (stderr: %s)", got, errBuf.String())
	}

	if got := replicaCount(t, app, "pvc-x"); got != 3 {
		t.Errorf("adjust left %d diskful replicas, want the group's 3", got)
	}
}
