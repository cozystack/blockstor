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

package cli

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"

	"github.com/cozystack/blockstor/pkg/store"

	"github.com/cozystack/blockstor/internal/cli/command"
	"github.com/cozystack/blockstor/internal/cli/output"
	"github.com/cozystack/blockstor/internal/cli/table"
	"github.com/cozystack/blockstor/internal/cli/view"
)

// handlers maps a canonical command to its implementation. A command
// present in the grammar but absent here is reported as
// not-implemented rather than failing obscurely, and
// App.UnimplementedCommands lists the gap.
//
//nolint:gochecknoglobals // static dispatch table
var handlers = map[string]handler{
	"node list": listing("nodes",
		func(ctx context.Context, st store.Store) ([]apiv1.Node, error) { return st.Nodes().List(ctx) },
		func(node *apiv1.Node, flags *flagSet) bool { return matches(flags.Nodes, node.Name) },
		func(nodes []apiv1.Node, _ *runContext) *metav1.Table { return view.NodeList(nodes) },
		"State",
	),

	"storage-pool list": listing("storage pools",
		func(ctx context.Context, st store.Store) ([]apiv1.StoragePool, error) {
			return st.StoragePools().List(ctx)
		},
		func(pool *apiv1.StoragePool, flags *flagSet) bool {
			return matches(flags.Nodes, pool.NodeName) &&
				matches(splitList(flags.Values["storage-pools"]), pool.StoragePoolName)
		},
		func(pools []apiv1.StoragePool, _ *runContext) *metav1.Table { return view.StoragePoolList(pools) },
		"State",
	),

	"resource-definition list": listing("resource definitions",
		fetchDefinitions,
		func(def *apiv1.ResourceDefinition, flags *flagSet) bool { return matches(flags.Resources, def.Name) },
		func(defs []apiv1.ResourceDefinition, _ *runContext) *metav1.Table {
			return view.ResourceDefinitionList(defs)
		},
		"State",
	),

	"resource list": resourceList,

	"volume list": listing("resources",
		fetchResources,
		keepResource,
		func(resources []apiv1.Resource, _ *runContext) *metav1.Table { return view.VolumeList(resources) },
		"State",
	),

	"snapshot list": listing("snapshots",
		func(ctx context.Context, st store.Store) ([]apiv1.Snapshot, error) { return st.Snapshots().List(ctx) },
		func(snap *apiv1.Snapshot, flags *flagSet) bool { return matches(flags.Resources, snap.ResourceName) },
		func(snaps []apiv1.Snapshot, _ *runContext) *metav1.Table { return view.SnapshotList(snaps) },
		"State",
	),

	"resource-group list": listing("resource groups",
		func(ctx context.Context, st store.Store) ([]apiv1.ResourceGroup, error) {
			return st.ResourceGroups().List(ctx)
		},
		func(*apiv1.ResourceGroup, *flagSet) bool { return true },
		func(groups []apiv1.ResourceGroup, _ *runContext) *metav1.Table { return view.ResourceGroupList(groups) },
	),

	"volume-definition list": volumeDefinitionList,

	"resource-definition create": resourceDefinitionCreate,
	"resource-definition delete": resourceDefinitionDelete,

	"node create":   nodeCreate,
	"node delete":   nodeDelete,
	"node evacuate": nodeEvacuate,
	"node evict":    nodeEvacuate,
	"node restore":  nodeRestore,
	"node lost":     nodeLost,
	"node info":     nodeInfo,

	"volume-definition create":   volumeDefinitionCreate,
	"volume-definition delete":   volumeDefinitionDelete,
	"volume-definition set-size": volumeDefinitionSetSize,

	"resource create": resourceCreate,
	"resource delete": resourceDelete,

	"snapshot create": snapshotCreate,
	"snapshot delete": snapshotDelete,

	"resource-definition auto-place": resourceDefinitionAutoPlace,

	"resource-group spawn-resources": resourceGroupSpawn,
	"resource-group adjust":          resourceGroupAdjust,

	"resource-group create": resourceGroupCreate,
	"resource-group modify": resourceGroupModify,
	"resource-group delete": resourceGroupDelete,

	"resource toggle-disk": resourceToggleDisk,
	"resource activate":    resourceActivate,
	"resource deactivate":  resourceDeactivate,

	"resource list-volumes": listing("resources",
		fetchResources,
		keepResource,
		func(resources []apiv1.Resource, _ *runContext) *metav1.Table { return view.VolumeList(resources) },
		"State",
	),

	"storage-pool create": storagePoolCreate,
	"storage-pool delete": storagePoolDelete,

	"volume-group create": volumeGroupCreate,
	"volume-group list":   volumeGroupList,

	"encryption create-passphrase": encryptionCreatePassphrase,
	"encryption enter-passphrase":  encryptionEnterPassphrase,

	"resource-definition modify":     resourceDefinitionModify,
	"resource-definition clone":      resourceDefinitionClone,
	"resource-group query-size-info": resourceGroupQuerySizeInfo,

	"resource-group query-max-volume-size": resourceGroupQuerySizeInfo,

	"snapshot create-multiple":             snapshotCreateMultiple,
	"snapshot rollback":                    snapshotRollback,
	"snapshot resource restore":            snapshotRestoreResource,
	"snapshot resource-definition restore": snapshotRestoreResource,
	"snapshot volume-definition restore":   snapshotRestoreVolumeDefinition,

	"physical-storage create-device-pool": physicalStorageCreateDevicePool,

	"physical-storage list": listing("physical devices",
		func(ctx context.Context, st store.Store) ([]apiv1.PhysicalDevice, error) {
			return st.PhysicalDevices().List(ctx)
		},
		func(device *apiv1.PhysicalDevice, flags *flagSet) bool { return matches(flags.Nodes, device.NodeName) },
		func(devices []apiv1.PhysicalDevice, _ *runContext) *metav1.Table {
			return view.PhysicalDeviceList(devices)
		},
	),

	"controller version": controllerVersion,
}

// propertyNouns maps every noun that carries a property bag to its
// accessor. The three property verbs are registered from this one
// table so a noun cannot end up with, say, set-property but no
// delete-property — a gap an operator would only discover mid-runbook.
//
//nolint:gochecknoglobals // static dispatch table
var propertyNouns = map[string]propertyAccessor{
	"resource-definition": rdProps,
	"node":                nodeProps,
	"controller":          controllerProps,
	"resource":            resourceProps,
	"storage-pool":        storagePoolProps,
	"resource-group":      resourceGroupProps,
	"volume-definition":   volumeDefinitionProps,
	"volume-group":        volumeGroupProps,
}

//nolint:gochecknoinits // wires one static table into another
func init() {
	for noun, accessor := range propertyNouns {
		handlers[noun+" set-property"] = setProperty(accessor)
		handlers[noun+" list-properties"] = listProperties(accessor)
		handlers[noun+" delete-property"] = deleteProperty(accessor)

		if command.Has(noun, "drbd-options") {
			handlers[noun+" drbd-options"] = drbdOptions(accessor)
		}
	}
}

// listing builds a handler for the shared list shape: fetch, filter,
// then either the machine envelope or a rendered table. Keeping that
// shape in one place means a new listing cannot accidentally skip the
// `-m` branch or the filters.
func listing[T any](
	label string,
	fetch func(context.Context, store.Store) ([]T, error),
	keep func(*T, *flagSet) bool,
	build func([]T, *runContext) *metav1.Table,
	stateColumns ...string,
) handler {
	return func(ctx context.Context, run *runContext) error {
		items, err := fetch(ctx, run.Store)
		if err != nil {
			return fmt.Errorf("list %s: %w", label, err)
		}

		kept := make([]T, 0, len(items))

		for i := range items {
			if keep(&items[i], run.Flags) {
				kept = append(kept, items[i])
			}
		}

		kept = applyLimit(kept, run.Flags)

		if run.Flags.Machine {
			return machineOut(run, kept)
		}

		return run.render(build(kept, run), stateColumns...)
	}
}

func fetchResources(ctx context.Context, st store.Store) ([]apiv1.Resource, error) {
	return st.Resources().List(ctx) //nolint:wrapcheck // listing() adds the context
}

func fetchDefinitions(ctx context.Context, st store.Store) ([]apiv1.ResourceDefinition, error) {
	return st.ResourceDefinitions().List(ctx) //nolint:wrapcheck // listing() adds the context
}

func keepResource(res *apiv1.Resource, flags *flagSet) bool {
	return matches(flags.Nodes, res.NodeName) && matches(flags.Resources, res.Name)
}

// keepFaultyResource applies --faulty as part of the FILTER, not as a
// rendering decision. Filtering during render would leave `-m` — which
// skips the renderer — returning every replica for a command whose
// whole point is to narrow to the broken ones.
func keepFaultyResource(res *apiv1.Resource, flags *flagSet) bool {
	if !keepResource(res, flags) {
		return false
	}

	return !flags.Faulty || view.IsFaulty(res)
}

// volumeDefinitionList implements `volume-definition list` (`vd l`).
//
// Volume definitions are keyed by their parent definition, so an
// unfiltered listing walks every definition rather than silently
// showing none.
func volumeDefinitionList(ctx context.Context, run *runContext) error {
	names := run.Flags.Resources

	if len(names) == 0 {
		defs, err := fetchDefinitions(ctx, run.Store)
		if err != nil {
			return fmt.Errorf("list resource definitions: %w", err)
		}

		for i := range defs {
			names = append(names, defs[i].Name)
		}
	}

	tbl := view.VolumeDefinitionList("", nil)
	collected := make([]apiv1.VolumeDefinition, 0)

	for _, name := range names {
		vds, err := run.Store.VolumeDefinitions().List(ctx, name)
		if err != nil {
			return fmt.Errorf("list volume definitions of %s: %w", name, err)
		}

		collected = append(collected, vds...)
		tbl.Rows = append(tbl.Rows, view.VolumeDefinitionList(name, vds).Rows...)
	}

	if run.Flags.Machine {
		return machineOut(run, collected)
	}

	return run.render(tbl, "State")
}

// resourceList implements `resource list`.
//
// It fetches the volume sizes as well as the replicas, because the
// State column renders `SyncTarget(NN%)` from them. Without the sizes
// the percentage silently disappeared during exactly the resync an
// operator is watching, while the design doc promised it and the
// colour classifier went to the trouble of stripping it.
func resourceList(ctx context.Context, run *runContext) error {
	resources, err := fetchResources(ctx, run.Store)
	if err != nil {
		return fmt.Errorf("list resources: %w", err)
	}

	kept := make([]apiv1.Resource, 0, len(resources))

	for i := range resources {
		if keepFaultyResource(&resources[i], run.Flags) {
			kept = append(kept, resources[i])
		}
	}

	kept = applyLimit(kept, run.Flags)

	if run.Flags.Machine {
		return machineOut(run, kept)
	}

	return run.render(view.ResourceList(view.ResourceListInput{
		Resources:      kept,
		VolumeSizesKib: volumeSizesFor(ctx, run, kept),
		FaultyOnly:     run.Flags.Faulty,
	}), "State", "Conns")
}

// volumeSizesFor collects the per-volume sizes of the definitions in a
// listing, keyed the way the view expects. A definition whose sizes
// cannot be read is skipped rather than failing the listing: a missing
// percentage is a cosmetic loss, an unreadable `resource list` during
// an incident is not.
func volumeSizesFor(ctx context.Context, run *runContext, resources []apiv1.Resource) map[string]map[int32]int64 {
	seen := make(map[string]struct{}, len(resources))
	sizes := make(map[string]map[int32]int64, len(resources))

	for i := range resources {
		name := resources[i].Name
		if _, done := seen[name]; done {
			continue
		}

		seen[name] = struct{}{}

		vds, err := run.Store.VolumeDefinitions().List(ctx, name)
		if err != nil {
			continue
		}

		perVolume := make(map[int32]int64, len(vds))
		for j := range vds {
			perVolume[vds[j].VolumeNumber] = vds[j].SizeKib
		}

		sizes[name] = perVolume
	}

	return sizes
}

// machineOut writes the machine-readable envelope.
func machineOut[T any](run *runContext, items []T) error {
	err := output.MachineList(run.Out, items)
	if err != nil {
		return fmt.Errorf("write machine-readable output: %w", err)
	}

	return nil
}

// applyLimit caps a listing at --limit rows; zero is unlimited. The
// value was validated at parse time, so a malformed one never reaches
// here.
func applyLimit[T any](items []T, flags *flagSet) []T {
	if flags.Limit <= 0 || flags.Limit >= len(items) {
		return items
	}

	return items[:flags.Limit]
}

// render writes a table, painting the named state columns.
func (run *runContext) render(tbl *metav1.Table, stateColumns ...string) error {
	err := table.Render(run.Out, tbl, table.Options{
		Color:        run.Color,
		StateColumns: stateColumns,
		Pastable:     run.Flags.Pastable,
	})
	if err != nil {
		return fmt.Errorf("render table: %w", err)
	}

	return nil
}
