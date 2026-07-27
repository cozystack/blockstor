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
		func(pool *apiv1.StoragePool, flags *flagSet) bool { return matches(flags.Nodes, pool.NodeName) },
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

	"resource list": listing("resources",
		fetchResources,
		keepResource,
		func(resources []apiv1.Resource, run *runContext) *metav1.Table {
			return view.ResourceList(view.ResourceListInput{Resources: resources, FaultyOnly: run.Flags.Faulty})
		},
		"State", "Conns",
	),

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

// machineOut writes the machine-readable envelope.
func machineOut[T any](run *runContext, items []T) error {
	err := output.MachineList(run.Out, items)
	if err != nil {
		return fmt.Errorf("write machine-readable output: %w", err)
	}

	return nil
}

// render writes a table, painting the named state columns.
func (run *runContext) render(tbl *metav1.Table, stateColumns ...string) error {
	err := table.Render(run.Out, tbl, table.Options{
		Color:        run.Color,
		StateColumns: stateColumns,
	})
	if err != nil {
		return fmt.Errorf("render table: %w", err)
	}

	return nil
}
