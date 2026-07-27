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

	"github.com/cozystack/blockstor/internal/cli/output"
	"github.com/cozystack/blockstor/internal/cli/table"
	"github.com/cozystack/blockstor/internal/cli/view"
)

// handlers maps a canonical command to its implementation. A command
// registered in the grammar but absent here is reported as
// not-implemented rather than failing obscurely, and App's
// UnimplementedCommands lists the gap.
//
//nolint:gochecknoglobals // static dispatch table
var handlers = map[string]handler{
	"resource list": resourceList,
	"node list":     nodeList,
}

// resourceList implements `resource list` (`r l`).
func resourceList(ctx context.Context, run *runContext) error {
	resources, err := run.Store.Resources().List(ctx)
	if err != nil {
		return fmt.Errorf("list resources: %w", err)
	}

	kept := make([]apiv1.Resource, 0, len(resources))

	for i := range resources {
		if !matches(run.Flags.Nodes, resources[i].NodeName) {
			continue
		}

		if !matches(run.Flags.Resources, resources[i].Name) {
			continue
		}

		kept = append(kept, resources[i])
	}

	if run.Flags.Machine {
		err = output.MachineList(run.Out, kept)
		if err != nil {
			return fmt.Errorf("write machine-readable output: %w", err)
		}

		return nil
	}

	return run.render(view.ResourceList(view.ResourceListInput{
		Resources:  kept,
		FaultyOnly: run.Flags.Faulty,
	}), "State", "Conns")
}

// nodeList implements `node list` (`n l`).
func nodeList(ctx context.Context, run *runContext) error {
	nodes, err := run.Store.Nodes().List(ctx)
	if err != nil {
		return fmt.Errorf("list nodes: %w", err)
	}

	kept := make([]apiv1.Node, 0, len(nodes))

	for i := range nodes {
		if matches(run.Flags.Nodes, nodes[i].Name) {
			kept = append(kept, nodes[i])
		}
	}

	if run.Flags.Machine {
		err = output.MachineList(run.Out, kept)
		if err != nil {
			return fmt.Errorf("write machine-readable output: %w", err)
		}

		return nil
	}

	return run.render(view.NodeList(kept), "State")
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
