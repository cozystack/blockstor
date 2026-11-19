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
	"slices"

	"github.com/cockroachdb/errors"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/naming"
)

// withLayerStacks stamps each replica with its definition's layer stack.
//
// A replica's CR does not carry the stack, so the store hands every
// replica back as DRBD over STORAGE. The State column reads the top
// layer to decide whether a volume has a DRBD disk state to report at
// all, and with the default stack a local STORAGE volume, or LUKS over
// STORAGE, read as Unknown forever: nothing ever reports a disk state
// for a layer that is not there. The REST view re-stamps the same way,
// so `linstor r l` and this CLI read one shape.
//
// The reads follow the volume-size reads: per definition up to the
// cutoff, one listing past it. A definition that cannot be read keeps
// the default stack, which is how the listing looked before, rather
// than failing it.
func withLayerStacks(ctx context.Context, run *runContext, resources []apiv1.Resource) {
	if len(resources) == 0 {
		return
	}

	stacks := layerStacksFor(ctx, run, distinctDefinitionNames(resources))

	for i := range resources {
		stack := stacks[naming.Key(resources[i].Name)]
		if len(stack) == 0 {
			continue
		}

		// The volumes are re-stamped in place, and a store may hand back
		// a slice it still holds.
		resources[i].Volumes = slices.Clone(resources[i].Volumes)

		apiv1.ApplyLayerStack(&resources[i], stack)
		apiv1.ApplyLayerStackToVolumes(resources[i].Volumes, stack)
	}
}

// layerStacksFor reads the layer stack of each named definition, keyed
// by naming.Key.
func layerStacksFor(ctx context.Context, run *runContext, names []string) map[string][]string {
	if len(names) <= volumeSizesBulkCutoff {
		return layerStacksPerDefinition(ctx, run, names)
	}

	stacks, err := layerStacksInOneRequest(ctx, run)
	if err == nil {
		return stacks
	}

	if !perDefinitionCanAnswer(ctx, err) {
		warnLayerStacksUnavailable(run, err)

		return nil
	}

	return layerStacksPerDefinition(ctx, run, names)
}

func layerStacksPerDefinition(ctx context.Context, run *runContext, names []string) map[string][]string {
	stacks := make(map[string][]string, len(names))

	var firstErr error

	for _, name := range names {
		def, err := run.Store.ResourceDefinitions().Get(ctx, name)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}

			continue
		}

		stacks[naming.Key(def.Name)] = def.LayerStack
	}

	if len(stacks) == 0 && firstErr != nil {
		warnLayerStacksUnavailable(run, firstErr)
	}

	return stacks
}

func layerStacksInOneRequest(ctx context.Context, run *runContext) (map[string][]string, error) {
	defs, err := fetchDefinitions(ctx, run.Store)
	if err != nil {
		return nil, errors.Wrap(err, "read every resource definition")
	}

	stacks := make(map[string][]string, len(defs))
	for i := range defs {
		stacks[naming.Key(defs[i].Name)] = defs[i].LayerStack
	}

	return stacks, nil
}

// warnLayerStacksUnavailable says why every replica reads as DRBD. On
// stderr, so the table on stdout stays the contract `awk -F'|'` parses.
func warnLayerStacksUnavailable(run *runContext, err error) {
	if run.Err == nil {
		return
	}

	fmt.Fprintf(run.Err, "warning: layer stacks unavailable, every replica is shown as DRBD: %v\n", err)
}
