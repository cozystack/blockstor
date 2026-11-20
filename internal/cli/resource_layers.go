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
//
// It returns each definition's own properties, keyed by naming.Key,
// from the same read: the State column shows a SkipDisk set on the
// definition, and reading the definitions a second time for it would
// double the round trips of the listing.
func withLayerStacks(ctx context.Context, run *runContext, resources []apiv1.Resource) map[string]map[string]string {
	if len(resources) == 0 {
		return nil
	}

	defs := definitionsFor(ctx, run, distinctDefinitionNames(resources))

	props := make(map[string]map[string]string, len(defs))
	for key := range defs {
		props[key] = defs[key].props
	}

	for i := range resources {
		stack := defs[naming.Key(resources[i].Name)].stack
		if len(stack) == 0 {
			continue
		}

		// The volumes are re-stamped in place, and a store may hand back
		// a slice it still holds.
		resources[i].Volumes = slices.Clone(resources[i].Volumes)

		apiv1.ApplyLayerStack(&resources[i], stack)
		apiv1.ApplyLayerStackToVolumes(resources[i].Volumes, stack)
	}

	return props
}

// definitionFacts is what a listing reads off a replica's definition.
type definitionFacts struct {
	stack []string
	props map[string]string
}

func factsOf(def *apiv1.ResourceDefinition) definitionFacts {
	return definitionFacts{stack: def.LayerStack, props: def.Props}
}

// definitionsFor reads the layer stack and properties of each named
// definition, keyed by naming.Key.
func definitionsFor(ctx context.Context, run *runContext, names []string) map[string]definitionFacts {
	if len(names) <= volumeSizesBulkCutoff {
		return definitionsPerName(ctx, run, names)
	}

	defs, err := definitionsInOneRequest(ctx, run)
	if err == nil {
		return defs
	}

	if !perDefinitionCanAnswer(ctx, err) {
		warnLayerStacksUnavailable(run, err)

		return nil
	}

	return definitionsPerName(ctx, run, names)
}

func definitionsPerName(ctx context.Context, run *runContext, names []string) map[string]definitionFacts {
	out := make(map[string]definitionFacts, len(names))

	var firstErr error

	for _, name := range names {
		def, err := run.Store.ResourceDefinitions().Get(ctx, name)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}

			continue
		}

		out[naming.Key(def.Name)] = factsOf(&def)
	}

	if len(out) == 0 && firstErr != nil {
		warnLayerStacksUnavailable(run, firstErr)
	}

	return out
}

func definitionsInOneRequest(ctx context.Context, run *runContext) (map[string]definitionFacts, error) {
	defs, err := fetchDefinitions(ctx, run.Store)
	if err != nil {
		return nil, errors.Wrap(err, "read every resource definition")
	}

	out := make(map[string]definitionFacts, len(defs))
	for i := range defs {
		out[naming.Key(defs[i].Name)] = factsOf(&defs[i])
	}

	return out, nil
}

// warnLayerStacksUnavailable says why every replica reads as DRBD. On
// stderr, so the table on stdout stays the contract `awk -F'|'` parses.
func warnLayerStacksUnavailable(run *runContext, err error) {
	if run.Err == nil {
		return
	}

	fmt.Fprintf(run.Err, "warning: layer stacks unavailable, every replica is shown as DRBD: %v\n", err)
}
