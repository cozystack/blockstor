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
	apierrors "k8s.io/apimachinery/pkg/api/errors"

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
// The reads are per definition up to definitionsBulkCutoff, one
// listing past it. A definition that cannot be read keeps
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
	if len(names) <= definitionsBulkCutoff {
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

// definitionsBulkCutoff is where reading the definitions one at a time stops
// being the cheaper of the two reads. Below it a listing narrowed by `-r`,
// `-n` or `--limit` pays that many GETs; above it, one request for the lot.
//
// It counts the definitions the rendered rows cover, not the definitions in
// the cluster. The replica read behind those rows is whole whatever the
// filter (see resourceList), so this is the one read a narrowing makes
// cheaper, and the choice is between two costs rather than a guess about
// the cluster: up to the cutoff, that many sequential GETs; past it, one
// LIST that carries every definition, those without a replica included.
// So `-n` on a node holding more definitions than the cutoff takes the
// LIST to render its rows. That is the trade, not an oversight, and
// removing it means making the per-definition read concurrent rather than
// moving this number.
const definitionsBulkCutoff = 16

// perDefinitionCanAnswer says whether reading the definitions one at a time
// can answer what the single read could not.
//
// SelectorUnsupported (pkg/store/k8s) draws the same line one layer down, and
// for the same reason: a fallback is worth taking when the first read failed
// on its shape, not when it ran out of the budget the second read spends
// again. A cancelled context and a refusal aimed at the caller rather than at
// the request repeat identically once per definition, so the retry buys the
// same answer at N times the cost, in the command an operator is running
// because something is already wrong.
//
// A server that asked the caller to slow down is the same case as a refusal,
// by the same rule: 429 and 503 say the budget is the problem, and answering
// them with one narrow request per definition sends N requests to the server
// that just asked for fewer.
//
// A timeout is deliberately on the other side. One request covering every
// definition in the cluster is the read most likely to exceed a deadline, and
// the narrow ones after it are each small enough to land.
func perDefinitionCanAnswer(ctx context.Context, err error) bool {
	if ctx.Err() != nil {
		return false
	}

	if apierrors.IsTooManyRequests(err) || apierrors.IsServiceUnavailable(err) {
		return false
	}

	return !apierrors.IsForbidden(err) && !apierrors.IsUnauthorized(err)
}

// distinctDefinitionNames is the set of definitions a listing covers, in
// first-seen order — every replica of one definition asks the same question.
// Names are compared as naming.Key, as LINSTOR compares them.
func distinctDefinitionNames(resources []apiv1.Resource) []string {
	seen := make(map[string]struct{}, len(resources))
	names := make([]string, 0, len(resources))

	for i := range resources {
		name := resources[i].Name

		key := naming.Key(name)
		if _, dup := seen[key]; dup {
			continue
		}

		seen[key] = struct{}{}

		names = append(names, name)
	}

	return names
}
