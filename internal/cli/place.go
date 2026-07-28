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
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/placer"
	"github.com/cozystack/blockstor/pkg/store"

	"github.com/cozystack/blockstor/internal/cli/command"
)

// errNotEnoughNodes is the shortfall an explicit placement request
// fails on.
var errNotEnoughNodes = errors.New("not enough candidate storage pools")

// autoPlace runs the placer for one definition.
//
// The placer is the controller's own selection code, driven from the
// same objects — reimplementing the choice client-side would give two
// answers to "where should this replica go?" that drift apart the
// moment either changes.
//
// `bestEffort` decides what a shortfall means, and the two answers are NOT
// interchangeable. An explicit placement request FAILS when it cannot
// seat every replica: the operator asked for N and must find out they
// did not get N. A group spawn or rebalance succeeds and reports,
// because there the group's policy is a target the rebalance
// reconciler keeps working towards as capacity appears — failing would
// break provisioning ahead of the hardware.
func autoPlace(
	ctx context.Context, run *runContext, rdName string, filter *apiv1.AutoSelectFilter, bestEffort bool,
) error {
	err := resolveAdditionalPlaceCount(ctx, run.Store, rdName, filter)
	if err != nil {
		return err
	}

	if filter.PlaceCount <= 0 {
		return nil
	}

	placed, want, err := placer.New(run.Store).Place(ctx, rdName, filter)
	if err != nil {
		return fmt.Errorf("autoplace %s: %w", rdName, err)
	}

	if placed >= want {
		return nil
	}

	if !bestEffort {
		return fmt.Errorf("%w: placed %d of %d replica(s) for %s", errNotEnoughNodes, placed, want, rdName)
	}

	_, err = fmt.Fprintf(run.Err,
		"autoplace deferred: placed %d of %d replica(s) for %s; no candidate storage pool for the rest\n",
		placed, want, rdName)
	if err != nil {
		return fmt.Errorf("report placement shortfall: %w", err)
	}

	return nil
}

// resolveAdditionalPlaceCount turns the `--auto-place +N` delta into an
// absolute target: the replicas that already exist, plus N.
//
// Only diskful replicas count, matching the placer's own tally — a
// diskless witness must not suppress the increment, or `+1` on a
// 2-diskful-plus-tiebreaker resource would place nothing.
func resolveAdditionalPlaceCount(
	ctx context.Context, backend store.Store, rdName string, filter *apiv1.AutoSelectFilter,
) error {
	if filter.AdditionalPlaceCount <= 0 {
		return nil
	}

	existing, err := backend.Resources().ListByDefinition(ctx, rdName)
	if err != nil {
		return fmt.Errorf("list replicas of %s: %w", rdName, err)
	}

	diskful := 0

	for i := range existing {
		if !slices.Contains(existing[i].Flags, apiv1.ResourceFlagDiskless) {
			diskful++
		}
	}

	filter.PlaceCount = apiv1.LaxInt32(diskful) + filter.AdditionalPlaceCount
	filter.AdditionalPlaceCount = 0

	return nil
}

// placementFilter builds the selection constraints from the flags, and
// falls back to the definition's resource group for anything the
// operator did not override.
func placementFilter(ctx context.Context, run *runContext, rdName string) (*apiv1.AutoSelectFilter, error) {
	filter := &apiv1.AutoSelectFilter{}

	err := applyPlaceCountFlags(run.Flags, filter)
	if err != nil {
		return nil, err
	}

	if pool := run.Flags.Values["storage-pool"]; pool != "" {
		filter.StoragePool = pool
	}

	if layers := run.Flags.Values["layer-list"]; layers != "" {
		filter.LayerStack = splitList(layers)
	}

	group, err := groupOf(ctx, run, rdName)
	if err != nil {
		return nil, err
	}

	if group == nil {
		return filter, nil
	}

	if filter.PlaceCount == 0 && filter.AdditionalPlaceCount == 0 {
		filter.PlaceCount = group.SelectFilter.PlaceCount
	}

	if filter.StoragePool == "" {
		filter.StoragePool = group.SelectFilter.StoragePool
	}

	if len(filter.LayerStack) == 0 {
		filter.LayerStack = group.SelectFilter.LayerStack
	}

	return filter, nil
}

// applyPlaceCountFlags reads --place-count and --auto-place, including
// the `+N` delta spelling.
func applyPlaceCountFlags(flags *flagSet, filter *apiv1.AutoSelectFilter) error {
	for _, name := range []string{"place-count", "auto-place"} {
		raw := flags.Values[name]
		if raw == "" {
			continue
		}

		delta := strings.HasPrefix(raw, "+")

		count, err := parseInt32(strings.TrimPrefix(raw, "+"), "--"+name)
		if err != nil {
			return err
		}

		if delta {
			filter.AdditionalPlaceCount = apiv1.LaxInt32(count)
		} else {
			filter.PlaceCount = apiv1.LaxInt32(count)
		}
	}

	return nil
}

// groupOf returns the definition's resource group, or nil when it has
// none.
func groupOf(ctx context.Context, run *runContext, rdName string) (*apiv1.ResourceGroup, error) {
	def, err := run.Store.ResourceDefinitions().Get(ctx, rdName)
	if err != nil {
		return nil, fmt.Errorf("get resource definition %s: %w", rdName, err)
	}

	if def.ResourceGroupName == "" {
		return nil, nil //nolint:nilnil // "no group" is not an error
	}

	group, err := run.Store.ResourceGroups().Get(ctx, def.ResourceGroupName)
	if isNotFound(err) {
		return nil, nil //nolint:nilnil // a dangling group name is not fatal to placement
	}

	if err != nil {
		return nil, fmt.Errorf("get resource group %s: %w", def.ResourceGroupName, err)
	}

	return &group, nil
}

// resourceDefinitionAutoPlace implements `resource-definition
// auto-place <rd>`.
func resourceDefinitionAutoPlace(ctx context.Context, run *runContext) error {
	if len(run.Flags.Positionals) < 1 {
		return fmt.Errorf("%w: auto-place needs a resource definition", command.ErrUsage)
	}

	rdName := run.Flags.Positionals[0]

	filter, err := placementFilter(ctx, run, rdName)
	if err != nil {
		return err
	}

	return autoPlace(ctx, run, rdName, filter, false)
}

// resourceGroupSpawn implements `resource-group spawn-resources <rg>
// <rd> <size>...`.
//
// Spawn is create-plus-place: the definition, one volume definition
// per size, then placement per the group's policy. Stopping at the
// definition would leave `resource list` empty until someone ran a
// separate placement, which is not what the CSI flow or any runbook
// expects.
func resourceGroupSpawn(ctx context.Context, run *runContext) error {
	const wantArgs = 2

	if len(run.Flags.Positionals) < wantArgs {
		return fmt.Errorf("%w: spawn-resources needs a group and a definition name", command.ErrUsage)
	}

	rgName, rdName := run.Flags.Positionals[0], run.Flags.Positionals[1]

	group, err := run.Store.ResourceGroups().Get(ctx, rgName)
	if err != nil {
		return fmt.Errorf("get resource group %s: %w", rgName, err)
	}

	err = spawnDefinition(ctx, run, &group, rdName)
	if err != nil {
		return err
	}

	for _, size := range run.Flags.Positionals[wantArgs:] {
		sizeKib, sizeErr := ParseSize(size)
		if sizeErr != nil {
			return sizeErr
		}

		sizeErr = checkVolumeSize(sizeKib)
		if sizeErr != nil {
			return sizeErr
		}

		_, sizeErr = run.Store.VolumeDefinitions().CreateAutoNumbered(ctx, rdName,
			&apiv1.VolumeDefinition{SizeKib: sizeKib})
		if sizeErr != nil {
			return fmt.Errorf("create volume definition on %s: %w", rdName, sizeErr)
		}
	}

	filter, err := placementFilter(ctx, run, rdName)
	if err != nil {
		return err
	}

	return autoPlace(ctx, run, rdName, filter, true)
}

// spawnDefinition creates the definition, carrying over the group's
// properties and pinned layer stack so the spawned resource is
// composed the way the group says.
func spawnDefinition(ctx context.Context, run *runContext, group *apiv1.ResourceGroup, rdName string) error {
	def := &apiv1.ResourceDefinition{
		Name:              rdName,
		ResourceGroupName: group.Name,
		LayerStack:        group.SelectFilter.LayerStack,
	}

	if len(group.Props) > 0 {
		def.Props = maps.Clone(group.Props)
	}

	err := run.Store.ResourceDefinitions().Create(ctx, def)
	if err != nil {
		return fmt.Errorf("create resource definition %s: %w", rdName, err)
	}

	return nil
}

// resourceGroupAdjust implements `resource-group adjust [<rg>]`:
// re-run placement for every definition the group owns, which is how
// an operator applies a place-count change to existing resources.
func resourceGroupAdjust(ctx context.Context, run *runContext) error {
	wanted := ""
	if len(run.Flags.Positionals) > 0 {
		wanted = run.Flags.Positionals[0]
	}

	defs, err := fetchDefinitions(ctx, run.Store)
	if err != nil {
		return fmt.Errorf("list resource definitions: %w", err)
	}

	for i := range defs {
		if defs[i].ResourceGroupName == "" {
			continue
		}

		if wanted != "" && !strings.EqualFold(wanted, defs[i].ResourceGroupName) {
			continue
		}

		filter, filterErr := placementFilter(ctx, run, defs[i].Name)
		if filterErr != nil {
			return filterErr
		}

		filterErr = autoPlace(ctx, run, defs[i].Name, filter, true)
		if filterErr != nil {
			return filterErr
		}
	}

	return nil
}
