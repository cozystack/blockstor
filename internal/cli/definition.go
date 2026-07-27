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
	"sort"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"

	"github.com/cozystack/blockstor/internal/cli/command"
	"github.com/cozystack/blockstor/internal/cli/view"
)

var errSourceBeingDeleted = errors.New("the source is being deleted; its backing data is being torn down")

// resourceDefinitionModify implements `resource-definition modify
// <rd>`. Only the fields the operator named are touched — the verb is
// a partial update, so re-parenting a definition must not silently
// blank its layer stack.
func resourceDefinitionModify(ctx context.Context, run *runContext) error {
	if len(run.Flags.Positionals) < 1 {
		return fmt.Errorf("%w: modify needs a resource-definition name", command.ErrUsage)
	}

	name := run.Flags.Positionals[0]

	def, err := run.Store.ResourceDefinitions().Get(ctx, name)
	if err != nil {
		return fmt.Errorf("get resource definition %s: %w", name, err)
	}

	changed := false

	if group := run.Flags.Values["resource-group"]; group != "" {
		def.ResourceGroupName = group
		changed = true
	}

	if layers := run.Flags.Values["layer-list"]; layers != "" {
		def.LayerStack = splitList(layers)
		changed = true
	}

	if !changed {
		return fmt.Errorf("%w: modify needs something to change", command.ErrUsage)
	}

	err = run.Store.ResourceDefinitions().Update(ctx, &def)
	if err != nil {
		return fmt.Errorf("update resource definition %s: %w", name, err)
	}

	return nil
}

// cloneSnapshotName is the internal snapshot a data-plane clone is
// taken from. It is deterministic so an interrupted clone that gets
// retried reuses the same snapshot instead of accreting one per
// attempt, and it stays visible in `snapshot list` — a cloned volume
// remains dependent on its origin snapshot, so reaping it would break
// the clone.
func cloneSnapshotName(target string) string {
	return "clone-" + target
}

// resourceDefinitionClone implements `resource-definition clone <src>
// <target>`.
//
// The clone is an internal snapshot plus a restore from it, which is
// how the data actually reaches the target. Cloning a definition that
// is being torn down is refused: the source's data is going away
// underneath the restore.
func resourceDefinitionClone(ctx context.Context, run *runContext) error {
	const wantArgs = 2

	if len(run.Flags.Positionals) < wantArgs {
		return fmt.Errorf("%w: clone needs a source and a target name", command.ErrUsage)
	}

	srcName, target := run.Flags.Positionals[0], run.Flags.Positionals[1]

	src, err := run.Store.ResourceDefinitions().Get(ctx, srcName)
	if err != nil {
		return fmt.Errorf("get resource definition %s: %w", srcName, err)
	}

	for _, flag := range src.Flags {
		if flag == apiv1.ResourceFlagDelete || flag == apiv1.ResourceFlagDeleting {
			return fmt.Errorf("clone of %s refused: %w", srcName, errSourceBeingDeleted)
		}
	}

	snapName := cloneSnapshotName(target)

	err = ensureCloneSnapshot(ctx, run, &src, snapName)
	if err != nil {
		return err
	}

	// The restore runs through the same code path an operator's
	// explicit `snapshot resource restore` takes, so a clone and a
	// restore cannot diverge in what they produce.
	run.Flags.Values["from-resource"] = srcName
	run.Flags.Values["from-snapshot"] = snapName
	run.Flags.Values["to-resource"] = target

	return snapshotRestoreResource(ctx, run)
}

// ensureCloneSnapshot takes the internal snapshot, tolerating one that
// a previous attempt already left behind.
func ensureCloneSnapshot(ctx context.Context, run *runContext, src *apiv1.ResourceDefinition, snapName string) error {
	_, err := run.Store.Snapshots().Get(ctx, src.Name, snapName)
	if err == nil {
		return nil
	}

	if !isNotFound(err) {
		return fmt.Errorf("get clone snapshot %s: %w", snapName, err)
	}

	replicas, err := run.Store.Resources().ListByDefinition(ctx, src.Name)
	if err != nil {
		return fmt.Errorf("list replicas of %s: %w", src.Name, err)
	}

	snap := &apiv1.Snapshot{Name: snapName, ResourceName: src.Name}

	for i := range replicas {
		snap.Nodes = append(snap.Nodes, replicas[i].NodeName)
	}

	vds, err := run.Store.VolumeDefinitions().List(ctx, src.Name)
	if err != nil {
		return fmt.Errorf("list volume definitions of %s: %w", src.Name, err)
	}

	for i := range vds {
		snap.VolumeDefinitions = append(snap.VolumeDefinitions, apiv1.SnapshotVolumeDef{
			VolumeNumber: vds[i].VolumeNumber,
			SizeKib:      vds[i].SizeKib,
		})
	}

	err = run.Store.Snapshots().Create(ctx, snap)
	if err != nil {
		return fmt.Errorf("create clone snapshot %s: %w", snapName, err)
	}

	return nil
}

// resourceGroupQuerySizeInfo implements `resource-group
// query-size-info` and `query-max-volume-size`.
//
// The answer is derived from the free capacity the satellites report
// for the pools the group's policy selects: the largest volume that
// fits is bounded by the SMALLEST of the pools a replica set would
// land on, because every replica has to hold the whole volume.
//
// The controller additionally applies the oversubscription policy for
// thin pools, which lives in the apiserver and is not reproduced here
// — this figure is the physical bound, so it can only be more
// conservative than the controller's, never less.
func resourceGroupQuerySizeInfo(ctx context.Context, run *runContext) error {
	if len(run.Flags.Positionals) < 1 {
		return fmt.Errorf("%w: query needs a resource group", command.ErrUsage)
	}

	name := run.Flags.Positionals[0]

	group, err := run.Store.ResourceGroups().Get(ctx, name)
	if err != nil {
		return fmt.Errorf("get resource group %s: %w", name, err)
	}

	pools, err := candidatePools(ctx, run, &group)
	if err != nil {
		return err
	}

	if run.Flags.Machine {
		return machineOut(run, pools)
	}

	tbl := &metav1.Table{ColumnDefinitions: view.SizeInfoColumns()}
	tbl.Rows = view.SizeInfoRows(name, maxVolumeSizeKib(pools, int(group.SelectFilter.PlaceCount)), pools)

	return run.render(tbl)
}

// candidatePools lists the pools the group's policy would draw from,
// widest free capacity first — the order the placer prefers.
func candidatePools(ctx context.Context, run *runContext, group *apiv1.ResourceGroup) ([]apiv1.StoragePool, error) {
	all, err := run.Store.StoragePools().List(ctx)
	if err != nil {
		return nil, fmt.Errorf("list storage pools: %w", err)
	}

	wanted := group.SelectFilter.StoragePool
	pools := make([]apiv1.StoragePool, 0, len(all))

	for i := range all {
		if all[i].ProviderKind == apiv1.StoragePoolKindDiskless {
			continue
		}

		if wanted != "" && all[i].StoragePoolName != wanted {
			continue
		}

		pools = append(pools, all[i])
	}

	sort.SliceStable(pools, func(a, b int) bool {
		return pools[a].FreeCapacity > pools[b].FreeCapacity
	})

	return pools, nil
}

// maxVolumeSizeKib is the smallest free capacity among the pools a
// full replica set would occupy — every replica stores the whole
// volume, so the tightest one sets the bound. Too few candidate pools
// for the requested redundancy means nothing fits.
func maxVolumeSizeKib(pools []apiv1.StoragePool, placeCount int) int64 {
	if placeCount <= 0 {
		placeCount = 1
	}

	if len(pools) < placeCount {
		return 0
	}

	smallest := pools[placeCount-1].FreeCapacity
	if smallest < 0 {
		return 0
	}

	return smallest
}
