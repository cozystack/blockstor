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
	"slices"
	"sort"
	"strings"

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
	group := run.Flags.Values["resource-group"]
	layers := run.Flags.Values["layer-list"]

	// Which fields this edits is a property of the command line, not of
	// the object, so an empty modify is refused before anything is
	// written. Deciding it after the patch would spend a no-op write on
	// the way to a usage error.
	if group == "" && layers == "" {
		return fmt.Errorf("%w: modify needs something to change", command.ErrUsage)
	}

	err := run.Store.ResourceDefinitions().PatchResourceDefinitionSpec(ctx, name,
		func(def *apiv1.ResourceDefinition) error {
			return modifyDefinition(ctx, run, def, group, layers)
		})
	if err != nil {
		return fmt.Errorf("update resource definition %s: %w", name, err)
	}

	return nil
}

// modifyDefinition applies the named edits to current state. It runs
// inside the store's patch retry, so the resource-group lookup and the
// re-parent it authorises cannot be separated by a concurrent delete of
// that group.
func modifyDefinition(
	ctx context.Context, run *runContext, def *apiv1.ResourceDefinition, group, layers string,
) error {
	if group != "" {
		// The group drives placement: how many replicas a spawn makes,
		// which pools they may land on, the layer stack they inherit.
		// Nothing downstream rejects a definition that names a group
		// which does not exist — the controller treats an already
		// materialised definition as self-sufficient — so a typo here
		// would silently leave the definition pointing at nothing and
		// only surface much later, when someone spawns from it.
		_, err := run.Store.ResourceGroups().Get(ctx, group)
		if err != nil {
			return fmt.Errorf("get resource group %s: %w", group, err)
		}

		def.ResourceGroupName = group
	}

	if layers != "" {
		parsed, err := parseLayerList(layers)
		if err != nil {
			return err
		}

		// Same prerequisite as on create: a stack asking for LUKS with
		// no cluster passphrase brings the replicas up plaintext. The
		// layer stack reaches the satellite the same way whichever verb
		// wrote it, so guarding only create leaves the door open.
		luksErr := checkLUKSPrerequisite(ctx, run, parsed)
		if luksErr != nil {
			return luksErr
		}

		def.LayerStack = parsed
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

	err = checkClonePreconditions(ctx, run, &src, target)
	if err != nil {
		return err
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
// errStaleCloneSnapshot is returned when the snapshot a retried clone
// would reuse no longer describes the source.
var errStaleCloneSnapshot = errors.New("the clone snapshot predates a change to the source")

// checkCloneSnapshotIsCurrent refuses a snapshot that no longer matches
// the source's volume layout.
//
// The snapshot name is deterministic on purpose, so an interrupted
// clone can be retried without accreting one snapshot per attempt. But
// "found" is not the same as "still right": if the first attempt took
// the snapshot and then failed, and a volume was added to the source
// before the retry, reusing it produces a target missing that volume —
// and reports success.
//
// Refusing rather than re-taking is deliberate: the existing snapshot
// may be the only copy of something, and deleting it is the operator's
// call, not this verb's.
func checkCloneSnapshotIsCurrent(
	ctx context.Context, run *runContext, src *apiv1.ResourceDefinition, snap *apiv1.Snapshot,
) error {
	current, err := run.Store.VolumeDefinitions().List(ctx, src.Name)
	if err != nil {
		return fmt.Errorf("list volumes of %s: %w", src.Name, err)
	}

	if len(current) != len(snap.VolumeDefinitions) {
		return fmt.Errorf("%w: %s covers %d volume(s) but %s now has %d; "+
			"delete the snapshot to retake it",
			errStaleCloneSnapshot, snap.Name, len(snap.VolumeDefinitions), src.Name, len(current))
	}

	// A resize leaves the count alone, so counting volumes answers only
	// half the question: a snapshot taken before the source grew still
	// matches on cardinality, and reusing it materialises the target at
	// the old size while reporting success. Compare the sizes the
	// snapshot actually captured, keyed by volume number rather than by
	// position, since neither list promises an order.
	captured := make(map[int32]int64, len(snap.VolumeDefinitions))
	for _, vol := range snap.VolumeDefinitions {
		captured[vol.VolumeNumber] = vol.SizeKib
	}

	for i := range current {
		was, ok := captured[current[i].VolumeNumber]
		if !ok {
			return fmt.Errorf("%w: %s does not cover volume %d of %s; "+
				"delete the snapshot to retake it",
				errStaleCloneSnapshot, snap.Name, current[i].VolumeNumber, src.Name)
		}

		if was != current[i].SizeKib {
			return fmt.Errorf("%w: %s captured volume %d at %d KiB but %s is now %d KiB; "+
				"delete the snapshot to retake it",
				errStaleCloneSnapshot, snap.Name, current[i].VolumeNumber,
				was, src.Name, current[i].SizeKib)
		}
	}

	return nil
}

func ensureCloneSnapshot(ctx context.Context, run *runContext, src *apiv1.ResourceDefinition, snapName string) error {
	existing, err := run.Store.Snapshots().Get(ctx, src.Name, snapName)
	if err == nil {
		return checkCloneSnapshotIsCurrent(ctx, run, src, &existing)
	}

	if !isNotFound(err) {
		return fmt.Errorf("get clone snapshot %s: %w", snapName, err)
	}

	snap := &apiv1.Snapshot{Name: snapName, ResourceName: src.Name}

	// Nodes and volumes come from the shared hydration, which picks
	// DISKFUL replicas only. Snapshotting a diskless witness asks its
	// satellite to capture a volume it does not have: that node fails,
	// and a failed node fails the snapshot, which aborts the clone.
	err = hydrateSnapshot(ctx, run, snap)
	if err != nil {
		return err
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

	maxKib := maxVolumeSizeKib(pools, int(group.SelectFilter.PlaceCount))

	if run.Flags.Machine {
		// The computed size is the whole point of the command, so the
		// machine envelope carries it alongside the pools it was derived
		// from. Emitting only the pools left `-m` consumers unable to
		// obtain the one number the table leads with.
		return machineOut(run, []sizeInfo{{
			ResourceGroup:    name,
			MaxVolumeSizeKib: maxKib,
			Pools:            pools,
		}})
	}

	tbl := &metav1.Table{ColumnDefinitions: view.SizeInfoColumns()}
	tbl.Rows = view.SizeInfoRows(name, maxKib, pools)

	return run.render(tbl)
}

// sizeInfo is the machine-readable answer to a size query: the
// computed bound plus the candidates it came from.
//
//nolint:tagliatelle // the machine envelope mirrors LINSTOR's snake_case wire shape
type sizeInfo struct {
	ResourceGroup    string              `json:"resource_group"`
	MaxVolumeSizeKib int64               `json:"max_volume_size_kib"`
	Pools            []apiv1.StoragePool `json:"pools,omitempty"`
}

// candidatePools lists the pools the group's policy would draw from,
// widest free capacity first — the order the placer prefers.
func candidatePools(ctx context.Context, run *runContext, group *apiv1.ResourceGroup) ([]apiv1.StoragePool, error) {
	all, err := run.Store.StoragePools().List(ctx)
	if err != nil {
		return nil, fmt.Errorf("list storage pools: %w", err)
	}

	wanted := group.SelectFilter.StoragePoolList
	if group.SelectFilter.StoragePool != "" {
		wanted = append([]string{group.SelectFilter.StoragePool}, wanted...)
	}

	pools := make([]apiv1.StoragePool, 0, len(all))
	// One replica per NODE, not per pool: a node offering three
	// eligible pools cannot host three replicas of the same volume, so
	// counting them separately would promise redundancy that cannot be
	// placed.
	seen := make(map[string]bool, len(all))

	for i := range all {
		pool := &all[i]

		switch {
		case pool.ProviderKind == apiv1.StoragePoolKindDiskless:
			continue
		// A pool whose backing storage has disappeared is not a
		// candidate; the placer drops it too.
		case pool.PoolMissing:
			continue
		case !matchesAny(wanted, pool.StoragePoolName):
			continue
		case seen[pool.NodeName]:
			continue
		}

		seen[pool.NodeName] = true

		pools = append(pools, *pool)
	}

	sort.SliceStable(pools, func(a, b int) bool {
		return pools[a].FreeCapacity > pools[b].FreeCapacity
	})

	return pools, nil
}

// matchesAny reports whether a pool name passes the group's pool
// filter; an empty filter accepts every pool.
func matchesAny(filter []string, name string) bool {
	if len(filter) == 0 {
		return true
	}

	return slices.Contains(filter, name)
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

var (
	// errCloneTargetExists refuses a clone onto a name already taken.
	errCloneTargetExists = errors.New("clone target already exists")

	// errPoolCannotSnapshot refuses a clone whose source sits on a pool
	// that cannot take one.
	errPoolCannotSnapshot = errors.New("storage pool does not support snapshots")
)

// checkClonePreconditions refuses a clone that cannot succeed, BEFORE the
// first write.
//
// Order is the whole point here. A clone takes an internal snapshot of the
// source and restores the target from it, so a precondition checked after
// that snapshot is not a precondition: cloning onto an occupied name used to
// take the snapshot first and fail on the create afterwards, leaving the
// snapshot behind for someone to find and wonder about. And a source on a
// thick pool cannot be snapshotted at all, which the data plane discovers far
// from where the operator typed the command.
//
// REST answers 400 on both before it writes anything; this path writes the
// CRDs directly, so the same two questions have to be asked here.
func checkClonePreconditions(
	ctx context.Context, run *runContext, src *apiv1.ResourceDefinition, target string,
) error {
	_, err := run.Store.ResourceDefinitions().Get(ctx, target)

	switch {
	case err == nil:
		return fmt.Errorf("%w: %s", errCloneTargetExists, target)
	case !isNotFound(err):
		return fmt.Errorf("get resource definition %s: %w", target, err)
	}

	return checkSourcePoolsSnapshot(ctx, run, src.Name)
}

// checkSourcePoolsSnapshot refuses a clone whose diskful source replicas sit
// on pools that cannot take a copy-on-write snapshot — thick LVM, plain FILE,
// diskless. Only diskful, active replicas materialise a volume to snapshot,
// so the others are not gated on a pool they do not have.
func checkSourcePoolsSnapshot(ctx context.Context, run *runContext, srcName string) error {
	replicas, err := run.Store.Resources().ListByDefinition(ctx, srcName)
	if err != nil {
		return fmt.Errorf("list replicas of %s: %w", srcName, err)
	}

	offenders := make([]string, 0, len(replicas))
	seen := map[string]bool{}

	for i := range replicas {
		res := &replicas[i]
		if slices.Contains(res.Flags, apiv1.ResourceFlagDiskless) ||
			slices.Contains(res.Flags, apiv1.ResourceFlagInactive) {
			continue
		}

		poolName := res.Props[storPoolNameProp]
		if poolName == "" {
			continue
		}

		key := res.NodeName + "/" + poolName
		if seen[key] {
			continue
		}

		seen[key] = true

		pool, poolErr := run.Store.StoragePools().Get(ctx, res.NodeName, poolName)
		if poolErr != nil {
			// A pool the CLI cannot read is not evidence of anything; the
			// data plane will surface a real problem with it.
			continue
		}

		if !poolSupportsSnapshots(pool.ProviderKind) {
			offenders = append(offenders, key+" ("+pool.ProviderKind+")")
		}
	}

	if len(offenders) == 0 {
		return nil
	}

	return fmt.Errorf("%w: %s; place the source on a thin-provisioned pool before cloning",
		errPoolCannotSnapshot, strings.Join(offenders, ", "))
}

// poolSupportsSnapshots reports whether a provider can take a
// copy-on-write snapshot. Thick LVM, plain FILE and DISKLESS cannot.
func poolSupportsSnapshots(kind string) bool {
	switch kind {
	case apiv1.StoragePoolKindLVMThin, apiv1.StoragePoolKindZFS,
		apiv1.StoragePoolKindZFSThin, apiv1.StoragePoolKindFileThin:
		return true
	default:
		return false
	}
}
