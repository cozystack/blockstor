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

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"

	"github.com/cozystack/blockstor/internal/cli/command"
)

// migratingFromProp is the destination-side stamp the migration
// reconciler watches: it names the source replica to prune once the
// destination's volumes reach UpToDate. Without it the controller has
// no way to know which replica the migration was moving off.
const migratingFromProp = "BlockstorMigratingFrom"

// storPoolNameProp is where a replica's chosen storage pool is
// recorded.
const storPoolNameProp = "StorPoolName"

// The migrate-disk preconditions. Both are API-level refusals: the
// command is well-formed, the cluster is simply not in a state where
// the move is safe.
var (
	errNothingToMigrate = errors.New("the source replica is DISKLESS")
	errSourceInUse      = errors.New("demote the consumer before migrating")

	// errLastDiskfulReplica refuses the demotion that would delete the
	// only copy of the data. Pass --force to override.
	errLastDiskfulReplica = errors.New("add a diskful replica first, or pass --force")

	// errReplicaInUse refuses demoting a replica a consumer still has
	// open. Pass --force to override.
	errReplicaInUse = errors.New("stop the consumer first, or pass --force")
)

// resourceToggleDisk implements `resource toggle-disk <node> <rd>`.
//
// Four shapes share the verb, in the order they are checked:
//
//	--cancel                 unwind an in-flight conversion
//	--migrate-from <src>     add-before-drop move onto this node
//	--diskless               force storage-free
//	--storage-pool <pool>    force diskful (bare form flips)
func resourceToggleDisk(ctx context.Context, run *runContext) error {
	node, rdName, err := resourcePair(run, "toggle-disk")
	if err != nil {
		return err
	}

	switch {
	case run.Flags.Cancel:
		return toggleDiskCancel(ctx, run, node, rdName)
	case run.Flags.Values["migrate-from"] != "":
		return migrateDisk(ctx, run, node, rdName)
	case run.Flags.Diskless:
		return setDiskless(ctx, run, node, rdName, true)
	default:
		return toggleDiskful(ctx, run, node, rdName)
	}
}

// resourcePair reads the `<node> <resource-definition>` positionals
// every per-replica verb takes.
func resourcePair(run *runContext, verb string) (string, string, error) {
	const wantArgs = 2

	if len(run.Flags.Positionals) < wantArgs {
		return "", "", fmt.Errorf("%w: %s needs a node and a definition", command.ErrUsage, verb)
	}

	return run.Flags.Positionals[0], run.Flags.Positionals[1], nil
}

// toggleDiskCancel asks the satellite to unwind a partial conversion.
//
// It deliberately does NOT flip DISKLESS: the reconciler clears it
// itself as the last step of the rollback, so an external observer
// sees the previous state return only once the storage really is back.
func toggleDiskCancel(ctx context.Context, run *runContext, node, rdName string) error {
	return patchResource(ctx, run, node, rdName, func(res *apiv1.Resource) {
		res.ToggleDiskCancel = true
	})
}

// setDiskless forces a replica's disk state, in either direction.
//
// Promotion clears TIE_BREAKER along with DISKLESS: an auto-managed
// witness carries both, and a diskful replica left holding TIE_BREAKER
// is counted as a witness by the tiebreaker reconciler, which then
// double-counts the slot.
func setDiskless(ctx context.Context, run *runContext, node, rdName string, diskless bool) error {
	pool := run.Flags.Values["storage-pool"]

	// Promotion gives a replica storage, so it needs a pool. The flag
	// flip alone leaves the satellite with `unknown storage pool ""`
	// and the replica stuck in Provisioning, so an omitted
	// --storage-pool is resolved the way the REST path resolves it.
	if !diskless && pool == "" {
		resolved, err := resolveStorPool(ctx, run, rdName, node)
		if err != nil {
			return err
		}

		pool = resolved
	}

	return patchResource(ctx, run, node, rdName, func(res *apiv1.Resource) {
		res.Flags = setFlag(res.Flags, apiv1.ResourceFlagDiskless, diskless)

		if diskless {
			return
		}

		res.Flags = setFlag(res.Flags, apiv1.ResourceFlagTieBreaker, false)

		if pool != "" {
			stampProp(res, storPoolNameProp, pool)
		}
	})
}

// toggleDiskful promotes a replica to diskful. With no pool argument
// and a replica that is already diskful, the verb flips it the other
// way — the bare `r td <node> <rd>` shape.
func toggleDiskful(ctx context.Context, run *runContext, node, rdName string) error {
	res, err := run.Store.Resources().Get(ctx, rdName, node)
	if err != nil {
		return fmt.Errorf("get resource %s on %s: %w", rdName, node, err)
	}

	wasDiskless := slices.Contains(res.Flags, apiv1.ResourceFlagDiskless)
	if !wasDiskless && run.Flags.Values["storage-pool"] == "" {
		demoteErr := checkDemotable(ctx, run, &res, node, rdName)
		if demoteErr != nil {
			return demoteErr
		}

		return setDiskless(ctx, run, node, rdName, true)
	}

	return setDiskless(ctx, run, node, rdName, false)
}

// checkDemotable refuses a demotion that would leave the definition
// with no data.
//
// Demoting is not a metadata edit: the satellite reconciles the
// DISKLESS flag by detaching DRBD, closing LUKS and then deleting the
// backing volume, so the last diskful replica takes the only copy of
// the data with it. Upstream LINSTOR refuses the same move, and this
// CLI writes the CRD directly, so a guard living anywhere else is
// simply not on this path.
//
// The count is read before the patch rather than inside it, so a
// replica demoted concurrently elsewhere can still slip through. That
// window is the controller's to close — the redundancy invariant is
// its to hold. What this guard is for is the operator who names the
// wrong node, and for that a pre-check is exactly as good.
func checkDemotable(
	ctx context.Context, run *runContext, res *apiv1.Resource, node, rdName string,
) error {
	if run.Flags.Force {
		return nil
	}

	if res.State.InUse != nil && *res.State.InUse {
		return fmt.Errorf("refusing to demote %s on %s while it is in use: %w",
			rdName, node, errReplicaInUse)
	}

	siblings, err := run.Store.Resources().ListByDefinition(ctx, rdName)
	if err != nil {
		return fmt.Errorf("list replicas of %s: %w", rdName, err)
	}

	diskful := 0

	for i := range siblings {
		if !slices.Contains(siblings[i].Flags, apiv1.ResourceFlagDiskless) {
			diskful++
		}
	}

	if diskful <= 1 {
		return fmt.Errorf("refusing to demote the last diskful replica of %s (on %s); "+
			"its backing volume is deleted with it: %w", rdName, node, errLastDiskfulReplica)
	}

	return nil
}

// migrateDisk implements `--migrate-from <src>`: strict
// add-before-drop.
//
// The destination is promoted and stamped with the source's name; the
// SOURCE IS LEFT ALONE. The migration reconciler removes it only once
// the destination's volumes are UpToDate, so redundancy holds for the
// whole resync instead of dropping for its duration.
func migrateDisk(ctx context.Context, run *runContext, dst, rdName string) error {
	src := run.Flags.Values["migrate-from"]
	pool := run.Flags.Values["storage-pool"]

	srcRes, err := run.Store.Resources().Get(ctx, rdName, src)
	if err != nil {
		return fmt.Errorf("migrate-disk: source replica %s on %s: %w", rdName, src, err)
	}

	if slices.Contains(srcRes.Flags, apiv1.ResourceFlagDiskless) {
		return fmt.Errorf("migrate-disk: source replica %s on %s has no diskful storage to migrate: %w",
			rdName, src, errNothingToMigrate)
	}

	if srcRes.State.InUse != nil && *srcRes.State.InUse {
		return fmt.Errorf("migrate-disk: source replica %s on %s is Primary InUse: %w",
			rdName, src, errSourceInUse)
	}

	_, err = run.Store.Resources().Get(ctx, rdName, dst)
	if isNotFound(err) {
		return createMigrationTarget(ctx, run, dst, rdName, pool, src)
	}

	if err != nil {
		return fmt.Errorf("get resource %s on %s: %w", rdName, dst, err)
	}

	return patchResource(ctx, run, dst, rdName, func(res *apiv1.Resource) {
		stampProp(res, storPoolNameProp, pool)
		stampProp(res, migratingFromProp, src)
		res.Flags = setFlag(res.Flags, apiv1.ResourceFlagDiskless, false)
	})
}

func createMigrationTarget(ctx context.Context, run *runContext, dst, rdName, pool, src string) error {
	res := &apiv1.Resource{Name: rdName, NodeName: dst}

	// A migration target is diskful by construction, and the satellite
	// cannot bind one without a pool. stampProp with an empty value is
	// a no-op, so an omitted --storage-pool would leave the target
	// wedged in Provisioning; resolve it the way the REST path does.
	if pool == "" {
		resolved, err := resolveStorPool(ctx, run, rdName, dst)
		if err != nil {
			return err
		}

		pool = resolved
	}

	stampProp(res, storPoolNameProp, pool)
	stampProp(res, migratingFromProp, src)

	err := run.Store.Resources().Create(ctx, res)
	if err != nil {
		return fmt.Errorf("create migration target %s on %s: %w", rdName, dst, err)
	}

	return nil
}

// resourceActivate and resourceDeactivate flip the INACTIVE flag, the
// one the placer and the dispatcher read to skip a replica.
func resourceActivate(ctx context.Context, run *runContext) error {
	return setInactive(ctx, run, false)
}

func resourceDeactivate(ctx context.Context, run *runContext) error {
	return setInactive(ctx, run, true)
}

func setInactive(ctx context.Context, run *runContext, inactive bool) error {
	verb := "activate"
	if inactive {
		verb = "deactivate"
	}

	node, rdName, err := resourcePair(run, verb)
	if err != nil {
		return err
	}

	return patchResource(ctx, run, node, rdName, func(res *apiv1.Resource) {
		res.Flags = setFlag(res.Flags, apiv1.ResourceFlagInactive, inactive)
	})
}

// patchResource applies an edit to one replica.
func patchResource(ctx context.Context, run *runContext, node, rdName string, edit func(*apiv1.Resource)) error {
	err := run.Store.Resources().PatchResourceSpec(ctx, rdName, node, func(res *apiv1.Resource) error {
		edit(res)

		return nil
	})
	if err != nil {
		return fmt.Errorf("update resource %s on %s: %w", rdName, node, err)
	}

	return nil
}

// setFlag adds or removes a flag, leaving the rest of the list alone.
func setFlag(flags []string, flag string, want bool) []string {
	has := slices.Contains(flags, flag)

	switch {
	case want && !has:
		return append(flags, flag)
	case !want && has:
		return slices.DeleteFunc(flags, func(f string) bool { return f == flag })
	default:
		return flags
	}
}

// stampProp records a property, skipping an empty value so a flag the
// operator did not pass cannot blank one that is already set.
func stampProp(res *apiv1.Resource, key, value string) {
	if value == "" {
		return
	}

	if res.Props == nil {
		res.Props = map[string]string{}
	}

	res.Props[key] = value
}
