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

	"github.com/google/uuid"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
	"github.com/cozystack/blockstor/pkg/validate"

	"github.com/cozystack/blockstor/internal/cli/command"
)

// restoreFromSnapshotProp routes the satellite to
// RestoreVolumeFromSnapshot instead of CreateVolume. Its value is
// `<source-definition>:<snapshot>`; the satellite splits on the colon.
// It is stamped on the definition rather than per replica because
// every replica of a restored definition clones from the same source.
const restoreFromSnapshotProp = "BlockstorRestoreFromSnapshot"

// errRollbackUnsupported explains a deliberate omission.
//
// In-place rollback destroys every snapshot newer than the target
// (`zfs rollback`, `lvconvert --merge`) and upstream additionally
// resurrects replicas that were deleted after the snapshot was taken.
// Restoring into a NEW definition and switching over is recoverable at
// every step; a rollback is not.
var errRollbackUnsupported = errors.New(
	"in-place rollback is not supported: it destroys every newer snapshot. " +
		"Restore into a new resource instead: `snapshot resource restore " +
		"--from-resource <rd> --from-snapshot <snap> --to-resource <new>`")

var errVolumeNumberTaken = errors.New("the target already has a volume definition with that number")

// errNothingToCapture guards the phantom-snapshot case: a snapshot
// with no diskful node behind it captures nothing while reporting
// success.
var errNothingToCapture = errors.New("place a diskful replica first")

// snapshotRollback implements `snapshot rollback`.
func snapshotRollback(_ context.Context, _ *runContext) error {
	return errRollbackUnsupported
}

// snapshotCreateMultiple implements `snapshot create-multiple`.
//
// Two spellings reach it: `<rd>:<snap>` pairs, or a snapshot name
// followed by the definitions to capture. Every member is stamped with
// one group id so the controller opens a SINGLE suspend-io barrier
// across the union of their nodes — capturing them under separate
// barriers would give a set of snapshots that are individually
// consistent but not consistent with each other, which is the whole
// point of the verb.
func snapshotCreateMultiple(ctx context.Context, run *runContext) error {
	pairs, err := snapshotBatch(run)
	if err != nil {
		return err
	}

	// Every definition is checked before the first snapshot is
	// written. A group whose members are fewer than its declared
	// GroupSize is one the controller waits on forever, since it opens
	// the suspend-io barrier only once the whole group has landed.
	for _, pair := range pairs {
		_, err = run.Store.ResourceDefinitions().Get(ctx, pair.resource)
		if err != nil {
			return fmt.Errorf("get resource definition %s: %w", pair.resource, err)
		}
	}

	groupID := uuid.NewString()
	created := make([]snapshotPair, 0, len(pairs))

	for _, pair := range pairs {
		snap := &apiv1.Snapshot{
			ResourceName: pair.resource,
			Name:         pair.snapshot,
			Nodes:        run.Flags.Nodes,
			GroupID:      groupID,
			GroupSize:    int32(len(pairs)), //nolint:gosec // bounded by the argument count
		}

		err = hydrateSnapshot(ctx, run, snap)
		if err != nil {
			rollbackSnapshots(ctx, run, created)

			return err
		}

		err = run.Store.Snapshots().Create(ctx, snap)
		if err != nil {
			// Unwind what this call created. Leaving a short group
			// behind would strand the barrier; these snapshots are
			// ours and nothing has consumed them yet.
			rollbackSnapshots(ctx, run, created)

			return fmt.Errorf("create snapshot %s of %s: %w", snap.Name, snap.ResourceName, err)
		}

		created = append(created, pair)
	}

	return nil
}

// rollbackSnapshots removes the members of a batch that did land, so a
// failed create-multiple leaves no group with fewer members than its
// declared size. A failure to unwind is reported and otherwise
// ignored: the caller is already returning the original error, which
// is the one the operator needs.
func rollbackSnapshots(ctx context.Context, run *runContext, created []snapshotPair) {
	for _, pair := range created {
		err := run.Store.Snapshots().Delete(ctx, pair.resource, pair.snapshot)
		if err != nil && !isNotFound(err) {
			fmt.Fprintf(run.Err, "warning: could not roll back snapshot %s of %s: %v\n",
				pair.snapshot, pair.resource, err)
		}
	}
}

// snapshotPair is one member of a create-multiple batch.
type snapshotPair struct {
	resource string
	snapshot string
}

// snapshotBatch reads the batch members from either spelling.
func snapshotBatch(run *runContext) ([]snapshotPair, error) {
	positionals := run.Flags.Positionals
	if len(positionals) == 0 {
		return nil, fmt.Errorf("%w: create-multiple needs at least one snapshot to take", command.ErrUsage)
	}

	if strings.Contains(positionals[0], ":") {
		return explicitPairs(positionals)
	}

	// `<snapshot> [<rd>...]` with the rest of the definitions in -r.
	name := positionals[0]

	resources := append(append([]string{}, run.Flags.Resources...), positionals[1:]...)
	if len(resources) == 0 {
		return nil, fmt.Errorf("%w: create-multiple needs the definitions to capture", command.ErrUsage)
	}

	pairs := make([]snapshotPair, 0, len(resources))
	for _, resource := range resources {
		pairs = append(pairs, snapshotPair{resource: resource, snapshot: name})
	}

	return pairs, nil
}

func explicitPairs(positionals []string) ([]snapshotPair, error) {
	pairs := make([]snapshotPair, 0, len(positionals))

	for _, arg := range positionals {
		resource, snapshot, ok := strings.Cut(arg, ":")
		if !ok || resource == "" || snapshot == "" {
			return nil, fmt.Errorf("%w: %q is not a <resource>:<snapshot> pair", command.ErrUsage, arg)
		}

		pairs = append(pairs, snapshotPair{resource: resource, snapshot: snapshot})
	}

	return pairs, nil
}

// hydrateSnapshot fills in what makes a snapshot real: the nodes to
// capture on and the volume layout to capture.
//
// This is NOT optional decoration. A Snapshot written with an empty
// Nodes slice is treated as degenerate by the snapshot controller,
// which returns without capturing anything — so the command would
// report success, the listing would render the snapshot as healthy,
// and there would be no data behind it. An empty VolumeDefinitions
// slice has the matching effect on the way back out: restoring such a
// snapshot hydrates zero volumes, also with exit 0.
//
// The apiserver does this in `hydrateSnapshotFromRD` before it
// persists. That hydration is wire-to-CRD translation which lives in
// the REST layer rather than in the store, so a client that talks to
// the store directly has to carry it too.
func hydrateSnapshot(ctx context.Context, run *runContext, snap *apiv1.Snapshot) error {
	def, err := run.Store.ResourceDefinitions().Get(ctx, snap.ResourceName)
	if err != nil {
		return fmt.Errorf("get resource definition %s: %w", snap.ResourceName, err)
	}

	if len(snap.VolumeDefinitions) == 0 {
		vds, vdErr := run.Store.VolumeDefinitions().List(ctx, snap.ResourceName)
		if vdErr != nil {
			return fmt.Errorf("list volume definitions of %s: %w", snap.ResourceName, vdErr)
		}

		snap.VolumeDefinitions = make([]apiv1.SnapshotVolumeDef, 0, len(vds))
		for i := range vds {
			snap.VolumeDefinitions = append(snap.VolumeDefinitions, apiv1.SnapshotVolumeDef{
				VolumeNumber:          vds[i].VolumeNumber,
				SizeKib:               vds[i].SizeKib,
				VolumeDefinitionProps: vds[i].Props,
			})
		}
	}

	if len(snap.Nodes) == 0 {
		snap.Nodes, err = diskfulNodesOf(ctx, run, snap.ResourceName)
		if err != nil {
			return err
		}
	}

	if snap.Props == nil {
		snap.Props = def.Props
	}

	if snap.SnapshotDefinitionProps == nil {
		snap.SnapshotDefinitionProps = snap.Props
	}

	if snap.ResourceDefinitionProps == nil && len(def.Props) > 0 {
		snap.ResourceDefinitionProps = def.Props
	}

	return nil
}

// diskfulNodesOf lists the nodes that actually hold data for a
// definition.
//
// A diskless replica has no backing volume to snapshot: asking its
// satellite to capture one fails that node, which fails the whole
// snapshot. An inactive replica is skipped for the same reason.
func diskfulNodesOf(ctx context.Context, run *runContext, rdName string) ([]string, error) {
	replicas, err := run.Store.Resources().ListByDefinition(ctx, rdName)
	if err != nil {
		return nil, fmt.Errorf("list replicas of %s: %w", rdName, err)
	}

	nodes := make([]string, 0, len(replicas))

	for i := range replicas {
		if slices.Contains(replicas[i].Flags, apiv1.ResourceFlagDiskless) ||
			slices.Contains(replicas[i].Flags, apiv1.ResourceFlagInactive) {
			continue
		}

		nodes = append(nodes, replicas[i].NodeName)
	}

	if len(nodes) == 0 {
		return nil, fmt.Errorf("%s has no diskful replica to snapshot: %w", rdName, errNothingToCapture)
	}

	return nodes, nil
}

// restoreArgs are the three flags every restore verb takes.
type restoreArgs struct {
	fromResource string
	fromSnapshot string
	toResource   string
}

func readRestoreArgs(run *runContext) (restoreArgs, error) {
	args := restoreArgs{
		fromResource: run.Flags.Values["from-resource"],
		fromSnapshot: run.Flags.Values["from-snapshot"],
		toResource:   run.Flags.Values["to-resource"],
	}

	if args.fromResource == "" || args.fromSnapshot == "" || args.toResource == "" {
		return args, fmt.Errorf(
			"%w: restore needs --from-resource, --from-snapshot and --to-resource", command.ErrUsage)
	}

	return args, nil
}

// snapshotRestoreResource implements `snapshot resource restore` and
// its resource-definition spelling: a NEW definition is materialised
// from the snapshot, its volumes hydrated, and replicas stamped.
func snapshotRestoreResource(ctx context.Context, run *runContext) error {
	args, err := readRestoreArgs(run)
	if err != nil {
		return err
	}

	src, err := run.Store.ResourceDefinitions().Get(ctx, args.fromResource)
	if err != nil {
		return fmt.Errorf("get resource definition %s: %w", args.fromResource, err)
	}

	snap, err := run.Store.Snapshots().Get(ctx, args.fromResource, args.fromSnapshot)
	if err != nil {
		return fmt.Errorf("get snapshot %s of %s: %w", args.fromSnapshot, args.fromResource, err)
	}

	def := &apiv1.ResourceDefinition{
		Name: args.toResource,
		// The parent group carries over: it drives later placement and
		// property inheritance, and a restored definition with a blank
		// group breaks the restore-then-list workflow.
		ResourceGroupName: src.ResourceGroupName,
		LayerStack:        src.LayerStack,
		Props:             maps.Clone(snap.Props),
	}

	if def.Props == nil {
		def.Props = maps.Clone(src.Props)
	}

	if def.Props == nil {
		def.Props = map[string]string{}
	}

	def.Props[restoreFromSnapshotProp] = args.fromResource + ":" + snap.Name

	err = run.Store.ResourceDefinitions().Create(ctx, def)
	if err != nil {
		return fmt.Errorf("create resource definition %s: %w", def.Name, err)
	}

	err = hydrateVolumes(ctx, run, def.Name, &snap)
	if err != nil {
		return rollbackRestore(ctx, run, def.Name, err)
	}

	err = placeRestored(ctx, run, args.fromResource, def.Name, &snap)
	if err != nil {
		return rollbackRestore(ctx, run, def.Name, err)
	}

	return nil
}

// rollbackRestore removes the definition a failed restore had already
// created, and returns the failure that caused it.
//
// Without it a restore that dies partway — one volume of several
// created, a size the API server now refuses, a conflict on the create
// — leaves the definition and whatever volumes it got behind. The
// obvious response, running the same command again, then fails on
// "already exists" and the operator has to delete by hand first. That
// is the half-restored state this verb goes out of its way to avoid
// elsewhere, and the sibling snapshot create-multiple already unwinds
// this way.
//
// A rollback that itself fails must not replace the original error:
// the first one is what the operator needs, the second is a note about
// what was left behind.
func rollbackRestore(ctx context.Context, run *runContext, rdName string, cause error) error {
	err := run.Store.ResourceDefinitions().Delete(ctx, rdName)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return fmt.Errorf("%w (rolling back %s also failed: %w)", cause, rdName, err)
	}

	return cause
}

// snapshotRestoreVolumeDefinition implements `snapshot
// volume-definition restore`: the snapshot's recorded volume layout is
// hydrated onto an EXISTING definition, which is created separately.
func snapshotRestoreVolumeDefinition(ctx context.Context, run *runContext) error {
	args, err := readRestoreArgs(run)
	if err != nil {
		return err
	}

	snap, err := run.Store.Snapshots().Get(ctx, args.fromResource, args.fromSnapshot)
	if err != nil {
		return fmt.Errorf("get snapshot %s of %s: %w", args.fromSnapshot, args.fromResource, err)
	}

	_, err = run.Store.ResourceDefinitions().Get(ctx, args.toResource)
	if err != nil {
		return fmt.Errorf("get resource definition %s: %w", args.toResource, err)
	}

	// The collision is refused BEFORE anything is written: hydrating
	// volume by volume would land the non-colliding ones first and
	// leave the target half-restored.
	existing, err := run.Store.VolumeDefinitions().List(ctx, args.toResource)
	if err != nil {
		return fmt.Errorf("list volume definitions of %s: %w", args.toResource, err)
	}

	taken := make(map[int32]bool, len(existing))
	for i := range existing {
		taken[existing[i].VolumeNumber] = true
	}

	for i := range snap.VolumeDefinitions {
		number := snap.VolumeDefinitions[i].VolumeNumber
		if taken[number] {
			return fmt.Errorf("cannot restore volume %d onto %s: %w",
				number, args.toResource, errVolumeNumberTaken)
		}
	}

	return hydrateVolumes(ctx, run, args.toResource, &snap)
}

// hydrateVolumes recreates the snapshot's volume layout on a
// definition.
func hydrateVolumes(ctx context.Context, run *runContext, rdName string, snap *apiv1.Snapshot) error {
	if len(snap.VolumeDefinitions) == 0 {
		return fmt.Errorf("%s captured no volumes to restore onto %s: %w",
			snap.Name, rdName, errNothingToRestore)
	}

	added := make([]int32, 0, len(snap.VolumeDefinitions))

	for i := range snap.VolumeDefinitions {
		svd := &snap.VolumeDefinitions[i]

		err := run.Store.VolumeDefinitions().Create(ctx, rdName, &apiv1.VolumeDefinition{
			VolumeNumber: svd.VolumeNumber,
			SizeKib:      svd.SizeKib,
		})
		if err != nil {
			// This variant restores onto a definition it does not own,
			// so there is no RD to drop as a whole. Leaving the volumes
			// already created behind would hand back a pre-existing
			// definition carrying an arbitrary subset of the snapshot —
			// a shape nothing downstream expects. Take back exactly
			// what this call added.
			unwindVolumes(ctx, run, rdName, added)

			return fmt.Errorf("restore volume %d onto %s: %w", svd.VolumeNumber, rdName, err)
		}

		added = append(added, svd.VolumeNumber)
	}

	return nil
}

// unwindVolumes removes the volumes a failed restore had already
// created. A failure here is reported through the original error, not
// this one: the caller is already on the way out, and the restore
// error is the one that explains why.
func unwindVolumes(ctx context.Context, run *runContext, rdName string, added []int32) {
	for _, number := range added {
		delErr := run.Store.VolumeDefinitions().Delete(ctx, rdName, number)
		if delErr != nil {
			fmt.Fprintf(run.Err, "warning: could not remove volume %d from %s after a failed restore: %v\n",
				number, rdName, delErr)
		}
	}
}

// placeRestored stamps replicas of the restored definition.
//
// They land on the nodes that HOLD THE SNAPSHOT, in the same pool the
// source replica uses there — never via the autoplacer. A restored
// replica on a different backend makes the satellite pipe the
// snapshot stream into the wrong receiver, which never converges.
// Explicit `--node-name` values win when the operator gave them.
func placeRestored(ctx context.Context, run *runContext, srcRD, rdName string, snap *apiv1.Snapshot) error {
	nodes := run.Flags.Nodes

	// Named nodes have to actually hold the snapshot. Restoring onto one
	// that does not produces a replica with nothing behind it: the objects
	// are created, the satellite finds no snapshot to receive, and the
	// command reports success over an empty volume.
	err := validate.RestoreNodesHoldSnapshot(nodes, snap.Nodes)
	if err != nil {
		return fmt.Errorf("%w: %w", command.ErrUsage, err)
	}

	if len(nodes) == 0 {
		nodes = snap.Nodes
	}

	// Zero nodes would walk the loop zero times and report success,
	// leaving a definition with no replicas and no data — the same
	// silent-success the capture side refuses via errNothingToCapture.
	// A Snapshot CR not written through this CLI can carry an empty
	// node list, so the restore side needs the matching guard.
	if len(nodes) == 0 {
		return fmt.Errorf("%s names no node to restore %s onto: %w",
			snap.Name, rdName, errNothingToRestore)
	}

	for _, node := range nodes {
		res := &apiv1.Resource{Name: rdName, NodeName: node}

		pool, err := sourcePoolOn(ctx, run, srcRD, node)
		if err != nil {
			return err
		}

		stampProp(res, storPoolNameProp, pool)

		err = run.Store.Resources().Create(ctx, res)
		if err != nil {
			return fmt.Errorf("create restored replica %s on %s: %w", rdName, node, err)
		}
	}

	return nil
}

// errNoSourcePool is returned when a restore cannot work out which
// storage pool the new replica belongs in and the operator did not say.
var errNoSourcePool = errors.New("no storage pool to restore into")

// errNothingToRestore refuses a restore that would report success
// while materialising no data — no node to place on, or no volume
// captured. Mirrors errNothingToCapture on the create side.
var errNothingToRestore = errors.New("the snapshot describes nothing to restore")

// sourcePoolOn reports which pool the restored replica should use on a
// node: `--storage-pool` when the operator named one, otherwise the
// pool the source uses on that node, otherwise the source's first
// diskful pool.
//
// It REFUSES rather than returning an empty pool. A diskful Resource
// with no StorPoolName is created happily — nothing in the CRD requires
// the field — and then the satellite fails every reconcile with
// `unknown storage pool ""`, which this repository has already been
// bitten by once (pkg/store/k8s/resources.go). The verb would report
// success while producing a replica that can never come up.
//
// The case is not exotic: it is the disaster-recovery one restore
// exists for. A snapshot outlives its source, the source degrades to
// diskless-only or loses its replicas, and there is no pool left to
// infer. The operator knows where it should land; the code does not.
func sourcePoolOn(ctx context.Context, run *runContext, srcRD, node string) (string, error) {
	if chosen := run.Flags.Values["storage-pool"]; chosen != "" {
		return chosen, nil
	}

	replicas, err := run.Store.Resources().ListByDefinition(ctx, srcRD)
	if err != nil {
		return "", fmt.Errorf("list replicas of %s: %w", srcRD, err)
	}

	fallback := ""

	for i := range replicas {
		pool := replicas[i].Props[storPoolNameProp]
		if pool == "" {
			continue
		}

		if replicas[i].NodeName == node {
			return pool, nil
		}

		if fallback == "" {
			fallback = pool
		}
	}

	if fallback == "" {
		return "", fmt.Errorf("%w: %s has no diskful replica to take a storage pool from; "+
			"name one with --storage-pool", errNoSourcePool, srcRD)
	}

	return fallback, nil
}
