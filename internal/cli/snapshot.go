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
	"strings"

	"github.com/google/uuid"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"

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
		return err
	}

	return placeRestored(ctx, run, args.fromResource, def.Name, &snap)
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
	for i := range snap.VolumeDefinitions {
		svd := &snap.VolumeDefinitions[i]

		err := run.Store.VolumeDefinitions().Create(ctx, rdName, &apiv1.VolumeDefinition{
			VolumeNumber: svd.VolumeNumber,
			SizeKib:      svd.SizeKib,
		})
		if err != nil {
			return fmt.Errorf("restore volume %d onto %s: %w", svd.VolumeNumber, rdName, err)
		}
	}

	return nil
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
	if len(nodes) == 0 {
		nodes = snap.Nodes
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

// sourcePoolOn reports which pool the source replica uses on a node,
// falling back to the source's first diskful pool so a restore onto a
// node the source does not occupy still pins a backend.
func sourcePoolOn(ctx context.Context, run *runContext, srcRD, node string) (string, error) {
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

	return fallback, nil
}
