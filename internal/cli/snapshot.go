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
	"strings"

	"github.com/google/uuid"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
	"github.com/cozystack/blockstor/pkg/validate"

	"github.com/cozystack/blockstor/internal/cli/command"
)

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
		snap.Props = store.TravellingProps(def.Props)
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

	// A definition already under the name is judged before anything about
	// the source is read or planned: a finished restore is left alone even
	// once the source or its snapshot is gone.
	exists, handled, err := replayOwnRestore(ctx, run, args)
	if handled || err != nil {
		return err
	}

	src, snap, err := readRestoreSource(ctx, run, args)
	if err != nil {
		return err
	}

	// A snapshot that recorded no volumes has nothing to restore, and is
	// refused before anything is written or resumed, as the REST door refuses
	// it: a resume over a leftover of one hydrated nothing and placed
	// replicas, exiting 0 over a definition with no volume.
	if len(snap.VolumeDefinitions) == 0 {
		return fmt.Errorf("%s captured no volumes to restore onto %s: %w",
			snap.Name, args.toResource, errNothingToRestore)
	}

	if exists {
		return restoreIntoExistingTarget(ctx, run, args, &snap, nil)
	}

	// Everything about where the replicas go is settled before anything is
	// written: a node that does not hold the snapshot, or a pool that cannot
	// be inferred, refused after the definition or its marker had landed.
	planned, err := planRestoredReplicas(ctx, run, args.fromResource, args.toResource, &snap)
	if err != nil {
		return err
	}

	def := newRestoredDefinition(args.toResource, &src, &snap)

	err = run.Store.ResourceDefinitions().Create(ctx, def)
	if errors.Is(err, store.ErrAlreadyExists) {
		return restoreIntoExistingTarget(ctx, run, args, &snap, err)
	}

	if err != nil {
		return fmt.Errorf("create resource definition %s: %w", def.Name, err)
	}

	// The snapshot was read before the definition existed; see
	// store.ReapClonedSnapshot.
	err = store.RestoreSourceWithdrawn(ctx, run.Store, snap.ResourceName, snap.Name)
	if err != nil {
		return rollbackRestore(ctx, run, def.Name, planned, err)
	}

	// The definition is this command's own, but a concurrent run may already
	// have adopted it and be hydrating the same volumes. A volume that is
	// already there at the snapshot's size is the restore's, not a collision
	// to unwind: taking it back would delete what the adopter answered for.
	err = hydrateMissingVolumes(ctx, run, def.Name, &snap)
	if err != nil {
		return rollbackRestore(ctx, run, def.Name, planned, err)
	}

	err = stampRestoredReplicas(ctx, run, planned)
	if err != nil {
		return rollbackRestore(ctx, run, def.Name, planned, err)
	}

	return nil
}

// newRestoredDefinition is the definition a restore creates: the source's
// group and stack, the snapshot's props, and this restore's marker.
func newRestoredDefinition(rdName string, src *apiv1.ResourceDefinition, snap *apiv1.Snapshot) *apiv1.ResourceDefinition {
	def := &apiv1.ResourceDefinition{
		Name: rdName,
		// The parent group carries over: it drives later placement and
		// property inheritance, and a restored definition with a blank
		// group breaks the restore-then-list workflow.
		ResourceGroupName: src.ResourceGroupName,
		LayerStack:        store.EffectiveLayerStack(src.LayerStack),
		Props:             store.TravellingProps(snap.Props),
	}

	if def.Props == nil {
		def.Props = store.TravellingProps(src.Props)
	}

	// Both halves off the stored objects, never off what the operator typed:
	// LINSTOR folds name case, and a REST retry over this leftover compares
	// the marker it finds against one built from the stored snapshot, while
	// the placer looks the source half up as a store key.
	def.Props = store.WithRestoreMarker(def.Props, snap)

	// The owner prop belongs to the snapshot; see store.CloneSnapshotOwnerProp.
	delete(def.Props, store.CloneSnapshotOwnerProp)
	delete(def.Props, store.CloneSnapshotReapingProp)

	return def
}

// restoreIntoExistingTarget finishes a restore over a definition that is
// already there, in one of two states, and refuses anything else with the
// create's own error.
//
// A target the operator prepared the way LINSTOR documents it (`rd c`, then
// `s vd restore`, then this command) is marked and placed; see
// store.PreparedRestoreTarget. The REST door takes the same state, which is
// also how linstor-csi restores.
//
// This restore's own leftover is judged the way the REST door judges it; see
// store.JudgeRestoreLeftover. The marker goes on before placement, so a
// placement that failed after it is finished by running the same command
// again rather than by deleting the definition, while a finished one is left
// alone.
//
// The definition is the operator's, so a failure past this point leaves it in
// place rather than deleting it.
func restoreIntoExistingTarget(
	ctx context.Context, run *runContext, args restoreArgs, snap *apiv1.Snapshot, createErr error,
) error {
	rdName := args.toResource

	existing, err := run.Store.ResourceDefinitions().Get(ctx, rdName)
	if err != nil {
		return fmt.Errorf("get resource definition %s: %w", rdName, err)
	}

	if createErr == nil {
		createErr = fmt.Errorf("%w: resource definition %q", store.ErrAlreadyExists, existing.Name)
	}

	progress, own, err := store.JudgeRestoreLeftover(ctx, run.Store, existing.Name, snap)
	if err != nil {
		return fmt.Errorf("restore into %s: %w", existing.Name, err)
	}

	if own {
		return finishRestoreLeftover(ctx, run, args, existing.Name, snap, progress)
	}

	// A layer stack that is not the source's, or replicas still being
	// deleted, refuse here, before the marker goes on a definition that is
	// the operator's; see store.PreparedRestoreTarget.
	prepared, err := store.PreparedRestoreTarget(ctx, run.Store, &existing, snap)
	if err != nil {
		return fmt.Errorf("restore into %s: %w", existing.Name, err)
	}

	if !prepared {
		return fmt.Errorf("create resource definition %s: %w", rdName, createErr)
	}

	// The group and the placement are settled before the marker, the way the
	// REST door settles them: a marker left on a definition parented to a
	// group that is gone, or one no node can be placed on, is a definition the
	// next run refuses over.
	err = store.RestoreTargetGroupSurvived(ctx, run.Store, &existing)
	if err != nil {
		return fmt.Errorf("restore into %s: %w", existing.Name, err)
	}

	planned, err := planRestoredReplicas(ctx, run, args.fromResource, existing.Name, snap)
	if err != nil {
		return err
	}

	adopted, err := store.AdoptPreparedRestoreTarget(ctx, run.Store, existing.Name, snap)
	if err != nil {
		return fmt.Errorf("restore into %s: %w", existing.Name, err)
	}

	// Not adopted with no error means the same marker landed between the
	// judgement above and the stamp: a concurrent run of this restore got
	// there first. Its definition is judged and finished as a leftover, the
	// way the REST door does, rather than placed over with this run's nodes.
	if !adopted {
		return finishConcurrentRestore(ctx, run, args, existing.Name, snap, createErr)
	}

	return placeOverMarkedTarget(ctx, run, existing.Name, snap, planned)
}

// placeOverMarkedTarget places the planned replicas on a target that carries
// this restore's marker. The snapshot was read before the marker went on, or
// before this run found it there, so it is read back first; see
// store.ReapClonedSnapshot.
func placeOverMarkedTarget(
	ctx context.Context, run *runContext, rdName string, snap *apiv1.Snapshot, planned []apiv1.Resource,
) error {
	err := store.RestoreSourceWithdrawn(ctx, run.Store, snap.ResourceName, snap.Name)
	if err != nil {
		return fmt.Errorf("restore into %s: %w", rdName, err)
	}

	return placeOverExisting(ctx, run, planned)
}

// finishConcurrentRestore judges a target another run of the same restore
// marked first, and finishes it as a leftover of this restore.
func finishConcurrentRestore(
	ctx context.Context, run *runContext, args restoreArgs, rdName string, snap *apiv1.Snapshot, createErr error,
) error {
	progress, own, err := store.JudgeRestoreLeftover(ctx, run.Store, rdName, snap)
	if err != nil {
		return fmt.Errorf("restore into %s: %w", rdName, err)
	}

	if !own {
		return fmt.Errorf("create resource definition %s: %w", rdName, createErr)
	}

	return finishRestoreLeftover(ctx, run, args, rdName, snap, progress)
}

// readRestoreSource reads the definition and the snapshot a restore is taken
// from.
func readRestoreSource(ctx context.Context, run *runContext, args restoreArgs) (apiv1.ResourceDefinition, apiv1.Snapshot, error) {
	src, err := run.Store.ResourceDefinitions().Get(ctx, args.fromResource)
	if err != nil {
		return src, apiv1.Snapshot{}, fmt.Errorf("get resource definition %s: %w", args.fromResource, err)
	}

	snap, err := run.Store.Snapshots().Get(ctx, args.fromResource, args.fromSnapshot)
	if err != nil {
		return src, snap, fmt.Errorf("get snapshot %s of %s: %w", args.fromSnapshot, args.fromResource, err)
	}

	return src, snap, nil
}

// sayAlreadyRestored reports a re-run over a finished restore. Said out loud:
// a re-run that exits 0 with nothing on the screen reads as a restore that
// just happened.
func sayAlreadyRestored(run *runContext, rdName, srcRD, snapName string) error {
	_, err := fmt.Fprintf(run.Out, "%s is already restored from %s/%s, nothing to do\n", rdName, srcRD, snapName)
	if err != nil {
		return fmt.Errorf("write output: %w", err)
	}

	return nil
}

// errSnapshotGoneUnfinished refuses a restore whose own leftover never
// finished and whose snapshot is gone: what is missing can only come from the
// snapshot.
var errSnapshotGoneUnfinished = errors.New("the snapshot is gone and the restore into the target never finished")

// replayOwnRestore answers a re-run over this restore's own leftover before
// anything about the source is read. Deleting the snapshot, or the source,
// once the restore finished is ordinary cleanup, and a re-run after it is
// still a re-run: a finished leftover is judged against the volumes it
// recorded and left alone, the way the REST replay answers it. An unfinished
// one with its snapshot gone is refused with the way out. It reports whether
// a definition is under the name, and whether it answered; not answered hands
// the request to the path that reads the source.
func replayOwnRestore(ctx context.Context, run *runContext, args restoreArgs) (bool, bool, error) {
	existing, err := run.Store.ResourceDefinitions().Get(ctx, args.toResource)
	if errors.Is(err, store.ErrNotFound) {
		return false, false, nil
	}

	if err != nil {
		return false, true, fmt.Errorf("get resource definition %s: %w", args.toResource, err)
	}

	if !strings.EqualFold(existing.Props[store.RestoreFromSnapshotProp], args.fromResource+":"+args.fromSnapshot) {
		return true, false, nil
	}

	handled, err := judgeOwnRestore(ctx, run, args, &existing)

	return true, handled, err
}

// judgeOwnRestore is replayOwnRestore over a definition that carries this
// restore's marker.
func judgeOwnRestore(
	ctx context.Context, run *runContext, args restoreArgs, existing *apiv1.ResourceDefinition,
) (bool, error) {
	var snapshot *apiv1.Snapshot

	snap, err := run.Store.Snapshots().Get(ctx, args.fromResource, args.fromSnapshot)

	switch {
	case err == nil:
		snapshot = &snap
	case errors.Is(err, store.ErrNotFound):
		if _, recorded := store.RecordedRestoreVolumes(existing.Props); !recorded {
			return true, fmt.Errorf("get snapshot %s of %s: %w", args.fromSnapshot, args.fromResource, err)
		}
	default:
		return true, fmt.Errorf("get snapshot %s of %s: %w", args.fromSnapshot, args.fromResource, err)
	}

	progress, own, err := store.JudgeRestoreLeftoverOf(ctx, run.Store, existing.Name,
		args.fromResource, args.fromSnapshot, snapshot)
	if err != nil {
		return true, fmt.Errorf("restore into %s: %w", existing.Name, err)
	}

	if !own {
		return false, nil
	}

	if progress == store.LeftoverFinished {
		return true, answerFinishedLeftover(ctx, run, existing.Name, args.fromResource, args.fromSnapshot)
	}

	if snapshot == nil {
		return true, fmt.Errorf("%w: %s/%s, %s; delete %s and restore it from another snapshot",
			errSnapshotGoneUnfinished, args.fromResource, args.fromSnapshot, existing.Name, existing.Name)
	}

	return false, nil
}

// finishRestoreLeftover acts on this restore's own leftover as judged by
// store.JudgeRestoreLeftover. A finished one is left as it is, the way the REST
// replay answers it, and nothing about the source is planned for it:
// re-placing it would put a replica back on a node the operator emptied,
// restored from the point-in-time, and a source that can no longer be planned
// from must not fail a restore that is already done. An unfinished one is
// claimed the way the REST door claims a leftover, so a rollback of the
// attempt that created it yields rather than deleting what this run finishes,
// then gets the volumes it is still missing and its placement.
func finishRestoreLeftover(
	ctx context.Context, run *runContext, args restoreArgs, rdName string, snap *apiv1.Snapshot,
	progress store.LeftoverProgress,
) error {
	// Done before, and left alone. Said out loud: a re-run that exits 0
	// with nothing on the screen reads as a restore that just happened.
	if progress == store.LeftoverFinished {
		return answerFinishedLeftover(ctx, run, rdName, snap.ResourceName, snap.Name)
	}

	planned, err := planRestoredReplicas(ctx, run, args.fromResource, rdName, snap)
	if err != nil {
		return err
	}

	err = store.ClaimAdoptedLeftover(ctx, run.Store, rdName, snap)
	if err != nil {
		return fmt.Errorf("restore into %s: %w", rdName, err)
	}

	err = hydrateMissingVolumes(ctx, run, rdName, snap)
	if errors.Is(err, errVolumeGoneOnReadBack) {
		// Nothing rolls a resume back, so the way out is said here.
		return fmt.Errorf("%w; %s", err, restoreAgain(run))
	}

	if err != nil {
		return err
	}

	return placeOverMarkedTarget(ctx, run, rdName, snap, planned)
}

// answerFinishedLeftover reports a finished leftover of this restore as
// restored, after taking the adoption mark the way the REST replay does
// (store.ClaimAdoptedLeftover with no snapshot, recording nothing). The mark is
// what tells the attempt that created the definition, if it is still rolling
// back, that somebody was told it is done; without it that rollback deletes
// the definition this run just vouched for. A creator already rolling back
// refuses the claim, and the run then reports the restore as not done.
func answerFinishedLeftover(ctx context.Context, run *runContext, rdName, fromResource, fromSnapshot string) error {
	err := store.ClaimAdoptedLeftover(ctx, run.Store, rdName, nil)
	if err != nil {
		return fmt.Errorf("restore into %s: %w", rdName, err)
	}

	return sayAlreadyRestored(run, rdName, fromResource, fromSnapshot)
}

// hydrateMissingVolumes creates the snapshot's volumes a definition of this
// restore's does not hold yet, and keeps the ones it does: a volume already
// there at the snapshot's size is the restore's own, whether an earlier run or
// a concurrent one wrote it, and so is one grown since; a smaller one is not,
// and is refused.
func hydrateMissingVolumes(ctx context.Context, run *runContext, rdName string, snap *apiv1.Snapshot) error {
	for i := range snap.VolumeDefinitions {
		svd := &snap.VolumeDefinitions[i]

		err := run.Store.VolumeDefinitions().Create(ctx, rdName, &apiv1.VolumeDefinition{
			VolumeNumber: svd.VolumeNumber,
			SizeKib:      svd.SizeKib,
		})
		if errors.Is(err, store.ErrAlreadyExists) {
			err = volumeAlreadyRestored(ctx, run, rdName, svd)
		}

		if err != nil {
			return fmt.Errorf("restore volume %d onto %s: %w", svd.VolumeNumber, rdName, err)
		}
	}

	return nil
}

// errVolumeGoneOnReadBack reports a volume the create said exists and the
// read-back did not find: it was deleted in between, which is not a taken
// number, and the restore can simply be run again.
var errVolumeGoneOnReadBack = errors.New("the create said it exists, but it was gone when read back")

// volumeAlreadyRestored accepts an existing volume as the one the snapshot
// would have written when it is at least that volume's size: grown is the
// restore's own volume expanded since, the way store.AssessLeftover and the
// REST door judge it, while smaller is somebody else's.
func volumeAlreadyRestored(ctx context.Context, run *runContext, rdName string, svd *apiv1.SnapshotVolumeDef) error {
	vds, err := run.Store.VolumeDefinitions().List(ctx, rdName)
	if err != nil {
		return fmt.Errorf("read back the existing volume: %w", err)
	}

	for i := range vds {
		if vds[i].VolumeNumber != svd.VolumeNumber {
			continue
		}

		if vds[i].SizeKib < svd.SizeKib {
			return fmt.Errorf("%w: it holds %d KiB where the snapshot recorded %d KiB",
				errVolumeNumberTaken, vds[i].SizeKib, svd.SizeKib)
		}

		return nil
	}

	return errVolumeGoneOnReadBack
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
//
// The definition this command created can be adopted by another request while
// it runs, which then finishes it and may already have answered for it. The
// rollback holds the same handshake the REST door does (store.HoldRollback)
// before it deletes anything, and leaves an adopted definition to its adopter.
// One that already reads as finished is left too: a re-run that found it so
// reported it restored without adopting it.
func rollbackRestore(
	ctx context.Context, run *runContext, rdName string, planned []apiv1.Resource, cause error,
) error {
	err := store.HoldRollback(ctx, run.Store, rdName, true)
	if errors.Is(err, store.ErrRollbackGone) {
		return cause
	}

	if errors.Is(err, store.ErrRollbackYielded) {
		return fmt.Errorf("%w (%s was not rolled back, because another request marked it adopted; "+
			"if that request did not finish it either, %s)", cause, rdName, restoreAgain(run))
	}

	if errors.Is(err, store.ErrRollbackAnswered) {
		return answeredRestoreLeft(ctx, run, rdName, planned, cause)
	}

	if err != nil {
		return fmt.Errorf("%w (%s was not rolled back, since whether another request took it over "+
			"or reported it restored could not be checked: %w; %s to finish it)",
			cause, rdName, err, restoreAgain(run))
	}

	err = store.EnterDestructiveRollback(ctx, run.Store, rdName, store.RollbackStepDeleteDefinition)
	if errors.Is(err, store.ErrRollbackGone) {
		return cause
	}

	if err != nil {
		return fmt.Errorf("%w (%s was not rolled back: %w; %s to finish it)",
			cause, rdName, err, restoreAgain(run))
	}

	err = run.Store.ResourceDefinitions().Delete(ctx, rdName)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		// The definition stays, so the mark that says it is being rolled
		// back must not: nothing is rolling it back now, and a retry that
		// met the mark would be refused for good instead of finishing it.
		releaseErr := store.ReleaseRollback(ctx, run.Store, rdName)
		if releaseErr != nil {
			return fmt.Errorf("%w (rolling back %s also failed: %w; it still carries the rollback mark, "+
				"so delete it by hand: %w)", cause, rdName, err, releaseErr)
		}

		return fmt.Errorf("%w (rolling back %s also failed: %w; %s to finish it)",
			cause, rdName, err, restoreAgain(run))
	}

	// A volume that went away under the read-back is a race, not a refusal;
	// with the target rolled back, the same command starts it over.
	if errors.Is(cause, errVolumeGoneOnReadBack) {
		return fmt.Errorf("%w (%s was rolled back; run the same command again)", cause, rdName)
	}

	return cause
}

// restoreAgain names the restore that finishes a definition the rollback left,
// as a command rather than "the same command again": `rd clone` runs this
// restore under the hood and refuses a target that already exists, so
// re-running the clone is a dead end, while the restore of its internal
// snapshot resumes it. The nodes and the pool the operator chose go with it:
// without them the resume places a replica on every node holding the snapshot,
// or finds no pool where the operator had to name one.
func restoreAgain(run *runContext) string {
	cmd := "blockstor snapshot resource restore --from-resource " + run.Flags.Values["from-resource"] +
		" --from-snapshot " + run.Flags.Values["from-snapshot"] +
		" --to-resource " + run.Flags.Values["to-resource"]

	if len(run.Flags.Nodes) > 0 {
		cmd += " --nodes " + strings.Join(run.Flags.Nodes, ",")
	}

	if pool := run.Flags.Values["storage-pool"]; pool != "" {
		cmd += " --storage-pool " + pool
	}

	return "run `" + cmd + "`"
}

// placeOverExisting places a restore's replicas onto a definition that was
// already there, prepared or this restore's own leftover. Nothing rolls such a
// placement back, and once one replica has landed a re-run judges the
// definition finished and places nothing, so a failure names the replicas
// that never landed and the command that places them.
func placeOverExisting(ctx context.Context, run *runContext, planned []apiv1.Resource) error {
	err := stampRestoredReplicas(ctx, run, planned)
	if err == nil || len(planned) == 0 {
		return err
	}

	rdName := planned[0].Name

	missing, missErr := store.MissingReplicas(ctx, run.Store, rdName, plannedNodes(planned))
	if missErr != nil || len(missing) == 0 || len(missing) == len(planned) {
		return err
	}

	return fmt.Errorf("%w (%s has no replica on %s, which a re-run will not place: "+
		"run `blockstor resource create <node> %s` for each)", err, rdName, strings.Join(missing, ", "), rdName)
}

// plannedNodes names the nodes a restore planned replicas on.
func plannedNodes(planned []apiv1.Resource) []string {
	nodes := make([]string, 0, len(planned))
	for i := range planned {
		nodes = append(nodes, planned[i].NodeName)
	}

	return nodes
}

// answeredRestoreLeft is the error for a definition the rollback left because
// it already reads as finished. A re-run judges it finished too and places
// nothing, so a replica this run planned and never placed is named with the
// command that places it, rather than left for a re-run that will not.
func answeredRestoreLeft(
	ctx context.Context, run *runContext, rdName string, planned []apiv1.Resource, cause error,
) error {
	missing, err := store.MissingReplicas(ctx, run.Store, rdName, plannedNodes(planned))
	if err != nil {
		return fmt.Errorf("%w (%s was not rolled back: it already holds every volume and a replica, "+
			"and another run may have reported it restored; which of its replicas are missing "+
			"could not be read: %w)", cause, rdName, err)
	}

	if len(missing) == 0 {
		return fmt.Errorf("%w (%s was not rolled back: it already holds every volume and a replica, "+
			"and another run may have reported it restored)", cause, rdName)
	}

	return fmt.Errorf("%w (%s was not rolled back: it already holds every volume and a replica, "+
		"and another run may have reported it restored; it has no replica on %s, which a re-run "+
		"will not place: run `blockstor resource create <node> %s` for each)",
		cause, rdName, strings.Join(missing, ", "), rdName)
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

	// Read past the cache, for the marker below.
	target, err := run.Store.ResourceDefinitions().GetUncached(ctx, args.toResource)
	if err != nil {
		return fmt.Errorf("get resource definition %s: %w", args.toResource, err)
	}

	// A definition being deleted is refused, as `s resource restore` refuses
	// it through store.PreparedRestoreTarget: the volumes written here would
	// be reaped as they land, and the command would report a restore that is
	// not there.
	if slices.Contains(target.Flags, apiv1.ResourceFlagDelete) {
		return fmt.Errorf("%w: %s; wait for the delete to finish, then restore into a fresh definition",
			errTargetBeingDeleted, args.toResource)
	}

	// A definition carrying a restore marker is being restored or cloned by
	// another operation, which hydrates its volumes itself and keeps one of
	// its own snapshot's size. Writing them here as well lets this command
	// fail on a later volume and unwind one that operation answered for.
	// The marker goes on with the definition, so it is always seen.
	if marker := target.Props[store.RestoreFromSnapshotProp]; marker != "" {
		return fmt.Errorf("%w: %s carries %q; restore it with `s resource restore` instead",
			errTargetBeingRestored, args.toResource, marker)
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
			//
			// A volume a concurrent restore of the same snapshot wrote is a
			// failure here too, not one to accept: both create in the
			// snapshot's order, so the run that loses fails on the first
			// volume with nothing added, and its unwind takes nothing the
			// other run answered for. Accepting it lets the loser go on,
			// fail later, and unwind volumes the winner is using.
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

// planRestoredReplicas decides the replicas a restore places, without
// writing anything.
//
// They land on the nodes that HOLD THE SNAPSHOT, in the same pool the
// source replica uses there — never via the autoplacer. A restored
// replica on a different backend makes the satellite pipe the
// snapshot stream into the wrong receiver, which never converges.
// Explicit `--nodes` values win when the operator gave them.
func planRestoredReplicas(
	ctx context.Context, run *runContext, srcRD, rdName string, snap *apiv1.Snapshot,
) ([]apiv1.Resource, error) {
	nodes := run.Flags.Nodes

	// Named nodes have to actually hold the snapshot. Restoring onto one
	// that does not produces a replica with nothing behind it: the objects
	// are created, the satellite finds no snapshot to receive, and the
	// command reports success over an empty volume.
	err := validate.RestoreNodesHoldSnapshot(nodes, snap.Nodes)
	if err != nil {
		//nolint:wrapcheck // a semantic refusal; see checkVolumeSize
		return nil, err
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
		return nil, fmt.Errorf("%s names no node to restore %s onto: %w",
			snap.Name, rdName, errNothingToRestore)
	}

	planned := make([]apiv1.Resource, 0, len(nodes))

	for _, node := range nodes {
		res := apiv1.Resource{Name: rdName, NodeName: node}

		pool, err := sourcePoolOn(ctx, run, srcRD, node)
		if err != nil {
			return nil, err
		}

		stampProp(&res, storPoolNameProp, pool)

		planned = append(planned, res)
	}

	return planned, nil
}

// errReplicaStillDeleting refuses a restore whose replica on a node is
// still being torn down: counting it as placed would report a restore
// with no live replica there.
var errReplicaStillDeleting = errors.New("a replica of the restore target is still being deleted")

// stampRestoredReplicas creates the planned replicas. One already on its
// node is the replica an earlier run of this restore placed and is taken
// as placed, unless it is being deleted.
func stampRestoredReplicas(ctx context.Context, run *runContext, planned []apiv1.Resource) error {
	for i := range planned {
		err := stampRestoredReplica(ctx, run, &planned[i])
		if err != nil {
			return err
		}
	}

	return nil
}

// stampRestoredReplica places one replica. An existing one is the restore's
// own unless it is being deleted or is an operator's diskless replica. A
// tie-breaker is the controller's witness and is promoted to the replica the
// restore wanted there; see promoteWitness. One that the create collided with and that
// is gone when read back was deleted in between, and is created again, the way
// the REST door does; a collision that keeps recurring is an error.
func stampRestoredReplica(ctx context.Context, run *runContext, res *apiv1.Resource) error {
	for range replicaCollisionAttempts {
		err := run.Store.Resources().Create(ctx, res)
		if err == nil {
			return nil
		}

		if !errors.Is(err, store.ErrAlreadyExists) {
			return fmt.Errorf("create restored replica %s on %s: %w", res.Name, res.NodeName, err)
		}

		existing, getErr := store.GetResourceUncached(ctx, run.Store, res.Name, res.NodeName)
		if errors.Is(getErr, store.ErrNotFound) {
			continue
		}

		if getErr != nil {
			return fmt.Errorf("read replica %s on %s: %w", res.Name, res.NodeName, getErr)
		}

		if slices.Contains(existing.Flags, apiv1.ResourceFlagDelete) {
			return fmt.Errorf("%w: %s on %s", errReplicaStillDeleting, res.Name, res.NodeName)
		}

		if slices.Contains(existing.Flags, apiv1.ResourceFlagTieBreaker) {
			return promoteWitness(ctx, run, res)
		}

		if !store.HoldsData(&existing) {
			return fmt.Errorf("%w: %s on %s; delete it, then retry", errReplicaHoldsNoData, res.Name, res.NodeName)
		}

		return nil
	}

	return fmt.Errorf("%w: %s on %s", errReplicaCollisionRecurs, res.Name, res.NodeName)
}

// promoteWitness turns the controller's tie-breaker witness on a node the
// restore places onto into the replica it asked for, the way the REST door's
// autoplace promotes one. Its volumes are then restored from the snapshot like
// any other replica of a marked target.
func promoteWitness(ctx context.Context, run *runContext, res *apiv1.Resource) error {
	err := run.Store.Resources().PatchResourceSpec(ctx, res.Name, res.NodeName, func(live *apiv1.Resource) error {
		if slices.Contains(live.Flags, apiv1.ResourceFlagDelete) {
			return fmt.Errorf("%w: %s on %s", errReplicaStillDeleting, res.Name, res.NodeName)
		}

		if !slices.Contains(live.Flags, apiv1.ResourceFlagTieBreaker) {
			return fmt.Errorf("%w: %s on %s is no longer the controller's witness", errReplicaHoldsNoData, res.Name, res.NodeName)
		}

		live.Flags, _ = apiv1.PromoteWitnessFlags(live.Flags, true)

		if pool := res.Props["StorPoolName"]; pool != "" {
			if live.Props == nil {
				live.Props = map[string]string{}
			}

			live.Props["StorPoolName"] = pool
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("promote the witness of %s on %s: %w", res.Name, res.NodeName, err)
	}

	return nil
}

// replicaCollisionAttempts bounds the create-then-read-back loop for a
// replica that keeps colliding with one that is gone when read back.
const replicaCollisionAttempts = 3

// errTargetBeingRestored refuses a volume-definition restore into a
// definition another restore or clone has marked as its own.
var errTargetBeingRestored = errors.New("the definition is the target of a restore or clone")

// errTargetBeingDeleted refuses a volume-definition restore into a definition
// being deleted.
var errTargetBeingDeleted = errors.New("the definition is being deleted")

// errReplicaHoldsNoData refuses a restore whose replica on a node already
// exists diskless: counted as placed, the restore reports success with no copy
// of the data there.
var errReplicaHoldsNoData = errors.New("a replica of the restore target is diskless and holds no copy of the data")

// errReplicaCollisionRecurs reports a replica create that kept colliding with
// a replica nobody could read back.
var errReplicaCollisionRecurs = errors.New("the replica create kept colliding with a replica that was gone when read back")

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
