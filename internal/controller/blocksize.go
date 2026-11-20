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

package controller

import (
	"context"
	"maps"
	"slices"

	cerrors "github.com/cockroachdb/errors"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/blocksize"
	"github.com/cozystack/blockstor/pkg/naming"
	"github.com/cozystack/blockstor/pkg/store"
)

const (
	// blockSizeIncompatibleReason is the Warning Event a replica gets
	// when LINSTOR would have refused to create it over its block size.
	blockSizeIncompatibleReason = "IncompatibleBlockSize"

	// blockSizeAction names the step in the Event.
	blockSizeAction = "DecideBlockSize"

	// rolePrimary is the DRBD role a replica in use reports.
	rolePrimary = "Primary"
)

// awaitingIdentity reports whether a replica has not been given its DRBD
// identity yet: a replica the controller is seeing for the first time. A
// replica adopted from LINSTOR arrives with its identity already set.
func awaitingIdentity(res *blockstoriov1alpha1.Resource) bool {
	return res.Spec.DRBDNodeID == nil && res.Status.DRBDNodeID == nil
}

// hasIdentity is the opposite: the replica was allocated, so the
// satellite may already run it.
func hasIdentity(res *blockstoriov1alpha1.Resource) bool {
	return !awaitingIdentity(res)
}

// replicaDiskless reports whether a replica has no backing disk.
func replicaDiskless(res *blockstoriov1alpha1.Resource) bool {
	return slices.Contains(res.Spec.Flags, apiv1.ResourceFlagDiskless) ||
		slices.Contains(res.Spec.Flags, apiv1.ResourceFlagTieBreaker)
}

// replicaPrimary reports whether a replica is, or reports being, in use.
func replicaPrimary(res *blockstoriov1alpha1.Resource) bool {
	return res.Status.InUse || res.Status.Role == rolePrimary
}

// blockSizeScope is everything above a volume definition that
// Linstor/Drbd/auto-block-size can be set on.
type blockSizeScope struct {
	rd   *blockstoriov1alpha1.ResourceDefinition
	rg   *blockstoriov1alpha1.ResourceGroup
	ctrl *blockstoriov1alpha1.ControllerConfig
}

// auto resolves the switch for one volume definition, in LINSTOR's
// order: volume definition, resource definition, volume group, resource
// group, controller.
func (s *blockSizeScope) auto(vd *blockstoriov1alpha1.ResourceDefinitionVolume) bool {
	levels := []map[string]string{vd.Props, s.rd.Spec.Props, s.rd.Spec.ExtraProps}

	if s.rg != nil {
		for i := range s.rg.Spec.VolumeGroups {
			if s.rg.Spec.VolumeGroups[i].VolumeNumber == vd.VolumeNumber {
				levels = append(levels, s.rg.Spec.VolumeGroups[i].Props)
			}
		}

		levels = append(levels, s.rg.Spec.Props, s.rg.Spec.ExtraProps)
	}

	if s.ctrl != nil {
		levels = append(levels, s.ctrl.Spec.ExtraProps)
	}

	return blocksize.AutoEnabled(levels...)
}

// allAuto is LINSTOR's definition-wide switch: on only when every volume
// definition has it on.
func (s *blockSizeScope) allAuto() bool {
	for i := range s.rd.Spec.VolumeDefinitions {
		if !s.auto(&s.rd.Spec.VolumeDefinitions[i]) {
			return false
		}
	}

	return true
}

// loadBlockSizeScope reads the definition's group and the controller
// configuration through reader. Either may be absent.
func loadBlockSizeScope(ctx context.Context, reader client.Reader, rd *blockstoriov1alpha1.ResourceDefinition) (*blockSizeScope, error) {
	scope := &blockSizeScope{rd: rd}

	if rd.Spec.ResourceGroupName != "" {
		var rg blockstoriov1alpha1.ResourceGroup

		err := reader.Get(ctx, client.ObjectKey{Name: naming.Key(rd.Spec.ResourceGroupName)}, &rg)
		switch {
		case err == nil:
			scope.rg = &rg
		case !errors.IsNotFound(err):
			return nil, cerrors.Wrapf(err, "read resource group %q", rd.Spec.ResourceGroupName)
		}
	}

	var cfg blockstoriov1alpha1.ControllerConfig

	err := reader.Get(ctx, client.ObjectKey{Name: blockstoriov1alpha1.ControllerConfigName}, &cfg)
	switch {
	case err == nil:
		scope.ctrl = &cfg
	case !errors.IsNotFound(err):
		return nil, cerrors.Wrap(err, "read controller config")
	}

	return scope, nil
}

// decideBlockSize runs LINSTOR's block size rule for a replica that is
// being created, before it gets its DRBD identity. The satellite does not
// bring a replica up until that identity exists, and drbdsetup never
// changes the block size of an attached device, so the size has to be on
// the volume definition before the first attach or the device keeps
// DRBD's default, 512 on a zvol.
//
// LINSTOR does this when it creates the replica (rscMinIoSizeCheck): a
// definition with no size takes the size of the replica's pool, a larger
// pool raises it while the size can still change, and a pool needing more
// than a size that may no longer change refuses the replica. Two
// differences, both because blockstor's replica already exists and its
// peers may already run:
//
//   - A volume definition with no size whose other replicas already have
//     their identity predates this rule: its devices run at DRBD's
//     default. It gets that default written down, so the new replica
//     matches its peers and the geometry does not change under the data
//     on the next attach.
//   - A raise while a diskful peer already has its identity would need
//     LINSTOR's DRBD restart of every replica, which blockstor does not
//     do; it is reported like a refusal instead, and the size stays.
func (r *ResourceReconciler) decideBlockSize(ctx context.Context, target *blockstoriov1alpha1.Resource) error {
	mu := r.rdAllocMu(target.Spec.ResourceDefinitionName)
	mu.Lock()
	defer mu.Unlock()

	var rd blockstoriov1alpha1.ResourceDefinition

	err := r.apiReader().Get(ctx, client.ObjectKey{Name: naming.Key(target.Spec.ResourceDefinitionName)}, &rd)
	if err != nil {
		return client.IgnoreNotFound(err)
	}

	// A restored or cloned definition has its source's size, copied from
	// the snapshot before any replica was placed; LINSTOR runs no block
	// size check for a restored replica, and the data is laid out at that
	// size whatever the pool reports.
	if len(rd.Spec.VolumeDefinitions) == 0 || rd.Spec.Props[store.RestoreFromSnapshotProp] != "" {
		return nil
	}

	scope, in, err := r.blockSizeInputsFor(ctx, target, &rd)
	if err != nil {
		return err
	}

	base := rd.DeepCopy()
	changed := false

	for i := range rd.Spec.VolumeDefinitions {
		vd := &rd.Spec.VolumeDefinitions[i]
		if !scope.auto(vd) {
			continue
		}

		size, ok := r.blockSizeFor(ctx, target, vd, in)
		if !ok {
			continue
		}

		if vd.Props == nil {
			vd.Props = map[string]string{}
		}

		vd.Props[blocksize.PropBlockSize] = blocksize.Format(size)
		changed = true
	}

	if !changed {
		return nil
	}

	err = r.Patch(ctx, &rd, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}))
	if err != nil {
		return cerrors.Wrapf(err, "write block size of %q", rd.Name)
	}

	return nil
}

// blockSizeInputsFor reads what the decision for a new replica needs
// beyond the definition: its other replicas, the levels the switch can be
// set on, and the size the replica's pool needs.
func (r *ResourceReconciler) blockSizeInputsFor(
	ctx context.Context, target *blockstoriov1alpha1.Resource, rd *blockstoriov1alpha1.ResourceDefinition,
) (*blockSizeScope, blockSizeInputs, error) {
	peers, err := r.otherReplicas(ctx, target)
	if err != nil {
		return nil, blockSizeInputs{}, err
	}

	scope, err := loadBlockSizeScope(ctx, r.apiReader(), rd)
	if err != nil {
		return nil, blockSizeInputs{}, err
	}

	replicaSize, err := r.replicaIoSize(ctx, target, rd)
	if err != nil {
		return nil, blockSizeInputs{}, err
	}

	return scope, blockSizeInputs{
		replicaSize: replicaSize,
		canChange:   canChangeBlockSize(scope, peers),
		running: slices.ContainsFunc(peers, func(p blockstoriov1alpha1.Resource) bool {
			return hasIdentity(&p)
		}),
		diskfulRunning: slices.ContainsFunc(peers, func(p blockstoriov1alpha1.Resource) bool {
			return hasIdentity(&p) && !replicaDiskless(&p)
		}),
	}, nil
}

// blockSizeInputs is the replica-wide part of the decision.
type blockSizeInputs struct {
	replicaSize    int64
	canChange      bool
	running        bool
	diskfulRunning bool
}

// blockSizeFor decides one volume definition, reporting the size to write
// and whether to write it.
func (r *ResourceReconciler) blockSizeFor(
	ctx context.Context, target *blockstoriov1alpha1.Resource,
	vd *blockstoriov1alpha1.ResourceDefinitionVolume, in blockSizeInputs,
) (int64, bool) {
	_, has := blocksize.Parse(vd.Props[blocksize.PropBlockSize])

	if !has && in.running {
		return blocksize.DefaultIoSize, true
	}

	decision := blocksize.Decide(blocksize.Input{
		VDProps:     vd.Props,
		Auto:        true,
		ReplicaSize: in.replicaSize,
		CanChange:   in.canChange,
	})

	if decision.Verdict == blocksize.Set && has && in.diskfulRunning {
		decision = blocksize.Decision{Verdict: blocksize.Incompatible}
	}

	switch decision.Verdict {
	case blocksize.Set:
		return decision.Size, true
	case blocksize.Incompatible:
		r.reportIncompatibleBlockSize(ctx, target, vd, in.replicaSize)
	case blocksize.Keep:
	}

	return 0, false
}

// reportIncompatibleBlockSize logs and, with a recorder wired, raises a
// Warning Event on the replica.
func (r *ResourceReconciler) reportIncompatibleBlockSize(
	ctx context.Context, target *blockstoriov1alpha1.Resource,
	vd *blockstoriov1alpha1.ResourceDefinitionVolume, replicaSize int64,
) {
	current := vd.Props[blocksize.PropBlockSize]
	if current == "" {
		current = blocksize.Format(blocksize.DefaultIoSize)
	}

	logf.FromContext(ctx).Info("storage pool needs a larger block size than the volume definition may take",
		"resource", target.Name, "volume", vd.VolumeNumber, "blockSize", current, "poolMinIoSize", replicaSize)

	if r.Recorder == nil {
		return
	}

	r.Recorder.Eventf(target, nil, corev1.EventTypeWarning, blockSizeIncompatibleReason, blockSizeAction,
		"volume %d keeps block size %s although the storage pool needs %d: the size is frozen, a replica is Primary, or another diskful replica already runs at it; LINSTOR refuses to create such a replica",
		vd.VolumeNumber, current, replicaSize)
}

// canChangeBlockSize is LINSTOR's canChangeMinIoSize over the replicas
// the definition already has.
func canChangeBlockSize(scope *blockSizeScope, peers []blockstoriov1alpha1.Resource) bool {
	if !scope.allAuto() {
		return false
	}

	if len(peers) == 0 {
		return true
	}

	if slices.ContainsFunc(peers, func(p blockstoriov1alpha1.Resource) bool { return replicaPrimary(&p) }) {
		return false
	}

	for i := range scope.rd.Spec.VolumeDefinitions {
		if blocksize.Frozen(scope.rd.Spec.VolumeDefinitions[i].Props) {
			return false
		}
	}

	return true
}

// otherReplicas lists the definition's other replicas, read past the
// cache so a replica created a moment ago counts.
func (r *ResourceReconciler) otherReplicas(ctx context.Context, target *blockstoriov1alpha1.Resource) ([]blockstoriov1alpha1.Resource, error) {
	var list blockstoriov1alpha1.ResourceList

	err := r.apiReader().List(ctx, &list)
	if err != nil {
		return nil, cerrors.Wrap(err, "list resources")
	}

	out := make([]blockstoriov1alpha1.Resource, 0, len(list.Items))

	for i := range list.Items {
		if list.Items[i].Name == target.Name {
			continue
		}

		if naming.Same(list.Items[i].Spec.ResourceDefinitionName, target.Spec.ResourceDefinitionName) {
			out = append(out, list.Items[i])
		}
	}

	return out, nil
}

// replicaIoSize is the size a replica counts as: 4096 when its stack has
// a layer other than DRBD and STORAGE, the default for a diskless one,
// and otherwise the largest minimum I/O size among the pools it uses.
func (r *ResourceReconciler) replicaIoSize(
	ctx context.Context, target *blockstoriov1alpha1.Resource, rd *blockstoriov1alpha1.ResourceDefinition,
) (int64, error) {
	if blocksize.HasSpecialLayers(rd.Spec.LayerStack) {
		return blocksize.SpecialLayerIoSize, nil
	}

	if replicaDiskless(target) {
		return blocksize.DefaultIoSize, nil
	}

	names := replicaPoolNames(target, rd)
	if len(names) == 0 {
		return blocksize.DefaultIoSize, nil
	}

	var pools blockstoriov1alpha1.StoragePoolList

	err := r.apiReader().List(ctx, &pools)
	if err != nil {
		return 0, cerrors.Wrap(err, "list storage pools")
	}

	size := blocksize.DefaultIoSize

	for i := range pools.Items {
		pool := &pools.Items[i]
		if !naming.Same(pool.Spec.NodeName, target.Spec.NodeName) {
			continue
		}

		if !slices.ContainsFunc(names, func(n string) bool { return naming.Same(n, pool.Spec.PoolName) }) {
			continue
		}

		size = max(size, blocksize.PoolIoSize(pool.Spec.Props))
	}

	return size, nil
}

// replicaPoolNames lists the pools a diskful replica's volumes land in,
// resolved the way the dispatcher does: a volume definition's own
// StorPoolName, else the replica's pool, else the definition's.
func replicaPoolNames(target *blockstoriov1alpha1.Resource, rd *blockstoriov1alpha1.ResourceDefinition) []string {
	fallback := target.Spec.StoragePool
	if fallback == "" {
		fallback = target.Spec.Props["StorPoolName"]
	}

	if fallback == "" {
		fallback = rd.Spec.Props["StorPoolName"]
	}

	var names []string

	for i := range rd.Spec.VolumeDefinitions {
		name := rd.Spec.VolumeDefinitions[i].Props["StorPoolName"]
		if name == "" {
			name = fallback
		}

		if name != "" && !slices.Contains(names, name) {
			names = append(names, name)
		}
	}

	return names
}

// freezeBlockSize stamps Linstor/Drbd/freeze-block-size on every volume
// definition with the automatic size on, once a replica is in use. That
// is when LINSTOR does it: the first in-use report from a satellite. A
// filesystem made on the device from then on depends on the size.
func (r *ResourceReconciler) freezeBlockSize(ctx context.Context, target *blockstoriov1alpha1.Resource) error {
	if !replicaPrimary(target) {
		return nil
	}

	rd, err := r.lookupRD(ctx, target.Spec.ResourceDefinitionName)
	if err != nil || rd == nil || allFrozen(rd) {
		return err
	}

	// The group and the controller configuration come through the cache:
	// a replica in use reconciles on every status report, and a volume
	// with the automatic size off is never frozen, so it gets here each
	// time.
	scope, err := loadBlockSizeScope(ctx, r.Client, rd)
	if err != nil {
		return err
	}

	if !scope.needsFreeze() {
		return nil
	}

	mu := r.rdAllocMu(target.Spec.ResourceDefinitionName)
	mu.Lock()
	defer mu.Unlock()

	var fresh blockstoriov1alpha1.ResourceDefinition

	err = r.apiReader().Get(ctx, client.ObjectKey{Name: naming.Key(target.Spec.ResourceDefinitionName)}, &fresh)
	if err != nil {
		return client.IgnoreNotFound(err)
	}

	scope.rd = &fresh
	base := fresh.DeepCopy()
	changed := false

	for i := range fresh.Spec.VolumeDefinitions {
		vd := &fresh.Spec.VolumeDefinitions[i]
		if !scope.auto(vd) || blocksize.Frozen(vd.Props) {
			continue
		}

		props := maps.Clone(vd.Props)
		if props == nil {
			props = map[string]string{}
		}

		blocksize.Freeze(props)
		vd.Props = props
		changed = true
	}

	if !changed {
		return nil
	}

	err = r.Patch(ctx, &fresh, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}))
	if err != nil {
		return cerrors.Wrapf(err, "freeze block size of %q", fresh.Name)
	}

	return nil
}

// needsFreeze reports whether a volume definition with the automatic size
// on is not frozen yet.
func (s *blockSizeScope) needsFreeze() bool {
	for i := range s.rd.Spec.VolumeDefinitions {
		vd := &s.rd.Spec.VolumeDefinitions[i]
		if s.auto(vd) && !blocksize.Frozen(vd.Props) {
			return true
		}
	}

	return false
}

// allFrozen reports whether every volume definition is frozen already,
// the common case, which then costs no read.
func allFrozen(rd *blockstoriov1alpha1.ResourceDefinition) bool {
	for i := range rd.Spec.VolumeDefinitions {
		if !blocksize.Frozen(rd.Spec.VolumeDefinitions[i].Props) {
			return false
		}
	}

	return true
}
