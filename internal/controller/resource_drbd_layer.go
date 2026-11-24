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
	"time"

	cerrors "github.com/cockroachdb/errors"
	"k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/naming"
)

// errLayerStackUndecided reports a definition that leaves its layer stack
// to a group which sets one, before the RD controller has pinned it.
var errLayerStackUndecided = cerrors.New("the layer stack of the resource definition is not pinned yet")

// layerStackUndecidedRequeue is how soon a replica whose definition is
// not pinned yet is tried again. The RD watch usually gets there first.
const layerStackUndecidedRequeue = 2 * time.Second

// replicaUsesDRBD reports whether replicas of the named definition get a
// DRBD node-id, port and minor. LINSTOR creates those with the DRBD layer
// data of a resource, so a definition whose stack has no DRBD layer holds
// none, and blockstor allocating them anyway took ports and minors out of
// a finite range for volumes that never listen.
//
// The stack is read the way the satellite reads it: the RD's own, the
// default when neither the RD nor its group sets one. One the group sets
// but the RD controller has not pinned yet is not guessed at, since the
// pin may keep the default for a definition that already holds data;
// errLayerStackUndecided asks the caller to wait. A definition that is
// gone keeps the old behaviour and allocates.
//
// This runs on every live Resource reconcile, RD-watch and sibling
// fan-out included, so it reads through the cache. A stale copy cannot
// flip the answer: a stack is set once and never cleared, and the
// satellite applies the same gate. Only a miss goes to the apiserver,
// because a definition created a moment before its replica may not be
// cached yet, and reading that as gone would allocate for it.
func (r *ResourceReconciler) replicaUsesDRBD(ctx context.Context, rdName string) (bool, error) {
	var rd blockstoriov1alpha1.ResourceDefinition

	err := r.getCachedOrLive(ctx, rdName, &rd)
	if errors.IsNotFound(err) {
		return true, nil
	}

	if err != nil {
		return false, cerrors.Wrapf(err, "read resource definition %q", rdName)
	}

	if len(rd.Spec.LayerStack) > 0 {
		return apiv1.LayerInStack(rd.Spec.LayerStack, apiv1.LayerKindDRBD), nil
	}

	if rd.Spec.ResourceGroupName == "" {
		return true, nil
	}

	var rg blockstoriov1alpha1.ResourceGroup

	err = r.getCachedOrLive(ctx, rd.Spec.ResourceGroupName, &rg)
	if errors.IsNotFound(err) {
		return true, nil
	}

	if err != nil {
		return false, cerrors.Wrapf(err, "read resource group %q of %q", rd.Spec.ResourceGroupName, rdName)
	}

	if len(rg.Spec.SelectFilter.LayerStack) == 0 {
		return true, nil
	}

	return false, cerrors.Wrapf(errLayerStackUndecided, "%q in group %q", rdName, rd.Spec.ResourceGroupName)
}

// getCachedOrLive reads a cluster-scoped object from the cache and falls
// back to the apiserver only when the cache does not have it.
func (r *ResourceReconciler) getCachedOrLive(ctx context.Context, name string, obj client.Object) error {
	key := client.ObjectKey{Name: naming.Key(name)}

	err := r.Get(ctx, key, obj)
	if !errors.IsNotFound(err) {
		return err
	}

	return r.apiReader().Get(ctx, key, obj)
}

// allocateDRBDIDsIfUsed runs the Bug 302 allocation for a replica whose
// definition has a DRBD layer, the block size of its volume definitions
// first. done is true when the caller should
// return result instead of going on to the apply: the IDs just changed,
// or the stack is not pinned yet. A definition without DRBD gets no
// DRBD-IDs at all, and the satellite does not wait for them on it.
func (r *ResourceReconciler) allocateDRBDIDsIfUsed(
	ctx context.Context, target *blockstoriov1alpha1.Resource,
) (ctrl.Result, bool, error) {
	usesDRBD, err := r.replicaUsesDRBD(ctx, target.Spec.ResourceDefinitionName)
	if cerrors.Is(err, errLayerStackUndecided) {
		return ctrl.Result{RequeueAfter: layerStackUndecidedRequeue}, true, nil
	}

	if err != nil {
		return ctrl.Result{}, true, err
	}

	if !usesDRBD {
		return ctrl.Result{}, false, nil
	}

	// The block size goes on the volume definitions before the replica
	// gets its identity: the satellite brings nothing up before that,
	// and the size of an attached device never changes afterwards.
	if awaitingIdentity(target) {
		err = r.decideBlockSize(ctx, target)
		if err != nil {
			return ctrl.Result{}, true, err
		}
	}

	allocated, err := r.ensureDRBDIDs(ctx, target.DeepCopy(), nil)
	if err != nil {
		return ctrl.Result{}, true, err
	}

	// A definition adopted or created before counts were recorded gets
	// one once its replicas have told theirs.
	err = r.recordDefinitionPeerSlots(ctx, target.Spec.ResourceDefinitionName)
	if err != nil {
		return ctrl.Result{}, true, err
	}

	if allocated {
		return ctrl.Result{Requeue: true}, true, nil
	}

	return ctrl.Result{}, false, nil
}
