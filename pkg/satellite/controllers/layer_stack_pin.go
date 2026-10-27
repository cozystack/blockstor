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

package controllers

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// errLayerStackNotPinned reports an RD that leaves its layer stack to its
// resource group, which the controller has not yet written onto it.
var errLayerStackNotPinned = errors.New("the resource definition's layer stack comes from its group and is not pinned yet")

// layerStackPinRequeue is how soon a replica waiting on the pin is tried
// again; the controller pins on the definition's own reconcile.
const layerStackPinRequeue = 2 * time.Second

// checkLayerStackPinned refuses an RD whose layer stack is empty while
// its resource group sets one.
//
// Every reader on the data path (the intent, the DRBD gate, the LUKS
// passphrase) reads rd.Spec.LayerStack, and an empty one there means the
// default stack, which has no LUKS. An RD created without a stack, by
// kubectl or GitOps or a door that does not copy the group's, would then
// go to disk unencrypted while the gates that resolve through the group
// called it encrypted. The stack is fixed once, by the controller, which
// knows whether the definition already holds data under the default stack
// (ResourceDefinitionReconciler.pinLayerStack); the satellite waits for
// that rather than resolve it a second time and disagree.
//
// A group that is gone, or sets no stack, leaves the default in force, as
// before. A group that cannot be read fails closed.
func (r *ResourceReconciler) checkLayerStackPinned(ctx context.Context, rd *blockstoriov1alpha1.ResourceDefinition) error {
	if len(rd.Spec.LayerStack) > 0 || rd.Spec.ResourceGroupName == "" {
		return nil
	}

	var rg blockstoriov1alpha1.ResourceGroup

	// The RD keeps the group's name as the operator spelled it; the CR is
	// stored under the folded one.
	err := r.Get(ctx, client.ObjectKey{Name: k8s.Name(rd.Spec.ResourceGroupName)}, &rg)
	if apierrors.IsNotFound(err) {
		return nil
	}

	if err != nil {
		return errors.Wrapf(err, "read resource group %q for the layer stack of %q", rd.Spec.ResourceGroupName, rd.Name)
	}

	if len(rg.Spec.SelectFilter.LayerStack) == 0 {
		return nil
	}

	return errors.Wrapf(errLayerStackNotPinned, "%q in group %q", rd.Name, rd.Spec.ResourceGroupName)
}
