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
	"slices"
	"strings"

	"github.com/cockroachdb/errors"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store/k8s"
)

// layerStackNotInheritedReason is the Event reason for an RD that kept
// the default stack because it already held data under it.
const layerStackNotInheritedReason = "LayerStackNotInherited"

// layerStackPinAction is the Event action: the controller fixing the
// RD's layer stack.
const layerStackPinAction = "PinLayerStack"

// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

// pinLayerStack writes the layer stack onto an RD that leaves it to its
// resource group. The stack is decided once, here, and every later
// reader takes it from the RD: the satellite waits for this pin rather
// than resolve the group itself, so a change to the group never
// reshapes a volume that already exists.
//
// An RD that holds no data yet takes the group's stack. One that does
// already went to disk under the default stack; giving it the group's
// now would have the satellite build a different stack over that data
// (luksFormat over a plaintext volume). It is pinned to the stack it
// actually has, the group's stack is recorded in an annotation, and a
// warning Event names it, so the volume reads as what it is.
func (r *ResourceDefinitionReconciler) pinLayerStack(ctx context.Context, rd *blockstoriov1alpha1.ResourceDefinition) error {
	if len(rd.Spec.LayerStack) > 0 || rd.Spec.ResourceGroupName == "" {
		return nil
	}

	reader := r.directOrCached()

	var rg blockstoriov1alpha1.ResourceGroup

	// The RD keeps the group's name as the operator spelled it; the CR is
	// stored under the folded one.
	err := reader.Get(ctx, client.ObjectKey{Name: k8s.Name(rd.Spec.ResourceGroupName)}, &rg)
	if apierrors.IsNotFound(err) {
		return nil
	}

	if err != nil {
		return errors.Wrapf(err, "read resource group %q", rd.Spec.ResourceGroupName)
	}

	groupStack := rg.Spec.SelectFilter.LayerStack
	if len(groupStack) == 0 {
		return nil
	}

	holdsData, err := r.rdHoldsData(ctx, rd.Name)
	if err != nil {
		return err
	}

	stack := groupStack
	if holdsData {
		stack = apiv1.DefaultLayerStack()
	}

	base := rd.DeepCopy()
	rd.Spec.LayerStack = slices.Clone(stack)

	unapplied := !slices.Equal(stack, groupStack)
	if unapplied {
		if rd.Annotations == nil {
			rd.Annotations = map[string]string{}
		}

		rd.Annotations[apiv1.UnappliedGroupLayerStackAnnotation] = strings.Join(groupStack, ",")
	}

	err = r.Patch(ctx, rd, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}))
	if err != nil {
		return errors.Wrapf(err, "pin layer stack of %q", rd.Name)
	}

	logf.FromContext(ctx).Info("pinned layer stack", "rd", rd.Name, "stack", stack, "group", rg.Name,
		"groupStack", groupStack, "groupStackApplied", !unapplied)

	if unapplied && r.Recorder != nil {
		r.Recorder.Eventf(rd, nil, corev1.EventTypeWarning, layerStackNotInheritedReason, layerStackPinAction,
			"already holds data under layer stack %s, so the stack %s of resource group %s was not applied; migrate the data to a new volume to take it",
			strings.Join(stack, ","), strings.Join(groupStack, ","), rg.Name)
	}

	return nil
}

// rdHoldsData reports whether any replica of the RD has gone to disk:
// DRBD metadata created, or a volume the satellite reported a device
// for. Either means the storage under it was built with the stack the
// RD carried then.
func (r *ResourceDefinitionReconciler) rdHoldsData(ctx context.Context, rdName string) (bool, error) {
	var list blockstoriov1alpha1.ResourceList

	err := r.directOrCached().List(ctx, &list)
	if err != nil {
		return false, errors.Wrapf(err, "list replicas of %q", rdName)
	}

	for i := range list.Items {
		res := &list.Items[i]
		if res.Spec.ResourceDefinitionName != rdName {
			continue
		}

		if meta.IsStatusConditionTrue(res.Status.Conditions, blockstoriov1alpha1.ConditionMetadataCreated) {
			return true, nil
		}

		for j := range res.Status.Volumes {
			if res.Status.Volumes[j].DevicePath != "" {
				return true, nil
			}
		}
	}

	return false, nil
}
