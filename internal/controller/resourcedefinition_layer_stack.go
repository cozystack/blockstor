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
	if len(rd.Spec.LayerStack) > 0 {
		return r.refreshUnappliedGroupStack(ctx, rd)
	}

	if rd.Spec.ResourceGroupName == "" {
		return nil
	}

	rg, err := r.readGroup(ctx, rd)
	if err != nil || rg == nil {
		return err
	}

	groupStack := rg.Spec.SelectFilter.LayerStack
	if len(groupStack) == 0 {
		return nil
	}

	stack, err := r.stackToPin(ctx, rd, groupStack)
	if err != nil {
		return err
	}

	base := rd.DeepCopy()
	rd.Spec.LayerStack = slices.Clone(stack)

	unapplied := apiv1.LacksLayerOf(stack, groupStack)
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
		"groupStack", groupStack, "shortOfGroup", unapplied)

	if unapplied && r.Recorder != nil {
		r.Recorder.Eventf(rd, nil, corev1.EventTypeWarning, layerStackNotInheritedReason, layerStackPinAction,
			"already holds data under layer stack %s, so the stack %s of resource group %s was not applied; migrate the data to a new volume to take it",
			strings.Join(stack, ","), strings.Join(groupStack, ","), rg.Name)
	}

	return nil
}

// readGroup reads the RD's resource group, nil when it does not exist. The
// RD keeps the group's name as the operator spelled it; the CR is stored
// under the folded one.
func (r *ResourceDefinitionReconciler) readGroup(
	ctx context.Context, rd *blockstoriov1alpha1.ResourceDefinition,
) (*blockstoriov1alpha1.ResourceGroup, error) {
	var rg blockstoriov1alpha1.ResourceGroup

	err := r.directOrCached().Get(ctx, client.ObjectKey{Name: k8s.Name(rd.Spec.ResourceGroupName)}, &rg)
	if apierrors.IsNotFound(err) {
		return nil, nil //nolint:nilnil // no group is an answer, not an error
	}

	if err != nil {
		return nil, errors.Wrapf(err, "read resource group %q", rd.Spec.ResourceGroupName)
	}

	return &rg, nil
}

// refreshUnappliedGroupStack keeps the annotation on a pinned RD true to its
// group as the group is now: the layers the group asks for while the RD's
// stack lacks one of them, and no annotation once it no longer does or the
// group is gone. An RD without the annotation is left alone; it was never
// short of its group when its stack was decided.
func (r *ResourceDefinitionReconciler) refreshUnappliedGroupStack(
	ctx context.Context, rd *blockstoriov1alpha1.ResourceDefinition,
) error {
	recorded, flagged := rd.Annotations[apiv1.UnappliedGroupLayerStackAnnotation]
	if !flagged {
		return nil
	}

	want := ""

	if rd.Spec.ResourceGroupName != "" {
		rg, err := r.readGroup(ctx, rd)
		if err != nil {
			return err
		}

		if rg != nil && apiv1.LacksLayerOf(rd.Spec.LayerStack, rg.Spec.SelectFilter.LayerStack) {
			want = strings.Join(rg.Spec.SelectFilter.LayerStack, ",")
		}
	}

	if want == recorded {
		return nil
	}

	base := rd.DeepCopy()
	if want == "" {
		delete(rd.Annotations, apiv1.UnappliedGroupLayerStackAnnotation)
	} else {
		rd.Annotations[apiv1.UnappliedGroupLayerStackAnnotation] = want
	}

	err := r.Patch(ctx, rd, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}))
	if err != nil {
		return errors.Wrapf(err, "refresh the unapplied group stack of %q", rd.Name)
	}

	return nil
}

// stackToPin is the group's stack for an RD with nothing on disk, and
// the default stack, the one an empty stack was built as, for an RD
// that already holds data.
func (r *ResourceDefinitionReconciler) stackToPin(ctx context.Context, rd *blockstoriov1alpha1.ResourceDefinition, groupStack []string) ([]string, error) {
	if rdInitialized(rd) {
		return apiv1.DefaultLayerStack(), nil
	}

	holdsData, err := r.rdHoldsData(ctx, rd.Name)
	if err != nil {
		return nil, err
	}

	if holdsData {
		return apiv1.DefaultLayerStack(), nil
	}

	return groupStack, nil
}

// rdHoldsData reports whether any replica of the RD has gone to disk:
// DRBD metadata created, or a volume the satellite reported a device
// for. Either means the storage under it was built with the stack the
// RD carried then. The caller checks the Initialized latch first, which
// covers the case no replica has reported yet: an RD adopted by
// linstor-migrate, or one whose data was written before its replicas'
// status was.
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
