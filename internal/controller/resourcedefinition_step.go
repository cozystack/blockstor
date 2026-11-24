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

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// stepFailed reports a failed step of an RD pass. A NotFound from a
// definition that has since been deleted is the pass racing that
// delete: there is nothing left to do for it, so it ends the pass
// quietly. A conflict goes back unlogged for conflict.Retrier to
// retry. Anything else is a real failure and is logged as one.
func (r *ResourceDefinitionReconciler) stepFailed(
	ctx context.Context,
	log logr.Logger,
	rd *blockstoriov1alpha1.ResourceDefinition,
	step string,
	err error,
) (ctrl.Result, error) {
	if apierrors.IsNotFound(err) && r.rdGone(ctx, rd) {
		log.V(1).Info(step+" raced the definition's deletion", "rd", rd.Name, "error", err.Error())

		return ctrl.Result{}, nil
	}

	if !apierrors.IsConflict(err) {
		log.Error(err, step, "rd", rd.Name)
	}

	return ctrl.Result{}, err
}

// rdGone reports whether rd has been deleted, or is being deleted,
// since the pass read it. A failed read is not taken as gone.
func (r *ResourceDefinitionReconciler) rdGone(ctx context.Context, rd *blockstoriov1alpha1.ResourceDefinition) bool {
	var fresh blockstoriov1alpha1.ResourceDefinition

	err := r.directOrCached().Get(ctx, client.ObjectKeyFromObject(rd), &fresh)
	if err != nil {
		return apierrors.IsNotFound(err)
	}

	return !fresh.DeletionTimestamp.IsZero()
}
