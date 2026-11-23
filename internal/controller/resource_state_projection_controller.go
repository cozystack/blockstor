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
	"encoding/json"

	"github.com/cockroachdb/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/conflict"
)

// ResourceStateProjectionFieldOwner is the SSA field manager under which
// earlier controllers wrote `Status.DrbdState=Unknown`, with
// ForceOwnership, over the replicas of an OFFLINE node.
const ResourceStateProjectionFieldOwner = "blockstor-controller-resource-projection"

// ResourceStateProjectionReleaseReconciler gives `Status.DrbdState` back
// to the satellite on every replica where an earlier controller still
// holds it under ResourceStateProjectionFieldOwner.
//
// The controller no longer writes a state of its own for an offline
// replica. Like LINSTOR, which keeps no satellite state once the
// connection closes, the read path answers such a replica with no
// state at all (pkg/api/v1/offline.go), and keeps doing so until the
// satellite process now running on the node has reported it. The
// stored write was a second owner of a field the satellite is the
// source of truth for: the satellite applies without ForceOwnership,
// so a returning satellite's every status write conflicted until the
// heartbeat watchdog flipped the node ONLINE and the claim was
// released, and its replicas kept reading Unknown meanwhile.
//
// A claim taken before the upgrade would make that conflict permanent,
// since nothing else releases it. The release is an apply of an empty
// status under the same manager: SSA drops the field the manager alone
// owned, and the satellite's next write sets it again. It does not
// depend on the node's state, so a replica whose node is still offline
// is freed too and its satellite writes on the first try when it
// returns.
type ResourceStateProjectionReleaseReconciler struct {
	client.Client

	// conflicts retries a pass that lost an optimistic-concurrency
	// race and counts such races per object.
	conflicts conflict.Retrier
}

// +kubebuilder:rbac:groups=blockstor.org,resources=resources,verbs=get;list;watch
// +kubebuilder:rbac:groups=blockstor.org,resources=resources/status,verbs=get;patch;update

// Reconcile runs one pass and retries one that lost an
// optimistic-concurrency race instead of reporting it as an error;
// see conflict.Retrier.
func (r *ResourceStateProjectionReleaseReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	return r.conflicts.Reconcile(ctx, req, r.ReconcilePass)
}

// ReconcilePass releases the projection's claim on the replica, if any.
func (r *ResourceStateProjectionReleaseReconciler) ReconcilePass(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var res blockstoriov1alpha1.Resource

	err := r.Get(ctx, req.NamespacedName, &res)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}

		return ctrl.Result{}, errors.Wrap(err, "get Resource")
	}

	if !holdsProjectionClaim(&res) {
		return ctrl.Result{}, nil
	}

	apply := &blockstoriov1alpha1.Resource{
		TypeMeta: metav1.TypeMeta{
			Kind:       resourceProjectionResourceKind,
			APIVersion: blockstoriov1alpha1.GroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{Name: res.Name},
	}

	err = r.Status().Patch(ctx, apply,
		client.Apply, //nolint:staticcheck // SA1019: applyconfiguration-gen output not yet available
		client.FieldOwner(ResourceStateProjectionFieldOwner))
	if err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}

		return ctrl.Result{}, errors.Wrapf(err, "release projection claim on %s", res.Name)
	}

	return ctrl.Result{}, nil
}

// holdsProjectionClaim reports whether the projection's field manager
// still owns any status field of the replica.
func holdsProjectionClaim(res *blockstoriov1alpha1.Resource) bool {
	for i := range res.ManagedFields {
		entry := &res.ManagedFields[i]
		if entry.Manager != ResourceStateProjectionFieldOwner || entry.FieldsV1 == nil {
			continue
		}

		var fields map[string]json.RawMessage

		err := json.Unmarshal(entry.FieldsV1.Raw, &fields)
		if err != nil || len(fields) > 0 {
			// An entry that cannot be read is released all the same:
			// the release is a no-op when nothing is held.
			return true
		}
	}

	return false
}

// resourceProjectionResourceKind mirrors the Resource CRD's Kind for
// the SSA TypeMeta header.
const resourceProjectionResourceKind = "Resource"

// SetupWithManager reconciles only replicas the projection still holds a
// field of, so the release costs nothing once every claim is gone.
func (r *ResourceStateProjectionReleaseReconciler) SetupWithManager(mgr ctrl.Manager) error {
	err := ctrl.NewControllerManagedBy(mgr).
		Named("resource-state-projection").
		For(&blockstoriov1alpha1.Resource{},
			builder.WithPredicates(predicate.NewPredicateFuncs(func(obj client.Object) bool {
				res, ok := obj.(*blockstoriov1alpha1.Resource)

				return ok && holdsProjectionClaim(res)
			}))).
		Complete(r)
	if err != nil {
		return errors.Wrap(err, "register ResourceStateProjectionReleaseReconciler")
	}

	return nil
}
