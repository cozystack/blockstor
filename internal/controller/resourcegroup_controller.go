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

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/placer"
	"github.com/cozystack/blockstor/pkg/store"
)

// AnnotationRebalancePending is re-exported here so the in-package
// reconciler can reference it without an extra import. See
// apiv1.AnnotationRGRebalancePending for the canonical definition.
const AnnotationRebalancePending = apiv1.AnnotationRGRebalancePending

// ResourceGroupReconciler watches RG CRDs and propagates spec changes
// to every spawned ResourceDefinition. The two cases that matter:
//
//   - place_count bumped (e.g. 2 → 3): the placer fills the gap on
//     each spawned RD so existing PVCs gain the new replica without
//     a manual `linstor r m` per RD.
//   - place_count reduced: we don't auto-evict — the operator picks
//     which replica to remove. Logged as a TODO once the eviction
//     reconciler grows replica selection.
//   - SelectFilter changes (storage_pool, replicas_on_*, etc.) — the
//     placer's next pass honours the new filter; existing replicas
//     not matching the constraint stay (no auto-shuffle, same
//     reason as place_count reduction).
//
// DRBD-options changes on the RG are picked up automatically by the
// option-hierarchy resolver on the next satellite reconcile, so the
// RG controller only owns the placement-side propagation.
type ResourceGroupReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// Store is the shared blockstor store (same instance used by
	// the REST server and the other reconcilers). Required for the
	// placer integration.
	Store store.Store
}

// +kubebuilder:rbac:groups=blockstor.io.blockstor.io,resources=resourcegroups,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=blockstor.io.blockstor.io,resources=resourcegroups/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=blockstor.io.blockstor.io,resources=resourcegroups/finalizers,verbs=update
// +kubebuilder:rbac:groups=blockstor.io.blockstor.io,resources=resourcedefinitions,verbs=get;list;watch
// +kubebuilder:rbac:groups=blockstor.io.blockstor.io,resources=resources,verbs=get;list;watch;create;update;patch;delete

// Reconcile finds every RD spawned from this RG and runs the placer
// to backfill replicas missing under the new spec. Idempotent: an
// RG without any change still passes through, the placer's
// already-placed accounting prevents extra Resources.
func (r *ResourceGroupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	if r.Store == nil {
		return ctrl.Result{}, nil
	}

	rg, err := r.Store.ResourceGroups().Get(ctx, req.Name)
	if err != nil {
		// Treat missing RG as deletion — child RDs handle their
		// own lifecycle.
		return ctrl.Result{}, nil //nolint:nilerr // missing RG isn't an error worth requeueing
	}

	rds, err := r.Store.ResourceDefinitions().List(ctx)
	if err != nil {
		return ctrl.Result{}, err
	}

	for i := range rds {
		if rds[i].ResourceGroupName != rg.Name {
			continue
		}

		err := r.applyRGToRD(ctx, &rg, &rds[i])
		if err != nil {
			log.Error(err, "apply RG to RD",
				"rg", rg.Name, "rd", rds[i].Name)
			// Don't bail on one RD — the next pass retries.
			continue
		}
	}

	return ctrl.Result{}, nil
}

// applyRGToRD re-runs the placer with the RG's current SelectFilter.
// The placer is idempotent: if the RD already has place_count
// replicas, no change. If the RG bumped place_count, the gap is
// filled. Reductions are not auto-acted on here.
func (r *ResourceGroupReconciler) applyRGToRD(ctx context.Context, rg *apiv1.ResourceGroup, rd *apiv1.ResourceDefinition) error {
	filter := rg.SelectFilter
	if filter.PlaceCount == 0 {
		filter.PlaceCount = 1
	}

	_, _, err := placer.New(r.Store).Place(ctx, rd.Name, &filter)

	return err
}

// SetupWithManager sets up the controller with the Manager.
func (r *ResourceGroupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&blockstoriov1alpha1.ResourceGroup{}).
		Named("resourcegroup").
		Complete(r)
}
