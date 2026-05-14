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

// RGRebalanceReconciler is the controller-side half of the Bug 60
// fix. It complements the existing ResourceGroupReconciler with an
// explicit, REST-driven trigger:
//
//  1. REST `linstor rg modify` stamps the
//     `blockstor.io/rebalance-pending` annotation on the RG CRD when
//     PlaceCount strictly increases or a placement-affecting
//     SelectFilter changes (see pkg/rest/resource_groups.go).
//  2. The controller-runtime watch on RG CRDs delivers a reconcile
//     request. This reconciler is annotation-gated: it skips RGs
//     without the marker, so prop-only writes / status updates
//     don't churn the placer.
//  3. When the marker is present, it lists every RD with
//     `Spec.ResourceGroupName == rg.Name` and calls the additive
//     placer for each. The placer's already-placed accounting keeps
//     the call idempotent for RDs that don't need new replicas.
//  4. Once all RDs have been processed, the annotation is stripped
//     so the next reconcile is a clean no-op.
//
// Strictly additive: the placer's Place method only spawns replicas,
// never removes them. Matches upstream LINSTOR's contract that
// scale-down requires explicit `linstor r d`.
type RGRebalanceReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// Store is the shared blockstor store. Required for placer
	// invocation and for the deferred annotation strip.
	Store store.Store
}

// +kubebuilder:rbac:groups=blockstor.io.blockstor.io,resources=resourcegroups,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=blockstor.io.blockstor.io,resources=resourcedefinitions,verbs=get;list;watch
// +kubebuilder:rbac:groups=blockstor.io.blockstor.io,resources=resources,verbs=get;list;watch;create;update;patch

// Reconcile is the annotation-gated rebalance pass. See the type
// comment for the four-step lifecycle.
func (r *RGRebalanceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx).WithValues("rg", req.Name)

	if r.Store == nil {
		return ctrl.Result{}, nil
	}

	rg, err := r.Store.ResourceGroups().Get(ctx, req.Name)
	if err != nil {
		// RG already gone; nothing to rebalance.
		return ctrl.Result{}, nil //nolint:nilerr
	}

	if _, ok := rg.Annotations[apiv1.AnnotationRGRebalancePending]; !ok {
		// No pending trigger — fast no-op for every RG-change event
		// that isn't a rebalance request (prop writes, status churn).
		return ctrl.Result{}, nil
	}

	// Scenario 2.W02 — operator kill-switch. Controller-scope
	// `BalanceResourcesEnabled=false` disables the periodic rebalance
	// pass cluster-wide; existing placements are left untouched.
	// We still strip the annotation so a transient flip doesn't leave
	// every subsequent RG event re-firing this code path.
	if balanceResourcesDisabled(ctx, r.Store) {
		log.Info("rebalance disabled by BalanceResourcesEnabled=false; skipping")

		return ctrl.Result{}, r.stripRebalanceAnnotation(ctx, &rg)
	}

	count, err := r.rebalanceChildRDs(ctx, &rg)
	if err != nil {
		// Surface the error so controller-runtime requeues; we do
		// NOT strip the annotation, the next reconcile retries.
		return ctrl.Result{}, err
	}

	log.Info("rebalance pass complete", "rds_processed", count)

	// All RDs handled (even if some individual Place calls logged a
	// soft error inside rebalanceChildRDs). Drop the marker so the
	// next CRD event reconciles cleanly. A subsequent REST modify
	// will re-stamp it.
	return ctrl.Result{}, r.stripRebalanceAnnotation(ctx, &rg)
}

// rebalanceChildRDs walks every RD that points at this RG and runs
// the additive placer with the RG's current SelectFilter. Errors on
// individual RDs are logged and accumulated as a soft skip — the
// reconciler still strips the annotation so a permanently-broken RD
// doesn't block the others. (The dedicated RD-level reconcilers
// surface that breakage on their own paths.)
func (r *RGRebalanceReconciler) rebalanceChildRDs(ctx context.Context, rg *apiv1.ResourceGroup) (int, error) {
	log := logf.FromContext(ctx)

	rds, err := r.Store.ResourceDefinitions().List(ctx)
	if err != nil {
		return 0, err
	}

	p := placer.New(r.Store)
	processed := 0

	for i := range rds {
		if rds[i].ResourceGroupName != rg.Name {
			continue
		}

		filter := rg.SelectFilter
		if filter.PlaceCount == 0 {
			filter.PlaceCount = 1
		}

		_, _, perr := p.Place(ctx, rds[i].Name, &filter)
		if perr != nil {
			log.Error(perr, "placer call failed during RG rebalance",
				"rg", rg.Name, "rd", rds[i].Name)
		}

		processed++
	}

	return processed, nil
}

// stripRebalanceAnnotation removes the rebalance-pending marker from
// the RG and persists the change. Idempotent: a re-run after a
// successful strip is a no-op because the annotation is already
// gone.
func (r *RGRebalanceReconciler) stripRebalanceAnnotation(ctx context.Context, rg *apiv1.ResourceGroup) error {
	if _, ok := rg.Annotations[apiv1.AnnotationRGRebalancePending]; !ok {
		return nil
	}

	delete(rg.Annotations, apiv1.AnnotationRGRebalancePending)

	// Nil-out an empty map so the wire envelope round-trips as the
	// pre-Bug-60 shape — round-trip stability matters for the JSON
	// goldens that pin the RG payload.
	if len(rg.Annotations) == 0 {
		rg.Annotations = nil
	}

	return r.Store.ResourceGroups().Update(ctx, rg)
}

// balanceResourcesDisabled returns true when the controller-scope
// `BalanceResourcesEnabled` prop is explicitly set to "false". Any
// other value (including missing, empty, or a typo) keeps the
// rebalance reconciler armed — matches upstream LINSTOR's "default
// = enabled" semantics so an unconfigured cluster behaves the same
// way it did before scenario 2.W02 landed.
func balanceResourcesDisabled(ctx context.Context, st store.Store) bool {
	if st == nil {
		return false
	}

	props, err := st.ControllerProps().Get(ctx)
	if err != nil || props == nil {
		// A read error must not silently disable the reconciler — we
		// fail OPEN, the operator still has the annotation gate + the
		// RG-level CRD as throttling layers. The error itself will be
		// surfaced on the next placer call when it hits the same row.
		return false
	}

	return props[apiv1.PropBalanceResourcesEnabled] == "false"
}

// SetupWithManager wires the reconciler into the manager. We share
// the RG CRD watch with ResourceGroupReconciler — controller-runtime
// supports multiple controllers on the same `For` resource as long
// as each has a distinct Named ID.
func (r *RGRebalanceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&blockstoriov1alpha1.ResourceGroup{}).
		Named("rg-rebalance").
		Complete(r)
}
