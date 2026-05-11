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
	"slices"
	"time"

	"github.com/cockroachdb/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/dispatcher"
	"github.com/cozystack/blockstor/pkg/effectiveprops"
	satellitepb "github.com/cozystack/blockstor/pkg/satellite/proto"
)

// SatelliteResourceFinalizer is the per-satellite-instance
// finalizer key the satellite reconciler stamps on every
// Resource it owns. The controller-side reconciler uses a
// distinct key (`blockstor.io.blockstor.io/resource`) so the
// two paths coexist during the Phase 10.6 cutover without
// stepping on each other.
const SatelliteResourceFinalizer = "blockstor.io.blockstor.io/satellite-resource"

// ResourceReconciler watches Resource CRDs filtered to those
// placed on this satellite's node and translates them into the
// existing apply chain. Phase 10.1: replaces the gRPC
// `ApplyResources` consumer.
//
// The reconciler is intentionally minimal in this initial
// commit — it logs what it sees and exits. Subsequent commits
// fill in the desired-state-builder + apply path by delegating
// to `Config.Apply` (the pre-existing satellite reconciler that
// owns storage + DRBD + LUKS).
type ResourceReconciler struct {
	client.Client

	Config Config
}

// Reconcile reads a Resource and drives the satellite-side
// apply chain when the Resource is placed on this node.
// Finalizer-aware: stamps `SatelliteResourceFinalizer` on
// every live Resource so kube-apiserver waits for our tear-down
// before allowing the object to disappear.
func (r *ResourceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("resource", req.Name)

	var res blockstoriov1alpha1.Resource

	err := r.Get(ctx, req.NamespacedName, &res)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}

		return ctrl.Result{}, errors.Wrap(err, "get Resource")
	}

	if res.Spec.NodeName != r.Config.NodeName {
		// Predicate should have filtered this out — defensive
		// check in case the watch cache is mid-resync.
		return ctrl.Result{}, nil
	}

	if !res.DeletionTimestamp.IsZero() {
		return r.handleDelete(ctx, &res)
	}

	if !slices.Contains(res.Finalizers, SatelliteResourceFinalizer) {
		res.Finalizers = append(res.Finalizers, SatelliteResourceFinalizer)

		err := r.Update(ctx, &res)
		if err != nil {
			return ctrl.Result{}, errors.Wrap(err, "add finalizer")
		}

		return ctrl.Result{Requeue: true}, nil
	}

	desired, err := r.buildDesiredFromCRD(ctx, &res)
	if err != nil {
		return ctrl.Result{}, err
	}

	results, err := r.Config.Apply.Apply(ctx, []*satellitepb.DesiredResource{desired})
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "satellite Apply")
	}

	var anyFailed bool

	for _, ar := range results {
		if !ar.GetOk() {
			anyFailed = true

			logger.Info("Apply per-resource failure", "name", ar.GetName(), "message", ar.GetMessage())
		}
	}

	// Apply chain reports its own per-resource errors via results (e.g.
	// drbdadm adjust failing on a stale .res rendered from a partially-
	// allocated peer Status). Returning `nil` would let c-r stop and
	// never retry until an external event (peer Status update) drove
	// another reconcile. Requeue with a short backoff so the next
	// attempt sees the freshly-committed peer state.
	if anyFailed {
		return ctrl.Result{RequeueAfter: applyFailureRequeue}, nil
	}

	return ctrl.Result{}, nil
}

// applyFailureRequeue is the backoff between satellite Apply
// retries when at least one per-resource result reported failure.
// Short enough that a transient stale-peer .res renders converge
// within a couple of ticks; long enough that a permanently failing
// drbdadm command (e.g. missing kernel module) doesn't flood the
// log.
const applyFailureRequeue = 5 * time.Second

// resolveDeleteStoragePool picks the pool name the
// satellite's DeleteVolume should route through. Phase 10.3
// typed `Spec.StoragePool` wins; legacy `Props["StorPoolName"]`
// is the forward-compat fallback (pre-migration Resources may
// still carry only the prop). DISKLESS replicas have no pool
// — the satellite reconciler handles the empty case fine.
func resolveDeleteStoragePool(res *blockstoriov1alpha1.Resource) string {
	if slices.Contains(res.Spec.Flags, apiv1.ResourceFlagDiskless) {
		return ""
	}

	if res.Spec.StoragePool != "" {
		return res.Spec.StoragePool
	}

	return res.Spec.Props["StorPoolName"]
}

// SetupWithManager wires this reconciler with a node-name
// predicate so only Resources placed on this satellite trigger
// reconciles. The predicate also fires on Delete events (the
// CRD finalizer dance still needs us to clean up local state).
func (r *ResourceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	err := ctrl.NewControllerManagedBy(mgr).
		For(&blockstoriov1alpha1.Resource{},
			builder.WithPredicates(nodeNamePredicate(r.Config.NodeName))).
		Named("satellite-resource").
		Complete(r)
	if err != nil {
		return errors.Wrap(err, "register ResourceReconciler")
	}

	return nil
}

// buildDesiredFromCRD packages a Resource CRD into the
// satellite-facing DesiredResource by walking the apiserver
// for parent RD + same-RD peers + Node list, then resolving
// effective DRBD options via the shared
// `pkg/effectiveprops` package and finally handing everything
// to `dispatcher.BuildDesired`.
//
// This is the load-bearing replacement for the gRPC dispatch
// path — the controller-runtime reconciler does exactly the
// same work the controller's dispatcher did, just on the
// satellite side. Phase 10.1.
func (r *ResourceReconciler) buildDesiredFromCRD(ctx context.Context, target *blockstoriov1alpha1.Resource) (*satellitepb.DesiredResource, error) {
	var rd blockstoriov1alpha1.ResourceDefinition

	err := r.Get(ctx, client.ObjectKey{Name: target.Spec.ResourceDefinitionName}, &rd)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, errors.Errorf("parent RD %q not found", target.Spec.ResourceDefinitionName)
		}

		return nil, errors.Wrap(err, "get parent RD")
	}

	var resList blockstoriov1alpha1.ResourceList

	err = r.List(ctx, &resList)
	if err != nil {
		return nil, errors.Wrap(err, "list Resources for peer set")
	}

	peers := make([]blockstoriov1alpha1.Resource, 0, len(resList.Items))

	for i := range resList.Items {
		if resList.Items[i].Spec.ResourceDefinitionName == target.Spec.ResourceDefinitionName {
			peers = append(peers, resList.Items[i])
		}
	}

	var nodeList blockstoriov1alpha1.NodeList

	err = r.List(ctx, &nodeList)
	if err != nil {
		return nil, errors.Wrap(err, "list Nodes")
	}

	effectiveProps, err := effectiveprops.Resolve(ctx, r.Client, target, &rd)
	if err != nil {
		return nil, errors.Wrap(err, "resolve effective props")
	}

	return dispatcher.BuildDesired(target, peers, nodeList.Items, &rd, effectiveProps), nil
}

// handleDelete runs the satellite-side teardown when a Resource
// gets a DeletionTimestamp. Idempotent — re-runs after a
// satellite restart safely re-issue `drbdadm down` /
// `DeleteVolume` / `cryptsetup luksClose`, all of which are
// no-ops on already-torn-down state. Removes our finalizer on
// success so kube-apiserver finalises the delete.
//
// Phase 10.1. Once Phase 10.6 retires the gRPC contract,
// `internal/controller.ResourceReconciler.runDelete` and its
// finalizer (`blockstor.io.blockstor.io/resource`) go away —
// satellite-side teardown becomes the only finalizer path.
func (r *ResourceReconciler) handleDelete(ctx context.Context, res *blockstoriov1alpha1.Resource) (ctrl.Result, error) {
	if !slices.Contains(res.Finalizers, SatelliteResourceFinalizer) {
		// Either we never stamped it (object created before this
		// satellite came up) or someone already stripped it.
		// Either way, nothing for us to do — let the apiserver
		// finalise.
		return ctrl.Result{}, nil
	}

	volumeNumbers, err := r.lookupVolumeNumbers(ctx, res.Spec.ResourceDefinitionName)
	if err != nil {
		return ctrl.Result{}, err
	}

	req := &satellitepb.DeleteResourceRequest{
		Name:          res.Spec.ResourceDefinitionName,
		StoragePool:   resolveDeleteStoragePool(res),
		VolumeNumbers: volumeNumbers,
	}

	resp, err := r.Config.Apply.DeleteResource(ctx, req)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "DeleteResource")
	}

	if !resp.GetOk() {
		// Body-level failure — log and let controller-runtime
		// back-off retry. We keep the finalizer in place so the
		// CRD doesn't vanish before our tear-down succeeds.
		log.FromContext(ctx).Info("DeleteResource per-resource failure",
			"resource", res.Spec.ResourceDefinitionName, "message", resp.GetMessage())

		return ctrl.Result{Requeue: true}, nil
	}

	res.Finalizers = slices.DeleteFunc(res.Finalizers,
		func(f string) bool { return f == SatelliteResourceFinalizer })

	err = r.Update(ctx, res)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "strip finalizer")
	}

	return ctrl.Result{}, nil
}

// lookupVolumeNumbers reads the parent RD and returns its
// VolumeDefinitions' numbers. The satellite's DeleteResource
// uses the list to drop matching LVs / loopfiles. A missing RD
// (e.g. cascade-delete already removed it) silently returns an
// empty list — the satellite's `DeleteVolume` paths short-
// circuit on missing storage anyway.
func (r *ResourceReconciler) lookupVolumeNumbers(ctx context.Context, rdName string) ([]int32, error) {
	var rd blockstoriov1alpha1.ResourceDefinition

	err := r.Get(ctx, client.ObjectKey{Name: rdName}, &rd)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}

		return nil, errors.Wrap(err, "get parent RD for delete")
	}

	out := make([]int32, 0, len(rd.Spec.VolumeDefinitions))
	for i := range rd.Spec.VolumeDefinitions {
		out = append(out, rd.Spec.VolumeDefinitions[i].VolumeNumber)
	}

	return out, nil
}

// nodeNamePredicate filters events down to objects whose
// `Spec.NodeName` matches the given node. Centralised so the
// StoragePool reconciler can reuse the same shape.
func nodeNamePredicate(nodeName string) predicate.Predicate {
	matches := func(obj client.Object) bool {
		if r, ok := obj.(*blockstoriov1alpha1.Resource); ok {
			return r.Spec.NodeName == nodeName
		}

		if sp, ok := obj.(*blockstoriov1alpha1.StoragePool); ok {
			return sp.Spec.NodeName == nodeName
		}

		return false
	}

	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool { return matches(e.Object) },
		UpdateFunc: func(e event.UpdateEvent) bool {
			return matches(e.ObjectNew) || matches(e.ObjectOld)
		},
		DeleteFunc:  func(e event.DeleteEvent) bool { return matches(e.Object) },
		GenericFunc: func(e event.GenericEvent) bool { return matches(e.Object) },
	}
}
