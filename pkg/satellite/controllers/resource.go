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
	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/dispatcher"
	"github.com/cozystack/blockstor/pkg/effectiveprops"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
)

// SatelliteResourceFinalizer is the per-satellite-instance
// finalizer key the satellite reconciler stamps on every
// Resource it owns. The controller-side reconciler uses a
// distinct key (`blockstor.io.blockstor.io/resource`) so the
// two paths coexist during the Phase 10.6 cutover without
// stepping on each other.
const SatelliteResourceFinalizer = "blockstor.io.blockstor.io/satellite-resource"

// resourceKind is the apiserver Kind for the blockstor Resource
// CRD. Defined once so SSA Patch payloads in this package share
// one source of truth.
const resourceKind = "Resource"

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

		return ctrl.Result{RequeueAfter: applyFailureRequeue}, nil
	}

	return r.runApply(ctx, &res, logger)
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
//
// We additionally watch sibling Resources (same RD, different
// node) via Watches — when a peer's Resource is created /
// updated / deleted on another satellite, this satellite must
// re-render its .res to add or drop the `on <peer>` block.
// Without that hook, the initial reconcile saw only the local
// Resource (peer hadn't been observed yet through the cache),
// rendered a peer-less .res, and never re-rendered because
// later peer events get filtered out by nodeNamePredicate
// before they reach this controller.
func (r *ResourceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	err := ctrl.NewControllerManagedBy(mgr).
		For(&blockstoriov1alpha1.Resource{},
			builder.WithPredicates(nodeNamePredicate(r.Config.NodeName))).
		Watches(&blockstoriov1alpha1.Resource{},
			handler.EnqueueRequestsFromMapFunc(r.enqueueLocalSiblings)).
		Watches(&blockstoriov1alpha1.ResourceDefinition{},
			handler.EnqueueRequestsFromMapFunc(r.enqueueResourcesForRD)).
		Named("satellite-resource").
		Complete(r)
	if err != nil {
		return errors.Wrap(err, "register ResourceReconciler")
	}

	return nil
}

// enqueueResourcesForRD maps a ResourceDefinition event to the local
// Resource (if any) of that RD. The satellite-side reconciler needs
// this hook so spec-level RD changes (volume resize, layerStack edit,
// new VolumeDefinition added) trigger a local re-apply — without it
// the user could PUT a bigger size_kib via REST, the CRD updates
// cleanly, but the satellite never picks up the delta and the
// kernel device stays at the old size.
//
// Returns nothing when this satellite has no replica of the affected
// RD — we don't process RDs we're not party to.
func (r *ResourceReconciler) enqueueResourcesForRD(ctx context.Context, obj client.Object) []reconcile.Request {
	rd, ok := obj.(*blockstoriov1alpha1.ResourceDefinition)
	if !ok {
		return nil
	}

	var localList blockstoriov1alpha1.ResourceList

	err := r.List(ctx, &localList)
	if err != nil {
		return nil
	}

	for i := range localList.Items {
		local := &localList.Items[i]
		if local.Spec.ResourceDefinitionName == rd.Name &&
			local.Spec.NodeName == r.Config.NodeName {
			return []reconcile.Request{{NamespacedName: client.ObjectKey{Name: local.Name}}}
		}
	}

	return nil
}

// runApply gates on the controller-side allocation, builds the
// DesiredResource for this satellite, and drives the apply chain.
// Returns RequeueAfter when allocation is stale or any per-resource
// apply failed so the reconcile self-heals once peer state catches
// up. Pulled out of Reconcile to keep the orchestration under the
// cyclomatic / funlen budgets.
func (r *ResourceReconciler) runApply(ctx context.Context, res *blockstoriov1alpha1.Resource, logger logr.Logger) (ctrl.Result, error) {
	var rd blockstoriov1alpha1.ResourceDefinition

	err := r.Get(ctx, client.ObjectKey{Name: res.Spec.ResourceDefinitionName}, &rd)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}

		return ctrl.Result{}, errors.Wrap(err, "get parent RD")
	}

	// The DRBD-ID allocation gate only matters when the RD actually
	// uses DRBD. A LayerStack=["STORAGE"] (or ["LUKS","STORAGE"]) RD
	// renders no .res and the kernel never sees a node-id, so waiting
	// for the controller to stamp Status.DRBDNodeID/Port/Minor would
	// block apply forever — they never come.
	if rdNeedsDRBD(&rd) {
		if waitResult, waitOK := r.waitForControllerAllocation(res, logger); !waitOK {
			return waitResult, nil
		}
	}

	desired, err := r.buildDesiredFromCRD(ctx, res, &rd)
	if err != nil {
		return ctrl.Result{}, err
	}

	results, err := r.Config.Apply.Apply(ctx, []*intent.DesiredResource{desired})
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

	// Stamp per-volume DevicePath into Status.Volumes so
	// linstor-csi / any consumer that reads the CRD sees the
	// /dev path the satellite materialised. Done even when one
	// volume's apply failed: a partial success is still useful
	// to surface (volumes that did apply have a real device).
	err = r.stampVolumeStatus(ctx, res, results)
	if err != nil {
		logger.Error(err, "stamp Status.Volumes")
	}

	// Apply chain surfaces per-resource errors via results (e.g.
	// drbdadm adjust failing on a stale .res rendered before the
	// peer's Status caught up). Returning nil here would let c-r
	// stop until an external event drove another reconcile;
	// RequeueAfter ensures the next attempt sees the freshly-
	// committed peer state.
	if anyFailed {
		return ctrl.Result{RequeueAfter: applyFailureRequeue}, nil
	}

	return ctrl.Result{}, nil
}

// stampVolumeStatus SSA-patches Resource.Status.Volumes with the
// per-volume DevicePath the apply chain materialised. Uses a
// dedicated FieldOwner so the apiserver merges cleanly against the
// observer's DiskState / CurrentGi writes on the same Volume[i]
// (listMapKey=volumeNumber means the slice is merged by volume
// number, not replaced wholesale).
func (r *ResourceReconciler) stampVolumeStatus(ctx context.Context, res *blockstoriov1alpha1.Resource, results []*intent.ResourceApplyResult) error {
	if len(results) == 0 || len(results[0].GetVolumes()) == 0 {
		return nil
	}

	vols := make([]blockstoriov1alpha1.ResourceVolumeStatus, 0, len(results[0].GetVolumes()))
	for _, v := range results[0].GetVolumes() {
		vols = append(vols, blockstoriov1alpha1.ResourceVolumeStatus{
			VolumeNumber: v.GetVolumeNumber(),
			DevicePath:   v.GetDevicePath(),
		})
	}

	apply := &blockstoriov1alpha1.Resource{
		TypeMeta: metav1.TypeMeta{
			Kind:       resourceKind,
			APIVersion: blockstoriov1alpha1.GroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{Name: res.Name},
		Status:     blockstoriov1alpha1.ResourceStatus{Volumes: vols},
	}

	// Intentionally NO ForceOwnership: this writer only owns
	// devicePath. Force-ownership on a listMap entry that the
	// observer also touches causes the apiserver to evict
	// observer's subfield claims (diskState / currentGi /
	// outOfSyncKib) for the same listMap key. Without the force,
	// SSA merges per-field cleanly and both owners' claims survive.
	err := r.Status().Patch(ctx, apply,
		client.Apply, //nolint:staticcheck // SA1019: applyconfiguration-gen output not yet available for our CRDs
		client.FieldOwner(volumeStatusFieldOwner))
	if err != nil {
		return errors.Wrap(err, "ssa Status.Volumes")
	}

	return nil
}

// volumeStatusFieldOwner is the SSA field-manager the satellite
// uses when it writes per-volume DevicePath. Distinct from the
// observer's owner (which writes DiskState / CurrentGi) so the
// apiserver merges the two slices cleanly under
// `listMapKey=volumeNumber`.
const volumeStatusFieldOwner = "blockstor-satellite-volume-status"

// enqueueLocalSiblings maps a Resource event to the LOCAL Resource
// of the same RD (if any). When a peer on another node appears or
// changes, the local satellite must re-reconcile so its .res
// picks up the new peer block / port / node-id.
//
// Returns nothing when the local node has no replica for the
// affected RD — we don't watch RDs we're not party to.
func (r *ResourceReconciler) enqueueLocalSiblings(ctx context.Context, obj client.Object) []reconcile.Request {
	res, ok := obj.(*blockstoriov1alpha1.Resource)
	if !ok {
		return nil
	}

	// Ignore events for the local Resource itself — `For` already
	// drives that path through nodeNamePredicate.
	if res.Spec.NodeName == r.Config.NodeName {
		return nil
	}

	var localList blockstoriov1alpha1.ResourceList

	err := r.List(ctx, &localList)
	if err != nil {
		return nil
	}

	for i := range localList.Items {
		local := &localList.Items[i]
		if local.Spec.ResourceDefinitionName == res.Spec.ResourceDefinitionName &&
			local.Spec.NodeName == r.Config.NodeName {
			return []reconcile.Request{{NamespacedName: client.ObjectKey{Name: local.Name}}}
		}
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
func (r *ResourceReconciler) buildDesiredFromCRD(ctx context.Context, target *blockstoriov1alpha1.Resource, rd *blockstoriov1alpha1.ResourceDefinition) (*intent.DesiredResource, error) {
	var resList blockstoriov1alpha1.ResourceList

	err := r.List(ctx, &resList)
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

	var poolList blockstoriov1alpha1.StoragePoolList

	err = r.List(ctx, &poolList)
	if err != nil {
		return nil, errors.Wrap(err, "list StoragePools")
	}

	effectiveProps, err := effectiveprops.Resolve(ctx, r.Client, target, rd)
	if err != nil {
		return nil, errors.Wrap(err, "resolve effective props")
	}

	return dispatcher.BuildDesired(target, peers, nodeList.Items, poolList.Items, rd, effectiveProps), nil
}

// rdNeedsDRBD mirrors pkg/satellite/reconciler.go's needsDRBD but
// over the RD CRD: empty LayerStack defaults to ["DRBD","STORAGE"]
// (legacy semantics), any explicit stack must list "DRBD".
func rdNeedsDRBD(rd *blockstoriov1alpha1.ResourceDefinition) bool {
	stack := rd.Spec.LayerStack
	if len(stack) == 0 {
		return true
	}

	return slices.Contains(stack, "DRBD")
}

// waitForControllerAllocation gates the apply path on the
// controller-side allocator having stamped Status.DRBDNodeID /
// DRBDPort / DRBDMinor. Without this, dispatcher.BuildDesired
// silently defaults the missing fields to 0 / "0.0.0.0" / minor 0
// and we'd render a .res claiming the local node is `node-id 0`.
// drbdsetup new-resource burns that into kernel state at first
// adjust, and the next reconcile (after the controller writes the
// real allocation) hits `peer node id cannot be my own node id`
// forever because the kernel's recorded my-id never moves.
//
// Returns ok=true when allocation is fresh; ok=false with a
// short-backoff requeue Result when one of the IDs is still nil.
func (r *ResourceReconciler) waitForControllerAllocation(res *blockstoriov1alpha1.Resource, logger logr.Logger) (ctrl.Result, bool) {
	if res.Status.DRBDNodeID == nil || res.Status.DRBDPort == nil || res.Status.DRBDMinor == nil {
		logger.Info("waiting for controller-side DRBD-ID allocation",
			"nodeID", res.Status.DRBDNodeID,
			"port", res.Status.DRBDPort,
			"minor", res.Status.DRBDMinor)

		return ctrl.Result{RequeueAfter: applyFailureRequeue}, false
	}

	return ctrl.Result{}, true
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

	req := &intent.DeleteResourceRequest{
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
