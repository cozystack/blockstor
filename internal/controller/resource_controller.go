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
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/dispatcher"
	"github.com/cozystack/blockstor/pkg/drbd"
)

// resourceFinalizer guards on-disk + DRBD-side teardown when the
// Resource CRD is deleted. Without it the satellite would never see
// the delete, leaving an orphan .res file + LV / loopfile.
const resourceFinalizer = "blockstor.io.blockstor.io/resource"

// ResourceReconciler dispatches Resource CRD changes to the right
// satellite via the Dispatcher. It collects same-RD peers and the
// full Node list (for endpoint resolution) on every reconcile —
// fine for the stand smoke; once Resource counts grow we'll switch
// to a cached lister or label-selector watch.
type ResourceReconciler struct {
	client.Client
	Scheme     *runtime.Scheme
	Dispatcher *dispatcher.Dispatcher
}

// +kubebuilder:rbac:groups=blockstor.io.blockstor.io,resources=resources,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=blockstor.io.blockstor.io,resources=resources/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=blockstor.io.blockstor.io,resources=resources/finalizers,verbs=update
// +kubebuilder:rbac:groups=blockstor.io.blockstor.io,resources=nodes,verbs=get;list;watch
// +kubebuilder:rbac:groups=blockstor.io.blockstor.io,resources=resourcedefinitions,verbs=get;list;watch

// Reconcile reads a Resource and pushes the matching DesiredResource
// to the satellite that hosts it. Per-replica errors land in the
// log; transport faults trigger a 10s requeue.
func (r *ResourceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	if r.Dispatcher == nil {
		// envtest scaffolding (suite_test.go) constructs the reconciler
		// without a Dispatcher — keep the original no-op behaviour for
		// it so the boilerplate test stays green.
		return ctrl.Result{}, nil
	}

	var target blockstoriov1alpha1.Resource

	err := r.Get(ctx, req.NamespacedName, &target)
	if err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}

		return ctrl.Result{}, err
	}

	// Deletion path: tell the satellite to drop the resource, then
	// remove the finalizer so kube-apiserver finishes the delete.
	if !target.DeletionTimestamp.IsZero() {
		return r.runDelete(ctx, &target)
	}

	// Ensure finalizer is present on every live Resource so the
	// delete path above can run.
	if !slices.Contains(target.Finalizers, resourceFinalizer) {
		target.Finalizers = append(target.Finalizers, resourceFinalizer)

		err = r.Update(ctx, &target)
		if err != nil {
			return ctrl.Result{}, err
		}

		return ctrl.Result{Requeue: true}, nil
	}

	return r.runApply(ctx, &target)
}

// runApply is the apply branch of Reconcile. Pulled out to keep
// Reconcile under the funlen budget — the body deals with the
// finalizer dance, this with the actual gRPC dispatch.
func (r *ResourceReconciler) runApply(ctx context.Context, target *blockstoriov1alpha1.Resource) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var resList blockstoriov1alpha1.ResourceList

	err := r.List(ctx, &resList)
	if err != nil {
		return ctrl.Result{}, err
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
		return ctrl.Result{}, err
	}

	rdPtr, err := r.lookupRD(ctx, target.Spec.ResourceDefinitionName)
	if err != nil {
		return ctrl.Result{}, err
	}

	// Allocate DRBD node-id (and port/minor on the first replica) and
	// persist via Status before pushing to the satellite. node-id MUST
	// be stable for the lifetime of the replica — re-numbering would
	// re-map DRBD bitmaps and corrupt data on resync. If allocation
	// changes Status we requeue so the next reconcile sees the
	// committed value before dispatching.
	allocated, err := r.ensureDRBDIDs(ctx, target, peers)
	if err != nil {
		return ctrl.Result{}, err
	}

	if allocated {
		return ctrl.Result{Requeue: true}, nil
	}

	result, err := r.Dispatcher.Apply(ctx, target, peers, nodeList.Items, rdPtr)
	if err != nil {
		log.Error(err, "Apply RPC failed", "resource", target.Name, "node", target.Spec.NodeName)

		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	if !result.GetOk() {
		log.Info("satellite rejected apply", "msg", result.GetMessage(),
			"resource", target.Name, "node", target.Spec.NodeName)
	} else {
		log.Info("satellite accepted apply",
			"resource", target.Name, "node", target.Spec.NodeName)
	}

	return ctrl.Result{}, nil
}

// runDelete is the finalizer-driven teardown. We dial the satellite
// to drop the resource (drbdadm down → DeleteVolume → rm .res), then
// strip the finalizer so kube-apiserver completes the delete.
// Failures requeue with a 10 s back-off so the resource isn't stuck
// half-gone if a satellite is briefly unreachable.
func (r *ResourceReconciler) runDelete(ctx context.Context, target *blockstoriov1alpha1.Resource) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	if !slices.Contains(target.Finalizers, resourceFinalizer) {
		return ctrl.Result{}, nil
	}

	rdPtr, _ := r.lookupRD(ctx, target.Spec.ResourceDefinitionName)

	var nodeList blockstoriov1alpha1.NodeList

	err := r.List(ctx, &nodeList)
	if err != nil {
		return ctrl.Result{}, err
	}

	resp, err := r.Dispatcher.DeleteResource(ctx, target, rdPtr, nodeList.Items)
	if err != nil {
		log.Error(err, "DeleteResource RPC failed",
			"resource", target.Name, "node", target.Spec.NodeName)

		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	if !resp.GetOk() {
		log.Info("satellite rejected delete", "msg", resp.GetMessage(),
			"resource", target.Name, "node", target.Spec.NodeName)

		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	target.Finalizers = slices.DeleteFunc(target.Finalizers,
		func(s string) bool { return s == resourceFinalizer })

	err = r.Update(ctx, target)
	if err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// lookupRD fetches the parent ResourceDefinition. A NotFound is
// converted to (nil, nil) so the dispatcher can still push the
// .res for connection setup; any other error bubbles.
func (r *ResourceReconciler) lookupRD(ctx context.Context, name string) (*blockstoriov1alpha1.ResourceDefinition, error) {
	var rd blockstoriov1alpha1.ResourceDefinition

	err := r.Get(ctx, client.ObjectKey{Name: name}, &rd)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil, nil //nolint:nilnil // intentional: caller treats nil RD as no-volume push
		}

		return nil, err
	}

	return &rd, nil
}

// ensureDRBDIDs allocates a stable DRBD node-id for target (and a
// port + minor for the RD if no sibling has one yet) and persists the
// values on Status. Returns true when Status was changed; the caller
// requeues so the next reconcile dispatches with the committed values.
//
// node-id allocation is the lowest free 0..MaxPeers-1 not held by
// any sibling Resource of the same RD — sibling ids stay put, only
// the unallocated target gets a new value. This is the load-bearing
// invariant: re-using ids on a different node would re-map DRBD
// bitmaps mid-flight and corrupt data.
func (r *ResourceReconciler) ensureDRBDIDs(ctx context.Context, target *blockstoriov1alpha1.Resource, peers []blockstoriov1alpha1.Resource) (bool, error) {
	original := target.Status.DeepCopy()

	if target.Status.DRBDNodeID == nil {
		taken := make([]int32, 0, len(peers))

		for i := range peers {
			if peers[i].Name == target.Name {
				continue
			}

			if peers[i].Status.DRBDNodeID != nil {
				taken = append(taken, *peers[i].Status.DRBDNodeID)
			}
		}

		id, err := drbd.LowestFreeNodeID(taken)
		if err != nil {
			return false, err
		}

		target.Status.DRBDNodeID = &id
	}

	if target.Status.DRBDPort == nil {
		// Inherit from any sibling that already has one — every
		// replica of an RD shares the port. If none exists yet,
		// derive it from the RD name; the port-pool refactor will
		// replace this with a controller-side allocator.
		port := siblingPort(peers)
		if port == 0 {
			port = int32(derivePort(target.Spec.ResourceDefinitionName)) //nolint:gosec // deterministic port in 7000-7999
		}

		target.Status.DRBDPort = &port
	}

	if target.Status.DRBDMinor == nil {
		minor := siblingMinor(peers)
		if minor == 0 {
			minor = int32(deriveMinor(target.Spec.ResourceDefinitionName)) //nolint:gosec // deterministic minor in the reserved range
		}

		target.Status.DRBDMinor = &minor
	}

	if equalStatus(original, &target.Status) {
		return false, nil
	}

	err := r.Status().Update(ctx, target)
	if err != nil {
		return false, err
	}

	return true, nil
}

// siblingPort returns the first non-nil DRBDPort from the peer list,
// or 0 when nobody has one yet.
func siblingPort(peers []blockstoriov1alpha1.Resource) int32 {
	for i := range peers {
		if peers[i].Status.DRBDPort != nil {
			return *peers[i].Status.DRBDPort
		}
	}

	return 0
}

func siblingMinor(peers []blockstoriov1alpha1.Resource) int32 {
	for i := range peers {
		if peers[i].Status.DRBDMinor != nil {
			return *peers[i].Status.DRBDMinor
		}
	}

	return 0
}

func equalStatus(a, b *blockstoriov1alpha1.ResourceStatus) bool {
	return ptrEqI32(a.DRBDNodeID, b.DRBDNodeID) &&
		ptrEqI32(a.DRBDPort, b.DRBDPort) &&
		ptrEqI32(a.DRBDMinor, b.DRBDMinor)
}

func ptrEqI32(a, b *int32) bool {
	switch {
	case a == nil && b == nil:
		return true
	case a == nil || b == nil:
		return false
	default:
		return *a == *b
	}
}

// derivePort / deriveMinor are kept for the transitional bootstrap
// path; the production allocator is the cluster-wide pool tracked in
// Phase 8.1. We re-export them via dispatcher to avoid duplication.
func derivePort(rd string) int  { return dispatcher.DerivePort(rd) }
func deriveMinor(rd string) int { return dispatcher.DeriveMinor(rd) }

// EnsureDRBDIDsForTest is an exported alias for ensureDRBDIDs. The
// allocator is package-private because it's an internal reconciler
// step, but the property tests live in package controller_test and
// need a way in. Production callers use Reconcile.
func (r *ResourceReconciler) EnsureDRBDIDsForTest(ctx context.Context, target *blockstoriov1alpha1.Resource, peers []blockstoriov1alpha1.Resource) (bool, error) {
	return r.ensureDRBDIDs(ctx, target, peers)
}

// SetupWithManager sets up the controller with the Manager.
func (r *ResourceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&blockstoriov1alpha1.Resource{}).
		Named("resource").
		Complete(r)
}
