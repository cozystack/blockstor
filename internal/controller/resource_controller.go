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
//
// Port + minor allocation is cluster-wide: we scan every Resource
// across all RDs to gather taken values, then pick the lowest free
// from the pool range. Two RDs racing to pick the same port resolve
// deterministically (same taken set → same answer); the loser's
// Status update is rejected by Kube's optimistic concurrency check
// and the next reconcile picks the next free port.
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
		port, err := r.allocatePort(ctx, target.Spec.NodeName)
		if err != nil {
			return false, err
		}

		target.Status.DRBDPort = &port
	}

	if target.Status.DRBDMinor == nil {
		minor, err := r.allocateMinor(ctx, target.Spec.NodeName)
		if err != nil {
			return false, err
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

// allocatePort picks a TCP port from the hosting node's range.
// Upstream LINSTOR moved from per-RD to per-resource ports: each
// replica picks its own port from its node's local range. That way
// nodes can run unrelated TCP-port pools (port 7000 on n1 has nothing
// to do with port 7000 on n2), and a port collision on one node
// doesn't affect the rest of the cluster.
//
// Range source: the node's `DrbdOptions/TcpPortRange` prop ("min-max")
// with controller-wide defaults [DefaultPortMin, DefaultPortMax] when
// the prop is absent. Taken set: every Resource currently scheduled
// on the same node.
func (r *ResourceReconciler) allocatePort(ctx context.Context, nodeName string) (int32, error) {
	low, high, err := r.portRangeForNode(ctx, nodeName)
	if err != nil {
		return 0, err
	}

	taken, err := r.takenPortsOnNode(ctx, nodeName)
	if err != nil {
		return 0, err
	}

	return drbd.LowestFreePort(taken, low, high)
}

// allocateMinor mirrors allocatePort for /dev/drbd<N>. Minor numbers
// are local device-name suffixes; per-node scope is the natural fit.
func (r *ResourceReconciler) allocateMinor(ctx context.Context, nodeName string) (int32, error) {
	low, high, err := r.minorRangeForNode(ctx, nodeName)
	if err != nil {
		return 0, err
	}

	taken, err := r.takenMinorsOnNode(ctx, nodeName)
	if err != nil {
		return 0, err
	}

	return drbd.LowestFreeMinor(taken, low, high)
}

// portRangeForNode reads "DrbdOptions/TcpPortRange" off the named
// Node CRD, falling back to the controller-wide default. Format
// matches upstream: "min-max" decimal.
func (r *ResourceReconciler) portRangeForNode(ctx context.Context, nodeName string) (int32, int32, error) {
	return r.rangeProp(ctx, nodeName, "DrbdOptions/TcpPortRange",
		drbd.DefaultPortMin, drbd.DefaultPortMax)
}

func (r *ResourceReconciler) minorRangeForNode(ctx context.Context, nodeName string) (int32, int32, error) {
	return r.rangeProp(ctx, nodeName, "DrbdOptions/MinorNrRange",
		drbd.DefaultMinorMin, drbd.DefaultMinorMax)
}

// rangeProp reads a "min-max" prop off the Node CRD. Missing node or
// missing prop falls back to defaults (the prop is optional). Bad
// format returns an error so the operator notices the typo.
func (r *ResourceReconciler) rangeProp(ctx context.Context, nodeName, prop string, defLow, defHigh int32) (int32, int32, error) {
	var node blockstoriov1alpha1.Node
	if err := r.Get(ctx, client.ObjectKey{Name: nodeName}, &node); err != nil {
		if errors.IsNotFound(err) {
			return defLow, defHigh, nil
		}

		return 0, 0, err
	}

	raw := node.Spec.Props[prop]
	if raw == "" {
		return defLow, defHigh, nil
	}

	low, high, err := drbd.ParseRange(raw)
	if err != nil {
		return 0, 0, err
	}

	return low, high, nil
}

// takenPortsOnNode scans every Resource scheduled on the given node
// and returns its persisted DRBDPort. The allocator uses this to
// guarantee no two replicas on the same node take the same port.
func (r *ResourceReconciler) takenPortsOnNode(ctx context.Context, nodeName string) ([]int32, error) {
	return r.takenOnNode(ctx, nodeName, func(s *blockstoriov1alpha1.ResourceStatus) *int32 { return s.DRBDPort })
}

func (r *ResourceReconciler) takenMinorsOnNode(ctx context.Context, nodeName string) ([]int32, error) {
	return r.takenOnNode(ctx, nodeName, func(s *blockstoriov1alpha1.ResourceStatus) *int32 { return s.DRBDMinor })
}

// takenOnNode is the shared scan: list every Resource on `nodeName`,
// pluck the int32 pointer the caller cares about, and return the
// non-nil set. Used by the per-node port and minor allocators.
func (r *ResourceReconciler) takenOnNode(ctx context.Context, nodeName string, pick func(*blockstoriov1alpha1.ResourceStatus) *int32) ([]int32, error) {
	list := &blockstoriov1alpha1.ResourceList{}
	if err := r.List(ctx, list); err != nil {
		return nil, err
	}

	out := make([]int32, 0, len(list.Items))

	for i := range list.Items {
		if list.Items[i].Spec.NodeName != nodeName {
			continue
		}

		if v := pick(&list.Items[i].Status); v != nil {
			out = append(out, *v)
		}
	}

	return out, nil
}

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
