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
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/dispatcher"
	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/store"
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

	// Store is the shared blockstor store. Used by the auto-diskful
	// promotion path to look up storage pools per node without
	// requiring a separate StoragePool client cache.
	Store store.Store
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

	// Auto-diskful: when a DISKLESS replica is actively used by a
	// consumer (InUse=true on this node) AND the hosting node has
	// a viable storage pool, promote it to diskful so reads stay
	// local. Cleanup (demote on idle) is intentionally not
	// automated yet — needs hysteresis to avoid flapping on
	// transient opens; operators demote via `linstor r d` until
	// then.
	promoted, err := r.maybeAutoDiskful(ctx, target)
	if err != nil {
		return ctrl.Result{}, err
	}

	if promoted {
		return ctrl.Result{Requeue: true}, nil
	}

	return r.dispatchApply(ctx, target, peers, nodeList.Items, rdPtr)
}

// maybeAutoDiskful flips a DISKLESS-but-actively-used replica to
// diskful when the hosting node has a usable storage pool. The
// satellite reconciler picks up the spec change on the next pass
// and creates the LV/zvol + attach. Returns true when Spec was
// mutated so the caller can requeue.
func (r *ResourceReconciler) maybeAutoDiskful(ctx context.Context, target *blockstoriov1alpha1.Resource) (bool, error) {
	if !slices.Contains(target.Spec.Flags, apiv1.ResourceFlagDiskless) {
		return false, nil
	}

	if !target.Status.InUse {
		return false, nil
	}

	if slices.Contains(target.Spec.Flags, apiv1.ResourceFlagTieBreaker) {
		// Tiebreaker witnesses must stay diskless — they're chosen
		// for the network presence, not local storage. Promoting a
		// tiebreaker would defeat the quorum semantic.
		return false, nil
	}

	pool, err := r.firstAvailablePool(ctx, target.Spec.NodeName)
	if err != nil {
		return false, err
	}

	if pool == "" {
		// No pool on this node → can't promote. Stay diskless.
		return false, nil
	}

	target.Spec.Flags = slices.DeleteFunc(target.Spec.Flags,
		func(s string) bool { return s == apiv1.ResourceFlagDiskless })

	if target.Spec.Props == nil {
		target.Spec.Props = map[string]string{}
	}

	target.Spec.Props["StorPoolName"] = pool

	err = r.Update(ctx, target)
	if err != nil {
		return false, err
	}

	return true, nil
}

// firstAvailablePool returns any non-diskless storage pool present on
// the named node. Used by the auto-diskful promotion to pick a
// destination for the freshly-attached LV. We don't try to be
// clever: production clusters typically have one pool per node.
func (r *ResourceReconciler) firstAvailablePool(ctx context.Context, nodeName string) (string, error) {
	pools, err := r.Store.StoragePools().List(ctx)
	if err != nil {
		return "", err
	}

	for i := range pools {
		if pools[i].NodeName != nodeName {
			continue
		}

		if pools[i].ProviderKind == apiv1.StoragePoolKindDiskless {
			continue
		}

		return pools[i].StoragePoolName, nil
	}

	return "", nil
}

// dispatchApply resolves DRBD options and pushes the desired state to
// the satellite. Pulled out of runApply so the latter stays under the
// funlen budget — the resolver step grew non-trivial with the option
// hierarchy.
func (r *ResourceReconciler) dispatchApply(ctx context.Context, target *blockstoriov1alpha1.Resource, peers []blockstoriov1alpha1.Resource, nodes []blockstoriov1alpha1.Node, rdPtr *blockstoriov1alpha1.ResourceDefinition) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	effective, err := r.resolveEffectiveProps(ctx, target, rdPtr)
	if err != nil {
		log.Error(err, "resolve effective props", "resource", target.Name)

		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	stack := r.resolveLayerStack(ctx, rdPtr)

	result, err := r.Dispatcher.Apply(ctx, target, peers, nodes, rdPtr,
		dispatcher.ApplyOptions{
			EffectiveProps: effective,
			LayerStack:     stack,
		})
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

// takenMinorsOnNode returns every minor consumed on the node. A
// multi-volume RD consumes N consecutive minors (the .res renderer
// emits volume k at base+k), so for each Resource we expand its
// recorded base minor to the full range based on the parent RD's
// VolumeDefinitions count. Without this expansion, a fresh
// Resource's allocator would happily pick base+1 on a node where a
// 2-volume sibling already owns base..base+1.
func (r *ResourceReconciler) takenMinorsOnNode(ctx context.Context, nodeName string) ([]int32, error) {
	list := &blockstoriov1alpha1.ResourceList{}
	if err := r.List(ctx, list); err != nil {
		return nil, err
	}

	rdVolCounts := map[string]int{}

	out := make([]int32, 0, len(list.Items))

	for i := range list.Items {
		if list.Items[i].Spec.NodeName != nodeName {
			continue
		}

		base := list.Items[i].Status.DRBDMinor
		if base == nil {
			continue
		}

		rdName := list.Items[i].Spec.ResourceDefinitionName
		volCount, cached := rdVolCounts[rdName]

		if !cached {
			volCount = 1 // safe default: at least the base is taken

			var rd blockstoriov1alpha1.ResourceDefinition
			if err := r.Get(ctx, client.ObjectKey{Name: rdName}, &rd); err == nil {
				if n := len(rd.Spec.VolumeDefinitions); n > 0 {
					volCount = n
				}
			}

			rdVolCounts[rdName] = volCount
		}

		for off := range int32(volCount) {
			out = append(out, *base+off)
		}
	}

	return out, nil
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

// resolveEffectiveProps walks the controller → RG → RD → Resource
// scopes and returns the merged DRBD-options bag the dispatcher
// hands to the satellite. Lower scopes override upper. Each scope is
// best-effort: a missing controller-props KV instance, missing RG
// reference, or missing RD all silently degrade to "empty" rather
// than block the dispatch — the resource can still come up with
// satellite-only defaults.
func (r *ResourceReconciler) resolveEffectiveProps(ctx context.Context, target *blockstoriov1alpha1.Resource, rdPtr *blockstoriov1alpha1.ResourceDefinition) (map[string]string, error) {
	controllerProps, err := r.controllerProps(ctx)
	if err != nil {
		return nil, err
	}

	var (
		rgProps map[string]string
		rdProps map[string]string
	)

	if rdPtr != nil {
		rdProps = rdPtr.Spec.Props

		if rdPtr.Spec.ResourceGroupName != "" {
			var rg blockstoriov1alpha1.ResourceGroup

			err := r.Get(ctx, client.ObjectKey{Name: rdPtr.Spec.ResourceGroupName}, &rg)
			switch {
			case err == nil:
				rgProps = rg.Spec.Props
			case errors.IsNotFound(err):
				// Soft-fail: the RG was deleted out from under
				// this RD. Skip the level rather than refuse to
				// dispatch — the rest of the hierarchy still
				// produces a usable .res.
			default:
				return nil, err
			}
		}
	}

	return drbd.ResolveOptions(controllerProps, rgProps, rdProps, target.Spec.Props), nil
}

// resolveLayerStack walks the RD → RG hierarchy and returns the
// effective layer composition. Returns nil for the dispatcher's
// default-fall-through behaviour when nothing is set anywhere — the
// dispatcher then defaults to whatever rd.Spec.LayerStack contains
// (also possibly nil → satellite-side default ["DRBD","STORAGE"]).
func (r *ResourceReconciler) resolveLayerStack(ctx context.Context, rd *blockstoriov1alpha1.ResourceDefinition) []string {
	if rd == nil {
		return nil
	}

	if len(rd.Spec.LayerStack) > 0 {
		return rd.Spec.LayerStack
	}

	if rd.Spec.ResourceGroupName == "" {
		return nil
	}

	var rg blockstoriov1alpha1.ResourceGroup
	if err := r.Get(ctx, client.ObjectKey{Name: rd.Spec.ResourceGroupName}, &rg); err != nil {
		return nil
	}

	return rg.Spec.SelectFilter.LayerStack
}

// controllerProps reads the cluster-wide ControllerProps KV instance
// via the KVEntry CRD. Empty when no entries exist (fresh cluster).
//
// We list every KVEntry and filter by Instance in-process; an
// indexed `client.MatchingFields{"spec.instance": "ControllerProps"}`
// would be cheaper but the controller-runtime cache rejects field
// selectors that aren't pre-registered as field indexers, and the
// extra wiring isn't worth it for a small KV store.
func (r *ResourceReconciler) controllerProps(ctx context.Context) (map[string]string, error) {
	var list blockstoriov1alpha1.KVEntryList

	err := r.List(ctx, &list)
	if err != nil {
		return nil, err
	}

	out := map[string]string{}

	for i := range list.Items {
		if list.Items[i].Spec.Instance != "ControllerProps" {
			continue
		}

		out[list.Items[i].Spec.Key] = list.Items[i].Spec.Value
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
//
// We Watch ResourceDefinitions and sibling Resources too:
//   - RD changes (volume size, prop bag, encryption passphrase, quorum
//     toggle) must re-fire every replica's reconcile so the satellite
//     gets the updated VolumeDefinitions / DRBD options bag.
//   - Sibling-Resource changes (a witness gets created or removed)
//     must re-fire the OTHER replicas so their rendered .res reflects
//     the new peer set. Without this, R1's .res keeps the pre-witness
//     peer list and R3 can't connect (R1 doesn't know it exists).
func (r *ResourceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&blockstoriov1alpha1.Resource{}).
		Watches(&blockstoriov1alpha1.ResourceDefinition{},
			handler.EnqueueRequestsFromMapFunc(r.enqueueResourcesForRD)).
		Watches(&blockstoriov1alpha1.Resource{},
			handler.EnqueueRequestsFromMapFunc(r.enqueueSiblings)).
		Named("resource").
		Complete(r)
}

// enqueueResourcesForRD maps an RD event to every Resource that
// references it via Spec.ResourceDefinitionName.
func (r *ResourceReconciler) enqueueResourcesForRD(ctx context.Context, obj client.Object) []reconcile.Request {
	rd, ok := obj.(*blockstoriov1alpha1.ResourceDefinition)
	if !ok {
		return nil
	}

	return r.requestsForRD(ctx, rd.Name, "")
}

// enqueueSiblings maps a Resource event to every OTHER Resource of
// the same RD. The originator's own reconcile fires through For(),
// so we exclude it from the fan-out to avoid the redundant requeue.
func (r *ResourceReconciler) enqueueSiblings(ctx context.Context, obj client.Object) []reconcile.Request {
	res, ok := obj.(*blockstoriov1alpha1.Resource)
	if !ok || res.Spec.ResourceDefinitionName == "" {
		return nil
	}

	return r.requestsForRD(ctx, res.Spec.ResourceDefinitionName, res.Name)
}

// requestsForRD returns reconcile.Request entries for every Resource
// of the named RD, optionally excluding `excludeName` (used when the
// originating Resource is already getting its own reconcile via For).
func (r *ResourceReconciler) requestsForRD(ctx context.Context, rdName, excludeName string) []reconcile.Request {
	var resList blockstoriov1alpha1.ResourceList

	if err := r.List(ctx, &resList); err != nil {
		return nil
	}

	out := make([]reconcile.Request, 0, len(resList.Items))

	for i := range resList.Items {
		if resList.Items[i].Spec.ResourceDefinitionName != rdName {
			continue
		}

		if resList.Items[i].Name == excludeName {
			continue
		}

		out = append(out, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: resList.Items[i].Name},
		})
	}

	return out
}
