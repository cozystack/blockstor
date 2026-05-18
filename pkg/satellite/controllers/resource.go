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
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	corev1 "k8s.io/api/core/v1"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/dispatcher"
	"github.com/cozystack/blockstor/pkg/effectiveprops"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
)

// kubeInternalNICName is the NetInterface name the dispatcher prefers
// over piraeus-operator's `default-ipv4` (which gets the pod-CIDR
// satellite-pod IP, not routable peer-to-peer when blockstor
// satellites run with hostNetwork:true). We synthesise it from
// corev1.Node InternalIP at dispatch time so the .res files carry
// the routable host IP for every peer. Mirrors the constant in
// pkg/dispatcher (kept in sync — duplicated to avoid widening the
// dispatcher package's API surface for what is a satellite-side
// pre-processing concern). Bug 283.
const kubeInternalNICName = "k8s-internal"

// enrichNodesWithInternalIP appends a `k8s-internal` NetInterface
// to each blockstor Node CRD whose name matches a corev1.Node,
// using that corev1.Node's InternalIP. The dispatcher's
// preferredNetInterfaceAddress already favours `k8s-internal` over
// any other NetInterface name, so adding the entry steers peer
// address resolution onto the host-routable IP without touching
// the dispatcher code. Idempotent: skips nodes that already carry a
// `k8s-internal` entry (e.g. tests pre-stamp it) and nodes whose
// corev1.Node InternalIP cannot be resolved (the existing fallback
// chain still applies in that case).
func enrichNodesWithInternalIP(nodes []blockstoriov1alpha1.Node, corev1Nodes []corev1.Node) {
	internalIPByName := make(map[string]string, len(corev1Nodes))

	for i := range corev1Nodes {
		for _, addr := range corev1Nodes[i].Status.Addresses {
			if addr.Type == corev1.NodeInternalIP && addr.Address != "" {
				internalIPByName[corev1Nodes[i].Name] = addr.Address

				break
			}
		}
	}

	for i := range nodes {
		internalIP, ok := internalIPByName[nodes[i].Name]
		if !ok || internalIP == "" {
			continue
		}

		hasInternal := false

		for j := range nodes[i].Spec.NetInterfaces {
			if nodes[i].Spec.NetInterfaces[j].Name == kubeInternalNICName {
				hasInternal = true

				break
			}
		}

		if hasInternal {
			continue
		}

		nodes[i].Spec.NetInterfaces = append(nodes[i].Spec.NetInterfaces, blockstoriov1alpha1.NodeNetInterface{
			Name:    kubeInternalNICName,
			Address: internalIP,
		})
	}
}

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

	// Bug 40: REST shim sets Spec.ToggleDiskCancel when the operator
	// passes `?cancel=true` to PUT toggle-disk. Honour it before
	// the normal apply path so a cancel issued mid-conversion
	// unwinds partial state instead of racing the same apply that
	// caused the cancel.
	if res.Spec.ToggleDiskCancel {
		return r.handleToggleDiskCancel(ctx, &res, logger)
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

// peerAllocationRequeue is the short backoff applied when the
// controller-side allocator is still trailing on one of this RD's
// diskful peers (peer Resource exists but its Status.DRBDNodeID is
// nil). The cache-trail typically clears within hundreds of
// milliseconds — the controller-side reconciler stamps Status as
// part of its very next pass — so a 2s requeue is comfortably
// faster than the user-visible auto-place latency budget while
// still giving the apiserver time to commit the Status patch.
const peerAllocationRequeue = 2 * time.Second

// activeDRBDRequeue is the periodic self-evaluation requeue applied
// after a successful Apply on a DRBD-backed resource (Phase 11.7
// safety net). The primary-watch predicate filters out pure
// observer Status noise and the observer-trigger channel only fires
// on lifecycle changes, so the reconciler no longer wakes for every
// kernel statistics tick. This RequeueAfter is the belt-and-braces
// re-eval that catches the rare case where the events2 stream missed
// a frame (drbdsetup restart, kernel events2 stall) — without it the
// reconciler could go arbitrarily long without re-checking a
// long-lived active DRBD resource against its desired state.
//
// 10s is chosen to be:
//   - long enough that steady-state idle resources don't generate
//     pointless apiserver round-trips;
//   - short enough that any divergence between desired and observed
//     converges within a single user-visible scenario tick (e2e
//     tests poll at 1-2 s and budget ~30 s per assertion).
const activeDRBDRequeue = 10 * time.Second

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
	bldr := ctrl.NewControllerManagedBy(mgr).
		For(&blockstoriov1alpha1.Resource{},
			builder.WithPredicates(
				predicate.And(
					nodeNamePredicate(r.Config.NodeName),
					primaryWatchPredicate(),
				),
			)).
		Watches(&blockstoriov1alpha1.Resource{},
			handler.EnqueueRequestsFromMapFunc(r.enqueueLocalSiblings)).
		Watches(&blockstoriov1alpha1.ResourceDefinition{},
			handler.EnqueueRequestsFromMapFunc(r.enqueueResourcesForRD)).
		Named("satellite-resource")

	// Phase 11.7 second leg: the observer-trigger channel wakes
	// this reconciler on kernel-state changes that produce no
	// apiserver Spec write (peer flapping, role / disk / conn /
	// repl frames). Each GenericEvent carries the local Resource
	// object name; the EnqueueRequestForObject handler turns it
	// straight into a reconcile.Request, no per-event mapping
	// needed. Nil channel (unit-test path) short-circuits — the
	// builder skips the raw-source registration entirely.
	if r.Config.ReconcileTrigger != nil {
		bldr = bldr.WatchesRawSource(
			source.Channel(r.Config.ReconcileTrigger, &handler.EnqueueRequestForObject{}),
		)
	}

	err := bldr.Complete(r)
	if err != nil {
		return errors.Wrap(err, "register ResourceReconciler")
	}

	return nil
}

// primaryWatchPredicate filters Resource events on the primary For
// watch:
//   - Drop pure observer Status noise (Volumes / Conditions /
//     DrbdState / Connections / InUse / OutOfSync writes that
//     don't bump Generation).
//   - Pass Spec changes (Generation bump).
//   - Pass controller-side allocator Status writes
//     (Status.DRBDNodeID / Port / Minor transitions) — Phase 11.7:
//     without this whitelist the satellite's
//     `waitForControllerAllocation` gate never wakes after the
//     controller's allocator stamp, because the Status PATCH does
//     not bump Generation.
//   - Pass Create / Delete / Generic events (always) so the
//     observer-trigger channel and apiserver lifecycle events get
//     through unchanged.
//
// Pair with `nodeNamePredicate` via `predicate.And(...)` so this
// predicate ONLY filters events for the satellite's own Resources;
// foreign-node events are filtered out by nodeNamePredicate first.
//
// Safe to ship in Phase 11.7 because:
//  1. Status schema is now comprehensive (Phase 11.5.b P0+P1 landed
//     Role/Suspended/Quorum/PeerNodeId/PeerDiskState) — observer no
//     longer needs Reconcile-pulse writes to surface state.
//  2. Tests migrated to k8s-native Status reads (Phase 11.5.b) — they
//     depend on Status content, not on Reconcile firing on each
//     Status write.
//  3. .res file write is content-idempotent (Bug 315) — even if a
//     stray Reconcile fires from a Spec event, identical Spec
//     no-ops at the file layer.
//  4. Observer-trigger channel (second leg) gives the reconciler
//     wake-ups on observed kernel state that has no apiserver
//     Spec correlate, so the predicate drop doesn't strand
//     satellite-side recovery logic.
func primaryWatchPredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc:  func(_ event.CreateEvent) bool { return true },
		DeleteFunc:  func(_ event.DeleteEvent) bool { return true },
		GenericFunc: func(_ event.GenericEvent) bool { return true },
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldR, ok1 := e.ObjectOld.(*blockstoriov1alpha1.Resource)
			newR, ok2 := e.ObjectNew.(*blockstoriov1alpha1.Resource)

			if !ok1 || !ok2 {
				// Unknown wire type — fail open so an upstream c-r
				// change in event shape doesn't silently silence
				// every Update.
				return true
			}

			// Spec change → fire. Status subresource writes don't
			// touch Generation, so this catches every operator-
			// driven re-spec.
			if oldR.Generation != newR.Generation {
				return true
			}

			// Controller-allocator Status stamp → fire. The
			// satellite's `waitForControllerAllocation` gate is
			// load-bearing on these three fields; without the
			// whitelist a Status PATCH that fills them in never
			// wakes the reconciler and the recovery-down-reverses
			// scenario hangs (lesson from Bug 318).
			if !int32PtrEqual(oldR.Status.DRBDNodeID, newR.Status.DRBDNodeID) ||
				!int32PtrEqual(oldR.Status.DRBDPort, newR.Status.DRBDPort) ||
				!int32PtrEqual(oldR.Status.DRBDMinor, newR.Status.DRBDMinor) {
				return true
			}

			// Pure observer Status update (Volumes / Conditions /
			// DrbdState / Connections / InUse / OutOfSync). Drop —
			// the observer-trigger channel covers any genuine wake-
			// up need.
			return false
		},
	}
}

// int32PtrEqual returns true when two *int32 pointers describe the
// same value, treating both-nil as equal. Used by
// primaryWatchPredicate to compare the allocator's DRBD-ID Status
// fields between old/new event snapshots.
func int32PtrEqual(left, right *int32) bool {
	if left == nil && right == nil {
		return true
	}

	if left == nil || right == nil {
		return false
	}

	return *left == *right
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

	// Read peer Resources fresh via APIReader (uncached). The c-r
	// informer cache trails the apiserver by hundreds of milliseconds,
	// and the auto-primary election downstream needs every diskful
	// peer's Status.DRBDNodeID to make a deterministic choice — a
	// partial peer set rolls the dice and can elect two primaries
	// (see Bug 80 in CHANGELOG / lowestDiskfulID comment).
	peers, err := r.listPeerResources(ctx, res)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "list peer Resources")
	}

	// The DRBD-ID allocation gate only matters when the RD actually
	// uses DRBD. A LayerStack=["STORAGE"] (or ["LUKS","STORAGE"]) RD
	// renders no .res and the kernel never sees a node-id, so waiting
	// for the controller to stamp Status.DRBDNodeID/Port/Minor would
	// block apply forever — they never come.
	if rdNeedsDRBD(&rd) {
		if waitResult, waitOK := r.waitForControllerAllocation(ctx, res, peers, logger); !waitOK {
			return waitResult, nil
		}
	}

	desired, err := r.buildDesiredFromCRD(ctx, res, &rd, peers)
	if err != nil {
		return ctrl.Result{}, err
	}

	results, err := r.Config.Apply.Apply(ctx, []*intent.DesiredResource{desired})
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "satellite Apply")
	}

	anyFailed := r.recordPerResourceFailures(results, logger)

	r.stampPostApply(ctx, res, &rd, results, anyFailed, logger)

	// Apply chain surfaces per-resource errors via results (e.g.
	// drbdadm adjust failing on a stale .res rendered before the
	// peer's Status caught up). Returning nil here would let c-r
	// stop until an external event drove another reconcile;
	// RequeueAfter ensures the next attempt sees the freshly-
	// committed peer state.
	if anyFailed {
		return ctrl.Result{RequeueAfter: applyFailureRequeue}, nil
	}

	// Phase 11.7 safety net: schedule a periodic re-eval on
	// active-DRBD resources. The primary-watch predicate drops
	// pure observer Status noise and the observer-trigger channel
	// only fires on lifecycle changes; combined, the reconciler no
	// longer wakes on every kernel statistics tick. Without a
	// belt-and-braces requeue, a missed events2 frame (drbdsetup
	// restart, kernel stall) could leave the resource un-re-eval'd
	// for arbitrarily long. Only fires on DRBD-stack RDs — pure
	// STORAGE-only RDs have no kernel-state to drift against, so
	// keeping them quiet preserves the predicate's no-op-on-idle
	// property.
	if rdNeedsDRBD(&rd) {
		return ctrl.Result{RequeueAfter: activeDRBDRequeue}, nil
	}

	return ctrl.Result{}, nil
}

// recordPerResourceFailures logs each per-resource Apply failure and
// returns whether any of the results came back not-Ok. Extracted from
// runApply so the orchestration stays under the funlen budget.
func (r *ResourceReconciler) recordPerResourceFailures(results []*intent.ResourceApplyResult, logger logr.Logger) bool {
	var anyFailed bool

	for _, ar := range results {
		if !ar.GetOk() {
			anyFailed = true

			logger.Info("Apply per-resource failure", "name", ar.GetName(), "message", ar.GetMessage())
		}
	}

	return anyFailed
}

// stampPostApply runs the three best-effort post-apply stamps:
// Status.Volumes (per-volume DevicePath), the Bug-107 volume-numbers
// annotation, and the Bug-39 toggle-disk retry counter. Each helper
// is independently fallible — we log and move on so a transient
// apiserver hiccup on one stamp doesn't suppress the others.
func (r *ResourceReconciler) stampPostApply(ctx context.Context, res *blockstoriov1alpha1.Resource, rd *blockstoriov1alpha1.ResourceDefinition, results []*intent.ResourceApplyResult, anyFailed bool, logger logr.Logger) {
	// Stamp per-volume DevicePath into Status.Volumes so
	// linstor-csi / any consumer that reads the CRD sees the
	// /dev path the satellite materialised. Done even when one
	// volume's apply failed: a partial success is still useful
	// to surface (volumes that did apply have a real device).
	err := r.stampVolumeStatus(ctx, res, results)
	if err != nil {
		logger.Error(err, "stamp Status.Volumes")
	}

	// Bug 107: persist the parent RD's volume-number set onto the
	// Resource itself so a future cascade-delete (RD CRD removed
	// first, then per-Resource finalizer fires) can still tell
	// `Apply.DeleteResource` which volume numbers to clean up. The
	// annotation is the only surviving record once the RD is gone
	// — without it, `handleDelete` iterates over zero volumes and
	// the backing .img / ZVOL / LV leaks forever.
	err = r.stampVolumeNumbersAnnotation(ctx, res, rd)
	if err != nil {
		logger.Error(err, "stamp volume-numbers annotation")
	}

	// Bug 39: bump or clear Status.ToggleDiskRetries based on the
	// per-resource result. The helper is a no-op when the Resource
	// isn't mid diskless→diskful conversion, so it's safe to call
	// unconditionally on every apply pass.
	err = r.recordToggleDiskOutcome(ctx, res, !anyFailed)
	if err != nil {
		logger.Error(err, "record toggle-disk outcome")
	}
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

	// Source of truth for StoragePool is `Resource.Spec.StoragePool` —
	// authored by the controller-side dispatcher before the satellite
	// took over the reconcile. Carry it onto every per-volume Status
	// entry so the REST view layer's `volumesFromStatus()` projection
	// surfaces a real pool name in `linstor v l` instead of `None`
	// (Bug 75). Empty Spec.StoragePool is fine — DISKLESS replicas
	// legitimately have no pool and the field is `omitempty` so SSA
	// won't claim ownership of an empty string.
	pool := res.Spec.StoragePool

	vols := make([]blockstoriov1alpha1.ResourceVolumeStatus, 0, len(results[0].GetVolumes()))
	for _, v := range results[0].GetVolumes() {
		vols = append(vols, blockstoriov1alpha1.ResourceVolumeStatus{
			VolumeNumber: v.GetVolumeNumber(),
			StoragePool:  pool,
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

// stampVolumeNumbersAnnotation writes the parent RD's volume-number
// set onto the Resource's metadata.annotations under
// `blockstor.io/volume-numbers`. The value is a comma-separated list
// of int32 (e.g. "0,1,2"). This is the Bug-107 fallback record:
// when `linstor rd delete <X>` cascade-deletes the parent RD CRD via
// owner refs, the satellite's `handleDelete` runs AFTER the RD is
// already gone — without this annotation, `lookupVolumeNumbers`
// returns an empty slice on the NotFound path and the per-volume
// DeleteVolume loop iterates over zero items.
//
// Uses a JSON-merge patch with a re-fetch-and-compare guard to avoid
// hot-loop writes when the annotation value hasn't changed. The
// volume-number set only changes when an operator adds or removes a
// VolumeDefinition on the parent RD, so the typical apply pass
// short-circuits at the equality check.
//
// JSON-merge (rather than SSA Apply) keeps the patch minimal and
// avoids stomping other field-managers' annotation entries. The
// payload is `{"metadata":{"annotations":{"<key>":"<value>"}}}` —
// kube-apiserver merges this in place, preserving every other
// annotation already stamped on the Resource.
func (r *ResourceReconciler) stampVolumeNumbersAnnotation(ctx context.Context, res *blockstoriov1alpha1.Resource, rd *blockstoriov1alpha1.ResourceDefinition) error {
	want := formatVolumeNumbers(rd.Spec.VolumeDefinitions)
	if want == "" {
		// Defensive — an RD with zero VolumeDefinitions is a no-op
		// at the storage layer too. Don't claim a "0 volumes" record
		// that could confuse a later operator reading the annotation.
		return nil
	}

	if res.Annotations[blockstoriov1alpha1.ResourceAnnotationVolumeNumbers] == want {
		return nil
	}

	// JSON-merge: kube-apiserver applies the patch as a recursive
	// merge against the live object's metadata.annotations map.
	// JSON-encoding the value through %q gives correct quoting for
	// the (digit-only) annotation value the formatter emits.
	body := []byte(`{"metadata":{"annotations":{"` +
		blockstoriov1alpha1.ResourceAnnotationVolumeNumbers + `":"` + want + `"}}}`)

	err := r.Patch(ctx, res, client.RawPatch(types.MergePatchType, body))
	if err != nil {
		return errors.Wrap(err, "merge-patch annotation blockstor.io/volume-numbers")
	}

	return nil
}

// formatVolumeNumbers renders a slice of VolumeDefinitions as the
// comma-separated list stored in the Bug-107 fallback annotation.
// Empty input → empty string (caller short-circuits on empty).
func formatVolumeNumbers(defs []blockstoriov1alpha1.ResourceDefinitionVolume) string {
	if len(defs) == 0 {
		return ""
	}

	parts := make([]string, 0, len(defs))
	for i := range defs {
		parts = append(parts, strconv.FormatInt(int64(defs[i].VolumeNumber), 10))
	}

	return strings.Join(parts, ",")
}

// parseVolumeNumbers is the inverse of formatVolumeNumbers. Malformed
// entries (non-int32, empty after split) are skipped silently — the
// annotation is an operator-readable hint, not an authoritative wire
// contract, so a partial parse is preferable to refusing to delete
// anything on a single corrupted byte.
func parseVolumeNumbers(s string) []int32 {
	if s == "" {
		return nil
	}

	parts := strings.Split(s, ",")
	out := make([]int32, 0, len(parts))

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		n, err := strconv.ParseInt(part, 10, 32)
		if err != nil {
			continue
		}

		out = append(out, int32(n))
	}

	return out
}

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
// for the Node + StoragePool lists, resolving effective DRBD
// options via the shared `pkg/effectiveprops` package, and
// handing everything to `dispatcher.BuildDesired`.
//
// Peer Resources are supplied by the caller (already read
// uncached via APIReader in `runApply`) so the auto-primary
// election sees every diskful peer's allocated NodeID rather
// than racing the c-r informer cache.
//
// This is the load-bearing replacement for the gRPC dispatch
// path — the controller-runtime reconciler does exactly the
// same work the controller's dispatcher did, just on the
// satellite side. Phase 10.1.
func (r *ResourceReconciler) buildDesiredFromCRD(ctx context.Context, target *blockstoriov1alpha1.Resource, rd *blockstoriov1alpha1.ResourceDefinition, peers []blockstoriov1alpha1.Resource) (*intent.DesiredResource, error) {
	var nodeList blockstoriov1alpha1.NodeList

	err := r.List(ctx, &nodeList)
	if err != nil {
		return nil, errors.Wrap(err, "list Nodes")
	}

	// Bug 283: piraeus-operator overwrites Node CRD NetInterfaces
	// with a single `default-ipv4` entry carrying the satellite POD
	// IP — a pod-CIDR address that isn't routable between
	// hostNetwork:true blockstor satellites. Synthesise a
	// `k8s-internal` NetInterface from each corev1.Node's
	// InternalIP so the dispatcher's existing preferred-name lookup
	// (`k8s-internal` → `default` → first non-empty) lands on the
	// routable host IP for every peer block in the rendered .res.
	// Failures listing corev1.Nodes are non-fatal: the old
	// pod-CIDR fallback still produces a syntactically valid .res,
	// just one that can't connect — the same behaviour as before.
	var corev1NodeList corev1.NodeList

	listErr := r.List(ctx, &corev1NodeList)
	if listErr == nil {
		enrichNodesWithInternalIP(nodeList.Items, corev1NodeList.Items)
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

	desired := dispatcher.BuildDesired(target, peers, nodeList.Items, poolList.Items, rd, effectiveProps)

	// Phase 11.3 Stage 1: carry the `MetadataCreated=True` Status
	// Condition from the Resource CRD into the apply chain so the
	// satellite reconciler's `firstActivation` predicate (and the
	// FSM shadow's Observation.MetadataExists) can derive from the
	// apiserver view without round-tripping through the kernel
	// dump-md probe on every reconcile. The on-disk `.md-created`
	// marker remains as a migration-window fallback.
	desired.MetadataCreated = meta.IsStatusConditionTrue(target.Status.Conditions, blockstoriov1alpha1.ConditionMetadataCreated)

	return desired, nil
}

// listPeerResources lists every Resource that belongs to the same
// ResourceDefinition as target, including target itself. Reads from
// the APIReader (uncached, direct apiserver) when one is wired so
// the cache-trail race on Status.DRBDNodeID can't elect two
// auto-primaries during a fresh auto-place: the informer cache may
// not yet have observed the controller-side allocator's Status patch
// on the peer Resource, and acting on that stale view is exactly
// what produces Bug 80's stuck-Inconsistent outcome. Falls back to
// the cached client when APIReader is nil (unit-test path).
func (r *ResourceReconciler) listPeerResources(ctx context.Context, target *blockstoriov1alpha1.Resource) ([]blockstoriov1alpha1.Resource, error) {
	reader := r.peerReader()

	var resList blockstoriov1alpha1.ResourceList

	err := reader.List(ctx, &resList)
	if err != nil {
		return nil, errors.Wrap(err, "list Resources for peer set")
	}

	peers := make([]blockstoriov1alpha1.Resource, 0, len(resList.Items))

	for i := range resList.Items {
		if resList.Items[i].Spec.ResourceDefinitionName == target.Spec.ResourceDefinitionName {
			peers = append(peers, resList.Items[i])
		}
	}

	return peers, nil
}

// peerReader returns the APIReader (uncached) when the manager
// wired one in, falling back to the cached client for unit tests
// that construct the reconciler directly. Mirrors `deleteReader`.
func (r *ResourceReconciler) peerReader() client.Reader {
	if r.Config.APIReader != nil {
		return r.Config.APIReader
	}

	return r.Client
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
// DRBDPort / DRBDMinor — for THIS replica AND for every diskful
// peer of the same RD. Without the peer-side check, a fresh
// `linstor r c --auto-place=2` lands two Resources at the
// satellites before the controller's allocator finishes patching
// Status on both: each satellite sees its own NodeID but not the
// peer's, the auto-primary election in `dispatcher.BuildDesired`
// degenerates to "I'm the lowest because I'm the only one with an
// id", and BOTH replicas run `drbdadm primary --force` on first
// activation. Result: divergent UUIDs, split-brain or both stuck
// Inconsistent forever. The target-only check that used to live
// here was sufficient before per-replica allocation, but the
// allocator now races the satellite's cache for peer Status —
// hence the broadened gate.
//
// Without the target-only check, dispatcher.BuildDesired silently
// defaults the missing fields to 0 / "0.0.0.0" / minor 0 and we'd
// render a .res claiming the local node is `node-id 0`. drbdsetup
// new-resource burns that into kernel state at first adjust, and
// the next reconcile (after the controller writes the real
// allocation) hits `peer node id cannot be my own node id` forever
// because the kernel's recorded my-id never moves.
//
// Returns ok=true when allocation is fresh on every diskful
// replica; ok=false with a short-backoff requeue Result when any
// of the IDs is still nil. The peer-side check uses the shorter
// `peerAllocationRequeue` because the controller-side allocator
// typically completes within hundreds of milliseconds — no point
// waiting the full apply-failure backoff for a write that's
// already in flight.
//
// Bug 289: when the cached view of `res` reports nil DRBD-IDs the
// controller may have already stamped them — the satellite's c-r
// informer cache trails the apiserver during the initial-create
// burst, and a single stale watch event can pin the cache on a
// pre-allocation revision well past the controller's actual
// commit. We re-fetch the Resource through the uncached APIReader
// before declaring "still nil" so the recovery-down-reverses
// revive path (which depends on a previously-allocated Resource)
// no longer wedges behind a stale cache. Mutates `res` in place
// so the caller's downstream `dispatcher.BuildDesired` sees the
// fresh allocation in the same reconcile.
func (r *ResourceReconciler) waitForControllerAllocation(ctx context.Context, res *blockstoriov1alpha1.Resource, peers []blockstoriov1alpha1.Resource, logger logr.Logger) (ctrl.Result, bool) {
	if res.Status.DRBDNodeID == nil || res.Status.DRBDPort == nil || res.Status.DRBDMinor == nil {
		// APIReader fall-through (Bug 289): when the cached view
		// reports nil DRBD-IDs, the controller may have already
		// stamped them — re-fetch uncached before declaring the
		// wait. refreshTargetFromAPIReader mutates `res` in place
		// when the apiserver has the fresh allocation.
		if !r.refreshTargetFromAPIReader(ctx, res, logger) {
			logger.Info("waiting for controller-side DRBD-ID allocation",
				"nodeID", res.Status.DRBDNodeID,
				"port", res.Status.DRBDPort,
				"minor", res.Status.DRBDMinor)

			return ctrl.Result{RequeueAfter: applyFailureRequeue}, false
		}
	}

	if !dispatcher.DiskfulPeersAllocated(res, peers) {
		logger.Info("waiting for peer Status.DRBDNodeID allocation",
			"missingPeers", peersMissingNodeID(res, peers))

		return ctrl.Result{RequeueAfter: peerAllocationRequeue}, false
	}

	return ctrl.Result{}, true
}

// refreshTargetFromAPIReader does an uncached Get on the target
// Resource and overwrites `res` in place when the APIReader reports
// non-nil DRBD-IDs. Returns true iff every DRBD-ID is now populated,
// signalling the caller to fall through the wait gate. Returns false
// when the APIReader also reports nil (controller really hasn't
// stamped yet) or on lookup error (caller falls back to the cached
// view and the next requeue retries).
//
// Bug 289: the c-r informer cache can pin a Resource on a pre-
// allocation revision past the controller's commit (race window
// observable on the recovery-down-reverses scenario when satellite
// reconciles fire from a sibling-Resource Watches event before the
// target's own watch tick lands). APIReader bypasses the cache so a
// single requeue is enough — without this the satellite chains 5s
// requeues against the same stale cache entry indefinitely.
func (r *ResourceReconciler) refreshTargetFromAPIReader(ctx context.Context, res *blockstoriov1alpha1.Resource, logger logr.Logger) bool {
	reader := r.peerReader()
	if reader == nil {
		return false
	}

	var fresh blockstoriov1alpha1.Resource

	err := reader.Get(ctx, client.ObjectKey{Name: res.Name}, &fresh)
	if err != nil {
		logger.V(1).Info("APIReader refresh failed; falling back to cached view",
			"err", err.Error())

		return false
	}

	if fresh.Status.DRBDNodeID == nil || fresh.Status.DRBDPort == nil || fresh.Status.DRBDMinor == nil {
		return false
	}

	res.Status.DRBDNodeID = fresh.Status.DRBDNodeID
	res.Status.DRBDPort = fresh.Status.DRBDPort
	res.Status.DRBDMinor = fresh.Status.DRBDMinor

	return true
}

// peersMissingNodeID collects the names of diskful peer Resources
// whose Status.DRBDNodeID is still nil. Pure cosmetics — surfaces
// the laggard names in the wait log so operators don't have to
// kubectl-walk every peer to figure out which Status the controller
// hasn't stamped yet.
func peersMissingNodeID(target *blockstoriov1alpha1.Resource, peers []blockstoriov1alpha1.Resource) []string {
	missing := []string{}

	for i := range peers {
		peer := &peers[i]
		if peer.Spec.NodeName == target.Spec.NodeName {
			continue
		}

		if slices.Contains(peer.Spec.Flags, apiv1.ResourceFlagDiskless) {
			continue
		}

		if peer.Status.DRBDNodeID == nil {
			missing = append(missing, peer.Spec.NodeName)
		}
	}

	return missing
}

// handleDelete runs the satellite-side teardown when a Resource
// gets a DeletionTimestamp. Idempotent — re-runs after a
// satellite restart safely re-issue `drbdadm down` /
// `DeleteVolume` / `cryptsetup luksClose`, all of which are
// no-ops on already-torn-down state. Removes our finalizer on
// success so kube-apiserver finalises the delete.
//
// Bug 65 ordering contract (matches the tiebreaker-race fix in
// memory/blockstor_tiebreaker_race.md):
//
//  1. The Resource passed in came from the cached client at the
//     top of Reconcile, so its `Finalizers` slice may already be
//     stale by the time we get here (the controller-side path
//     force-strips its own `blockstor.io.blockstor.io/resource`
//     finalizer concurrently). Operate on the slice as-is for
//     the gating short-circuit only.
//  2. Run the provider tear-down (Apply.DeleteResource: drbdadm
//     down + DeleteVolume per volume + .res cleanup) BEFORE
//     touching the finalizer. The previous order was already
//     correct on this point; Bug 65 keeps it.
//  3. AFTER DeleteResource returns Ok, re-fetch the Resource via
//     the APIReader (uncached) so we see the freshest finalizer
//     set, then RemoveFinalizer + Update. Without this re-read,
//     the Update built from the cache-trailed snapshot can clobber
//     a concurrent finalizer edit and leave an orphan zvol on
//     one of two diskful peers (the bug's surface symptom).
//  4. Storage Provider's DeleteVolume is idempotent on missing
//     volumes — a second handleDelete pass after a partial first
//     pass (DeleteResource succeeded, Update failed) re-issues
//     the storage tear-down safely. Bug 43's storage sweeper is
//     the wider-scope backstop; this ordering fix is the
//     per-resource source-of-truth path.
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

	volumeNumbers, err := r.lookupVolumeNumbers(ctx, res)
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

	// Re-fetch via APIReader (uncached) so we strip ours off the
	// freshest finalizer list. Falls back to the cached client
	// when APIReader is nil (unit-test path).
	reader := r.deleteReader()

	var latest blockstoriov1alpha1.Resource

	err = reader.Get(ctx, client.ObjectKey{Name: res.Name}, &latest)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// Someone else (force-strip from the controller, the
			// sweeper after a hung apply) finished the delete
			// already — DeleteResource above was the idempotent
			// no-op re-issue, nothing more to do.
			return ctrl.Result{}, nil
		}

		return ctrl.Result{}, errors.Wrap(err, "APIReader re-fetch for finalizer strip")
	}

	if !controllerutil.RemoveFinalizer(&latest, SatelliteResourceFinalizer) {
		// Already gone — the Update on a previous pass either
		// landed or a concurrent actor stripped it. Either way,
		// no-op; the apiserver-finalise path takes over.
		return ctrl.Result{}, nil
	}

	err = r.Update(ctx, &latest)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "strip finalizer")
	}

	return ctrl.Result{}, nil
}

// deleteReader returns the APIReader-direct reader when the
// manager wired one in, falling back to the cached client for
// unit tests that construct the reconciler directly. Mirrors
// `internal/controller.ResourceDefinitionReconciler.directOrCached`.
func (r *ResourceReconciler) deleteReader() client.Reader {
	if r.Config.APIReader != nil {
		return r.Config.APIReader
	}

	return r.Client
}

// lookupVolumeNumbers reads the parent RD and returns its
// VolumeDefinitions' numbers. The satellite's DeleteResource uses
// the list to drop matching LVs / loopfiles.
//
// Bug 107: when `linstor rd delete` cascade-deletes the parent RD
// CRD via owner refs, the satellite's `handleDelete` runs AFTER the
// RD is already gone. The RD Get returns NotFound, but we still need
// to feed `DeleteResource` the volume-number set so the per-volume
// `provider.DeleteVolume` loop actually unlinks the backing storage.
// Fall back to the `blockstor.io/volume-numbers` annotation that
// `stampVolumeNumbersAnnotation` writes on every successful apply.
//
// The fallback is best-effort: an annotation parse miss (corrupted
// value, never-applied resource) returns nil, and the per-volume
// loop in DeleteResource no-ops. That's the pre-Bug-107 behaviour
// and matches the contract that DeleteVolume on a missing volume is
// already idempotent.
func (r *ResourceReconciler) lookupVolumeNumbers(ctx context.Context, res *blockstoriov1alpha1.Resource) ([]int32, error) {
	var rd blockstoriov1alpha1.ResourceDefinition

	err := r.Get(ctx, client.ObjectKey{Name: res.Spec.ResourceDefinitionName}, &rd)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// Cascade-delete path — RD CRD vanished before we got
			// here. Fall back to the annotation we stamped on the
			// last successful apply.
			return parseVolumeNumbers(res.Annotations[blockstoriov1alpha1.ResourceAnnotationVolumeNumbers]), nil
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
