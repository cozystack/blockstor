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

package controllers

import (
	"context"
	"encoding/json"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/cockroachdb/errors"
	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	corev1 "k8s.io/api/core/v1"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/conflict"
	"github.com/cozystack/blockstor/pkg/dispatcher"
	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/effectiveprops"
	"github.com/cozystack/blockstor/pkg/finalizers"
	"github.com/cozystack/blockstor/pkg/luks"
	"github.com/cozystack/blockstor/pkg/naming"
	"github.com/cozystack/blockstor/pkg/satellite"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	storek8s "github.com/cozystack/blockstor/pkg/store/k8s"
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
				internalIPByName[naming.Key(corev1Nodes[i].Name)] = addr.Address

				break
			}
		}
	}

	for i := range nodes {
		internalIP, ok := internalIPByName[naming.Key(nodes[i].Name)]
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
// distinct key (`blockstor.org/resource`) so the
// two paths coexist during the Phase 10.6 cutover without
// stepping on each other.
const SatelliteResourceFinalizer = finalizers.SatelliteResource

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

	// passes counts the passes by reason. SetupWithManager sets it and
	// counts Reconcile through it; a test calling Reconcile directly goes
	// around it.
	passes *passCounter

	// bringUp ranks the first pass of every local Resource in this
	// process ahead of every other pass (bringUpQueue). SetupWithManager
	// creates it.
	bringUp *bringUpTracker
	// conflicts retries a pass that lost an optimistic-concurrency
	// race and counts such races per object.
	conflicts conflict.Retrier

	// retries spaces the retries of a replica whose apply keeps failing.
	retries applyBackoff

	// deleteRetries spaces the retries of a replica whose delete keeps
	// failing.
	deleteRetries applyBackoff

	// waits reports an apply that waits too long for the controller.
	waits controllerWaits
}

// Reconcile runs one pass of reconcileResource, retries one that lost
// an optimistic-concurrency race instead of reporting it as an error
// (see conflict.Retrier), and settles the queue priority of the
// result.
//
// Every retry (a failed apply, an error, a wait on the allocator, work
// a pass put off) is pinned to normal priority: controller-runtime
// otherwise keeps the priority the item was dequeued with, so a replica
// first seen in the informer's initial list would be retried at low
// priority, behind every other replica of the start-up.
func (r *ResourceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	result, err := r.conflicts.Reconcile(ctx, req, r.reconcileResource)
	if result.Priority == nil {
		result.Priority = normalPriority()
	}

	return result, err //nolint:wrapcheck // the pass's own error, already wrapped
}

// applyFailureRequeue is the first backoff between satellite Apply
// retries when at least one per-resource result reported failure;
// applyBackoff doubles it for each retry that came due and failed.
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

// deleteFailureRequeue is the first delay before a failed delete of a
// replica is retried, doubled for each retry that fails, up to
// deleteFailureMaxRequeue. The usual failure is ZFS refusing to destroy
// a zvol it still counts as open (`dataset is busy`) for a moment after
// `drbdsetup down`, which clears within seconds, so the delay starts
// short; retried on every event and through the queue's own back-off,
// one such delete was logged seventeen times in six seconds.
const (
	deleteFailureRequeue    = 500 * time.Millisecond
	deleteFailureMaxRequeue = 30 * time.Second
)

// normalPriority is the queue priority of a real event, and of every
// retry of a pass that did not converge: an item first queued from the
// informer's initial list carries handler.LowPriority, and
// controller-runtime keeps the priority it was dequeued with.
func normalPriority() *int {
	priority := 0

	return &priority
}

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
//
// Every way an event reaches the queue is counted for the pass report:
// For, the kernel trigger and the drift check through r.passes.marker,
// every Watches map function through r.passes.mapFunc. A Watches added
// without it still works, but its passes are counted as retries.
//
// A converged replica gets no periodic pass. A change of anything else
// a pass renders from reaches it through watchRenderInputs, and the
// drift check (see driftCheckInterval) passes the replicas drbdadm
// lists as adjustable.
func (r *ResourceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.passes = newPassCounter()

	if r.bringUp == nil {
		r.bringUp = newBringUpTracker()
	}

	bldr := ctrl.NewControllerManagedBy(mgr).
		For(&blockstoriov1alpha1.Resource{},
			builder.WithPredicates(
				predicate.And(
					nodeNamePredicate(r.Config.NodeName),
					primaryWatchPredicate(),
					r.passes.marker(passResource),
				),
			)).
		Watches(&blockstoriov1alpha1.Resource{},
			handler.EnqueueRequestsFromMapFunc(r.passes.mapFunc(passPeer, r.enqueueLocalSiblings)),
			builder.WithPredicates(siblingWatchPredicate())).
		Watches(&blockstoriov1alpha1.ResourceDefinition{},
			handler.EnqueueRequestsFromMapFunc(r.passes.mapFunc(passDefinition, r.enqueueResourcesForRD))).
		Named("satellite-resource").
		WithOptions(controller.Options{
			MaxConcurrentReconciles: r.Config.resourceWorkers(),
			NewQueue:                newBringUpQueueFunc(r.bringUp, mgr.GetControllerOptions().Logger),
		})

	bldr = r.watchRenderInputs(bldr)

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
			source.Channel(r.Config.ReconcileTrigger, &handler.EnqueueRequestForObject{},
				source.WithPredicates[client.Object, reconcile.Request](r.passes.marker(passKernel))),
		)
	}

	bldr, drift := r.watchDrift(mgr, bldr)

	err := bldr.Complete(r.passes.counted(r))
	if err != nil {
		return errors.Wrap(err, "register ResourceReconciler")
	}

	logger := mgr.GetLogger().WithName("satellite-resource")

	err = mgr.Add(manager.RunnableFunc(r.passes.reportEvery(passReportInterval, logger)))
	if err != nil {
		return errors.Wrap(err, "register resource pass report")
	}

	if drift != nil {
		err = mgr.Add(manager.RunnableFunc(drift.run(logger.WithName("drift-check"))))
		if err != nil {
			return errors.Wrap(err, "register drift check")
		}
	}

	return nil
}

// watchDrift wires the drift check (see driftCheckInterval) into the
// builder; it returns nil when the satellite has no exec to ask drbdadm
// with.
func (r *ResourceReconciler) watchDrift(mgr ctrl.Manager, bldr *builder.Builder) (*builder.Builder, *driftCheck) {
	if r.Config.Exec == nil {
		return bldr, nil
	}

	trigger := make(chan event.GenericEvent, reconcileTriggerBuffer)
	drift := &driftCheck{
		reader: mgr.GetClient(), exec: r.Config.Exec, node: r.Config.NodeName,
		out: trigger, interval: r.Config.DriftCheckInterval,
	}

	return bldr.WatchesRawSource(
		source.Channel(trigger, &handler.EnqueueRequestForObject{},
			source.WithPredicates[client.Object, reconcile.Request](r.passes.marker(passDrift))),
	), drift
}

// reconcileResource reads a Resource and drives the satellite-side
// apply chain when the Resource is placed on this node.
// Finalizer-aware: stamps `SatelliteResourceFinalizer` on
// every live Resource so kube-apiserver waits for our tear-down
// before allowing the object to disappear.
func (r *ResourceReconciler) reconcileResource(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("resource", req.Name)

	var res blockstoriov1alpha1.Resource

	err := r.Get(ctx, req.NamespacedName, &res)
	if err != nil {
		if apierrors.IsNotFound(err) {
			r.bringUp.forget(req.Name)
			r.retries.reset(req.Name)
			r.deleteRetries.reset(req.Name)
			r.waits.forget(req.Name)

			return ctrl.Result{}, nil
		}

		return ctrl.Result{}, errors.Wrap(err, "get Resource")
	}

	if !naming.Same(res.Spec.NodeName, r.Config.NodeName) {
		// Predicate should have filtered this out — defensive
		// check in case the watch cache is mid-resync.
		return ctrl.Result{}, nil
	}

	// From here a pass of the replica counts as its first one, whatever
	// it ends with, so it gives up the bring-up rank (bringUpTracker).
	// A pass that only stamps the finalizer does not: the pass it
	// requeues is the one that brings the replica up.
	hasFinalizer := slices.Contains(res.Finalizers, SatelliteResourceFinalizer)
	if hasFinalizer || !res.DeletionTimestamp.IsZero() {
		r.bringUp.start(res.Name)
	}

	if !res.DeletionTimestamp.IsZero() {
		return r.handleDelete(ctx, &res)
	}

	if !hasFinalizer {
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

// siblingWatchPredicate filters the events of other nodes' Resources
// before enqueueLocalSiblings maps them onto this node's replica of
// the same definition. It passes only what the local apply reads off
// a peer: its spec (generation), its deletion, the allocator stamp,
// and the per-volume disk state and current GI the day0 seed and the
// auto-primary election consult (dispatcher.anyDiskfulPeerHasData).
//
// The peer's connections, role, in-use flag, out-of-sync counters and
// conditions are dropped. Each peer's observer rewrites them on every
// kernel frame, and every such write used to re-queue this node's
// replica at normal priority: on a node holding a hundred replicas the
// queue never drained, and a newly created Resource waited minutes
// behind replicas with nothing to do. This node's own kernel view of
// a peer reaches the reconciler through the observer-trigger channel.
func siblingWatchPredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc:  func(_ event.CreateEvent) bool { return true },
		DeleteFunc:  func(_ event.DeleteEvent) bool { return true },
		GenericFunc: func(_ event.GenericEvent) bool { return true },
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldR, ok1 := e.ObjectOld.(*blockstoriov1alpha1.Resource)
			newR, ok2 := e.ObjectNew.(*blockstoriov1alpha1.Resource)

			if !ok1 || !ok2 {
				return true
			}

			return siblingChangeAffectsApply(oldR, newR)
		},
	}
}

// siblingChangeAffectsApply reports whether an update of a peer
// Resource changes anything the local apply derives from that peer.
func siblingChangeAffectsApply(oldR, newR *blockstoriov1alpha1.Resource) bool {
	if oldR.Generation != newR.Generation {
		return true
	}

	if (oldR.DeletionTimestamp == nil) != (newR.DeletionTimestamp == nil) {
		return true
	}

	if !int32PtrEqual(oldR.Status.DRBDNodeID, newR.Status.DRBDNodeID) ||
		!int32PtrEqual(oldR.Status.DRBDPort, newR.Status.DRBDPort) ||
		!int32PtrEqual(oldR.Status.DRBDMinor, newR.Status.DRBDMinor) {
		return true
	}

	return !sameVolumeDataState(oldR.Status.Volumes, newR.Status.Volumes)
}

// sameVolumeDataState compares the per-volume disk state and current
// GI of two Status.Volumes lists, keyed by volume number.
func sameVolumeDataState(left, right []blockstoriov1alpha1.ResourceVolumeStatus) bool {
	if len(left) != len(right) {
		return false
	}

	type dataState struct{ disk, gi string }

	byNumber := make(map[int32]dataState, len(left))
	for i := range left {
		byNumber[left[i].VolumeNumber] = dataState{left[i].DiskState, left[i].CurrentGI}
	}

	for i := range right {
		prev, ok := byNumber[right[i].VolumeNumber]
		if !ok || prev != (dataState{right[i].DiskState, right[i].CurrentGI}) {
			return false
		}
	}

	return true
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
		if naming.Same(local.Spec.ResourceDefinitionName, rd.Name) &&
			naming.Same(local.Spec.NodeName, r.Config.NodeName) {
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
	rd, found, err := r.definitionForApply(ctx, res, logger)
	if !found || err != nil {
		return ctrl.Result{}, err
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

	// Bug 350 reactivate-resync stall: refresh the TARGET's own
	// Spec.Flags from the uncached APIReader view before building the
	// DesiredResource. The reconcile-entry `r.Get` reads the c-r
	// informer cache, which trails the apiserver by hundreds of
	// milliseconds. After `linstor r activate <node>` clears INACTIVE,
	// a reconcile that fires on a stale cache still sees INACTIVE set
	// → applyInactive runs `drbdadm down` on the just-reactivated
	// replica. That down tears down a slot that has already SyncTarget-
	// ed back to UpToDate; the immediately-following `up` re-handshakes
	// and DRBD-9 records "missed end of resync", which — together with a
	// diskless TIE_BREAKER peer's `resync-susp(connection dependency)` —
	// wedges the replica Inconsistent forever (out-of-sync=0, never
	// finalizes). `listPeerResources` already reads every Resource for
	// the RD uncached AND includes the target itself, so we adopt the
	// authoritative flags from that same read with zero extra API
	// calls. Mirrors the peer-side APIReader fix the auto-primary
	// election relies on (Bug 80).
	adoptAuthoritativeFlags(res, peers)

	err = r.checkLayerStackPinned(ctx, rd)
	if errors.Is(err, errLayerStackNotPinned) {
		logger.Info("waiting for the controller to pin the layer stack", "rd", rd.Name)

		return ctrl.Result{RequeueAfter: layerStackPinRequeue}, nil
	}

	if err != nil {
		return ctrl.Result{}, err
	}

	// The DRBD-ID allocation gate only matters when the RD actually
	// uses DRBD. A LayerStack=["STORAGE"] (or ["LUKS","STORAGE"]) RD
	// renders no .res and the kernel never sees a node-id, so waiting
	// for the controller to stamp Status.DRBDNodeID/Port/Minor would
	// block apply forever — they never come.
	if rdNeedsDRBD(rd) {
		if waitResult, waitOK := r.prepareDRBDApply(ctx, res, rd, peers, logger); !waitOK {
			return waitResult, nil
		}
	}

	desired, err := r.buildDesiredFromCRD(ctx, res, rd, peers)
	if err != nil {
		return ctrl.Result{}, err
	}

	results, err := r.applyLocked(ctx, desired)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "satellite Apply")
	}

	retry, retryAsked := r.Config.Apply.TakeRetry(desired.GetName())

	refusePeersTooSmall(selfIn(res, peers), peers, results)

	anyFailed, waiting := r.recordPerResourceFailures(res, results, logger)
	r.recordDroppedOptions(res, results)

	outcome := applyOutcome{failed: anyFailed, waiting: waiting}
	outcome.stampFailed = r.stampPostApply(ctx, res, rd, results, outcome, logger)

	return r.nextPass(res, results, outcome, retry, retryAsked, logger), nil
}

// nextPass is the result of an apply pass: a retry of what failed, or,
// for a converged replica, nothing but the retry work it put off asked
// for (retryAsked, after retry).
//
// A stamp that failed is retried the way a failed apply is: no periodic
// pass comes back for a device path the CSI node is waiting on, or for
// the volume-numbers record a later delete needs.
//
// A converged replica waits for its next event, the way LINSTOR's
// satellite dispatches only resources with a pending change. The one
// exception is work this pass put off for a while, a dwell window or a
// throttle, that no event is bound to bring back.
func (r *ResourceReconciler) nextPass(
	res *blockstoriov1alpha1.Resource,
	results []*intent.ResourceApplyResult,
	outcome applyOutcome,
	retry time.Duration,
	retryAsked bool,
	logger logr.Logger,
) ctrl.Result {
	if requeue := r.retryAfterApply(res.Name, results, outcome.failed, outcome.waiting); requeue > 0 {
		return ctrl.Result{RequeueAfter: requeue}
	}

	if outcome.stampFailed {
		return ctrl.Result{RequeueAfter: applyFailureRequeue}
	}

	r.recordRecovery(res, r.retries.reset(res.Name), logger)

	if retryAsked {
		return ctrl.Result{RequeueAfter: retry}
	}

	return ctrl.Result{}
}

// retryAfterApply is how soon a pass whose apply did not go through
// runs again, and zero after one that did.
//
// Apply chain surfaces per-resource errors via results (e.g. drbdadm
// adjust failing on a stale .res rendered before the peer's Status
// caught up). Returning nil here would let c-r stop until an external
// event drove another reconcile; RequeueAfter ensures the next attempt
// sees the freshly-committed peer state. A failure that persists, a
// DRBD port another resource holds say, is retried with back-off rather
// than every applyFailureRequeue, and one whose only failures are
// metadata refusals no sooner than refusedApplyRequeue (failureRequeue).
//
// An apply held until the controller stamps the replica is a wait like
// the allocation waits ahead of it: retried on the short timer and on
// the update the stamp makes, and kept out of the back-off and of the
// failures a recovery reports. A grow the apply left unfinished is
// retried on the same short timer, failed or not, so the resize waiting
// on it is not held back by the back-off.
func (r *ResourceReconciler) retryAfterApply(
	name string, results []*intent.ResourceApplyResult, anyFailed, waiting bool,
) time.Duration {
	grow := growPending(results)

	switch {
	case waiting, grow:
		return applyFailureRequeue
	case anyFailed:
		return max(r.retries.next(name), failureRequeue(results))
	default:
		return 0
	}
}

// applyLocked runs one apply pass under the resource's ApplyLocks
// entry, so the startup backfill does not probe the lower disks while
// this pass may be creating their metadata.
func (r *ResourceReconciler) applyLocked(ctx context.Context, desired *intent.DesiredResource) ([]*intent.ResourceApplyResult, error) {
	unlock := r.Config.ApplyLocks.Lock(desired.GetName())
	defer unlock()

	return r.Config.Apply.Apply(ctx, []*intent.DesiredResource{desired}) //nolint:wrapcheck // the caller wraps it
}

// prepareDRBDApply runs the DRBD-only pre-apply sequence: stale
// kernel slot prune (Bug 342 v3) and the controller-allocation wait
// gate. Returns (result, false) when the caller should bail out and
// let the requeue handle it; (zero, true) when apply can proceed.
// Pulled out of runApply so it stays under the gocognit budget.
//
// The peer re-incarnation race the old UID-mismatch eviction path
// guarded against is now covered by the controller-side node-id
// occupancy invariant (collectTakenNodeIDs unions every sibling's
// observed Status.Connections[].PeerDRBDNodeID, so a lingering
// kernel slot keeps its node-id reserved until the kernel-confirmed
// forget drops it) PLUS the kernel-observed reclaim (orphan sweeper
// + tearDownRemovedPeers del-peer/forget-peer on the .res diff). A
// re-incarnated peer therefore gets the NEXT free node-id and never
// collides with the predecessor's surviving slot.
func (r *ResourceReconciler) prepareDRBDApply(
	ctx context.Context,
	res *blockstoriov1alpha1.Resource,
	rd *blockstoriov1alpha1.ResourceDefinition,
	peers []blockstoriov1alpha1.Resource,
	logger logr.Logger,
) (ctrl.Result, bool) {
	// Bug 342 v3: prune zombie kernel slots BEFORE the allocation
	// gate. The gate short-circuits the entire Apply chain when
	// ANY peer's Status.DRBDNodeID is nil, which is exactly the
	// state a Phase-3 relocate lands in — and the kernel-side slot
	// from the previous incarnation (Connecting / StandAlone with
	// no peer-device) would survive the entire wait window
	// otherwise. Kernel-state cleanup doesn't need peer DRBDNodeID
	// allocation, only the current expected peer-name set.
	// Non-fatal: a Pass-1 del-peer failure logs and falls through
	// to the gate so the next reconcile retries; the safety net
	// is best-effort by design.
	pruneErr := r.Config.Apply.PruneStaleKernelSlots(
		ctx,
		rd.Name,
		peerExpectationFor(res, peers),
		volNumsOf(rd),
		nil, // devices unknown here: Pass 1 still forgets, Pass 3 does not
	)
	if pruneErr != nil {
		logger.Error(pruneErr, "PruneStaleKernelSlots failed; continuing to allocation gate")
	}

	waitResult, ok := r.waitForControllerAllocation(ctx, res, peers, logger)
	if !ok {
		return waitResult, false
	}

	return r.holdForAdoptedPeers(ctx, res, rd, peers, logger)
}

// holdForAdoptedPeers keeps the first render of an adopted resource
// back while its kernel slot is connected to a peer whose Resource has
// not been observed. That render's `drbdadm adjust` would delete the
// connection, so a peer applied a moment after this one (a bulk
// adoption delivers the Resources of a definition in name order) would
// be disconnected and resynced. The hold is bounded: once it runs out
// the render goes ahead without the peer and a Warning Event says so,
// rather than leaving the replica unapplied for good.
func (r *ResourceReconciler) holdForAdoptedPeers(
	ctx context.Context,
	res *blockstoriov1alpha1.Resource,
	rd *blockstoriov1alpha1.ResourceDefinition,
	peers []blockstoriov1alpha1.Resource,
	logger logr.Logger,
) (ctrl.Result, bool) {
	hold := r.Config.Apply.AdoptedPeersWithoutResource(ctx, rd.Name, expectedPeerNamesFor(res, peers), time.Now())
	if len(hold.Missing) == 0 {
		return ctrl.Result{}, true
	}

	if hold.HoldsRender() {
		logger.Info("holding the first render of an adopted resource until its kernel peers have Resources",
			"missingPeers", hold.Missing, "since", hold.Since)

		return ctrl.Result{RequeueAfter: peerAllocationRequeue}, false
	}

	logger.Info("adopted kernel peers still have no Resource; rendering without them",
		"missingPeers", hold.Missing, "since", hold.Since)

	if r.Config.Recorder != nil {
		r.Config.Recorder.Eventf(res, nil, corev1.EventTypeWarning, "AdoptedPeerWithoutResource", "Apply",
			"kernel peers %v have no Resource since %s; rendering without them disconnects them",
			hold.Missing, hold.Since.Format(time.RFC3339))
	}

	return ctrl.Result{}, true
}

// recordPerResourceFailures logs each per-resource Apply failure and
// returns whether any of the results came back not-Ok, and whether
// every one of those is only waiting for the controller to stamp the
// replica (WaitingForController). Extracted from runApply so
// the orchestration stays under the funlen budget.
//
// Each failure also gets a Warning Event on the Resource, under the
// reason of the refusal it carries when it carries one (applyRefusals:
// the LUKS layer's, or drbdmeta's over data on the lower disk): an Info
// line in this log is not where an operator looks for why a replica
// does not come up. A wait gets neither the Event nor the failure
// line: it is part of every fresh replica's bring-up, and a Warning
// for it reads as a fault that is not there. Only a wait that lasts
// past a minute is reported, once (trackControllerWait).
//
// A peer the apply reconnected after the kernel dropped it is reported
// here as well (recordReconnectedPeers), whether the apply went through
// or not.
func (r *ResourceReconciler) recordPerResourceFailures(
	res *blockstoriov1alpha1.Resource, results []*intent.ResourceApplyResult, logger logr.Logger,
) (bool, bool) {
	var anyFailed, anyFault bool

	r.recordReconnectedPeers(res, results)

	for _, result := range results {
		if result.GetOk() {
			continue
		}

		anyFailed = true

		if result.GetWaitingForController() {
			logger.Info("waiting for the controller to stamp the replica", "name", result.GetName(),
				"message", result.GetMessage())

			continue
		}

		anyFault = true

		logger.Info("Apply per-resource failure", "name", result.GetName(), "message", result.GetMessage())

		if r.Config.Recorder == nil {
			continue
		}

		reason, note := applyFailureEvent(result.GetMessage())
		r.Config.Recorder.Eventf(res, nil, corev1.EventTypeWarning, reason, "Apply", "%s", note)
	}

	waiting := anyFailed && !anyFault
	r.trackControllerWait(res, results, waiting)

	return anyFailed, waiting
}

// droppedOptionReason is the Event reason for an option the satellite left
// out of a replica's .res.
const droppedOptionReason = "DrbdOptionIgnored"

// recordDroppedOptions reports each option the satellite left out of the
// replica's .res as a Warning Event on the Resource. The option is one
// drbdadm would refuse, so leaving it out keeps the rest of the
// configuration applying, but the operator set it and it does not take
// effect; a satellite log line is not where they would look for why. The
// recorder folds a repeat of the same Event into its count.
func (r *ResourceReconciler) recordDroppedOptions(res *blockstoriov1alpha1.Resource, results []*intent.ResourceApplyResult) {
	if r.Config.Recorder == nil {
		return
	}

	for _, result := range results {
		for _, why := range result.DroppedOptions {
			r.Config.Recorder.Eventf(res, nil, corev1.EventTypeWarning, droppedOptionReason, "Apply",
				"option left out of the .res, drbdadm would refuse it: %s", why)
		}
	}
}

// recordApplyFailure keeps Status.ApplyFailure in step with the pass
// that just ran: the error and the volume sizes it was applying when it
// failed, nothing once one succeeds. A resize waiting for this replica
// reads it, so a pass that keeps failing ends the wait with its error
// rather than leaving the caller to time out. Only a change is written,
// so a steady replica costs no extra API call.
func (r *ResourceReconciler) recordApplyFailure(
	ctx context.Context, res *blockstoriov1alpha1.Resource,
	rd *blockstoriov1alpha1.ResourceDefinition, results []*intent.ResourceApplyResult,
) error {
	want := applyFailureOf(rd, results)
	if equality.Semantic.DeepEqual(want, res.Status.ApplyFailure) {
		return nil
	}

	base := res.DeepCopy()
	res.Status.ApplyFailure = want

	err := r.Status().Patch(ctx, res, client.MergeFrom(base))
	if err != nil {
		return errors.Wrap(err, "patch Status.ApplyFailure")
	}

	return nil
}

// applyFailureOf is the Status.ApplyFailure a pass leaves: nil when it
// succeeded, its error and the sizes it was applying otherwise. The
// pass applied the definition's sizes (buildVolumes).
func applyFailureOf(rd *blockstoriov1alpha1.ResourceDefinition, results []*intent.ResourceApplyResult) *blockstoriov1alpha1.ResourceApplyFailure {
	for _, result := range results {
		if result.GetOk() {
			continue
		}

		out := &blockstoriov1alpha1.ResourceApplyFailure{Message: result.GetMessage()}
		for _, vd := range rd.Spec.VolumeDefinitions {
			out.Volumes = append(out.Volumes, blockstoriov1alpha1.ResourceApplyFailureVolume{
				VolumeNumber: vd.VolumeNumber,
				SizeKib:      vd.SizeKib,
			})
		}

		return out
	}

	return nil
}

// growPending reports a pass that left a grow short of its device. The
// device grows when a peer's resize does, which wakes nothing here, so
// the pass is repeated to report the size once it shows.
func growPending(results []*intent.ResourceApplyResult) bool {
	for _, result := range results {
		if result.GetGrowPending() {
			return true
		}
	}

	return false
}

// deviceHoldsDataReason is the Event reason for a LUKS format refused over
// a device that carries a signature.
const deviceHoldsDataReason = "LUKSDeviceHoldsData"

// applyRefusal is a refusal of the apply an operator must act on, from the
// LUKS layer or from drbdmeta, and the Warning Event reason it is reported
// under.
type applyRefusal struct {
	err          error
	reason, what string
}

// createMDRefusedReason is the Event reason for metadata drbdmeta refused
// to write over the data on a lower disk.
const createMDRefusedReason = "CreateMdRefused"

// uncleanMetadataRefusedReason is the Event reason for metadata under an
// unapplied activity log that the satellite would neither keep nor
// re-create: no GI is recorded for the volume, and its superblock does
// not carry this replica's node id.
const uncleanMetadataRefusedReason = "UncleanMetadataRefused"

// applyRefusals lists every refusal reported as a Warning Event.
func applyRefusals() []applyRefusal {
	return append(append(luksRefusals(), metadataRefusals()...), applyRefusal{
		errPeerDiskTooSmall, peerDiskTooSmallReason,
		"the kernel refuses the connection to peers with a smaller disk",
	})
}

// metadataRefusals are the refusals to write DRBD metadata. Each one is
// answered the same way on every attempt until an operator acts on the
// lower disk.
func metadataRefusals() []applyRefusal {
	return []applyRefusal{
		{
			drbd.ErrCreateMDRefused, createMDRefusedReason,
			"drbdmeta refused to create metadata over the data on the lower disk",
		},
		{
			drbd.ErrUncleanMetadataUnrecorded, uncleanMetadataRefusedReason,
			"the volume's metadata was left with an unapplied activity log, no GI is recorded for it " +
				"and its superblock does not carry this replica's node id",
		},
	}
}

// refusedApplyRequeue is the retry interval of an apply whose only failures
// are metadata refusals. drbdmeta refuses the same lower disk the same way
// every time, and so does the satellite a volume it has no record of, so
// the 5 second retry of applyFailureRequeue only loops: it repeats the
// command, the log line and the Event, and holds the satellite's work
// queue, for nothing that can change by itself. A change to the Resource
// or its definition still reconciles at once.
const refusedApplyRequeue = 5 * time.Minute

// failureRequeue picks the retry interval of an apply that failed.
func failureRequeue(results []*intent.ResourceApplyResult) time.Duration {
	for _, result := range results {
		if !result.GetOk() && !isMetadataRefusal(result.GetMessage()) {
			return applyFailureRequeue
		}
	}

	return refusedApplyRequeue
}

// isMetadataRefusal reports whether an apply message carries one of the
// metadataRefusals.
func isMetadataRefusal(message string) bool {
	for _, refusal := range metadataRefusals() {
		if strings.Contains(message, refusal.err.Error()) {
			return true
		}
	}

	return false
}

// luksRefusals lists the refusals the LUKS layer reports through the apply
// message. Each leaves the volume down, and an Info line in this log is
// not where an operator looks for why.
func luksRefusals() []applyRefusal {
	return []applyRefusal{
		{luks.ErrDeviceHoldsData, deviceHoldsDataReason, "the LUKS layer refused to format a device that already holds data"},
		{luks.ErrLUKSKeyMissing, "LUKSKeyMissing", "the device has a LUKS header but the replica carries no password for it"},
		{luks.ErrLUKSKeyMismatch, "LUKSKeyMismatch", "the replica's password does not open the device's LUKS header"},
		{luks.ErrMasterKeyMismatch, "MasterKeyMismatch", "the replica's password is wrapped under another master key"},
		{luks.ErrMasterKeyLocked, "MasterKeyLocked", "the master key is locked"},
	}
}

// applyFailedReason is the Event reason for a failed apply that is not
// one of the applyRefusals.
const applyFailedReason = "ApplyFailed"

// applySucceededReason is the Event reason for an apply that went
// through after failures.
const applySucceededReason = "ApplySucceeded"

// recordRecovery reports an apply that went through after `failures`
// failed applies: an Info line and a Normal Event on the Resource. The
// Warning Events of the failures stay on the Resource until they
// expire and would otherwise read as a replica still down; LINSTOR
// likewise logs a failed resource leaving its retry task and clears the
// volume's error reports once the satellite reports it applied.
func (r *ResourceReconciler) recordRecovery(res *blockstoriov1alpha1.Resource, failures int, logger logr.Logger) {
	if failures == 0 {
		return
	}

	logger.Info("Apply succeeded after failures", "failedApplies", failures)

	if r.Config.Recorder == nil {
		return
	}

	r.Config.Recorder.Eventf(res, nil, corev1.EventTypeNormal, applySucceededReason, "Apply",
		"apply succeeded after %d failed attempts", failures)
}

// staleConnectionReason is the Event reason for a StandAlone connection
// the apply set up again because the kernel held it with a peer
// configuration the resource no longer has.
const staleConnectionReason = "StaleConnectionReconnected"

// recordReconnectedPeers reports each peer an apply reconnected after
// the kernel had dropped the connection to it over a stale peer
// configuration: a Warning Event on the Resource. Until that apply the
// replica was StandAlone towards the peer while its own disk stayed
// UpToDate, and nothing but this Event says why it was.
func (r *ResourceReconciler) recordReconnectedPeers(res *blockstoriov1alpha1.Resource, results []*intent.ResourceApplyResult) {
	if r.Config.Recorder == nil {
		return
	}

	for _, result := range results {
		for _, peer := range result.GetReconnectedPeers() {
			r.Config.Recorder.Eventf(res, nil, corev1.EventTypeWarning, staleConnectionReason, "Apply",
				"%s", eventNote("the connection to "+peer+" was StandAlone with a peer configuration "+
					"the resource no longer has; a full adjust set it up again"))
		}
	}
}

// applyFailureEvent is the Warning Event reason and note for a failed
// apply with message: those of the refusal it carries (applyRefusals),
// or ApplyFailed. Every failure gets one: a replica left StandAlone
// because its adjust failed looks healthy from the definition's other
// replicas, and LINSTOR reports such a failure to the client rather
// than only to its satellite log.
func applyFailureEvent(message string) (string, string) {
	for _, refusal := range applyRefusals() {
		if strings.Contains(message, refusal.err.Error()) {
			return refusal.reason, eventNote(refusal.what + ": " + message)
		}
	}

	return applyFailedReason, eventNote("apply failed, retrying with back-off: " + message)
}

// eventNoteLimit is the longest note the apiserver accepts on an
// events.k8s.io/v1 Event.
const eventNoteLimit = 1024

// eventNoteEllipsis marks a note cut to eventNoteLimit.
const eventNoteEllipsis = "..."

// eventNote cuts note to eventNoteLimit bytes, on a rune boundary. The
// apiserver rejects a longer one and the recorder drops the Event
// without a word, and a drbdadm failure carries the whole command line
// and stderr, past the limit with a few peers. The head names the
// failing step; the satellite log keeps the rest.
func eventNote(note string) string {
	if len(note) <= eventNoteLimit {
		return note
	}

	cut := eventNoteLimit - len(eventNoteEllipsis)
	for cut > 0 && !utf8.RuneStart(note[cut]) {
		cut--
	}

	return note[:cut] + eventNoteEllipsis
}

// stampPostApply runs the four best-effort post-apply stamps:
// Status.Volumes (per-volume DevicePath), the stale-Status.Volumes
// purge (Bug 355 followup), the Bug-107 volume-numbers annotation,
// and the Bug-39 toggle-disk retry counter. Each helper is
// independently fallible — we log and move on so a transient
// apiserver hiccup on one stamp doesn't suppress the others. It
// reports whether any of them failed, so the pass asks for another.
func (r *ResourceReconciler) stampPostApply(ctx context.Context, res *blockstoriov1alpha1.Resource, rd *blockstoriov1alpha1.ResourceDefinition, results []*intent.ResourceApplyResult, outcome applyOutcome, logger logr.Logger) bool {
	// Stamp per-volume DevicePath into Status.Volumes so
	// linstor-csi / any consumer that reads the CRD sees the
	// /dev path the satellite materialised. Done even when one
	// volume's apply failed: a partial success is still useful
	// to surface (volumes that did apply have a real device).
	failed := false

	err := r.stampVolumeStatus(ctx, res, results)
	if err != nil {
		failed = true

		logger.Error(err, "stamp Status.Volumes")
	}

	// Bug 355 followup: surgically drop `Status.Volumes[]` entries
	// whose VolumeNumber is no longer present in the parent RD's
	// VolumeDefinitions. The apiserver-side `vd d` cascade already
	// removed the entry from `Spec.Volumes` + the `volume-numbers`
	// annotation, and `stampVolumeStatus` above only re-stamps the
	// desired volumes under its own field-owner — but the observer's
	// `SatelliteFieldOwner` claims on `diskState`/`currentGi` for the
	// removed entry keep the listMap entry alive indefinitely (SSA
	// only collapses a listMap entry when NO field-owner claims any
	// subfield on it; observer's volCache never drops volume entries
	// so the claim is permanent). We use JSON Patch `remove` with a
	// `test` guard on the matching index so the removal is atomic
	// against concurrent observer writes AND field-surgical: no other
	// entry's subfields are touched. Wholesale array replacement
	// (the original v1 attempt) races observer writes on surviving
	// volumes and was reverted.
	err = r.purgeStaleVolumeStatus(ctx, res, rd)
	if err != nil {
		failed = true

		logger.Error(err, "purge stale Status.Volumes")
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
		failed = true

		logger.Error(err, "stamp volume-numbers annotation")
	}

	// Bug 39: bump or clear Status.ToggleDiskRetries based on the
	// per-resource result. The helper is a no-op when the Resource
	// isn't mid diskless→diskful conversion, so it's safe to call
	// unconditionally on every apply pass. A wait for the controller's
	// stamp is neither: a witness promoted before the controller
	// stamped it meets that wait, and counting it as a failed attempt
	// would spend the conversion's retries on a replica with no fault.
	if outcome.waiting {
		return failed
	}

	err = r.recordToggleDiskOutcome(ctx, res, !outcome.failed)
	if err != nil {
		failed = true

		logger.Error(err, "record toggle-disk outcome")
	}

	err = r.recordApplyFailure(ctx, res, rd, results)
	if err != nil {
		failed = true

		logger.Error(err, "record apply failure")
	}

	err = r.recordMetadataPeerSlots(ctx, res, results)
	if err != nil {
		failed = true

		logger.Error(err, "record the superblock's peer slots")
	}

	if !outcome.failed {
		err = r.stampLiveMigrateApplied(ctx, res, rd)
		if err != nil {
			failed = true

			logger.Error(err, "stamp live-migrate-applied annotation")
		}
	}

	return failed
}

// applyOutcome is how an apply pass ended: failed when a result came
// back not Ok, waiting when every such result only waits for the
// controller to stamp the replica, and stampFailed when a post-apply
// stamp did not land.
type applyOutcome struct {
	failed, waiting, stampFailed bool
}

// stampLiveMigrateApplied copies the definition's live migration token
// onto the Resource after an apply pass that succeeded: the .res this pass
// rendered and adjusted came from that definition, so the dual-Primary
// props written with the token are in effect on this node. make-available
// waits for this before it answers (see
// ResourceDefinitionAnnotationLiveMigrateToken).
func (r *ResourceReconciler) stampLiveMigrateApplied(ctx context.Context, res *blockstoriov1alpha1.Resource, rd *blockstoriov1alpha1.ResourceDefinition) error {
	token := rd.Annotations[blockstoriov1alpha1.ResourceDefinitionAnnotationLiveMigrateToken]
	if token == "" || res.Annotations[blockstoriov1alpha1.ResourceAnnotationLiveMigrateApplied] == token {
		return nil
	}

	body, err := json.Marshal(map[string]any{
		"metadata": map[string]any{
			"annotations": map[string]string{blockstoriov1alpha1.ResourceAnnotationLiveMigrateApplied: token},
		},
	})
	if err != nil {
		return errors.Wrap(err, "encode live-migrate-applied patch")
	}

	err = r.Patch(ctx, res, client.RawPatch(types.MergePatchType, body))
	if err != nil {
		return errors.Wrap(err, "merge-patch annotation "+blockstoriov1alpha1.ResourceAnnotationLiveMigrateApplied)
	}

	return nil
}

// recordMetadataPeerSlots publishes the peer-slot count the pass read
// from the replica's superblock (Status.MetadataPeerSlots), for the
// controller to record as the replica's count. Written once: a value
// already there is the one the superblock holds.
func (r *ResourceReconciler) recordMetadataPeerSlots(
	ctx context.Context, res *blockstoriov1alpha1.Resource, results []*intent.ResourceApplyResult,
) error {
	if len(results) == 0 || res.Status.MetadataPeerSlots != nil {
		return nil
	}

	slots := results[0].GetMetadataPeerSlots()
	if slots == 0 {
		return nil
	}

	base := res.DeepCopy()
	res.Status.MetadataPeerSlots = &slots

	err := r.Status().Patch(ctx, res, client.MergeFrom(base))
	if err != nil {
		return errors.Wrap(err, "patch Status.MetadataPeerSlots")
	}

	return nil
}

// stampVolumeStatus SSA-patches Resource.Status.Volumes with the
// per-volume DevicePath the apply chain materialised. Uses a
// dedicated FieldOwner so the apiserver merges cleanly against the
// observer's DiskState / CurrentGI writes on the same Volume[i]
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
			VolumeNumber:   v.GetVolumeNumber(),
			StoragePool:    pool,
			DevicePath:     v.GetDevicePath(),
			AppliedSizeKib: v.GetAppliedSizeKib(),
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
// observer's owner (which writes DiskState / CurrentGI) so the
// apiserver merges the two slices cleanly under
// `listMapKey=volumeNumber`.
const volumeStatusFieldOwner = "blockstor-satellite-volume-status"

// purgeStaleVolumeStatus drops Status.Volumes entries whose
// VolumeNumber is no longer present in the parent RD's
// VolumeDefinitions. Bug 355 followup.
//
// Background: the apiserver-side `vd d` cascade removes the
// VolumeDefinition (and the matching Spec.Volumes entry / Bug-107
// volume-numbers annotation), but Status.Volumes[*] entries authored
// by the observer's listMap field-owner persist. SSA only collapses
// a listMap entry when NO field-owner claims any subfield on it. The
// satellite-stamp owner (`volumeStatusFieldOwner`) already releases
// its DevicePath claim on the missing entry by NOT including it in
// the next Apply payload — but the observer's `DiskState` /
// `CurrentGI` claims on the same `volumeNumber=N` key keep the entry
// alive, and the observer's `volCache` never removes volume entries
// (events2 frames only ever ADD entries — there is no `destroy
// volume` frame on the satellite). The claim is therefore permanent
// without an explicit purge.
//
// Approach: for each Status.Volumes entry whose VolumeNumber is not
// in the desired set, emit a JSON Patch with a `test` op on
// `/status/volumes/<idx>/volumeNumber` (atomic check that the index
// still points at the same VolumeNumber) followed by a `remove` op
// on `/status/volumes/<idx>` (surgically removes the single listMap
// entry). The `test` op makes the patch atomic against concurrent
// observer writes that might reorder the slice: if the index moved
// the apiserver returns 409/422 and we drop the error — the next
// reconcile retries.
//
// Why JSON Patch (not JSON merge-patch / SSA / strategic-merge
// $patch:delete):
//   - JSON merge-patch on the array path REPLACES the array wholesale
//     and snapshot-stomps observer field-claims on the survivors —
//     this was the v1 attempt (3471e4a84) which got reverted
//     (1ec5d1499) because it raced observer writes and wiped fresh
//     diskState/currentGi.
//   - SSA Apply has no `delete` verb — releasing this owner's claims
//     is not enough because observer still claims subfields.
//   - Strategic merge `$patch:delete` requires server-side strategic
//     merge support for the CRD which our codegen does not configure.
//   - JSON Patch with `test`+`remove` on a numeric index is the
//     narrowest field-surgical mechanism: only the targeted entry
//     is touched; every other entry's subfields (and every other
//     Status field) is untouched.
//
// Why this does NOT race `r deactivate`: `r deactivate` toggles
// `Spec.Flags` (INACTIVE) — it does not touch Status.Volumes.
//
// No-op when:
//   - Status.Volumes is empty (nothing to purge);
//   - rd carries zero VolumeDefinitions (defensive — cascade in
//     flight or freshly created RD; don't pre-blank Status, the
//     per-Resource delete path owns that teardown);
//   - every Status.Volumes entry's VolumeNumber is in the desired set
//     (steady state).
//
// The `test`-op 409/422 path is logged at V(2) (debug) and swallowed
// — the next reconcile will retry against the fresh slice ordering.
// We never want a stale-prune retry to spin the reconciler.
func (r *ResourceReconciler) purgeStaleVolumeStatus(ctx context.Context, res *blockstoriov1alpha1.Resource, rd *blockstoriov1alpha1.ResourceDefinition) error {
	if len(res.Status.Volumes) == 0 {
		return nil
	}

	if len(rd.Spec.VolumeDefinitions) == 0 {
		// Defensive: an RD with zero VolumeDefinitions is either
		// freshly created (volumes not yet authored) or mid-cascade
		// to deletion (the parent RD finalizer will tear down the
		// Resource). Don't nuke Status.Volumes here — the per-Resource
		// delete path owns that teardown.
		return nil
	}

	desired := desiredVolumeNumbers(rd)
	stale, survivors := partitionStaleVolumeStatus(res.Status.Volumes, desired)

	if len(stale) == 0 {
		return nil
	}

	body, err := buildVolumeStatusPurgePatch(stale)
	if err != nil {
		return err
	}

	target := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: res.Name},
	}

	err = r.Status().Patch(ctx, target, client.RawPatch(types.JSONPatchType, body))
	if err != nil {
		// A 409/422 means the index→VolumeNumber mapping shifted
		// since we read `res` — observer (or another owner) authored
		// a concurrent write that reordered the slice. The next
		// reconcile pass picks up the fresh ordering and retries. We
		// deliberately don't wrap-and-return: a transient ordering
		// race must not poison the rest of the post-apply chain.
		if apierrors.IsConflict(err) || apierrors.IsInvalid(err) {
			return nil
		}

		return errors.Wrap(err, "json-patch Status.Volumes purge")
	}

	// Reflect the write back onto the caller's copy so subsequent
	// reads inside the same reconcile see the post-purge slice.
	res.Status.Volumes = survivors

	return nil
}

// jsonPatchOp keys used in every (test, remove) op pair the purge
// emits. Extracted as constants so the goconst linter is satisfied
// and so a future RFC 6902 helper rename happens in one place.
const (
	jsonPatchOpKey    = "op"
	jsonPatchPathKey  = "path"
	jsonPatchValueKey = "value"

	jsonPatchOpTest   = "test"
	jsonPatchOpRemove = "remove"
)

// staleVolumeStatus pairs a stale Status.Volumes index with the
// VolumeNumber the JSON-Patch `test` op pins on it.
type staleVolumeStatus struct {
	index  int
	volNum int32
}

// desiredVolumeNumbers returns the set of VolumeNumbers the parent
// RD's VolumeDefinitions currently declare — i.e. every volume that
// MUST survive in `Status.Volumes`.
func desiredVolumeNumbers(rd *blockstoriov1alpha1.ResourceDefinition) map[int32]struct{} {
	out := make(map[int32]struct{}, len(rd.Spec.VolumeDefinitions))
	for i := range rd.Spec.VolumeDefinitions {
		out[rd.Spec.VolumeDefinitions[i].VolumeNumber] = struct{}{}
	}

	return out
}

// partitionStaleVolumeStatus splits `status` into the stale entries
// (VolumeNumber not in `desired` — JSON-Patch remove targets) and the
// survivors (in original index order, suitable for caller-copy
// reflection). The stale slice carries the ORIGINAL index of each
// entry so the JSON-Patch document can target it by position.
func partitionStaleVolumeStatus(status []blockstoriov1alpha1.ResourceVolumeStatus, desired map[int32]struct{}) ([]staleVolumeStatus, []blockstoriov1alpha1.ResourceVolumeStatus) {
	var stale []staleVolumeStatus

	survivors := make([]blockstoriov1alpha1.ResourceVolumeStatus, 0, len(status))

	for i := range status {
		if _, ok := desired[status[i].VolumeNumber]; !ok {
			stale = append(stale, staleVolumeStatus{index: i, volNum: status[i].VolumeNumber})

			continue
		}

		survivors = append(survivors, status[i])
	}

	return stale, survivors
}

// buildVolumeStatusPurgePatch renders the JSON-Patch document that
// removes each stale `Status.Volumes` entry. Each entry gets a
// (test, remove) pair so a concurrent reorder (observer writes) is
// detected as a 409/422 rather than removing the wrong entry.
//
// Ops are emitted high-to-low: each `remove` shifts indices below
// it down by one, so processing the highest index first keeps every
// remaining index stable. Single-document atomicity at the apiserver
// — all ops apply, or none.
func buildVolumeStatusPurgePatch(stale []staleVolumeStatus) ([]byte, error) {
	ops := make([]map[string]any, 0, len(stale)*2)

	for i := range slices.Backward(stale) {
		path := "/status/volumes/" + strconv.Itoa(stale[i].index)

		ops = append(ops,
			map[string]any{
				jsonPatchOpKey:    jsonPatchOpTest,
				jsonPatchPathKey:  path + "/volumeNumber",
				jsonPatchValueKey: stale[i].volNum,
			},
			map[string]any{
				jsonPatchOpKey:   jsonPatchOpRemove,
				jsonPatchPathKey: path,
			},
		)
	}

	body, err := json.Marshal(ops)
	if err != nil {
		return nil, errors.Wrap(err, "marshal Status.Volumes purge patch")
	}

	return body, nil
}

// stampVolumeNumbersAnnotation writes the parent RD's volume-number
// set onto the Resource's metadata.annotations under
// `blockstor.org/volume-numbers`. The value is a comma-separated list
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
		return errors.Wrap(err, "merge-patch annotation blockstor.org/volume-numbers")
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
	if naming.Same(res.Spec.NodeName, r.Config.NodeName) {
		return nil
	}

	var localList blockstoriov1alpha1.ResourceList

	err := r.List(ctx, &localList)
	if err != nil {
		return nil
	}

	for i := range localList.Items {
		local := &localList.Items[i]
		if naming.Same(local.Spec.ResourceDefinitionName, res.Spec.ResourceDefinitionName) &&
			naming.Same(local.Spec.NodeName, r.Config.NodeName) {
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
	// Checked again here, not only in runApply: no intent is ever built
	// from a stack the definition does not carry yet.
	err := r.checkLayerStackPinned(ctx, rd)
	if err != nil {
		return nil, err
	}

	var nodeList blockstoriov1alpha1.NodeList

	err = r.List(ctx, &nodeList)
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

	// An RD that pins its own passphrase Secret gets a Condition
	// saying the key is ignored: the field is declared but not
	// implemented, so the volume gets the cluster's per-replica key.
	// Advisory, never fatal: see noteIgnoredPerRDKey in
	// luks_passphrase.go for why this is not a refusal.
	r.noteIgnoredPerRDKey(ctx, target, rd)

	// A passphrase property never reaches the apply chain: each LUKS
	// volume's password is its own (luks_keys.go).
	dropPlaintextPassphraseProps(ctx, rd, effectiveProps)

	desired := dispatcher.BuildDesired(target, peers, nodeList.Items, poolList.Items, rd, effectiveProps)

	volumeDisk, err := effectiveprops.VolumeDiskOptions(ctx, r.Client, target, rd, effectiveProps)
	if err != nil {
		return nil, errors.Wrap(err, "resolve volume disk options")
	}

	for _, vol := range desired.Volumes {
		vol.DiskOptions = volumeDisk[vol.VolumeNumber]
	}

	// Phase 11.3 Stage 1: carry the `MetadataCreated=True` Status
	// Condition from the Resource CRD into the apply chain so the
	// satellite reconciler's `firstActivation` predicate (and the
	// FSM shadow's Observation.MetadataExists) can derive from the
	// apiserver view without round-tripping through the kernel
	// dump-md probe on every reconcile. The on-disk `.md-created`
	// marker remains as a migration-window fallback.
	desired.MetadataCreated = meta.IsStatusConditionTrue(target.Status.Conditions, blockstoriov1alpha1.ConditionMetadataCreated)

	// Phase 11.3 Stage 2: same pattern for FilesystemFormatted —
	// the auto-mkfs fast-path and Bug-311 retry predicate read this
	// to short-circuit per-volume blkid probes from the apiserver
	// view. The on-disk `.mkfs.done` marker remains as a
	// migration-window fallback; the per-volume blkid probe inside
	// runAutoMkfs is the double-mkfs safety net.
	desired.FilesystemFormatted = meta.IsStatusConditionTrue(target.Status.Conditions, blockstoriov1alpha1.ConditionFilesystemFormatted)

	// Phase 11.3 Stage 3: KernelLoaded is stamped by the observer
	// off the events2 stream (`exists resource` → True, `destroy
	// resource` → False). The satellite reconciler's observeForFsm
	// reads it to skip the `drbdsetup status` probe on the hot
	// path; the kernel-direct probe stays as fallback when the
	// Condition is absent (cluster just upgraded, observer
	// restarting).
	desired.KernelLoaded = meta.IsStatusConditionTrue(target.Status.Conditions, blockstoriov1alpha1.ConditionKernelLoaded)

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
	var resList blockstoriov1alpha1.ResourceList

	// Only the definition's replicas: every pass of every replica on the
	// node runs this, and reading the whole cluster here took the
	// satellite's CPU (storek8s.ReplicasOf).
	err := storek8s.ReplicasOf(ctx, r.peerReader(), r.Client, target.Spec.ResourceDefinitionName, &resList)
	if err != nil {
		return nil, errors.Wrap(err, "list Resources for peer set")
	}

	return resList.Items, nil
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

// adoptAuthoritativeFlags overwrites target.Spec.Flags with the copy
// read uncached from the apiserver (the target's own entry in the
// APIReader-backed peer list), curing the c-r-cache trail on the
// INACTIVE flag that otherwise lets a post-`r activate` reconcile run
// applyInactive's `drbdadm down` on a just-reactivated replica (see
// the call site in runApply for the full wedge chain). No-op when the
// target isn't found in the list (peer list built from a different
// reader in a unit test, or the target's CRD was just deleted) — the
// cached flags then stand, which is the pre-fix behaviour.
//
// The deletion timestamp is adopted the same way. A `rd d` stamps every
// replica of the definition at once, so the uncached read that already
// misses the peers torn down first also shows this replica's own
// deletion; the cached copy can still lack it, and the desired state
// built from it would call a replica in the middle of its delete a lone
// survivor (dispatcher Deleting).
func adoptAuthoritativeFlags(target *blockstoriov1alpha1.Resource, peers []blockstoriov1alpha1.Resource) {
	for i := range peers {
		if naming.Same(peers[i].Spec.NodeName, target.Spec.NodeName) &&
			naming.Same(peers[i].Spec.ResourceDefinitionName, target.Spec.ResourceDefinitionName) {
			target.Spec.Flags = peers[i].Spec.Flags

			if target.DeletionTimestamp == nil && peers[i].DeletionTimestamp != nil {
				target.DeletionTimestamp = peers[i].DeletionTimestamp.DeepCopy()
			}

			return
		}
	}
}

// rdNeedsDRBD mirrors pkg/satellite/reconciler.go's needsDRBD but
// over the RD CRD: empty LayerStack defaults to ["DRBD","STORAGE"]
// (legacy semantics), any explicit stack must list "DRBD".
func rdNeedsDRBD(rd *blockstoriov1alpha1.ResourceDefinition) bool {
	stack := rd.Spec.LayerStack
	if len(stack) == 0 {
		return true
	}

	return apiv1.LayerInStack(stack, apiv1.LayerKindDRBD)
}

// expectedPeerNamesFor returns the peer-node-name set the Bug 342 v3
// kernel-slot prune compares against `drbdsetup show -j`. The local
// satellite's own node is NOT a peer from the kernel's view —
// drbdsetup enumerates remote peers only — so this filter drops
// `target.Spec.NodeName`. Returns nil-safe empty slice when peers
// is empty.
func expectedPeerNamesFor(target *blockstoriov1alpha1.Resource, peers []blockstoriov1alpha1.Resource) []string {
	out := make([]string, 0, len(peers))

	for i := range peers {
		name := peers[i].Spec.NodeName
		if name == "" || naming.Same(name, target.Spec.NodeName) {
			continue
		}

		out = append(out, name)
	}

	return out
}

// peerExpectationFor is what the prune may rely on about the RD's
// peers: the observed peer names, the node-ids the observed replicas
// hold, and whether they pass the same allocation gate the apply path
// waits on. peers comes from listPeerResources and includes the target.
func peerExpectationFor(target *blockstoriov1alpha1.Resource, peers []blockstoriov1alpha1.Resource) satellite.PeerExpectation {
	expect := satellite.PeerExpectation{
		PeerNames: expectedPeerNamesFor(target, peers),
		Allocated: allocationComplete(target, peers),
	}

	for i := range peers {
		if id, ok := dispatcher.AllocatedNodeID(&peers[i]); ok {
			expect.AllocatedNodeIDs = append(expect.AllocatedNodeIDs, id)
		}
	}

	return expect
}

// volNumsOf extracts the volume-number set from
// `rd.Spec.VolumeDefinitions`. The Bug 342 v3 Pass-3 stuck-slot
// probe needs the set to query the kernel's peer-device
// registration table (a connection slot with no peer-device for ANY
// volume is the zombie signature). Returns nil-safe empty slice
// when VolumeDefinitions is empty.
func volNumsOf(rd *blockstoriov1alpha1.ResourceDefinition) []int32 {
	out := make([]int32, 0, len(rd.Spec.VolumeDefinitions))

	for i := range rd.Spec.VolumeDefinitions {
		out = append(out, rd.Spec.VolumeDefinitions[i].VolumeNumber)
	}

	return out
}

// targetAllocated reports that the controller has stamped the
// target's own DRBD node-id, port and minor on Status, the half of
// the allocation gate that reads only the target.
func targetAllocated(res *blockstoriov1alpha1.Resource) bool {
	return res.Status.DRBDNodeID != nil && res.Status.DRBDPort != nil && res.Status.DRBDMinor != nil
}

// allocationComplete is the predicate waitForControllerAllocation
// waits on: the target's Status allocation and a node-id on every
// diskful replica. The gate itself also re-reads the target uncached
// before it gives up; callers of this predicate take the view they
// hold, which can only make it report false too early, never true.
func allocationComplete(target *blockstoriov1alpha1.Resource, peers []blockstoriov1alpha1.Resource) bool {
	return targetAllocated(target) && dispatcher.DiskfulPeersAllocated(target, peers)
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
	if !targetAllocated(res) {
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

// definitionForApply reads the replica's definition, past the cache
// while the replica has never been applied here (see
// refreshDefinitionForFirstApply). A definition that is gone is not
// found and no error: there is nothing to apply.
func (r *ResourceReconciler) definitionForApply(
	ctx context.Context, res *blockstoriov1alpha1.Resource, logger logr.Logger,
) (*blockstoriov1alpha1.ResourceDefinition, bool, error) {
	var rd blockstoriov1alpha1.ResourceDefinition

	err := r.Get(ctx, client.ObjectKey{Name: naming.Key(res.Spec.ResourceDefinitionName)}, &rd)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, false, nil
		}

		return nil, false, errors.Wrap(err, "get parent RD")
	}

	r.refreshDefinitionForFirstApply(ctx, res, &rd, logger)

	return &rd, true, nil
}

// refreshDefinitionForFirstApply replaces the cached definition with the
// API server's copy while the replica has never been applied here. The
// controller writes the volume block size onto the definition before it
// gives the replica its DRBD identity, but the two objects reach this
// satellite through separate watches, so the cached definition can still
// predate the size when the identity arrives. The first apply attaches
// the device, and drbdsetup never changes the block size of an attached
// device, so a size read late is a size lost until the next detach. A
// failed read keeps the cached copy.
func (r *ResourceReconciler) refreshDefinitionForFirstApply(
	ctx context.Context, res *blockstoriov1alpha1.Resource, rd *blockstoriov1alpha1.ResourceDefinition, logger logr.Logger,
) {
	if _, applied := res.Annotations[blockstoriov1alpha1.ResourceAnnotationVolumeNumbers]; applied {
		return
	}

	reader := r.peerReader()
	if reader == nil {
		return
	}

	var fresh blockstoriov1alpha1.ResourceDefinition

	err := reader.Get(ctx, client.ObjectKey{Name: rd.Name}, &fresh)
	if err != nil {
		logger.V(1).Info("definition refresh before the first apply failed; using the cached copy", "err", err.Error())

		return
	}

	*rd = fresh
}

// refreshTargetFromAPIReader does an uncached Get on the target
// Resource and replaces `res` with that revision when the APIReader
// reports non-nil DRBD-IDs. Returns true iff every DRBD-ID is now populated,
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

	// The whole revision, not the three ids: the controller writes the
	// replica's peer slots and skip-initial-sync decision in the spec
	// write before it mirrors the ids, so ids taken alone would sit next
	// to a cached spec that has neither. The apply would then size the
	// lower disk for 15 slots and create 7-slot metadata on it a pass
	// later, leaving the device larger than the definition and larger
	// than every replica the controller stamped first.
	*res = fresh

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
		if naming.Same(peer.Spec.NodeName, target.Spec.NodeName) {
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
//     force-strips its own `blockstor.org/resource`
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
//     the storage tear-down safely. Nothing else deletes the
//     storage: the finalizer stays until DeleteResource reports
//     that every volume is gone.
//
// Phase 10.1. Once Phase 10.6 retires the gRPC contract,
// `internal/controller.ResourceReconciler.runDelete` and its
// finalizer (`blockstor.org/resource`) go away —
// satellite-side teardown becomes the only finalizer path.
func (r *ResourceReconciler) handleDelete(ctx context.Context, res *blockstoriov1alpha1.Resource) (ctrl.Result, error) {
	if !slices.Contains(res.Finalizers, SatelliteResourceFinalizer) {
		// Either we never stamped it (object created before this
		// satellite came up) or someone already stripped it.
		// Either way, nothing for us to do — let the apiserver
		// finalise.
		return ctrl.Result{}, nil
	}

	// A delete that failed is retried when its back-off runs out, not on
	// every event that reaches the replica meanwhile.
	if wait := r.deleteRetries.wait(res.Name); wait > 0 {
		return ctrl.Result{RequeueAfter: wait}, nil
	}

	volumeNumbers, err := r.lookupVolumeNumbers(ctx, res)
	if err != nil {
		return ctrl.Result{}, err
	}

	req := &intent.DeleteResourceRequest{
		Name:          naming.Key(res.Spec.ResourceDefinitionName),
		StoragePool:   resolveDeleteStoragePool(res),
		VolumeNumbers: volumeNumbers,
	}

	if res.Annotations[finalizers.StorageAbandoned] != "" {
		// `node lost` removed this replica because the node is gone;
		// LINSTOR leaves the storage of a lost node in place, and so
		// does a satellite that turns out to be running after all: it
		// tears down DRBD and keeps the volumes.
		req.StoragePool = ""
	} else {
		req.DeletedSuffix, err = r.recordRenameSuffix(ctx, res)
		if err != nil {
			return ctrl.Result{}, err
		}
	}

	resp, err := r.Config.Apply.DeleteResource(ctx, req)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "DeleteResource")
	}

	if !resp.GetOk() {
		// Body-level failure: keep the finalizer, so the Resource does
		// not vanish before its storage is gone, and retry after a
		// back-off.
		retry := r.deleteRetries.after(res.Name, deleteFailureRequeue, deleteFailureMaxRequeue)

		log.FromContext(ctx).Info("DeleteResource per-resource failure",
			"resource", res.Spec.ResourceDefinitionName, "message", resp.GetMessage(), "retryIn", retry)

		return ctrl.Result{RequeueAfter: retry}, nil
	}

	r.deleteRetries.reset(res.Name)

	return r.stripSatelliteFinalizer(ctx, res.Name)
}

// stripSatelliteFinalizer releases the Resource once its storage is gone.
func (r *ResourceReconciler) stripSatelliteFinalizer(ctx context.Context, name string) (ctrl.Result, error) {
	// Re-fetch via APIReader (uncached) so we strip ours off the
	// freshest finalizer list. Falls back to the cached client
	// when APIReader is nil (unit-test path).
	reader := r.deleteReader()

	var latest blockstoriov1alpha1.Resource

	err := reader.Get(ctx, client.ObjectKey{Name: name}, &latest)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// Someone else (force-strip from the controller, the
			// sweeper after a hung apply) finished the delete
			// already — the DeleteResource before this was the idempotent
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

// lookupVolumeNumbers resolves the per-RD volume-number set the
// satellite's DeleteResource walks to drop matching LVs / loopfiles.
// It joins three sources:
//
//  1. The parent RD's VolumeDefinitions (the source of truth while
//     the RD CRD still exists).
//  2. The `blockstor.org/volume-numbers` annotation that
//     `stampVolumeNumbersAnnotation` writes on every successful apply
//     (Bug 107: `linstor rd delete` cascade-deletes the parent RD via
//     owner refs, so by the time `handleDelete` runs the RD Get
//     returns NotFound — but we still need the volume set to unlink
//     the backing storage).
//  3. The observer-stamped `Status.Volumes[].VolumeNumber`. The
//     observer records these very early (on the first `drbdsetup
//     events2` frame), well before the controller-side apply has a
//     chance to stamp the annotation. This covers the rolling-deploy
//     race: a Resource deleted inside the window after the satellite
//     observed its volumes but before its first successful apply
//     stamped the annotation has a missing RD AND a missing
//     annotation, yet Status.Volumes still names every volume.
//
// Tier 3 (finalizer-strip deadlock): when ALL THREE sources miss,
// the resource genuinely never had any volumes provisioned (or every
// trace of them is gone), so the apply chain is trivially complete —
// `DeleteResource` against an empty volume set is a safe no-op
// (`drbdadm down` runs unconditionally; the per-volume DeleteVolume
// loop iterates zero times). We log loudly and return nil so
// `handleDelete` proceeds to strip the finalizer rather than
// requeueing forever and wedging the Resource in Terminating.
func (r *ResourceReconciler) lookupVolumeNumbers(ctx context.Context, res *blockstoriov1alpha1.Resource) ([]int32, error) {
	var rd blockstoriov1alpha1.ResourceDefinition

	var fromRD []int32

	err := r.Get(ctx, client.ObjectKey{Name: naming.Key(res.Spec.ResourceDefinitionName)}, &rd)

	switch {
	case err == nil:
		for i := range rd.Spec.VolumeDefinitions {
			fromRD = append(fromRD, rd.Spec.VolumeDefinitions[i].VolumeNumber)
		}
	case !apierrors.IsNotFound(err):
		return nil, errors.Wrap(err, "get parent RD for delete")
	}

	// The three sources are joined rather than tried in turn: a volume any
	// of them names belongs to this replica, and one the delete skips is
	// left on disk with nothing to delete it later. Deleting a volume that
	// is already gone is a no-op.
	nums := slices.Concat(fromRD,
		parseVolumeNumbers(res.Annotations[blockstoriov1alpha1.ResourceAnnotationVolumeNumbers]),
		volumeNumbersFromStatus(res))
	slices.Sort(nums)
	nums = slices.Compact(nums)

	if len(nums) > 0 {
		return nums, nil
	}

	// All three sources miss — strip the finalizer anyway (the apply
	// chain is a trivially-complete no-op). Log loudly so operators
	// can correlate with the rolling-deploy / never-applied window.
	log.FromContext(ctx).Info(
		"lookupVolumeNumbers: RD, annotation, and Status.Volumes all empty — "+
			"treating delete as a no-op and stripping finalizer to avoid Terminating deadlock",
		"resource", res.Name,
		"resourceDefinition", res.Spec.ResourceDefinitionName)

	return nil, nil
}

// volumeNumbersFromStatus extracts the observer-stamped volume
// numbers from Resource.Status.Volumes. Returns nil when the
// observer has not recorded any volumes yet.
func volumeNumbersFromStatus(res *blockstoriov1alpha1.Resource) []int32 {
	if len(res.Status.Volumes) == 0 {
		return nil
	}

	out := make([]int32, 0, len(res.Status.Volumes))
	for i := range res.Status.Volumes {
		out = append(out, res.Status.Volumes[i].VolumeNumber)
	}

	return out
}

// nodeNamePredicate filters events down to objects whose
// `Spec.NodeName` matches the given node. Centralised so the
// StoragePool reconciler can reuse the same shape.
func nodeNamePredicate(nodeName string) predicate.Predicate {
	matches := func(obj client.Object) bool {
		if r, ok := obj.(*blockstoriov1alpha1.Resource); ok {
			return naming.Same(r.Spec.NodeName, nodeName)
		}

		if sp, ok := obj.(*blockstoriov1alpha1.StoragePool); ok {
			return naming.Same(sp.Spec.NodeName, nodeName)
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
