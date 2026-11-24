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
	"slices"
	"strconv"
	"strings"
	"sync"
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
	"github.com/cozystack/blockstor/pkg/finalizers"
	"github.com/cozystack/blockstor/pkg/naming"
	"github.com/cozystack/blockstor/pkg/satellite"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
)

// SatelliteSnapshotFinalizer is the finalizer older satellites shared. It is
// kept only to migrate Snapshots that still carry it; see
// finalizers.SharedSatelliteSnapshot.
const SatelliteSnapshotFinalizer = finalizers.SharedSatelliteSnapshot

// snapshotNodeFinalizer is the finalizer this satellite holds on a Snapshot
// while its own copy may still be on disk; see finalizers.SnapshotNode.
func snapshotNodeFinalizer(node string) string { return finalizers.SnapshotNode(node) }

// SnapshotNodeFinalizer is snapshotNodeFinalizer for tests outside the
// package.
func SnapshotNodeFinalizer(node string) string { return finalizers.SnapshotNode(node) }

// snapshotFinalizerRequeue is the short back-off between
// stamping the finalizer and the next Reconcile pass that
// actually runs `CreateSnapshot`. Matches the storagepool
// reconciler's one-second cadence.
const snapshotFinalizerRequeue = time.Second

// SnapshotReconciler watches Snapshot CRDs and acts on those
// whose `Spec.Nodes` list includes this satellite's name. The
// snapshot semantic is per-node — one Snapshot CRD ends up
// triggering CreateSnapshot on each node mentioned in its
// Nodes list. Phase 10.1 replaces the gRPC `CreateSnapshot` /
// `DeleteSnapshot` consumers.
type SnapshotReconciler struct {
	client.Client

	Config Config

	windowsOnce sync.Once
	windows     *snapshotWindows
	timing      snapshotWindowTiming
}

// Reconcile handles the satellite-side snapshot lifecycle:
// create on first appearance, delete on `DeletionTimestamp`.
// Finalizer-aware: stamps `SatelliteSnapshotFinalizer` on
// every observed Snapshot scoped to this node so kube-apiserver
// waits for our `DeleteSnapshot` before allowing the CRD to
// disappear. Idempotent — re-runs on a snapshot that already
// exists are safe.
func (r *SnapshotReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("snapshot", req.Name)

	var snap blockstoriov1alpha1.Snapshot

	err := r.Get(ctx, req.NamespacedName, &snap)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}

		return ctrl.Result{}, errors.Wrap(err, "get Snapshot")
	}

	if !slices.ContainsFunc(snap.Spec.Nodes, func(n string) bool { return naming.Same(n, r.Config.NodeName) }) {
		return ctrl.Result{}, nil
	}

	logger.V(1).Info("observed Snapshot",
		"rd", snap.Spec.ResourceDefinitionName,
		"name", snap.Spec.SnapshotName,
		"deletionTimestamp", snap.DeletionTimestamp)

	if !snap.DeletionTimestamp.IsZero() {
		return r.handleDelete(ctx, &snap)
	}

	mine := snapshotNodeFinalizer(r.Config.NodeName)
	if !slices.Contains(snap.Finalizers, mine) {
		snap.Finalizers = append(snap.Finalizers, mine)

		err := r.Update(ctx, &snap)
		if err != nil {
			return ctrl.Result{}, errors.Wrap(err, "add snapshot finalizer")
		}

		return ctrl.Result{RequeueAfter: snapshotFinalizerRequeue}, nil
	}

	err = r.dropSharedFinalizer(ctx, &snap)
	if err != nil {
		return ctrl.Result{}, err
	}

	return r.handleCreate(ctx, &snap)
}

// SetupWithManager wires the reconciler with a node-membership
// predicate: Spec.Nodes list contains our NodeName.
func (r *SnapshotReconciler) SetupWithManager(mgr ctrl.Manager) error {
	err := ctrl.NewControllerManagedBy(mgr).
		For(&blockstoriov1alpha1.Snapshot{},
			builder.WithPredicates(snapshotNodePredicate(r.Config.NodeName))).
		Named("satellite-snapshot").
		Complete(r)
	if err != nil {
		return errors.Wrap(err, "register SnapshotReconciler")
	}

	err = mgr.Add(&SnapshotSuspendWatchdog{Reconciler: r})
	if err != nil {
		return errors.Wrap(err, "register SnapshotSuspendWatchdog")
	}

	return nil
}

// dropSharedFinalizer retires the old shared finalizer from a live Snapshot
// once every node in its list holds its own, so nothing still depends on it.
// A node that has not stamped its own yet (an older satellite during a
// rolling upgrade) keeps the shared one in place.
func (r *SnapshotReconciler) dropSharedFinalizer(ctx context.Context, snap *blockstoriov1alpha1.Snapshot) error {
	if !slices.Contains(snap.Finalizers, SatelliteSnapshotFinalizer) {
		return nil
	}

	for _, node := range snap.Spec.Nodes {
		if !slices.Contains(snap.Finalizers, snapshotNodeFinalizer(node)) {
			return nil
		}
	}

	snap.Finalizers = slices.DeleteFunc(snap.Finalizers,
		func(f string) bool { return f == SatelliteSnapshotFinalizer })

	return errors.Wrap(r.Update(ctx, snap), "drop the shared snapshot finalizer")
}

// handleCreate dispatches the per-node half of the Bug-351
// orchestrated snapshot lifecycle. The controller-side
// `internal/controller/snapshot_controller.go` drives the
// Spec.SuspendIO / Spec.TakeSnapshot flag transitions; each
// satellite Reconcile observes the current Spec shape and acts on
// it idempotently:
//
//   - Spec.SuspendIO=true, this-node SuspendIOAcked=false →
//     call drbdsetup suspend-io for the parent RD, then stamp
//     Status.NodeStatus[us].SuspendIOAcked=true. Holds the local
//     DRBD I/O frozen until the controller flips SuspendIO=false.
//   - Spec.SuspendIO=false, this-node SuspendIOAcked=true →
//     call drbdsetup resume-io and clear SuspendIOAcked. This is
//     the success (Phase 3) and abort (any node Failed) drain.
//     Resume MUST happen even on abort, otherwise the application
//     writer hangs forever on the still-frozen siblings.
//   - Spec.TakeSnapshot=true, this-node Ready=false → dispatch
//     the existing provider.CreateSnapshot path and stamp
//     Status.NodeStatus[us] with Ready=true + the satellite-side
//     CreateTimestamp. Idempotent — the provider's lvExists /
//     datasetExists pre-check (Bug 216) folds an
//     already-materialised snapshot into success.
//   - Otherwise (already-in-target-state) → no-op. The Reconcile
//     terminates without churning the apiserver.
//
// Failure routing — F18 (cli-parity for `linstor s l` State column):
//   - Apply.CreateSnapshot returned Terminal=true ⇒ stamp
//     Status.Flags=["FAILED"] + Status.NodeStatus[us].Failed=true
//     on the Snapshot CRD and return without requeueing.
//     crdToWireSnapshot surfaces that as `flags: ["FAILED"]` on
//     /v1/view/snapshots, which the Python CLI maps to
//     State="Failed". The controller-side orchestrator reads the
//     per-node Failed=true and flips Spec.SuspendIO=false so the
//     suspended siblings drain.
//   - Apply.CreateSnapshot returned Terminal=false (transient) ⇒
//     log and return with Requeue=true; controller-runtime's rate
//     limiter handles back-off. Status stays empty so the wire
//     view stays "Incomplete".
//   - Apply.CreateSnapshot returned Ok=true ⇒ Status.NodeStatus
//     stamp lands, the controller flips Phase 3 once every node
//     is Ready.
func (r *SnapshotReconciler) handleCreate(ctx context.Context, snap *blockstoriov1alpha1.Snapshot) (ctrl.Result, error) {
	// Entries for this node under two spellings are folded into one
	// before any phase reads them: the phase checks look at the first,
	// the controller at the last, and an acknowledgment on one alias
	// would otherwise never reach the other.
	if nodeStatusHasAliases(snap.Status.NodeStatus, r.Config.NodeName) {
		err := r.upsertPerNodeStatusField(ctx, snap, func(*blockstoriov1alpha1.SnapshotPerNodeStatus) bool { return false })

		return ctrl.Result{RequeueAfter: time.Second}, err
	}

	acked := perNodeStatusSuspendIOAcked(snap.Status.NodeStatus, r.Config.NodeName)

	// This node gave up on the snapshot. Make sure its window is
	// closed and never open another one for it: the controller is
	// about to abort, and a second suspend would only freeze I/O again.
	if snap.Spec.SuspendIO && perNodeStatusFailed(snap.Status.NodeStatus, r.Config.NodeName) {
		return r.handleResumePhase(ctx, snap)
	}

	// Phase 1: parent RD I/O suspend. Per-node ack lives on
	// Status.NodeStatus[us].SuspendIOAcked.
	if snap.Spec.SuspendIO && !acked {
		return r.handleSuspendPhase(ctx, snap)
	}

	// Phase 3 (resume): controller flipped Spec.SuspendIO=false
	// after every diskful peer either succeeded or one of them
	// Failed. Drain our local suspend regardless — see the
	// "resume on abort" note on Spec.SuspendIO.
	if !snap.Spec.SuspendIO && (acked || r.windowsFor().get(snap.Name) != nil) {
		return r.handleResumePhase(ctx, snap)
	}

	// Already took our local snapshot: the controller is either still
	// waiting on slow siblings, or the take is over and it cleared both
	// flags. All that is left is to record which pool holds the copy,
	// which a snapshot taken before that record gets here too.
	if perNodeStatusReady(snap.Status.NodeStatus, r.Config.NodeName) {
		return ctrl.Result{}, r.recordSnapshotPool(ctx, snap)
	}

	// Phase 2: take the per-node snapshot. Gated on Spec.TakeSnapshot
	// (controller stamps it once every targeted node has acked Phase
	// 1) AND on this node not having already succeeded.
	if !snap.Spec.TakeSnapshot {
		// Suspend acked, but the controller hasn't promoted to
		// take-snapshot yet — siblings still suspending. Wait.
		return ctrl.Result{}, nil
	}

	return r.handleTakeSnapshotPhase(ctx, snap)
}

// handleSuspendPhase dispatches Phase 1: flush, suspend I/O for the
// parent RD and stamp Status.NodeStatus[us].SuspendIOAcked=true. A
// failed suspend stamps the per-node Failed=true so the
// controller-side orchestrator drains the siblings.
func (r *SnapshotReconciler) handleSuspendPhase(ctx context.Context, snap *blockstoriov1alpha1.Snapshot) (ctrl.Result, error) {
	layers, err := r.snapshotLayers(ctx, snap)
	if err != nil {
		return ctrl.Result{}, err
	}

	if !layers.drbd {
		// Nothing to suspend without DRBD; the take is point-in-time
		// on its own.
		return ctrl.Result{}, r.stampSnapshotPerNodeSuspendAcked(ctx, snap)
	}

	if r.windowsFor().get(snap.Name) != nil {
		// Suspended on an earlier pass whose ack stamp did not land.
		return ctrl.Result{}, r.stampSnapshotPerNodeSuspendAcked(ctx, snap)
	}

	if r.windowEnded(snap.Name) {
		return r.refuseReopen(ctx, snap)
	}

	// What the take needs from the API is read before the suspend, so
	// the take inside the window is the storage command alone, as in
	// LINSTOR, and a node that would refuse the take refuses it without
	// freezing anything. The reads are still not free: a sibling that
	// suspended first is frozen while this node reads, so they are
	// made only for a resource that can carry a LUKS password.
	if layers.luks {
		refused, err := r.refusesUnrecordedLUKSKey(ctx, snap)
		if err != nil {
			return ctrl.Result{}, err
		}

		if refused {
			return ctrl.Result{}, r.stampSnapshotPerNodeFailed(ctx, snap)
		}
	}

	err = r.openWindow(ctx, snap)

	switch {
	case errors.Is(err, errResourceBusy):
		// Another suspend holds the resource; its resume would end
		// ours. Nothing is suspended for this one yet.
		if r.mustNotWaitFor(ctx, snap) {
			log.FromContext(ctx).Info("resource held by a later snapshot; failing this one instead of waiting",
				"snapshot", snap.Spec.SnapshotName)

			return ctrl.Result{}, r.stampSnapshotPerNodeFailed(ctx, snap)
		}

		return ctrl.Result{RequeueAfter: snapshotBusyRetry}, nil
	case errors.Is(err, errSatelliteStopping):
		return ctrl.Result{}, err
	case err != nil:
		log.FromContext(ctx).Info("SuspendResource failed",
			"snapshot", snap.Spec.SnapshotName, "error", err.Error())

		// Terminal on this satellite: suspend-io on an already
		// frozen resource is a no-op, so a failure means a missing
		// kernel module or a broken resource, which a retry would
		// not fix.
		return ctrl.Result{}, r.stampSnapshotPerNodeFailed(ctx, snap)
	}

	// Don't fall through: wait for the controller to flip
	// Spec.TakeSnapshot once every node has acked.
	return ctrl.Result{}, r.stampSnapshotPerNodeSuspendAcked(ctx, snap)
}

// refuseReopen answers a suspend request for a snapshot whose window on
// this node is already over. The Snapshot can still ask for it when the
// stamp that ended the window did not land, as when the API is cut off
// at the deadline; suspending again would freeze I/O for another full
// window, and again after that, for as long as the stamps fail. The
// node fails the snapshot instead, unless its take already succeeded.
func (r *SnapshotReconciler) refuseReopen(ctx context.Context, snap *blockstoriov1alpha1.Snapshot) (ctrl.Result, error) {
	if perNodeStatusReady(snap.Status.NodeStatus, r.Config.NodeName) {
		return ctrl.Result{}, nil
	}

	if marker, ok := r.readMarker(snap.Name); ok {
		err := r.resumeLeftover(ctx, &marker)
		if err != nil {
			log.FromContext(ctx).Info("settle an ended suspend window; retrying",
				"snapshot", snap.Spec.SnapshotName, "error", err.Error())

			return ctrl.Result{RequeueAfter: snapshotResumeRetry}, nil
		}

		return ctrl.Result{}, nil
	}

	return ctrl.Result{}, r.stampSnapshotPerNodeFailed(ctx, snap)
}

// needsWindow reports whether snap's resource has a DRBD layer on this
// satellite to suspend. The answer is per resource, as LINSTOR only
// suspends layers that support it: a STORAGE or LUKS,STORAGE resource
// on a node that runs DRBD for others has nothing to suspend, and
// suspend-io on it would fail the snapshot. An RD that is not found
// keeps the default stack, which has DRBD.
func (r *SnapshotReconciler) needsWindow(ctx context.Context, snap *blockstoriov1alpha1.Snapshot) (bool, error) {
	layers, err := r.snapshotLayers(ctx, snap)

	return layers.drbd, err
}

// snapshotResourceLayers is what a snapshot's resource stacks on this
// satellite: drbd when it has a DRBD layer to suspend, luks when it can
// carry a LUKS password.
type snapshotResourceLayers struct {
	drbd bool
	luks bool
}

// snapshotLayers reads snap's layer stack from the cached definition.
// An RD that is not found, or whose stack is empty and may still come
// from its group, is assumed to carry LUKS, so the LUKS record is
// checked whenever the stack does not rule it out.
func (r *SnapshotReconciler) snapshotLayers(
	ctx context.Context, snap *blockstoriov1alpha1.Snapshot,
) (snapshotResourceLayers, error) {
	if !r.Config.Apply.DRBDEnabled() {
		return snapshotResourceLayers{luks: true}, nil
	}

	var rd blockstoriov1alpha1.ResourceDefinition

	err := r.Get(ctx, client.ObjectKey{Name: naming.Key(snap.Spec.ResourceDefinitionName)}, &rd)
	if apierrors.IsNotFound(err) {
		return snapshotResourceLayers{drbd: true, luks: true}, nil
	}

	if err != nil {
		return snapshotResourceLayers{}, errors.Wrapf(err, "get ResourceDefinition %s", snap.Spec.ResourceDefinitionName)
	}

	return snapshotResourceLayers{
		drbd: rdNeedsDRBD(&rd),
		luks: len(rd.Spec.LayerStack) == 0 || layerStackHasLUKS(rd.Spec.LayerStack),
	}, nil
}

// handleResumePhase closes this node's window for the snapshot: the
// controller flipped Spec.SuspendIO=false, or this node failed it. The
// ack is cleared only once the kernel reports the resource resumed; a
// resume it does not confirm keeps the ack, which is what the
// controller reports as I/O still suspended, and is retried.
func (r *SnapshotReconciler) handleResumePhase(ctx context.Context, snap *blockstoriov1alpha1.Snapshot) (ctrl.Result, error) {
	var err error

	if r.windowsFor().get(snap.Name) != nil {
		err = r.closeWindow(ctx, snap.Name, false, "")
	} else if perNodeStatusSuspendIOAcked(snap.Status.NodeStatus, r.Config.NodeName) {
		marker := r.markerFor(snap)
		err = r.resumeLeftover(ctx, &marker)
	}

	if err != nil {
		log.FromContext(ctx).Info("ResumeResource failed",
			"snapshot", snap.Spec.SnapshotName, "error", err.Error())

		return ctrl.Result{RequeueAfter: snapshotResumeRetry}, nil
	}

	return ctrl.Result{}, nil
}

// markerFor returns the marker recorded for snap, or one naming its
// resource when none was recorded.
func (r *SnapshotReconciler) markerFor(snap *blockstoriov1alpha1.Snapshot) suspendMarker {
	marker, ok := r.readMarker(snap.Name)
	if !ok || marker.Resource == "" {
		marker.Resource = naming.Key(snap.Spec.ResourceDefinitionName)
		marker.Snapshot = snap.Name
	}

	return marker
}

// handleTakeSnapshotPhase dispatches Phase 2. With DRBD the take runs
// on the window's own goroutine under the window's deadline, and this
// pass does not wait for it: the watchdog stamps the verdict and closes
// the window if the take outlives it. A node that acked a suspend no
// window here remembers lost it to a restart; the snapshot it would
// take is no longer point-in-time, so the node resumes and fails it,
// which is what LINSTOR's controller does when a satellite drops out of
// the take (CtrlSnapshotCrtApiCallHandler abortSnapshotInTransaction).
func (r *SnapshotReconciler) handleTakeSnapshotPhase(ctx context.Context, snap *blockstoriov1alpha1.Snapshot) (ctrl.Result, error) {
	needed, err := r.needsWindow(ctx, snap)
	if err != nil {
		return ctrl.Result{}, err
	}

	if !needed {
		return r.takeWithoutWindow(ctx, snap)
	}

	if r.windowsFor().get(snap.Name) == nil {
		marker := r.markerFor(snap)

		err := r.resumeLeftover(ctx, &marker)
		if err != nil {
			log.FromContext(ctx).Info("resume of an interrupted suspend window not confirmed; retrying",
				"snapshot", snap.Spec.SnapshotName, "error", err.Error())

			return ctrl.Result{RequeueAfter: snapshotResumeRetry}, nil
		}

		return ctrl.Result{}, nil
	}

	r.startTake(ctx, snap)

	return ctrl.Result{}, nil
}

// takeWithoutWindow takes the snapshot of a resource no DRBD layer
// sits on. Nothing is suspended, so the storage device is flushed right
// before the take and the take runs on this pass.
func (r *SnapshotReconciler) takeWithoutWindow(ctx context.Context, snap *blockstoriov1alpha1.Snapshot) (ctrl.Result, error) {
	r.Config.Apply.FlushStorageDevices(ctx, naming.Key(snap.Spec.ResourceDefinitionName),
		snapshotVolumeNumbers(snap))

	verdict, taken, err := r.takeVerdict(ctx, snap, true)
	if err != nil {
		return ctrl.Result{}, err
	}

	if verdict == takeTransient {
		return ctrl.Result{Requeue: true}, nil
	}

	return ctrl.Result{}, r.reportTake(ctx, snap, verdict, taken)
}

// takeOutcome is the three-way verdict of one storage-layer take.
type takeOutcome int

const (
	takeOK takeOutcome = iota
	takeTransient
	takeTerminal
)

// takenCopy is what a successful take reports about the copy: when it
// was taken and the pool that holds it.
type takenCopy struct {
	createdAt int64
	pool      string
}

// takeVerdict invokes provider.CreateSnapshot via the satellite
// Reconciler and classifies the result the way the pre-Bug-351
// single-step path did (Bug 106 / F18): Ok, a transient failure worth
// retrying, or a terminal one. It stamps nothing. checkLUKS reads
// whether the snapshot records this replica's LUKS password; a take in
// a suspend window passes false, its window was opened only after that
// read.
func (r *SnapshotReconciler) takeVerdict(
	ctx context.Context, snap *blockstoriov1alpha1.Snapshot, checkLUKS bool,
) (takeOutcome, takenCopy, error) {
	req := &intent.CreateSnapshotRequest{
		ResourceName:  naming.Key(snap.Spec.ResourceDefinitionName),
		SnapshotName:  snap.Spec.SnapshotName,
		VolumeNumbers: snapshotVolumeNumbers(snap),
	}

	if checkLUKS {
		refused, err := r.refusesUnrecordedLUKSKey(ctx, snap)
		if err != nil {
			return takeTransient, takenCopy{}, err
		}

		if refused {
			return takeTerminal, takenCopy{}, nil
		}
	}

	resp, err := r.Config.Apply.CreateSnapshot(ctx, req)
	if err != nil {
		return takeTransient, takenCopy{}, errors.Wrap(err, "CreateSnapshot")
	}

	if resp.GetOk() {
		return takeOK, takenCopy{createdAt: resp.CreateTimestampUnix, pool: resp.GetStoragePool()}, nil
	}

	log.FromContext(ctx).Info("CreateSnapshot per-snapshot failure",
		"snapshot", snap.Spec.SnapshotName,
		"message", resp.GetMessage(),
		"terminal", resp.GetTerminal())

	if !resp.GetTerminal() {
		return takeTransient, takenCopy{}, nil
	}

	return takeTerminal, takenCopy{}, nil
}

// refusesUnrecordedLUKSKey reports whether this node must refuse the
// take because the snapshot does not record the LUKS password of the
// replica it would copy: the copy would be encrypted under a password
// nothing keeps, so it could never be restored.
func (r *SnapshotReconciler) refusesUnrecordedLUKSKey(ctx context.Context, snap *blockstoriov1alpha1.Snapshot) (bool, error) {
	missing, err := r.unrecordedLUKSKey(ctx, snap, snapshotVolumeNumbers(snap))
	if err != nil {
		return false, err
	}

	if missing == "" {
		return false, nil
	}

	log.FromContext(ctx).Info("refusing to take a snapshot that does not record this replica's LUKS password",
		"snapshot", snap.Spec.SnapshotName, "volume", missing)

	return true, nil
}

// perNodeStatusSuspendIOAcked reports whether the NodeStatus
// slice already carries our entry with SuspendIOAcked=true.
// Mirrors perNodeStatusReady (Bug 106) but for the Phase-1
// suspend-io ack lifecycle. The controller-side orchestrator
// reads aggregated per-node SuspendIOAcked across every targeted
// node to decide when to promote to Phase 2; this satellite-side
// helper is the local short-circuit so a re-Reconcile after the
// stamp already landed doesn't re-fire drbdsetup or churn the
// Status subresource.
func perNodeStatusSuspendIOAcked(entries []blockstoriov1alpha1.SnapshotPerNodeStatus, nodeName string) bool {
	for i := range entries {
		if !naming.Same(entries[i].NodeName, nodeName) {
			continue
		}

		return entries[i].SuspendIOAcked
	}

	return false
}

// stampSnapshotPerNodeSuspendAcked upserts our node's entry in
// Status.NodeStatus with SuspendIOAcked=true after a successful
// `drbdsetup suspend-io`. Conflict-retry shape matches
// stampSnapshotPerNodeReady — sibling satellites race against us
// on the same Status subresource as they each stamp their own
// per-node ack. Idempotent: a re-stamp on an already-acked entry
// short-circuits without firing a Status().Update.
func (r *SnapshotReconciler) stampSnapshotPerNodeSuspendAcked(
	ctx context.Context, snap *blockstoriov1alpha1.Snapshot,
) error {
	return r.upsertPerNodeStatusField(ctx, snap, func(entry *blockstoriov1alpha1.SnapshotPerNodeStatus) bool {
		if entry.SuspendIOAcked {
			return false
		}

		entry.SuspendIOAcked = true

		return true
	})
}

// stampSnapshotPerNodeSuspendCleared clears our node's
// SuspendIOAcked back to false after a successful `drbdsetup
// resume-io`. The controller-side orchestrator and the
// satellite's own short-circuit both read SuspendIOAcked, so the
// clear is what stops the Phase-3 resume loop from re-firing on
// every Reconcile pass.
func (r *SnapshotReconciler) stampSnapshotPerNodeSuspendCleared(
	ctx context.Context, snap *blockstoriov1alpha1.Snapshot,
) error {
	return r.upsertPerNodeStatusField(ctx, snap, func(entry *blockstoriov1alpha1.SnapshotPerNodeStatus) bool {
		if !entry.SuspendIOAcked {
			return false
		}

		entry.SuspendIOAcked = false

		return true
	})
}

// stampSnapshotPerNodeFailed stamps both the global Status.Flags
// FAILED marker and our node's Status.NodeStatus[].Failed=true so
// the controller-side orchestrator (which reads aggregated per-node
// Failed across every targeted node) can drive the abort+drain
// path. Idempotent: a re-stamp on a Snapshot that already carries
// FAILED + per-node Failed=true is a no-op.
func (r *SnapshotReconciler) stampSnapshotPerNodeFailed(
	ctx context.Context, snap *blockstoriov1alpha1.Snapshot,
) error {
	key := client.ObjectKeyFromObject(snap)

	for attempt := range snapshotStatusUpdateRetries {
		_ = attempt

		var current blockstoriov1alpha1.Snapshot

		err := r.Get(ctx, key, &current)
		if err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}

			return errors.Wrap(err, "get Snapshot for FAILED stamp")
		}

		mutated := false

		if !slices.Contains(current.Status.Flags, blockstoriov1alpha1.SnapshotStatusFlagFailed) {
			current.Status.Flags = append(current.Status.Flags, blockstoriov1alpha1.SnapshotStatusFlagFailed)
			mutated = true
		}

		entryMutated := upsertPerNodeStatusInPlace(&current.Status.NodeStatus, r.Config.NodeName,
			func(entry *blockstoriov1alpha1.SnapshotPerNodeStatus) bool {
				if entry.Failed {
					return false
				}

				entry.Failed = true

				return true
			})
		mutated = mutated || entryMutated

		if !mutated {
			return nil
		}

		err = r.Status().Update(ctx, &current)
		if err == nil {
			return nil
		}

		if !apierrors.IsConflict(err) {
			return errors.Wrap(err, "stamp Status.Flags=FAILED + NodeStatus.Failed")
		}
	}

	return errors.New("Status FAILED stamp conflicted out of retries")
}

// upsertPerNodeStatusField is the Status.NodeStatus
// optimistic-lock loop shared by the SuspendIOAcked stampers. The
// mutate fn receives a writable pointer to the per-node entry
// (created on the fly when absent) and returns true iff it
// actually changed something — if false, the loop short-circuits
// without firing a Status().Update, mirroring
// stampSnapshotPerNodeReady's idempotence guard.
func (r *SnapshotReconciler) upsertPerNodeStatusField(
	ctx context.Context,
	snap *blockstoriov1alpha1.Snapshot,
	mutate func(*blockstoriov1alpha1.SnapshotPerNodeStatus) bool,
) error {
	key := client.ObjectKeyFromObject(snap)

	for attempt := range snapshotStatusUpdateRetries {
		_ = attempt

		var current blockstoriov1alpha1.Snapshot

		err := r.Get(ctx, key, &current)
		if err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}

			return errors.Wrap(err, "get Snapshot for NodeStatus stamp")
		}

		mutated := upsertPerNodeStatusInPlace(&current.Status.NodeStatus, r.Config.NodeName, mutate)
		if !mutated {
			return nil
		}

		err = r.Status().Update(ctx, &current)
		if err == nil {
			return nil
		}

		if !apierrors.IsConflict(err) {
			return errors.Wrap(err, "stamp Status.NodeStatus")
		}
	}

	return errors.New("Status.NodeStatus update conflicted out of retries")
}

// upsertPerNodeStatusInPlace finds the per-node entry for
// `nodeName` (or appends a fresh one) and runs `mutate` against
// it. Returns true iff the mutate fn reported a change. Mirrors
// upsertPerNodeStatus (the Ready/CreateTimestamp variant) but
// composable for the multiple Status bools the Bug-351
// orchestration touches (SuspendIOAcked, Failed).
func upsertPerNodeStatusInPlace(
	entries *[]blockstoriov1alpha1.SnapshotPerNodeStatus,
	nodeName string,
	mutate func(*blockstoriov1alpha1.SnapshotPerNodeStatus) bool,
) bool {
	collapsed, mine, folded := collapseNodeStatus(*entries, nodeName)
	if mine >= 0 {
		if !mutate(&collapsed[mine]) && !folded {
			return false
		}

		*entries = collapsed

		return true
	}

	fresh := blockstoriov1alpha1.SnapshotPerNodeStatus{NodeName: nodeName}
	if !mutate(&fresh) {
		return false
	}

	*entries = append(*entries, fresh)

	return true
}

// nodeStatusHasAliases reports whether entries hold more than one entry for
// nodeName, in any spelling.
func nodeStatusHasAliases(entries []blockstoriov1alpha1.SnapshotPerNodeStatus, nodeName string) bool {
	seen := false

	for i := range entries {
		if !naming.Same(entries[i].NodeName, nodeName) {
			continue
		}

		if seen {
			return true
		}

		seen = true
	}

	return false
}

// collapseNodeStatus folds every entry for one node, in whatever spelling
// it was written, into the first of them. Entries written before under
// Node-1 and node-1 are one node's: a writer that updated only the first
// and a reader that kept only the last would never meet. Returns the
// entries, the folded entry's index (-1 when the node has none), and
// whether any alias was folded away.
func collapseNodeStatus(
	entries []blockstoriov1alpha1.SnapshotPerNodeStatus, nodeName string,
) ([]blockstoriov1alpha1.SnapshotPerNodeStatus, int, bool) {
	out := make([]blockstoriov1alpha1.SnapshotPerNodeStatus, 0, len(entries))
	mine := -1
	folded := false

	for i := range entries {
		entry := entries[i]

		switch {
		case !naming.Same(entry.NodeName, nodeName):
			out = append(out, entry)
		case mine < 0:
			mine = len(out)
			out = append(out, entry)
		default:
			kept := &out[mine]
			kept.Ready = kept.Ready || entry.Ready
			kept.SuspendIOAcked = kept.SuspendIOAcked || entry.SuspendIOAcked
			kept.Failed = kept.Failed || entry.Failed
			kept.CreateTimestamp = max(kept.CreateTimestamp, entry.CreateTimestamp)

			if kept.StoragePool == "" {
				kept.StoragePool = entry.StoragePool
			}

			folded = true
		}
	}

	return out, mine, folded
}

// stampSnapshotPerNodeReady upserts our node's entry in
// Status.NodeStatus with Ready=true and the satellite-reported
// CreateTimestamp. This is the missing half of the Bug 106 fix:
// the apiserver's `stampSnapshotSuccessful` derivation reads
// `Snapshots[].CreateTimestamp` to decide whether every diskful
// peer reported back, and without this write nothing ever populates
// it (commit 3c593c5f7 shipped the apiserver-side derivation but
// no code path was writing the stamp). Once every diskful peer has
// upserted, the apiserver flips the snapshot from "Incomplete" to
// "Successful" in the wire view.
//
// Concurrent satellites race against each other on the same Status
// subresource — the optimistic-lock conflict is retried up to
// snapshotStatusUpdateRetries times against a fresh fetch, which
// converges in well under a second on a 3-node stand. After the
// retry budget the caller's outer reconcile rate-limiter backs off
// and we try again on the next pass.
//
// The same write records pool, the pool the copy was taken in. Written
// apart from Ready, the record would miss whenever the source replica went
// between the two writes, which is the usual CSI order (the PVC is deleted
// as soon as its VolumeSnapshot is ready), and every restore of the copy
// would then be refused.
func (r *SnapshotReconciler) stampSnapshotPerNodeReady(
	ctx context.Context, snap *blockstoriov1alpha1.Snapshot, createTimestamp int64, pool string,
) error {
	key := client.ObjectKeyFromObject(snap)

	for attempt := range snapshotStatusUpdateRetries {
		_ = attempt

		var current blockstoriov1alpha1.Snapshot

		err := r.Get(ctx, key, &current)
		if err != nil {
			if apierrors.IsNotFound(err) {
				// Snapshot vanished mid-reconcile — nothing to stamp.
				return nil
			}

			return errors.Wrap(err, "get Snapshot for NodeStatus stamp")
		}

		if perNodeStatusReady(current.Status.NodeStatus, r.Config.NodeName) {
			// Idempotent: another reconcile pass on this satellite
			// already wrote our entry. Skip the update so we don't
			// touch ResourceVersion needlessly.
			return nil
		}

		current.Status.NodeStatus = upsertPerNodeStatus(
			current.Status.NodeStatus, r.Config.NodeName, createTimestamp)
		recordPoolOnEntry(current.Status.NodeStatus, r.Config.NodeName, pool)

		err = r.Status().Update(ctx, &current)
		if err == nil {
			return nil
		}

		if !apierrors.IsConflict(err) {
			return errors.Wrap(err, "stamp Status.NodeStatus")
		}

		// Conflict — sibling satellite raced our write. Re-fetch
		// and try again. The loop budget bounds the worst case.
	}

	return errors.New("Status.NodeStatus update conflicted out of retries")
}

// snapshotStatusUpdateRetries bounds the optimistic-lock retry
// loop in stampSnapshotPerNodeReady. Three retries cover the
// 3-node stand's worst case (every other satellite writes
// between our fetch and our update); beyond that we yield to the
// outer reconcile rate limiter rather than hot-spinning.
const snapshotStatusUpdateRetries = 5

// perNodeStatusReady reports whether the NodeStatus slice already
// carries our entry with Ready=true and a non-zero CreateTimestamp.
// Used to skip no-op Status().Update calls on a satellite that has
// already stamped its row — the satellite picks `time.Now().Unix()`
// afresh on every CreateSnapshot, so comparing exact timestamps
// would defeat the idempotence guard. The Ready+timestamp pair is
// what the apiserver's success derivation actually reads, so any
// non-zero stamp keeps the wire shape correct.
func perNodeStatusReady(entries []blockstoriov1alpha1.SnapshotPerNodeStatus, nodeName string) bool {
	// Aliases still to fold are not stamped yet, whatever the first says.
	if nodeStatusHasAliases(entries, nodeName) {
		return false
	}

	for i := range entries {
		if !naming.Same(entries[i].NodeName, nodeName) {
			continue
		}

		return entries[i].Ready && entries[i].CreateTimestamp != 0
	}

	return false
}

// upsertPerNodeStatus returns a copy of the NodeStatus slice with
// our entry either updated (matching NodeName found) or appended.
// Preserves the existing slice order so concurrent siblings see a
// stable view in the conflict-retry race. Bug 351: preserves the
// pre-existing SuspendIOAcked / Failed bools on the replaced entry
// — the take-snapshot stamp must not clobber the Phase-1 ack
// (otherwise the controller-side orchestrator would see the ack
// drop and mis-classify the snapshot as still mid-suspend).
func upsertPerNodeStatus(entries []blockstoriov1alpha1.SnapshotPerNodeStatus, nodeName string, createTimestamp int64) []blockstoriov1alpha1.SnapshotPerNodeStatus {
	out, mine, _ := collapseNodeStatus(entries, nodeName)
	if mine < 0 {
		return append(out, blockstoriov1alpha1.SnapshotPerNodeStatus{
			NodeName:        nodeName,
			Ready:           true,
			CreateTimestamp: createTimestamp,
		})
	}

	out[mine].NodeName = nodeName
	out[mine].Ready = true
	out[mine].CreateTimestamp = createTimestamp

	return out
}

// handleDelete mirrors handleCreate for the DeletionTimestamp
// case. Drives `Apply.DeleteSnapshot` (which routes to the
// provider's `DeleteSnapshot` — `zfs destroy …@<snap>` for ZFS,
// `lvremove` for LVM-thin/thick, `os.Remove` for FILE) before
// releasing this node's finalizer, so kube-apiserver only finalises the
// CRD once every node's on-disk copy is gone. Idempotent — a
// `DeleteSnapshot` on a missing snapshot is a no-op so a re-run
// after a satellite restart is safe.
//
// Bug 64: prior to the finalizer the apiserver removed the
// Snapshot object before the satellite saw the delete event,
// leaving the on-disk snapshot as an orphan.
//
// A Snapshot that still carries the old shared finalizer and not this
// node's own was never migrated by this node: this node then releases the
// shared one after deleting its copy, which is what older satellites did. A
// node holding both releases them together, once every other listed node
// holds its own.
func (r *SnapshotReconciler) handleDelete(ctx context.Context, snap *blockstoriov1alpha1.Snapshot) (ctrl.Result, error) {
	mine := snapshotNodeFinalizer(r.Config.NodeName)
	holdsOwn := slices.Contains(snap.Finalizers, mine)
	holdsShared := slices.Contains(snap.Finalizers, SatelliteSnapshotFinalizer)

	if !holdsOwn && !holdsShared {
		// Either we never stamped it (Snapshot created before this
		// satellite came up) or it was already released.
		// Either way, nothing for us to do — let the apiserver
		// finalise.
		return ctrl.Result{}, nil
	}

	// Close this node's window before the snapshot goes: a deleted CR
	// is the one signal the watchdog cannot read back. The resume does
	// not wait for the take; the delete does. A take still running,
	// even one whose window the watchdog already closed, would create
	// the storage snapshot after the delete, and nothing would remove
	// it once the finalizer is gone.
	if w := r.windowsFor().get(snap.Name); w != nil {
		err := r.closeWindow(ctx, snap.Name, false, "")
		if err != nil {
			log.FromContext(ctx).Info("resume before delete not confirmed; the watchdog retries it",
				"snapshot", snap.Spec.SnapshotName, "error", err.Error())
		}
	} else if marker, ok := r.readMarker(snap.Name); ok {
		err := r.resumeLeftover(ctx, &marker)
		if err != nil {
			log.FromContext(ctx).Info("resume before delete not confirmed; the watchdog retries it",
				"snapshot", snap.Spec.SnapshotName, "error", err.Error())
		}
	}

	if r.takeInFlight(snap.Name) {
		return ctrl.Result{RequeueAfter: snapshotResumeRetry}, nil
	}

	pools, err := r.diskfulPools(ctx)
	if err != nil {
		return ctrl.Result{}, err
	}

	req := &intent.DeleteSnapshotRequest{
		ResourceName:  naming.Key(snap.Spec.ResourceDefinitionName),
		SnapshotName:  snap.Spec.SnapshotName,
		StoragePools:  pools,
		DeletedSuffix: snap.Spec.Props[snapshotRenameSuffixProp(r.Config.NodeName)],
	}

	resp, err := r.Config.Apply.DeleteSnapshot(ctx, req)
	if err != nil {
		stampErr := r.stampSnapshotDeleteFailure(ctx, snap, err.Error())
		if stampErr != nil {
			log.FromContext(ctx).Error(stampErr, "record snapshot delete failure")
		}

		return ctrl.Result{}, errors.Wrap(err, "DeleteSnapshot")
	}

	if !resp.GetOk() {
		// Body-level failure — log and let controller-runtime
		// back-off retry. Keep the finalizer in place so the
		// CRD doesn't vanish before tear-down succeeds.
		log.FromContext(ctx).Info("DeleteSnapshot per-snapshot failure",
			"snapshot", snap.Spec.SnapshotName, "message", resp.GetMessage())

		err = r.stampSnapshotDeleteFailure(ctx, snap, resp.GetMessage())
		if err != nil {
			log.FromContext(ctx).Error(err, "record snapshot delete failure")
		}

		return ctrl.Result{Requeue: true}, nil
	}

	// Holding both, this node keeps its own until the shared one is gone,
	// unless no other node depends on the shared one. Released alone, its
	// own would leave it holding only the shared one, and its next pass
	// would read that as a node that never stamped its own and release the
	// shared one too, before an older satellite in the list had deleted
	// its copy.
	if holdsOwn && holdsShared &&
		!finalizers.OthersHoldTheirOwn(snap.Spec.Nodes, snap.Finalizers, r.Config.NodeName) {
		return ctrl.Result{}, nil
	}

	snap.Finalizers = slices.DeleteFunc(snap.Finalizers, func(f string) bool {
		return f == mine || f == SatelliteSnapshotFinalizer
	})

	err = r.Update(ctx, snap)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "strip snapshot finalizer")
	}

	return ctrl.Result{}, nil
}

// diskfulPools names this node's pools that can hold a snapshot, so a
// snapshot whose replica is gone can still be found and deleted.
func (r *SnapshotReconciler) diskfulPools(ctx context.Context) ([]string, error) {
	var list blockstoriov1alpha1.StoragePoolList

	err := r.List(ctx, &list)
	if err != nil {
		return nil, errors.Wrap(err, "list storage pools")
	}

	var pools []string

	for i := range list.Items {
		pool := &list.Items[i]
		if naming.Same(pool.Spec.NodeName, r.Config.NodeName) &&
			!strings.EqualFold(pool.Spec.ProviderKind, satellite.ProviderKindDiskless) {
			pools = append(pools, pool.Spec.PoolName)
		}
	}

	slices.Sort(pools)

	return pools, nil
}

// stampSnapshotDeleteFailure records why this node failed to delete
// its copy in Status.DeleteFailures. LINSTOR answers a snapshot delete
// with the error its satellite returned; without the record the REST
// call could only keep waiting on a node that is online but failing,
// since the finalizer stays and nothing else tells the two apart.
func (r *SnapshotReconciler) stampSnapshotDeleteFailure(
	ctx context.Context, snap *blockstoriov1alpha1.Snapshot, message string,
) error {
	if message == "" {
		message = "the satellite could not delete its copy of the snapshot"
	}

	key := client.ObjectKeyFromObject(snap)

	for range snapshotStatusUpdateRetries {
		var current blockstoriov1alpha1.Snapshot

		err := r.Get(ctx, key, &current)
		if err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}

			return errors.Wrap(err, "get Snapshot for delete failure stamp")
		}

		idx := slices.IndexFunc(current.Status.DeleteFailures,
			func(f blockstoriov1alpha1.SnapshotDeleteFailure) bool {
				return naming.Same(f.NodeName, r.Config.NodeName)
			})

		switch {
		case idx < 0:
			current.Status.DeleteFailures = append(current.Status.DeleteFailures,
				blockstoriov1alpha1.SnapshotDeleteFailure{NodeName: r.Config.NodeName, Message: message})
		case current.Status.DeleteFailures[idx].Message == message:
			return nil
		default:
			current.Status.DeleteFailures[idx].Message = message
		}

		err = r.Status().Update(ctx, &current)
		if err == nil {
			return nil
		}

		if !apierrors.IsConflict(err) {
			return errors.Wrap(err, "stamp Status.DeleteFailures")
		}
	}

	return errors.New("Status.DeleteFailures stamp conflicted out of retries")
}

// snapshotNodePredicate filters Snapshot events to those whose
// Spec.Nodes contains the given node name. Like
// nodeNamePredicate but for the membership-list shape.
func snapshotNodePredicate(nodeName string) predicate.Predicate {
	matches := func(obj client.Object) bool {
		snap, ok := obj.(*blockstoriov1alpha1.Snapshot)
		if !ok {
			return false
		}

		return slices.ContainsFunc(snap.Spec.Nodes, func(n string) bool { return naming.Same(n, nodeName) })
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

// unrecordedLUKSKey names the first of vols whose replica on this node
// carries a LUKS password the snapshot did not record, or returns "". The
// snapshot copies the passwords when it is created; a replica formatted
// after that, before the take, is the gap this closes. The replica is read
// past the cache, as the store read it for the copy.
func (r *SnapshotReconciler) unrecordedLUKSKey(ctx context.Context, snap *blockstoriov1alpha1.Snapshot, vols []int32) (string, error) {
	reader := r.Config.APIReader
	if reader == nil {
		reader = r.Client
	}

	name, err := replicaCRName(ctx, reader, snap.Spec.ResourceDefinitionName, r.Config.NodeName)
	if err != nil {
		return "", err
	}

	var res blockstoriov1alpha1.Resource

	err = reader.Get(ctx, client.ObjectKey{Name: name}, &res)
	if apierrors.IsNotFound(err) {
		return "", nil
	}

	if err != nil {
		return "", errors.Wrapf(err, "get replica %s", name)
	}

	self := naming.Key(r.Config.NodeName)

	for _, vol := range vols {
		if luksKeyOf(&res, vol) == nil {
			continue
		}

		recorded := slices.ContainsFunc(snap.Spec.LUKSKeys, func(k blockstoriov1alpha1.SnapshotLUKSKey) bool {
			return k.VolumeNumber == vol && naming.Key(k.NodeName) == self
		})
		if !recorded {
			return name + "/" + strconv.Itoa(int(vol)), nil
		}
	}

	return "", nil
}
