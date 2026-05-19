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
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
)

// SatelliteSnapshotFinalizer guards a Snapshot CRD while one of
// its on-disk materialisations still exists on this satellite.
// Without the finalizer the apiserver removes the Snapshot
// object as soon as the user issues `kubectl delete`, often
// before the satellite has observed the DeletionTimestamp
// event — the on-disk ZFS snapshot / thin-pool LV survives as
// an orphan because handleDelete never runs (Bug 64).
//
// Mirrors the per-satellite finalizer scheme used by
// `SatelliteResourceFinalizer` and `StoragePoolFinalizer`.
const SatelliteSnapshotFinalizer = "blockstor.io.blockstor.io/satellite-snapshot"

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

	if !slices.Contains(snap.Spec.Nodes, r.Config.NodeName) {
		return ctrl.Result{}, nil
	}

	logger.V(1).Info("observed Snapshot",
		"rd", snap.Spec.ResourceDefinitionName,
		"name", snap.Spec.SnapshotName,
		"deletionTimestamp", snap.DeletionTimestamp)

	if !snap.DeletionTimestamp.IsZero() {
		return r.handleDelete(ctx, &snap)
	}

	if !slices.Contains(snap.Finalizers, SatelliteSnapshotFinalizer) {
		snap.Finalizers = append(snap.Finalizers, SatelliteSnapshotFinalizer)

		err := r.Update(ctx, &snap)
		if err != nil {
			return ctrl.Result{}, errors.Wrap(err, "add snapshot finalizer")
		}

		return ctrl.Result{RequeueAfter: snapshotFinalizerRequeue}, nil
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

	return nil
}

// handleCreate dispatches the per-node half of the Bug-351
// orchestrated snapshot lifecycle. The controller-side
// `internal/controller/snapshot_controller.go` drives the
// Spec.SuspendIo / Spec.TakeSnapshot flag transitions; each
// satellite Reconcile observes the current Spec shape and acts on
// it idempotently:
//
//   - Spec.SuspendIo=true, this-node SuspendIoAcked=false →
//     call drbdsetup suspend-io for the parent RD, then stamp
//     Status.NodeStatus[us].SuspendIoAcked=true. Holds the local
//     DRBD I/O frozen until the controller flips SuspendIo=false.
//   - Spec.SuspendIo=false, this-node SuspendIoAcked=true →
//     call drbdsetup resume-io and clear SuspendIoAcked. This is
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
//     per-node Failed=true and flips Spec.SuspendIo=false so the
//     suspended siblings drain.
//   - Apply.CreateSnapshot returned Terminal=false (transient) ⇒
//     log and return with Requeue=true; controller-runtime's rate
//     limiter handles back-off. Status stays empty so the wire
//     view stays "Incomplete".
//   - Apply.CreateSnapshot returned Ok=true ⇒ Status.NodeStatus
//     stamp lands, the controller flips Phase 3 once every node
//     is Ready.
func (r *SnapshotReconciler) handleCreate(ctx context.Context, snap *blockstoriov1alpha1.Snapshot) (ctrl.Result, error) {
	// Phase 1: parent RD I/O suspend. Per-node ack lives on
	// Status.NodeStatus[us].SuspendIoAcked. Idempotent — the
	// helper short-circuits when already acked.
	if snap.Spec.SuspendIo && !perNodeStatusSuspendIoAcked(snap.Status.NodeStatus, r.Config.NodeName) {
		return r.handleSuspendPhase(ctx, snap)
	}

	// Phase 3 (resume): controller flipped Spec.SuspendIo=false
	// after every diskful peer either succeeded or one of them
	// Failed. Drain our local suspend regardless — see the
	// "resume on abort" note on Spec.SuspendIo.
	if !snap.Spec.SuspendIo && perNodeStatusSuspendIoAcked(snap.Status.NodeStatus, r.Config.NodeName) {
		return r.handleResumePhase(ctx, snap)
	}

	// Phase 2: take the per-node snapshot. Gated on Spec.TakeSnapshot
	// (controller stamps it once every targeted node has acked Phase
	// 1) AND on this node not having already succeeded.
	if !snap.Spec.TakeSnapshot {
		// Suspend acked, but the controller hasn't promoted to
		// take-snapshot yet — siblings still suspending. Wait.
		return ctrl.Result{}, nil
	}

	if perNodeStatusReady(snap.Status.NodeStatus, r.Config.NodeName) {
		// Already took our local snapshot; the controller is
		// either still waiting on slow siblings or hasn't
		// observed our Ready stamp yet. No-op.
		return ctrl.Result{}, nil
	}

	return r.handleTakeSnapshotPhase(ctx, snap)
}

// handleSuspendPhase dispatches Phase 1 of the Bug-351
// orchestration: drive `drbdsetup suspend-io` for the parent RD
// and stamp Status.NodeStatus[us].SuspendIoAcked=true. A failed
// suspend stamps the per-node Failed=true so the controller-side
// orchestrator drains the siblings.
func (r *SnapshotReconciler) handleSuspendPhase(ctx context.Context, snap *blockstoriov1alpha1.Snapshot) (ctrl.Result, error) {
	err := r.Config.Apply.SuspendResource(ctx, snap.Spec.ResourceDefinitionName)
	if err != nil {
		log.FromContext(ctx).Info("SuspendResource failed",
			"snapshot", snap.Spec.SnapshotName, "error", err.Error())

		// Suspend failure is treated as terminal on this
		// satellite — the kernel-side suspend-io is a no-op on
		// an already-frozen resource, so a non-zero exit
		// indicates either a missing kernel module or a
		// permanently-broken resource, neither of which a
		// retry would resolve.
		stampErr := r.stampSnapshotPerNodeFailed(ctx, snap)
		if stampErr != nil {
			return ctrl.Result{}, stampErr
		}

		return ctrl.Result{}, nil
	}

	err = r.stampSnapshotPerNodeSuspendAcked(ctx, snap)
	if err != nil {
		return ctrl.Result{}, err
	}

	// Don't fall through: wait for the controller to flip
	// Spec.TakeSnapshot once every node has acked.
	return ctrl.Result{}, nil
}

// handleResumePhase dispatches Phase 3 of the orchestration:
// `drbdsetup resume-io` after the controller flipped
// Spec.SuspendIo=false. A failed resume requeues without
// clearing the ack so the orchestrator keeps poking us until
// the kernel actually accepts the unfreeze.
func (r *SnapshotReconciler) handleResumePhase(ctx context.Context, snap *blockstoriov1alpha1.Snapshot) (ctrl.Result, error) {
	err := r.Config.Apply.ResumeResource(ctx, snap.Spec.ResourceDefinitionName)
	if err != nil {
		log.FromContext(ctx).Info("ResumeResource failed",
			"snapshot", snap.Spec.SnapshotName, "error", err.Error())

		return ctrl.Result{Requeue: true}, nil
	}

	err = r.stampSnapshotPerNodeSuspendCleared(ctx, snap)
	if err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// handleTakeSnapshotPhase dispatches Phase 2 of the
// orchestration: invoke `provider.CreateSnapshot` via the
// satellite Reconciler and stamp Status.NodeStatus[us].Ready.
// Routes the Ok/Terminal/Transient three-way verdict from
// CreateSnapshotResponse the same way the pre-Bug-351 single-step
// path did (Bug 106 / F18) — orchestration just gates when this
// step runs.
func (r *SnapshotReconciler) handleTakeSnapshotPhase(ctx context.Context, snap *blockstoriov1alpha1.Snapshot) (ctrl.Result, error) {
	req := &intent.CreateSnapshotRequest{
		ResourceName: snap.Spec.ResourceDefinitionName,
		SnapshotName: snap.Spec.SnapshotName,
	}

	for i := range snap.Spec.VolumeDefinitions {
		req.VolumeNumbers = append(req.VolumeNumbers, snap.Spec.VolumeDefinitions[i].VolumeNumber)
	}

	resp, err := r.Config.Apply.CreateSnapshot(ctx, req)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "CreateSnapshot")
	}

	if resp.GetOk() {
		// Bug 106: stamp Status.NodeStatus[us].Ready so the
		// apiserver's success denominator can flip the snapshot
		// from "Incomplete" to "Successful".
		err = r.stampSnapshotPerNodeReady(ctx, snap, resp.CreateTimestampUnix)
		if err != nil {
			return ctrl.Result{}, err
		}

		return ctrl.Result{}, nil
	}

	log.FromContext(ctx).Info("CreateSnapshot per-snapshot failure",
		"snapshot", snap.Spec.SnapshotName,
		"message", resp.GetMessage(),
		"terminal", resp.GetTerminal())

	if !resp.GetTerminal() {
		// Transient — back off and try again.
		return ctrl.Result{Requeue: true}, nil
	}

	// Terminal failure — stamp FAILED + per-node Failed=true so
	// the orchestrator drains the suspended siblings.
	err = r.stampSnapshotPerNodeFailed(ctx, snap)
	if err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// perNodeStatusSuspendIoAcked reports whether the NodeStatus
// slice already carries our entry with SuspendIoAcked=true.
// Mirrors perNodeStatusReady (Bug 106) but for the Phase-1
// suspend-io ack lifecycle. The controller-side orchestrator
// reads aggregated per-node SuspendIoAcked across every targeted
// node to decide when to promote to Phase 2; this satellite-side
// helper is the local short-circuit so a re-Reconcile after the
// stamp already landed doesn't re-fire drbdsetup or churn the
// Status subresource.
func perNodeStatusSuspendIoAcked(entries []blockstoriov1alpha1.SnapshotPerNodeStatus, nodeName string) bool {
	for i := range entries {
		if entries[i].NodeName != nodeName {
			continue
		}

		return entries[i].SuspendIoAcked
	}

	return false
}

// stampSnapshotPerNodeSuspendAcked upserts our node's entry in
// Status.NodeStatus with SuspendIoAcked=true after a successful
// `drbdsetup suspend-io`. Conflict-retry shape matches
// stampSnapshotPerNodeReady — sibling satellites race against us
// on the same Status subresource as they each stamp their own
// per-node ack. Idempotent: a re-stamp on an already-acked entry
// short-circuits without firing a Status().Update.
func (r *SnapshotReconciler) stampSnapshotPerNodeSuspendAcked(
	ctx context.Context, snap *blockstoriov1alpha1.Snapshot,
) error {
	return r.upsertPerNodeStatusField(ctx, snap, func(entry *blockstoriov1alpha1.SnapshotPerNodeStatus) bool {
		if entry.SuspendIoAcked {
			return false
		}

		entry.SuspendIoAcked = true

		return true
	})
}

// stampSnapshotPerNodeSuspendCleared clears our node's
// SuspendIoAcked back to false after a successful `drbdsetup
// resume-io`. The controller-side orchestrator and the
// satellite's own short-circuit both read SuspendIoAcked, so the
// clear is what stops the Phase-3 resume loop from re-firing on
// every Reconcile pass.
func (r *SnapshotReconciler) stampSnapshotPerNodeSuspendCleared(
	ctx context.Context, snap *blockstoriov1alpha1.Snapshot,
) error {
	return r.upsertPerNodeStatusField(ctx, snap, func(entry *blockstoriov1alpha1.SnapshotPerNodeStatus) bool {
		if !entry.SuspendIoAcked {
			return false
		}

		entry.SuspendIoAcked = false

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
// optimistic-lock loop shared by the SuspendIoAcked stampers. The
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
// orchestration touches (SuspendIoAcked, Failed).
func upsertPerNodeStatusInPlace(
	entries *[]blockstoriov1alpha1.SnapshotPerNodeStatus,
	nodeName string,
	mutate func(*blockstoriov1alpha1.SnapshotPerNodeStatus) bool,
) bool {
	for i := range *entries {
		if (*entries)[i].NodeName != nodeName {
			continue
		}

		return mutate(&(*entries)[i])
	}

	fresh := blockstoriov1alpha1.SnapshotPerNodeStatus{NodeName: nodeName}
	if !mutate(&fresh) {
		return false
	}

	*entries = append(*entries, fresh)

	return true
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
func (r *SnapshotReconciler) stampSnapshotPerNodeReady(
	ctx context.Context, snap *blockstoriov1alpha1.Snapshot, createTimestamp int64,
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
	for i := range entries {
		if entries[i].NodeName != nodeName {
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
// pre-existing SuspendIoAcked / Failed bools on the replaced entry
// — the take-snapshot stamp must not clobber the Phase-1 ack
// (otherwise the controller-side orchestrator would see the ack
// drop and mis-classify the snapshot as still mid-suspend).
func upsertPerNodeStatus(entries []blockstoriov1alpha1.SnapshotPerNodeStatus, nodeName string, createTimestamp int64) []blockstoriov1alpha1.SnapshotPerNodeStatus {
	out := make([]blockstoriov1alpha1.SnapshotPerNodeStatus, 0, len(entries)+1)
	replaced := false

	for i := range entries {
		if entries[i].NodeName == nodeName {
			merged := entries[i]
			merged.NodeName = nodeName
			merged.Ready = true
			merged.CreateTimestamp = createTimestamp

			out = append(out, merged)

			replaced = true

			continue
		}

		out = append(out, entries[i])
	}

	if !replaced {
		out = append(out, blockstoriov1alpha1.SnapshotPerNodeStatus{
			NodeName:        nodeName,
			Ready:           true,
			CreateTimestamp: createTimestamp,
		})
	}

	return out
}

// handleDelete mirrors handleCreate for the DeletionTimestamp
// case. Drives `Apply.DeleteSnapshot` (which routes to the
// provider's `DeleteSnapshot` — `zfs destroy …@<snap>` for ZFS,
// `lvremove` for LVM-thin/thick, `os.Remove` for FILE) before
// stripping the finalizer so kube-apiserver only finalises the
// CRD once the on-disk snapshot is gone. Idempotent — a
// `DeleteSnapshot` on a missing snapshot is a no-op so a re-run
// after a satellite restart is safe.
//
// Bug 64: prior to the finalizer the apiserver removed the
// Snapshot object before the satellite saw the delete event,
// leaving the on-disk snapshot as an orphan.
func (r *SnapshotReconciler) handleDelete(ctx context.Context, snap *blockstoriov1alpha1.Snapshot) (ctrl.Result, error) {
	if !slices.Contains(snap.Finalizers, SatelliteSnapshotFinalizer) {
		// Either we never stamped it (Snapshot created before this
		// satellite came up) or someone already stripped it.
		// Either way, nothing for us to do — let the apiserver
		// finalise.
		return ctrl.Result{}, nil
	}

	req := &intent.DeleteSnapshotRequest{
		ResourceName: snap.Spec.ResourceDefinitionName,
		SnapshotName: snap.Spec.SnapshotName,
	}

	resp, err := r.Config.Apply.DeleteSnapshot(ctx, req)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "DeleteSnapshot")
	}

	if !resp.GetOk() {
		// Body-level failure — log and let controller-runtime
		// back-off retry. Keep the finalizer in place so the
		// CRD doesn't vanish before tear-down succeeds.
		log.FromContext(ctx).Info("DeleteSnapshot per-snapshot failure",
			"snapshot", snap.Spec.SnapshotName, "message", resp.GetMessage())

		return ctrl.Result{Requeue: true}, nil
	}

	snap.Finalizers = slices.DeleteFunc(snap.Finalizers,
		func(f string) bool { return f == SatelliteSnapshotFinalizer })

	err = r.Update(ctx, snap)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "strip snapshot finalizer")
	}

	return ctrl.Result{}, nil
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

		return slices.Contains(snap.Spec.Nodes, nodeName)
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
