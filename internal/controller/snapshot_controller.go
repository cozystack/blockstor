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

	"github.com/cockroachdb/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// SnapshotReconciler orchestrates the Bug-351 cross-satellite
// snapshot barrier:
//
//	Phase 1 ─ broadcast `Spec.SuspendIo=true`. Each satellite that
//	          hosts a diskful peer of the parent RD runs
//	          `drbdsetup suspend-io <rd>` and stamps
//	          Status.NodeStatus[].SuspendIoAcked.
//	Phase 2 ─ once every targeted node has acked, stamp
//	          `Spec.TakeSnapshot=true`. Satellites then dispatch the
//	          local provider.CreateSnapshot and stamp
//	          Status.NodeStatus[].Ready.
//	Phase 3 ─ once every targeted node is Ready (success path) OR
//	          any targeted node stamped Failed=true (abort path),
//	          flip `Spec.SuspendIo=false`. Satellites then issue
//	          `drbdsetup resume-io <rd>`.
//
// Without this barrier two diskful replicas snapshotting
// independently would capture divergent bytes while the
// application writer's traffic was still streaming through DRBD —
// the on-disk LV/zvol bytes on node A reflect a different
// point-in-time cursor than node B's, and any consumer that fans
// the snapshot out to backup / clone / restore loses the
// "consistent across replicas" invariant. Upstream LINSTOR's
// CtrlSnapshotCrtApiCallHandler runs the same 3-phase broadcast
// (setSuspendIo(true) → updateSatellites → ack → takeSnapshot →
// resumeIoPrivileged) so this controller mirrors that shape.
//
// The satellite-side `SnapshotReconciler` (in
// `pkg/satellite/controllers`) executes each per-node step; this
// controller-side reconciler owns the Spec flag transitions ONLY.
// Per-node state (provider.CreateSnapshot, drbdsetup calls,
// finalizer lifecycle) stays on the satellite side.
type SnapshotReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=blockstor.io.blockstor.io,resources=snapshots,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=blockstor.io.blockstor.io,resources=snapshots/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=blockstor.io.blockstor.io,resources=snapshots/finalizers,verbs=update
// +kubebuilder:rbac:groups=blockstor.io.blockstor.io,resources=resources,verbs=get;list;watch

// Reconcile drives the Spec.SuspendIo / Spec.TakeSnapshot
// transitions. Bug 351.
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

	// Tear-down is fully owned by the satellite-side reconciler
	// (finalizer-aware DeleteSnapshot dispatch in
	// pkg/satellite/controllers/snapshot.go). Skip orchestration
	// for a Snapshot that's already being deleted — flipping
	// Spec.SuspendIo on a terminating object would just race the
	// satellite's finalizer-strip.
	if !snap.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	// Abort path takes priority: any per-node Failed=true forces
	// us into Phase 3 (resume) regardless of where in the
	// suspend/take sequence we currently are. Without this, a
	// failure during Phase 1 on one node would leave its already-
	// acked siblings frozen forever waiting for the controller to
	// advance to Phase 2 (which never happens because the failed
	// node never acks).
	if anyNodeFailed(snap.Status.NodeStatus) {
		logger.Info("aborting snapshot: per-node Failed=true observed",
			"failed_node", firstFailedNode(snap.Status.NodeStatus))

		return r.maybeFlipSpec(ctx, &snap, false, false)
	}

	// A Snapshot whose Spec.Nodes is empty is degenerate at the
	// orchestration layer — we have no targeted denominator to
	// gate Phase 2 on. The apiserver populates Spec.Nodes via
	// hydrateSnapshotFromRD before persisting, so this state is
	// unreachable in production; defensive guard so a hand-crafted
	// Snapshot CRD with no Spec.Nodes doesn't hang the controller.
	if len(snap.Spec.Nodes) == 0 {
		return ctrl.Result{}, nil
	}

	next := r.nextPhase(&snap)
	if !next.Advance {
		return ctrl.Result{}, nil
	}

	logger.V(1).Info("advancing orchestration phase",
		"suspendIo", next.SuspendIo, "takeSnapshot", next.TakeSnapshot)

	return r.maybeFlipSpec(ctx, &snap, next.SuspendIo, next.TakeSnapshot)
}

// snapshotPhaseDecision is the (target Spec, advance?) verdict
// nextPhase emits. Named struct so the orchestrator's phase
// transitions read clearly at the call site without resorting to
// named returns (which trip our lint baseline).
type snapshotPhaseDecision struct {
	SuspendIo    bool
	TakeSnapshot bool
	Advance      bool
}

// nextPhase computes the (Spec.SuspendIo, Spec.TakeSnapshot) flag
// pair the Snapshot SHOULD carry on its next persisted state
// transition based on the current Spec view + per-node Status
// aggregates. The third return is true iff the current Spec
// doesn't already match — caller skips the apiserver write when
// false so we don't churn ResourceVersion against the satellite's
// Status writes on every Reconcile pass.
//
// Phase decisions (mirrors the upstream CtrlSnapshotCrtApiCallHandler
// flow):
//
//   - Phase-1 not started: every targeted node either hasn't reported
//     yet or already drained — stamp SuspendIo=true.
//   - Phase-1 done (every node acked): stamp TakeSnapshot=true.
//   - Phase-2 done (every node Ready): clear both flags → resume.
//   - Anything else (mid-phase): no-op until satellites finish.
func (r *SnapshotReconciler) nextPhase(snap *blockstoriov1alpha1.Snapshot) snapshotPhaseDecision {
	targets := snap.Spec.Nodes

	switch {
	case !snap.Spec.SuspendIo && !snap.Spec.TakeSnapshot:
		// Phase 1 not yet started (or already cleared post-abort
		// / post-success). If every target has either already
		// completed (Ready) or already drained (suspend cleared),
		// the orchestration is done.
		if allNodesReady(snap.Status.NodeStatus, targets) ||
			allNodesSuspendCleared(snap.Status.NodeStatus, targets) {
			return snapshotPhaseDecision{}
		}

		return snapshotPhaseDecision{SuspendIo: true, Advance: true}

	case snap.Spec.SuspendIo && !snap.Spec.TakeSnapshot:
		// Phase 1 in flight. Promote to Phase 2 once every
		// targeted node has acked the suspend.
		if !allNodesSuspendAcked(snap.Status.NodeStatus, targets) {
			return snapshotPhaseDecision{SuspendIo: true}
		}

		return snapshotPhaseDecision{SuspendIo: true, TakeSnapshot: true, Advance: true}

	case snap.Spec.SuspendIo && snap.Spec.TakeSnapshot:
		// Phase 2 in flight. Drop into Phase 3 (resume) once
		// every targeted node has stamped Ready=true.
		if !allNodesReady(snap.Status.NodeStatus, targets) {
			return snapshotPhaseDecision{SuspendIo: true, TakeSnapshot: true}
		}

		return snapshotPhaseDecision{Advance: true}
	}

	return snapshotPhaseDecision{
		SuspendIo:    snap.Spec.SuspendIo,
		TakeSnapshot: snap.Spec.TakeSnapshot,
	}
}

// maybeFlipSpec writes the (suspendIo, takeSnapshot) flag pair
// onto the Snapshot's Spec via an optimistic-lock loop. Skips the
// Update entirely when the Spec already matches — pointless
// ResourceVersion churn would race the satellite's Status.NodeStatus
// stamps on every Reconcile pass.
func (r *SnapshotReconciler) maybeFlipSpec(
	ctx context.Context, snap *blockstoriov1alpha1.Snapshot, suspendIo, takeSnapshot bool,
) (ctrl.Result, error) {
	if snap.Spec.SuspendIo == suspendIo && snap.Spec.TakeSnapshot == takeSnapshot {
		return ctrl.Result{}, nil
	}

	key := client.ObjectKeyFromObject(snap)

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var current blockstoriov1alpha1.Snapshot

		getErr := r.Get(ctx, key, &current)
		if getErr != nil {
			if apierrors.IsNotFound(getErr) {
				return nil
			}

			return errors.Wrap(getErr, "get Snapshot for Spec flip")
		}

		if current.Spec.SuspendIo == suspendIo && current.Spec.TakeSnapshot == takeSnapshot {
			return nil
		}

		current.Spec.SuspendIo = suspendIo
		current.Spec.TakeSnapshot = takeSnapshot

		return r.Update(ctx, &current)
	})
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "flip Snapshot Spec flags")
	}

	return ctrl.Result{}, nil
}

// allNodesSuspendAcked reports whether every targeted node has
// stamped Status.NodeStatus[].SuspendIoAcked=true. The denominator
// is Spec.Nodes (caller-restricted broadcast) — an empty Spec.Nodes
// returns false, see the defensive guard in Reconcile.
func allNodesSuspendAcked(entries []blockstoriov1alpha1.SnapshotPerNodeStatus, targets []string) bool {
	return allTargetsMatch(entries, targets, func(e blockstoriov1alpha1.SnapshotPerNodeStatus) bool {
		return e.SuspendIoAcked
	})
}

// allNodesSuspendCleared is the inverse: every targeted node has
// SuspendIoAcked=false (or no entry at all). Used as the
// orchestration's terminal-success signal — after Phase 3 the
// satellites flip their per-node acks back to false to indicate
// resume-io has fired.
func allNodesSuspendCleared(entries []blockstoriov1alpha1.SnapshotPerNodeStatus, targets []string) bool {
	return allTargetsMatch(entries, targets, func(e blockstoriov1alpha1.SnapshotPerNodeStatus) bool {
		return !e.SuspendIoAcked
	})
}

// allNodesReady reports whether every targeted node has
// Status.NodeStatus[].Ready=true.
func allNodesReady(entries []blockstoriov1alpha1.SnapshotPerNodeStatus, targets []string) bool {
	return allTargetsMatch(entries, targets, func(e blockstoriov1alpha1.SnapshotPerNodeStatus) bool {
		return e.Ready
	})
}

// allTargetsMatch is the shared "every targeted node passes the
// predicate" walk. Missing per-node entries fail the predicate by
// default (the satellite hasn't reported back yet).
func allTargetsMatch(
	entries []blockstoriov1alpha1.SnapshotPerNodeStatus,
	targets []string,
	pred func(blockstoriov1alpha1.SnapshotPerNodeStatus) bool,
) bool {
	byNode := make(map[string]blockstoriov1alpha1.SnapshotPerNodeStatus, len(entries))
	for i := range entries {
		byNode[entries[i].NodeName] = entries[i]
	}

	for _, t := range targets {
		entry, ok := byNode[t]
		if !ok {
			return false
		}

		if !pred(entry) {
			return false
		}
	}

	return true
}

// anyNodeFailed reports whether any per-node entry carries
// Failed=true. Used by the abort path so a single failed
// satellite drains the suspended siblings.
func anyNodeFailed(entries []blockstoriov1alpha1.SnapshotPerNodeStatus) bool {
	for i := range entries {
		if entries[i].Failed {
			return true
		}
	}

	return false
}

// firstFailedNode returns the name of the first per-node entry
// with Failed=true, for log-triage.
func firstFailedNode(entries []blockstoriov1alpha1.SnapshotPerNodeStatus) string {
	for i := range entries {
		if entries[i].Failed {
			return entries[i].NodeName
		}
	}

	return ""
}

// SetupWithManager sets up the controller with the Manager.
func (r *SnapshotReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&blockstoriov1alpha1.Snapshot{}).
		Named("snapshot").
		Complete(r)
}
