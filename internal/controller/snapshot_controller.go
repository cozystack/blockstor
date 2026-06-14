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

package controller

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/go-logr/logr"
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
//	          Status.NodeStatus[].SuspendIOAcked.
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
// (setSuspendIO(true) → updateSatellites → ack → takeSnapshot →
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

// +kubebuilder:rbac:groups=blockstor.cozystack.io,resources=snapshots,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=blockstor.cozystack.io,resources=snapshots/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=blockstor.cozystack.io,resources=snapshots/finalizers,verbs=update
// +kubebuilder:rbac:groups=blockstor.cozystack.io,resources=resources,verbs=get;list;watch

// Reconcile drives the Spec.SuspendIO / Spec.TakeSnapshot
// transitions. Bug 351 (single-Snapshot orchestration) + Bug 353
// (cross-Snapshot transactional batch via Spec.GroupID).
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
	// Spec.SuspendIO on a terminating object would just race the
	// satellite's finalizer-strip.
	if !snap.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	// b353: when Spec.GroupID is non-empty, the Snapshot participates
	// in a transactional multi-RD batch — phase advancement gates on
	// every sibling's per-node state, not just self. Empty GroupID
	// is the b351 single-snap path: siblings collapses to {self}, so
	// the aggregate predicates reduce to the self-only walks.
	siblings, err := r.fetchSiblings(ctx, &snap)
	if err != nil {
		return ctrl.Result{}, err
	}

	// Abort paths take priority over phase advancement and force us
	// straight into resume regardless of where in the suspend/take
	// sequence we are: (1) any per-node Failed=true (satellite gave
	// up), or (2) the suspend/take deadline expired (silently-hung
	// take). Both clear SuspendIO across the whole GroupID batch so
	// no frozen peer is left waiting. Returns aborted=true when it
	// has driven the abort, in which case Reconcile is done.
	aborted, result, err := r.checkAbortConditions(ctx, logger, &snap, siblings)
	if aborted || err != nil {
		return result, err
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

	next := r.nextPhase(&snap, siblings)
	if !next.Advance {
		return r.requeueIfSuspended(&snap, siblings), nil
	}

	return r.advancePhase(ctx, logger, &snap, siblings, next)
}

// advancePhase commits the Spec-flag transition nextPhase decided is
// due. Split out of Reconcile (which already carries the get / abort /
// degenerate-guard preamble) to keep each under the funlen budget and
// to keep the consistency-critical transitions — the group suspend
// barrier and the Phase-2 UpToDate guard — grouped where they read in
// sequence.
func (r *SnapshotReconciler) advancePhase(
	ctx context.Context,
	logger logr.Logger,
	snap *blockstoriov1alpha1.Snapshot,
	siblings []blockstoriov1alpha1.Snapshot,
	next snapshotPhaseDecision,
) (ctrl.Result, error) {
	// Bug 046 / Bug-353 suspend barrier: the Phase 0 → Phase 1 entry
	// (the very first SuspendIO=true flip) is the consistency-critical
	// transition for a grouped batch. If each sibling flipped its own
	// SuspendIO independently as the controller happened to reconcile
	// it, the satellites would start `drbdsetup suspend-io` at
	// staggered instants (the ~15s slip Bug 046 reports) and the
	// group's volumes would freeze at different points in time. The
	// barrier instead holds the whole group until every member exists
	// (groupAssembled) and then opens suspend on EVERY sibling in a
	// single pass, so they all enter suspend within one controller
	// cycle — bounding the suspend-entry slip far under the ≤5s
	// budget. Non-grouped (Bug-351 single-snap) Snapshots skip the
	// barrier entirely and keep flipping their own flag.
	if isPhase1Entry(snap, next) && snap.Spec.GroupID != "" {
		return r.openGroupSuspendBarrier(ctx, logger, snap, siblings)
	}

	// Consistency guard: before flipping Phase 1 → Phase 2
	// (TakeSnapshot=true), refuse to snapshot a non-UpToDate device.
	// The Phase-2 gate (allSiblingsSuspendAcked) only proves the
	// kernel froze I/O — it does NOT prove the frozen bytes are good.
	// Snapshotting an Inconsistent / SyncTarget / Outdated replica
	// captures torn data; upstream LINSTOR refuses with "Cannot take
	// snapshot from non-UpToDate DRBD device". On a non-UpToDate
	// target, abort+resume rather than take a bad snapshot.
	if isPhase2Promotion(snap, next) {
		ok, offending, err := r.allTargetedReplicasUpToDate(ctx, siblings)
		if err != nil {
			return ctrl.Result{}, err
		}

		if !ok {
			logger.Info("aborting snapshot group: targeted replica not UpToDate",
				"group_id", snap.Spec.GroupID, "node", offending)

			return r.abortGroupWithReason(ctx, siblings,
				"snapshot aborted: replica on node '"+offending+
					"' is not UpToDate; I/O resumed")
		}
	}

	logger.V(1).Info("advancing orchestration phase",
		"suspendIO", next.SuspendIO, "takeSnapshot", next.TakeSnapshot,
		"group_id", snap.Spec.GroupID)

	// Bug 046 / Bug-353: for a grouped batch, EVERY phase transition —
	// not just the Phase 0→1 suspend entry — must fan out across the
	// whole group in a single reconcile pass. The Phase 1→2 take
	// promotion and the Phase 2→3 / abort resume are equally
	// consistency-critical: if each sibling flipped its own
	// Spec.TakeSnapshot when the controller happened to reconcile it,
	// the per-sibling takes (and resumes) would fire at staggered
	// reconcile times — the same ~15s slip the suspend entry had — and
	// a sibling that resumed I/O while another was still mid-take would
	// reopen the write window before every snapshot was captured,
	// breaking the cross-RD point-in-time guarantee. Flipping the whole
	// group together keeps take-all and resume-all atomic relative to
	// the application writer. Non-grouped (single-snap) Snapshots flip
	// self only, exactly as before.
	if snap.Spec.GroupID != "" {
		return ctrl.Result{}, r.flipGroup(ctx, siblings, next.SuspendIO, next.TakeSnapshot)
	}

	return r.maybeFlipSpec(ctx, snap, next.SuspendIO, next.TakeSnapshot)
}

// flipGroup writes the (suspendIO, takeSnapshot) flag pair onto EVERY
// sibling of a grouped batch in one pass so the whole group advances
// phase together. Each per-sibling write goes through maybeFlipSpec,
// which is a no-op when the sibling already matches — so re-entering
// flipGroup after a partial flip is idempotent and converges. This is
// the general form of suspendGroup / abortGroup (which are the
// SuspendIO=true / SuspendIO=false special cases) and keeps the take
// (Phase 2) and resume (Phase 3) transitions group-atomic, not just the
// suspend entry.
func (r *SnapshotReconciler) flipGroup(
	ctx context.Context, siblings []blockstoriov1alpha1.Snapshot, suspendIO, takeSnapshot bool,
) error {
	for i := range siblings {
		_, err := r.maybeFlipSpec(ctx, &siblings[i], suspendIO, takeSnapshot)
		if err != nil {
			return errors.Wrapf(err,
				"group phase flip (suspendIO=%t takeSnapshot=%t) on sibling %q",
				suspendIO, takeSnapshot, siblings[i].Name)
		}
	}

	return nil
}

// checkAbortConditions evaluates the two abort triggers that outrank
// phase advancement and, when one fires, drives the abort+resume
// cascade across the whole GroupID batch. Returns aborted=true (with
// the resulting ctrl.Result) when it handled an abort, so Reconcile
// can return immediately; aborted=false means the orchestration should
// continue to phase advancement. Pulled out of Reconcile to keep it
// under the funlen budget while keeping the abort precedence explicit.
//
//  1. anySiblingFailed: a satellite stamped per-node Failed=true
//     (tried and gave up). b353 cascade resumes every sibling.
//  2. suspend/take deadline exceeded: a take hung past
//     snapshotSuspendDeadline without any per-node Failed=true stamp
//     (satellite stuck / unreachable / backend hang). The PRIMARY
//     outage fix — without it the volume stays frozen forever. Anchored
//     on metadata.CreationTimestamp (Phase 1 begins at create); records
//     FAILED_DISCONNECT so `linstor s l` shows why.
func (r *SnapshotReconciler) checkAbortConditions(
	ctx context.Context,
	logger logr.Logger,
	snap *blockstoriov1alpha1.Snapshot,
	siblings []blockstoriov1alpha1.Snapshot,
) (bool, ctrl.Result, error) {
	if anySiblingFailed(siblings) {
		logger.Info("aborting snapshot group: per-node Failed=true observed",
			"group_id", snap.Spec.GroupID,
			"failed_node", firstFailedNodeAcrossSiblings(siblings))

		return true, ctrl.Result{}, r.abortGroup(ctx, siblings)
	}

	if inSuspendPhase(snap, siblings) && suspendDeadlineExceeded(snap, time.Now()) {
		logger.Info("aborting snapshot group: suspend/take deadline exceeded",
			"group_id", snap.Spec.GroupID,
			"deadline", snapshotSuspendDeadline.String(),
			"created", snap.CreationTimestamp.Time)

		res, err := r.abortGroupWithReason(ctx, siblings,
			"snapshot aborted: suspend/take exceeded the "+
				snapshotSuspendDeadline.String()+" deadline; I/O resumed")

		return true, res, err
	}

	return false, ctrl.Result{}, nil
}

// isPhase1Entry reports whether the pending phase decision is the
// Phase 0 → Phase 1 transition (the very first SuspendIO=true flip on
// a Snapshot that is not yet suspended and has not taken its
// snapshot). This is the consistency-critical moment for a grouped
// batch — the barrier in Reconcile intercepts exactly this transition
// so every sibling enters suspend together rather than at staggered
// per-sibling reconcile times.
func isPhase1Entry(snap *blockstoriov1alpha1.Snapshot, next snapshotPhaseDecision) bool {
	return next.Advance && next.SuspendIO && !next.TakeSnapshot &&
		!snap.Spec.SuspendIO && !snap.Spec.TakeSnapshot
}

// openGroupSuspendBarrier drives the Phase 0 → Phase 1 entry for a
// grouped (multi-RD) batch as a single coordinated step:
//
//  1. Wait until the group is fully ASSEMBLED — every member CRD the
//     apiserver fanned out (Spec.GroupSize) is observable. Until then
//     no sibling's SuspendIO is flipped, so no volume freezes early.
//     A still-assembling group requeues so the barrier re-evaluates
//     promptly once the remaining siblings' CRDs propagate.
//  2. Before freezing anything, refuse to suspend a group whose
//     targeted replicas are not all UpToDate — there is no point
//     freezing application I/O for a snapshot that the Phase-2 gate
//     would only abort. On a non-UpToDate target, abort+resume the
//     group (a no-op resume here since nothing is suspended yet) and
//     record the reason.
//  3. Flip SuspendIO=true on EVERY sibling in one pass (suspendGroup)
//     so all satellites observe the suspend within one controller
//     cycle — the bounded, near-simultaneous suspend entry that gives
//     the group its single point-in-time.
func (r *SnapshotReconciler) openGroupSuspendBarrier(
	ctx context.Context,
	logger logr.Logger,
	snap *blockstoriov1alpha1.Snapshot,
	siblings []blockstoriov1alpha1.Snapshot,
) (ctrl.Result, error) {
	if !groupAssembled(snap, siblings) {
		logger.V(1).Info("snapshot group still assembling; holding suspend barrier",
			"group_id", snap.Spec.GroupID,
			"observed", len(siblings), "expected", snap.Spec.GroupSize)

		return ctrl.Result{RequeueAfter: groupAssembleRequeueAfter}, nil
	}

	ok, offending, err := r.allTargetedReplicasUpToDate(ctx, siblings)
	if err != nil {
		return ctrl.Result{}, err
	}

	if !ok {
		logger.Info("refusing to open suspend barrier: targeted replica not UpToDate",
			"group_id", snap.Spec.GroupID, "node", offending)

		return r.abortGroupWithReason(ctx, siblings,
			"snapshot aborted: replica on node '"+offending+
				"' is not UpToDate; I/O not suspended")
	}

	logger.V(1).Info("opening group suspend barrier",
		"group_id", snap.Spec.GroupID, "members", len(siblings))

	return ctrl.Result{}, r.suspendGroup(ctx, siblings)
}

// groupAssembled reports whether every member of the transactional
// batch is observable yet. The denominator is Spec.GroupSize (the
// count the apiserver fanned out); the numerator is the observed
// sibling count. A zero / unset GroupSize means "denominator unknown"
// (a legacy grouped Snapshot from before the field existed, or a
// hand-built CRD) — in that case we don't gate, so the group still
// makes progress on whatever siblings are present rather than hanging
// forever on a missing count.
func groupAssembled(snap *blockstoriov1alpha1.Snapshot, siblings []blockstoriov1alpha1.Snapshot) bool {
	if snap.Spec.GroupSize <= 0 {
		return true
	}

	return len(siblings) >= int(snap.Spec.GroupSize)
}

// suspendGroup opens the suspend-io barrier on the whole batch: it
// flips Spec.SuspendIO=true on EVERY sibling in one reconcile pass so
// the satellites all start `drbdsetup suspend-io` within one controller
// cycle. This single-pass fan-out is what bounds the suspend-entry slip
// — the cross-RD point-in-time guarantee Bug 046 / Bug-353 requires.
//
// Symmetric with abortGroup (which clears SuspendIO across the batch):
// each per-sibling flip goes through maybeFlipSpec, which is a no-op
// when the sibling already carries SuspendIO=true, so re-entering
// suspendGroup after a partial flip is idempotent.
func (r *SnapshotReconciler) suspendGroup(
	ctx context.Context, siblings []blockstoriov1alpha1.Snapshot,
) error {
	return r.flipGroup(ctx, siblings, true, false)
}

// isPhase2Promotion reports whether the pending phase decision is the
// Phase 1 → Phase 2 transition (flipping TakeSnapshot=true while
// SuspendIO stays true) for a Snapshot that has not already taken it.
// Used to scope the UpToDate consistency gate to exactly the moment
// the satellites are about to dispatch provider.CreateSnapshot.
func isPhase2Promotion(snap *blockstoriov1alpha1.Snapshot, next snapshotPhaseDecision) bool {
	return next.Advance && next.SuspendIO && next.TakeSnapshot && !snap.Spec.TakeSnapshot
}

// requeueIfSuspended returns the RequeueAfter the controller should
// carry when it has no Spec flip to make this pass but the Snapshot is
// still frozen in the suspend/take window. The controller is
// event-driven, but a silently-hung take emits no further events, so
// without this self-requeue the suspend deadline would never be
// re-evaluated and the timeout abort would never fire. When the
// Snapshot is not in the suspend window (already draining or
// complete), no requeue is needed — the satellite Status writes drive
// the remaining transitions.
func (r *SnapshotReconciler) requeueIfSuspended(
	snap *blockstoriov1alpha1.Snapshot, siblings []blockstoriov1alpha1.Snapshot,
) ctrl.Result {
	if !inSuspendPhase(snap, siblings) {
		return ctrl.Result{}
	}

	return ctrl.Result{RequeueAfter: suspendRequeueAfter(snap, time.Now())}
}

// snapshotPhaseDecision is the (target Spec, advance?) verdict
// nextPhase emits. Named struct so the orchestrator's phase
// transitions read clearly at the call site without resorting to
// named returns (which trip our lint baseline).
type snapshotPhaseDecision struct {
	SuspendIO    bool
	TakeSnapshot bool
	Advance      bool
}

// nextPhase computes the (Spec.SuspendIO, Spec.TakeSnapshot) flag
// pair the Snapshot SHOULD carry on its next persisted state
// transition based on the current Spec view + per-node Status
// aggregates across every same-Group sibling. The third return is
// true iff the current Spec doesn't already match — caller skips
// the apiserver write when false so we don't churn ResourceVersion
// against the satellite's Status writes on every Reconcile pass.
//
// `siblings` is the full transactional batch (self + every
// same-Group peer); for the b351 single-snap path it collapses to
// {self} so the aggregate predicates reduce to the self-only walks.
//
// Phase decisions (mirrors the upstream CtrlSnapshotCrtApiCallHandler
// flow):
//
//   - Phase-1 not started: every targeted node across every sibling
//     either hasn't reported yet or already drained — stamp
//     SuspendIO=true.
//   - Phase-1 done (every sibling's every node acked): stamp
//     TakeSnapshot=true.
//   - Phase-2 done (every sibling's every node Ready): clear both
//     flags → resume.
//   - Anything else (mid-phase): no-op until satellites finish.
func (r *SnapshotReconciler) nextPhase(
	snap *blockstoriov1alpha1.Snapshot, siblings []blockstoriov1alpha1.Snapshot,
) snapshotPhaseDecision {
	switch {
	case !snap.Spec.SuspendIO && !snap.Spec.TakeSnapshot:
		// Phase 1 not yet started (or already cleared post-abort
		// / post-success). If every target across every sibling
		// has either already completed (Ready) or already drained
		// (suspend cleared), the orchestration is done.
		if allSiblingsReady(siblings) || allSiblingsSuspendCleared(siblings) {
			return snapshotPhaseDecision{}
		}

		return snapshotPhaseDecision{SuspendIO: true, Advance: true}

	case snap.Spec.SuspendIO && !snap.Spec.TakeSnapshot:
		// Phase 1 in flight. Promote to Phase 2 once every
		// sibling's every targeted node has acked the suspend.
		if !allSiblingsSuspendAcked(siblings) {
			return snapshotPhaseDecision{SuspendIO: true}
		}

		return snapshotPhaseDecision{SuspendIO: true, TakeSnapshot: true, Advance: true}

	case snap.Spec.SuspendIO && snap.Spec.TakeSnapshot:
		// Phase 2 in flight. Drop into Phase 3 (resume) once
		// every sibling's every targeted node has stamped Ready=true.
		if !allSiblingsReady(siblings) {
			return snapshotPhaseDecision{SuspendIO: true, TakeSnapshot: true}
		}

		return snapshotPhaseDecision{Advance: true}
	}

	return snapshotPhaseDecision{
		SuspendIO:    snap.Spec.SuspendIO,
		TakeSnapshot: snap.Spec.TakeSnapshot,
	}
}

// maybeFlipSpec writes the (suspendIO, takeSnapshot) flag pair
// onto the Snapshot's Spec via an optimistic-lock loop. Skips the
// Update entirely when the Spec already matches — pointless
// ResourceVersion churn would race the satellite's Status.NodeStatus
// stamps on every Reconcile pass.
func (r *SnapshotReconciler) maybeFlipSpec(
	ctx context.Context, snap *blockstoriov1alpha1.Snapshot, suspendIO, takeSnapshot bool,
) (ctrl.Result, error) {
	if snap.Spec.SuspendIO == suspendIO && snap.Spec.TakeSnapshot == takeSnapshot {
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

		if current.Spec.SuspendIO == suspendIO && current.Spec.TakeSnapshot == takeSnapshot {
			return nil
		}

		current.Spec.SuspendIO = suspendIO
		current.Spec.TakeSnapshot = takeSnapshot

		return r.Update(ctx, &current)
	})
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "flip Snapshot Spec flags")
	}

	return ctrl.Result{}, nil
}

// allNodesSuspendAcked reports whether every targeted node has
// stamped Status.NodeStatus[].SuspendIOAcked=true. The denominator
// is Spec.Nodes (caller-restricted broadcast) — an empty Spec.Nodes
// returns false, see the defensive guard in Reconcile.
func allNodesSuspendAcked(entries []blockstoriov1alpha1.SnapshotPerNodeStatus, targets []string) bool {
	return allTargetsMatch(entries, targets, func(e blockstoriov1alpha1.SnapshotPerNodeStatus) bool {
		return e.SuspendIOAcked
	})
}

// allNodesSuspendCleared is the inverse: every targeted node has
// SuspendIOAcked=false (or no entry at all). Used as the
// orchestration's terminal-success signal — after Phase 3 the
// satellites flip their per-node acks back to false to indicate
// resume-io has fired.
func allNodesSuspendCleared(entries []blockstoriov1alpha1.SnapshotPerNodeStatus, targets []string) bool {
	return allTargetsMatch(entries, targets, func(e blockstoriov1alpha1.SnapshotPerNodeStatus) bool {
		return !e.SuspendIOAcked
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

// snapshotGroupIDLabel is the well-known label key that mirrors
// Spec.GroupID onto the Snapshot CRD's metadata.labels, set by the
// store-side `wireToCRDSnapshot` helper when the wire DTO carries a
// non-empty GroupID. Duplicating the literal here rather than
// importing pkg/store/k8s avoids a controller→store dependency that
// the store does not otherwise have. b353.
const snapshotGroupIDLabel = "blockstor.io/snapshot-group-id"

// fetchSiblings returns the transactional batch the Snapshot
// belongs to — self + every same-GroupID peer. For an empty
// Spec.GroupID (b351 single-snap path) the slice collapses to
// {self} so every downstream "every sibling …" predicate reduces
// to the self-only walk and the previous b351 behaviour is
// preserved byte-for-byte.
func (r *SnapshotReconciler) fetchSiblings(
	ctx context.Context, snap *blockstoriov1alpha1.Snapshot,
) ([]blockstoriov1alpha1.Snapshot, error) {
	if snap.Spec.GroupID == "" {
		return []blockstoriov1alpha1.Snapshot{*snap}, nil
	}

	var list blockstoriov1alpha1.SnapshotList

	err := r.List(ctx, &list, client.MatchingLabels{snapshotGroupIDLabel: snap.Spec.GroupID})
	if err != nil {
		return nil, errors.Wrapf(err, "list Snapshot siblings for group %q", snap.Spec.GroupID)
	}

	// Defensive: a freshly-created sibling whose label hasn't
	// propagated through the watch cache yet would surface as an
	// empty list. Fall back to the self-only batch in that edge
	// case so the orchestrator still makes progress on `snap`
	// instead of hanging on a missing-denominator gate.
	if len(list.Items) == 0 {
		return []blockstoriov1alpha1.Snapshot{*snap}, nil
	}

	// Belt-and-suspenders: confirm self is present in the listing.
	// An eventually-consistent cache may briefly omit self if the
	// label index hasn't caught up after Create. If self is
	// missing, append it so phase decisions still see its
	// authoritative Spec/Status.
	selfSeen := false

	for i := range list.Items {
		if list.Items[i].Name == snap.Name {
			selfSeen = true

			// Use the freshly-fetched copy for self so the
			// caller sees the up-to-date ResourceVersion.
			list.Items[i] = *snap

			break
		}
	}

	if !selfSeen {
		list.Items = append(list.Items, *snap)
	}

	return list.Items, nil
}

// allSiblingsSuspendAcked reports whether every sibling's every
// targeted node has stamped SuspendIOAcked=true. Phase 2 advancement
// gate for the cross-Snapshot transactional batch.
func allSiblingsSuspendAcked(siblings []blockstoriov1alpha1.Snapshot) bool {
	for i := range siblings {
		if !allNodesSuspendAcked(siblings[i].Status.NodeStatus, siblings[i].Spec.Nodes) {
			return false
		}
	}

	return true
}

// allSiblingsSuspendCleared reports whether every sibling's every
// targeted node has SuspendIOAcked=false (terminal-success drain).
func allSiblingsSuspendCleared(siblings []blockstoriov1alpha1.Snapshot) bool {
	for i := range siblings {
		if !allNodesSuspendCleared(siblings[i].Status.NodeStatus, siblings[i].Spec.Nodes) {
			return false
		}
	}

	return true
}

// allSiblingsReady reports whether every sibling's every targeted
// node has stamped Ready=true. Phase 3 advancement gate for the
// cross-Snapshot transactional batch.
func allSiblingsReady(siblings []blockstoriov1alpha1.Snapshot) bool {
	for i := range siblings {
		if !allNodesReady(siblings[i].Status.NodeStatus, siblings[i].Spec.Nodes) {
			return false
		}
	}

	return true
}

// anySiblingFailed reports whether any sibling has any per-node
// Failed=true. Triggers the abort cascade — clearing SuspendIO on
// every sibling, not just the failed one, so the still-frozen peers
// of the unaffected siblings also drain.
func anySiblingFailed(siblings []blockstoriov1alpha1.Snapshot) bool {
	for i := range siblings {
		if anyNodeFailed(siblings[i].Status.NodeStatus) {
			return true
		}
	}

	return false
}

// firstFailedNodeAcrossSiblings returns the first (sibling, node)
// pair with Failed=true, formatted as "<snap>/<node>" for log
// triage. Empty string when no sibling has any failed node.
func firstFailedNodeAcrossSiblings(siblings []blockstoriov1alpha1.Snapshot) string {
	for i := range siblings {
		node := firstFailedNode(siblings[i].Status.NodeStatus)
		if node != "" {
			return siblings[i].Name + "/" + node
		}
	}

	return ""
}

// abortGroup propagates the abort signal across every sibling in
// the transactional batch — clearing Spec.SuspendIO and
// Spec.TakeSnapshot on every Snapshot CRD that shares a GroupID
// with the one that observed a Failed=true node. Without this
// cascade, the un-failed siblings would stay in Phase 1 forever
// waiting for the doomed sibling to ack — and their already-
// suspended satellite peers would never resume I/O.
//
// Each per-sibling flip goes through `maybeFlipSpec`, which is a
// no-op when the sibling has already drained — so re-entering
// abortGroup on a partially-drained group is idempotent.
func (r *SnapshotReconciler) abortGroup(
	ctx context.Context, siblings []blockstoriov1alpha1.Snapshot,
) error {
	for i := range siblings {
		_, err := r.maybeFlipSpec(ctx, &siblings[i], false, false)
		if err != nil {
			return errors.Wrapf(err,
				"abort cascade: clear SuspendIO on sibling %q", siblings[i].Name)
		}
	}

	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *SnapshotReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&blockstoriov1alpha1.Snapshot{}).
		Named("snapshot").
		Complete(r)
}
