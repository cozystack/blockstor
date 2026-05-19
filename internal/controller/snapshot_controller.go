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
	// Spec.SuspendIo on a terminating object would just race the
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

	// Abort path takes priority: any per-node Failed=true (in self
	// or any sibling) forces us into Phase 3 (resume) regardless of
	// where in the suspend/take sequence we currently are. Without
	// this, a failure during Phase 1 on one node would leave its
	// already-acked siblings frozen forever waiting for the
	// controller to advance to Phase 2 (which never happens because
	// the failed node never acks).
	//
	// b353 cascade: when grouped, the abort signal MUST propagate
	// across the whole batch — clearing SuspendIo only on the
	// failed sibling would leave the other siblings' frozen peers
	// stuck waiting on a never-coming all-acked transition.
	if anySiblingFailed(siblings) {
		logger.Info("aborting snapshot group: per-node Failed=true observed",
			"group_id", snap.Spec.GroupID,
			"failed_node", firstFailedNodeAcrossSiblings(siblings))

		return r.abortGroup(ctx, siblings)
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
		return ctrl.Result{}, nil
	}

	logger.V(1).Info("advancing orchestration phase",
		"suspendIo", next.SuspendIo, "takeSnapshot", next.TakeSnapshot,
		"group_id", snap.Spec.GroupID)

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
//     SuspendIo=true.
//   - Phase-1 done (every sibling's every node acked): stamp
//     TakeSnapshot=true.
//   - Phase-2 done (every sibling's every node Ready): clear both
//     flags → resume.
//   - Anything else (mid-phase): no-op until satellites finish.
func (r *SnapshotReconciler) nextPhase(
	snap *blockstoriov1alpha1.Snapshot, siblings []blockstoriov1alpha1.Snapshot,
) snapshotPhaseDecision {
	switch {
	case !snap.Spec.SuspendIo && !snap.Spec.TakeSnapshot:
		// Phase 1 not yet started (or already cleared post-abort
		// / post-success). If every target across every sibling
		// has either already completed (Ready) or already drained
		// (suspend cleared), the orchestration is done.
		if allSiblingsReady(siblings) || allSiblingsSuspendCleared(siblings) {
			return snapshotPhaseDecision{}
		}

		return snapshotPhaseDecision{SuspendIo: true, Advance: true}

	case snap.Spec.SuspendIo && !snap.Spec.TakeSnapshot:
		// Phase 1 in flight. Promote to Phase 2 once every
		// sibling's every targeted node has acked the suspend.
		if !allSiblingsSuspendAcked(siblings) {
			return snapshotPhaseDecision{SuspendIo: true}
		}

		return snapshotPhaseDecision{SuspendIo: true, TakeSnapshot: true, Advance: true}

	case snap.Spec.SuspendIo && snap.Spec.TakeSnapshot:
		// Phase 2 in flight. Drop into Phase 3 (resume) once
		// every sibling's every targeted node has stamped Ready=true.
		if !allSiblingsReady(siblings) {
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
// targeted node has stamped SuspendIoAcked=true. Phase 2 advancement
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
// targeted node has SuspendIoAcked=false (terminal-success drain).
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
// Failed=true. Triggers the abort cascade — clearing SuspendIo on
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
// the transactional batch — clearing Spec.SuspendIo and
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
) (ctrl.Result, error) {
	for i := range siblings {
		_, err := r.maybeFlipSpec(ctx, &siblings[i], false, false)
		if err != nil {
			return ctrl.Result{}, errors.Wrapf(err,
				"abort cascade: clear SuspendIo on sibling %q", siblings[i].Name)
		}
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *SnapshotReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&blockstoriov1alpha1.Snapshot{}).
		Named("snapshot").
		Complete(r)
}
