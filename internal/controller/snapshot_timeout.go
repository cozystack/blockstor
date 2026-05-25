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

	"github.com/cockroachdb/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// snapshotSuspendDeadline bounds how long a Snapshot may stay frozen
// in the suspend/take phase before the controller force-aborts and
// resumes I/O. Mirrors upstream LINSTOR, which puts a hard 2-minute
// timeout on the snapshot take phase
// (CtrlSnapshotCrtApiCallHandler's take step) and, on expiry, aborts
// the snapshot and resumes I/O on every suspended replica.
//
// Without this bound a hung take (satellite stuck, zfs/lvm hang, a
// node silently unreachable without ever stamping a per-node
// Failed=true) would leave the controller waiting for all-Ready
// forever while the volume's DRBD I/O stayed suspended — a permanent
// application outage. The deadline is the fail-safe: when it expires
// the controller always drives toward resume, never toward staying
// frozen.
const snapshotSuspendDeadline = 2 * time.Minute

// snapshotSuspendRequeueCap caps the RequeueAfter the controller
// returns while a Snapshot is still frozen in suspend. The controller
// is event-driven, but a silently-hung take produces no further
// events (no satellite Status write ever lands), so without an
// explicit requeue the deadline above would never be re-evaluated and
// the timeout abort would never fire. We requeue at
// min(remaining-to-deadline, cap) so the controller wakes up at (or
// just after) the deadline even with zero other activity, while still
// re-checking periodically before then in case the deadline anchor
// (metadata.CreationTimestamp) is far in the future.
const snapshotSuspendRequeueCap = 15 * time.Second

// inSuspendPhase reports whether the Snapshot is currently frozen in
// the suspend/take window — Spec.SuspendIo=true and the orchestration
// has not yet drained back to all-Ready (or already-cleared). This is
// the window where a hung take wedges the volume, so it is the only
// window where the deadline / RequeueAfter machinery applies.
func inSuspendPhase(snap *blockstoriov1alpha1.Snapshot, siblings []blockstoriov1alpha1.Snapshot) bool {
	if !snap.Spec.SuspendIo {
		return false
	}

	// Once every sibling's every targeted node is Ready the success
	// drain is imminent (Phase 3 clears SuspendIo on the next pass);
	// there is no hang to guard against, so don't arm the deadline.
	return !allSiblingsReady(siblings)
}

// suspendDeadlineExceeded reports whether the Snapshot has been frozen
// in suspend longer than snapshotSuspendDeadline. The deadline anchor
// is metadata.CreationTimestamp: the store stamps Spec.SuspendIo=true
// at create time (pkg/store/k8s/snapshots.go::wireToCRDSnapshot), so
// Phase 1 begins effectively at create — CreationTimestamp is the
// earliest moment any satellite could have been told to freeze, which
// makes it a conservative (never-too-early) anchor for the abort.
//
// A zero CreationTimestamp (only reachable in a hand-built CRD that
// never went through the apiserver) is treated as "not yet expired"
// so a malformed object can't trip a spurious abort.
func suspendDeadlineExceeded(snap *blockstoriov1alpha1.Snapshot, now time.Time) bool {
	created := snap.CreationTimestamp.Time
	if created.IsZero() {
		return false
	}

	return now.Sub(created) > snapshotSuspendDeadline
}

// suspendRequeueAfter returns the RequeueAfter the controller should
// set while the Snapshot is frozen in suspend so the deadline is
// actually reached even with no other events. It is
// min(remaining-to-deadline, snapshotSuspendRequeueCap), clamped to a
// small positive floor so a just-expired deadline still re-enqueues
// promptly (the abort itself happens on the next pass).
func suspendRequeueAfter(snap *blockstoriov1alpha1.Snapshot, now time.Time) time.Duration {
	created := snap.CreationTimestamp.Time
	if created.IsZero() {
		// No usable anchor — fall back to the periodic cap so we at
		// least keep re-checking (e.g. for an UpToDate transition).
		return snapshotSuspendRequeueCap
	}

	remaining := created.Add(snapshotSuspendDeadline).Sub(now)
	if remaining <= 0 {
		// Deadline already passed; requeue almost immediately so the
		// abort fires on the next pass.
		return time.Second
	}

	if remaining < snapshotSuspendRequeueCap {
		return remaining
	}

	return snapshotSuspendRequeueCap
}

// abortGroupWithReason force-aborts the whole transactional batch the
// same way the per-node-Failed abort path does (clear Spec.SuspendIo /
// Spec.TakeSnapshot on every sibling so suspended peers resume), and
// additionally stamps the controller-side terminal markers on every
// sibling's Status so `linstor s l` shows WHY the snapshot aborted.
//
// This is the controller-driven half of the failure-reason contract:
// a timeout / non-UpToDate abort is one the satellites can't stamp
// themselves (a hung or unreachable satellite never writes a per-node
// Failed=true), so the controller records the FAILED_DISCONNECT flag +
// a Condition itself. Flag/Condition stamping is best-effort relative
// to the resume — the resume (clearing SuspendIo) is the safety-
// critical action and runs first via abortGroup.
func (r *SnapshotReconciler) abortGroupWithReason(
	ctx context.Context, siblings []blockstoriov1alpha1.Snapshot, reason string,
) (ctrl.Result, error) {
	// Resume first: clearing Spec.SuspendIo on every sibling is the
	// outage fix and must happen even if the Status stamps below fail.
	err := r.abortGroup(ctx, siblings)
	if err != nil {
		return ctrl.Result{}, err
	}

	for i := range siblings {
		stampErr := r.stampSnapshotFailedDisconnect(ctx, &siblings[i], reason)
		if stampErr != nil {
			return ctrl.Result{}, errors.Wrapf(stampErr,
				"stamp FAILED_DISCONNECT on sibling %q", siblings[i].Name)
		}
	}

	return ctrl.Result{}, nil
}

// stampSnapshotFailedDisconnect records the controller-side abort
// reason on the Snapshot's Status: the FAILED_DISCONNECT terminal flag
// (so the wire view / `linstor s l` renders "Satellite disconnected")
// plus a SnapshotComplete=False Condition carrying the human-readable
// reason. Idempotent — a re-stamp on a Snapshot that already carries
// the flag with the same condition message is a no-op. Optimistic-lock
// retried against a fresh fetch so it converges against concurrent
// satellite Status writes.
func (r *SnapshotReconciler) stampSnapshotFailedDisconnect(
	ctx context.Context, snap *blockstoriov1alpha1.Snapshot, reason string,
) error {
	key := client.ObjectKeyFromObject(snap)

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var current blockstoriov1alpha1.Snapshot

		getErr := r.Get(ctx, key, &current)
		if getErr != nil {
			if apierrors.IsNotFound(getErr) {
				return nil
			}

			return errors.Wrap(getErr, "get Snapshot for FAILED_DISCONNECT stamp")
		}

		mutated := false

		if !slices.Contains(current.Status.Flags, blockstoriov1alpha1.SnapshotStatusFlagFailedDisconnect) {
			current.Status.Flags = append(current.Status.Flags,
				blockstoriov1alpha1.SnapshotStatusFlagFailedDisconnect)
			mutated = true
		}

		if upsertSnapshotAbortCondition(&current, reason) {
			mutated = true
		}

		if !mutated {
			return nil
		}

		return r.Status().Update(ctx, &current)
	})
	if err != nil {
		return errors.Wrap(err, "stamp Snapshot Status FAILED_DISCONNECT")
	}

	return nil
}

// upsertSnapshotAbortCondition sets (or refreshes) the
// SnapshotComplete=False Condition carrying the abort reason. Returns
// true iff it changed the slice (so the caller can skip a no-op
// Status().Update). Uses meta.SetStatusCondition's idempotence:
// re-stamping the same Status+Reason+Message leaves LastTransitionTime
// untouched, so a steady-state re-reconcile doesn't churn the object.
func upsertSnapshotAbortCondition(snap *blockstoriov1alpha1.Snapshot, reason string) bool {
	cond := metav1.Condition{
		Type:    blockstoriov1alpha1.SnapshotStatusConditionType,
		Status:  metav1.ConditionFalse,
		Reason:  "Aborted",
		Message: reason,
	}

	return setSnapshotCondition(&snap.Status.Conditions, &cond)
}

// setSnapshotCondition is a minimal SetStatusCondition: upserts cond
// by Type, preserving LastTransitionTime when nothing observable
// changed. Returns true iff the slice was mutated. Implemented inline
// (rather than apimachinery's meta.SetStatusCondition) only to keep
// the controller's import surface small — semantics match.
func setSnapshotCondition(conditions *[]metav1.Condition, cond *metav1.Condition) bool {
	now := metav1.Now()

	for i := range *conditions {
		if (*conditions)[i].Type != cond.Type {
			continue
		}

		existing := &(*conditions)[i]
		if existing.Status == cond.Status &&
			existing.Reason == cond.Reason &&
			existing.Message == cond.Message {
			return false
		}

		if existing.Status != cond.Status {
			existing.LastTransitionTime = now
		}

		existing.Status = cond.Status
		existing.Reason = cond.Reason
		existing.Message = cond.Message

		return true
	}

	cond.LastTransitionTime = now
	*conditions = append(*conditions, *cond)

	return true
}

// allTargetedReplicasUpToDate verifies every targeted diskful replica
// of every sibling reports DiskState=UpToDate before the controller
// promotes Phase 1 → Phase 2 (TakeSnapshot=true). Snapshotting a
// non-UpToDate device (Inconsistent / SyncTarget / Outdated) captures
// torn bytes; upstream LINSTOR refuses with "Cannot take snapshot from
// non-UpToDate DRBD device". The Phase-2 gate otherwise only checks
// SuspendIoAcked, which says "the kernel froze I/O" but not "the
// frozen bytes are good".
//
// Returns (ok, offendingNode). ok=true means every targeted diskful
// replica is UpToDate (or — defensively — has not reported a disk
// state yet, in which case we don't block: the SuspendIoAcked gate
// already proves the satellite is live, and a missing observed state
// shouldn't wedge the snapshot). Diskless / tie-breaker replicas hold
// no data and are excluded from the denominator.
func (r *SnapshotReconciler) allTargetedReplicasUpToDate(
	ctx context.Context, siblings []blockstoriov1alpha1.Snapshot,
) (bool, string, error) {
	for i := range siblings {
		ok, node, err := r.siblingReplicasUpToDate(ctx, &siblings[i])
		if err != nil {
			return false, "", err
		}

		if !ok {
			return false, node, nil
		}
	}

	return true, "", nil
}

// siblingReplicasUpToDate is the per-sibling half of
// allTargetedReplicasUpToDate. Lists every Resource of the sibling's
// parent RD and, for each targeted diskful replica, requires that no
// observed volume disk-state is non-UpToDate.
func (r *SnapshotReconciler) siblingReplicasUpToDate(
	ctx context.Context, snap *blockstoriov1alpha1.Snapshot,
) (bool, string, error) {
	var resList blockstoriov1alpha1.ResourceList

	err := r.List(ctx, &resList)
	if err != nil {
		return false, "", errors.Wrap(err, "list Resources for UpToDate gate")
	}

	targets := make(map[string]struct{}, len(snap.Spec.Nodes))
	for _, n := range snap.Spec.Nodes {
		targets[n] = struct{}{}
	}

	for i := range resList.Items {
		res := &resList.Items[i]

		if res.Spec.ResourceDefinitionName != snap.Spec.ResourceDefinitionName {
			continue
		}

		if _, targeted := targets[res.Spec.NodeName]; !targeted {
			continue
		}

		// Diskless / tie-breaker replicas hold no data — they never
		// take the snapshot and have no meaningful UpToDate state.
		if slices.Contains(res.Spec.Flags, apiv1.ResourceFlagDiskless) ||
			slices.Contains(res.Spec.Flags, apiv1.ResourceFlagTieBreaker) {
			continue
		}

		if node, ok := replicaNotUpToDate(res); !ok {
			return false, node, nil
		}
	}

	return true, "", nil
}

// replicaNotUpToDate reports whether a diskful replica has any
// observed volume that is NOT UpToDate. Returns (nodeName, false) for
// the first such volume; (",", true) when every observed volume is
// UpToDate. A replica with no observed volume state yet is treated as
// UpToDate (not blocking) — see allTargetedReplicasUpToDate's
// rationale.
func replicaNotUpToDate(res *blockstoriov1alpha1.Resource) (string, bool) {
	for i := range res.Status.Volumes {
		state := res.Status.Volumes[i].DiskState
		if state == "" {
			// Not observed yet — don't block on a missing reading.
			continue
		}

		if state != drbdDiskStateUpToDate {
			return res.Spec.NodeName, false
		}
	}

	return "", true
}

// drbdDiskStateUpToDate is the DRBD-9 disk_state token that means the
// local replica holds a complete, consistent copy of the data — the
// only state safe to snapshot. Matches the literal the rest of the
// controller already compares against (resource_migration_controller).
const drbdDiskStateUpToDate = "UpToDate"
