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

// handleCreate translates a Snapshot CRD into a CreateSnapshot
// gRPC-shaped request and dispatches it to the existing
// `Config.Apply.CreateSnapshot` body. Idempotent — re-running
// on an existing snapshot is a no-op (the provider-side
// `lvcreate --snapshot` already short-circuits on existing LV).
//
// Failure routing — F18 (cli-parity for `linstor s l` State column):
//   - Apply.CreateSnapshot returned Terminal=true ⇒ stamp
//     Status.Flags=["FAILED"] on the Snapshot CRD and return without
//     requeueing. crdToWireSnapshot surfaces that as `flags: ["FAILED"]`
//     on /v1/view/snapshots, which the Python CLI maps to State="Failed".
//   - Apply.CreateSnapshot returned Terminal=false (transient) ⇒ log
//     and return with Requeue=true; controller-runtime's rate limiter
//     handles back-off. Status stays empty so the wire view stays
//     "Incomplete".
//   - Apply.CreateSnapshot returned Ok=true ⇒ no-op, the observer
//     reconciler stamps Status.NodeStatus on its own cadence.
func (r *SnapshotReconciler) handleCreate(ctx context.Context, snap *blockstoriov1alpha1.Snapshot) (ctrl.Result, error) {
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
		return ctrl.Result{}, nil
	}

	logger := log.FromContext(ctx)
	logger.Info("CreateSnapshot per-snapshot failure",
		"snapshot", snap.Spec.SnapshotName,
		"message", resp.GetMessage(),
		"terminal", resp.GetTerminal())

	if !resp.GetTerminal() {
		// Transient — back off and try again. Returning Requeue
		// (without an explicit backoff) lets controller-runtime's
		// rate limiter apply its exponential schedule.
		return ctrl.Result{Requeue: true}, nil
	}

	// Terminal failure — stamp Status.Flags=["FAILED"] so the
	// wire surface goes from "Incomplete" to "Failed" and stops
	// retrying. Idempotent: re-stamping on a Snapshot that
	// already carries FAILED is a no-op.
	if slices.Contains(snap.Status.Flags, blockstoriov1alpha1.SnapshotStatusFlagFailed) {
		return ctrl.Result{}, nil
	}

	snap.Status.Flags = append(snap.Status.Flags, blockstoriov1alpha1.SnapshotStatusFlagFailed)

	err = r.Status().Update(ctx, snap)
	if err != nil {
		// Could be a conflict against a concurrent NodeStatus
		// patch — requeue so the next pass retries the stamp.
		return ctrl.Result{}, errors.Wrap(err, "stamp Status.Flags=FAILED")
	}

	return ctrl.Result{}, nil
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
