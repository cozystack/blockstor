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

// SnapshotReconciler watches Snapshot CRDs and acts on those
// whose `Spec.Nodes` list includes this satellite's name. The
// snapshot semantic is per-node — one Snapshot CRD ends up
// triggering CreateSnapshot on each node mentioned in its
// Nodes list. Phase 10.1 replaces the gRPC `CreateSnapshot` /
// `DeleteSnapshot` consumers.
//
// As with the Resource reconciler, this initial commit is a
// skeleton: fetches the Snapshot, logs, exits. Follow-ups
// delegate to `Config.Apply.CreateSnapshot` /
// `Config.Apply.DeleteSnapshot`.
type SnapshotReconciler struct {
	client.Client

	Config Config
}

// Reconcile handles the satellite-side snapshot lifecycle:
// create on first appearance, delete on `DeletionTimestamp`.
// Idempotent — re-runs on a snapshot that already exists are
// safe.
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

	if !resp.GetOk() {
		// Body-level failure (per-snapshot, not transport). Log
		// and let the next reconcile retry — controller-runtime's
		// rate limiter handles back-off.
		log.FromContext(ctx).Info("CreateSnapshot per-snapshot failure",
			"snapshot", snap.Spec.SnapshotName, "message", resp.GetMessage())
	}

	return ctrl.Result{}, nil
}

// handleDelete mirrors handleCreate for the DeletionTimestamp
// case. Idempotent — DeleteSnapshot on a missing snapshot is
// a no-op so a re-run after a satellite restart is safe.
func (r *SnapshotReconciler) handleDelete(ctx context.Context, snap *blockstoriov1alpha1.Snapshot) (ctrl.Result, error) {
	req := &intent.DeleteSnapshotRequest{
		ResourceName: snap.Spec.ResourceDefinitionName,
		SnapshotName: snap.Spec.SnapshotName,
	}

	resp, err := r.Config.Apply.DeleteSnapshot(ctx, req)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "DeleteSnapshot")
	}

	if !resp.GetOk() {
		log.FromContext(ctx).Info("DeleteSnapshot per-snapshot failure",
			"snapshot", snap.Spec.SnapshotName, "message", resp.GetMessage())
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
