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
	"sigs.k8s.io/controller-runtime/pkg/log"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/satellite"
	"github.com/cozystack/blockstor/pkg/storage"
)

// StoragePoolFinalizer guards a StoragePool CRD while it is
// registered on this satellite. Strips on delete so the
// apiserver finalises only after the in-memory provider has
// been deregistered. Phase 10.8.
const StoragePoolFinalizer = "blockstor.io.blockstor.io/satellite-storagepool"

// capacityResyncInterval is the cadence the StoragePoolReconciler
// reschedules itself at to refresh `Status.FreeCapacity` /
// `TotalCapacity`. Long enough that the apiserver isn't peppered
// with Status updates for an unchanged pool, short enough that a
// freshly-allocated LV shows up in `/v1/view/storage-pools` within
// half a minute. Mirrors the cadence the retired gRPC
// `runCapacityLoop` ticked at.
const capacityResyncInterval = 30 * time.Second

// StoragePoolReconciler watches StoragePool CRDs filtered to
// those scoped to this satellite's node. Replaces the gRPC
// `ApplyStoragePools` consumer — on Spec.NodeName == self,
// instantiate the matching `storage.Provider` and register it
// on `Config.Apply` via `Reconciler.RegisterProvider`. Phase
// 10.1 + 10.5.
type StoragePoolReconciler struct {
	client.Client

	Config Config
}

// Reconcile materialises one StoragePool. Idempotent —
// re-registering an already-registered provider replaces the
// in-memory entry, which is the desired behaviour after a pool
// config edit.
func (r *StoragePoolReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("storagepool", req.Name)

	var pool blockstoriov1alpha1.StoragePool

	err := r.Get(ctx, req.NamespacedName, &pool)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}

		return ctrl.Result{}, errors.Wrap(err, "get StoragePool")
	}

	if pool.Spec.NodeName != r.Config.NodeName {
		return ctrl.Result{}, nil
	}

	logger.V(1).Info("observed StoragePool",
		"name", pool.Name,
		"kind", pool.Spec.ProviderKind,
		"deletionTimestamp", pool.DeletionTimestamp)

	if !pool.DeletionTimestamp.IsZero() {
		return r.handlePoolDelete(ctx, &pool)
	}

	if !slices.Contains(pool.Finalizers, StoragePoolFinalizer) {
		pool.Finalizers = append(pool.Finalizers, StoragePoolFinalizer)

		err := r.Update(ctx, &pool)
		if err != nil {
			return ctrl.Result{}, errors.Wrap(err, "add storagepool finalizer")
		}

		return ctrl.Result{RequeueAfter: time.Second}, nil
	}

	provider, err := satellite.NewProviderFromKind(pool.Spec.ProviderKind, pool.Spec.Props, r.Config.Exec)
	if err != nil {
		// Per-pool failure: log and let the next reconcile
		// retry (controller-runtime back-off handles the
		// retry cadence). The pool stays unavailable until
		// the operator fixes the config — same semantic the
		// gRPC `ApplyStoragePools` path had via Ok=false.
		logger.Info("NewProviderFromKind failed", "kind", pool.Spec.ProviderKind, "err", err)

		return ctrl.Result{}, nil
	}

	// nil provider = DISKLESS kind; RegisterProvider's nil path
	// deregisters, which is the right semantic.
	r.Config.Apply.RegisterProvider(pool.Spec.PoolName, provider)

	// Refresh Status.FreeCapacity / TotalCapacity from the
	// provider. Replaces the retired gRPC `runCapacityLoop`
	// push path — the satellite now writes capacity directly
	// via the apiserver. Best-effort: a transient PoolStatus
	// error logs + the next requeue retries.
	r.writeCapacity(ctx, &pool, provider)

	// Reschedule for capacity refresh. The c-r manager fires
	// Reconcile on any CRD event AND after the requeue
	// timeout, whichever lands first.
	return ctrl.Result{RequeueAfter: capacityResyncInterval}, nil
}

// SetupWithManager wires the reconciler with the same
// `nodeNamePredicate` the Resource reconciler uses — both
// filter on `Spec.NodeName`.
func (r *StoragePoolReconciler) SetupWithManager(mgr ctrl.Manager) error {
	err := ctrl.NewControllerManagedBy(mgr).
		For(&blockstoriov1alpha1.StoragePool{},
			builder.WithPredicates(nodeNamePredicate(r.Config.NodeName))).
		Named("satellite-storagepool").
		Complete(r)
	if err != nil {
		return errors.Wrap(err, "register StoragePoolReconciler")
	}

	return nil
}

// writeCapacity pushes the live PoolStatus from `provider` into
// the StoragePool CRD's Status subresource. Best-effort: a
// transient PoolStatus / apiserver error logs + the next
// Reconcile retries. A nil provider (DISKLESS) is a no-op since
// there's nothing to query.
func (r *StoragePoolReconciler) writeCapacity(ctx context.Context, pool *blockstoriov1alpha1.StoragePool, provider storage.Provider) {
	logger := log.FromContext(ctx).WithValues("storagepool", pool.Name)

	if provider == nil {
		return
	}

	status, err := provider.PoolStatus(ctx)
	if err != nil {
		logger.Info("PoolStatus failed", "err", err)

		return
	}

	if pool.Status.FreeCapacity == status.FreeCapacityKib &&
		pool.Status.TotalCapacity == status.TotalCapacityKib &&
		pool.Status.SupportsSnapshots == status.SupportsSnapshots {
		// No-op write: nothing changed since the last Reconcile.
		// Skipping the apiserver round-trip keeps the
		// every-30-seconds resync cheap.
		return
	}

	pool.Status.FreeCapacity = status.FreeCapacityKib
	pool.Status.TotalCapacity = status.TotalCapacityKib
	pool.Status.SupportsSnapshots = status.SupportsSnapshots

	err = r.Status().Update(ctx, pool)
	if err != nil {
		logger.Info("Status.Update for capacity", "err", err)
	}
}

// handlePoolDelete runs the satellite-side cleanup when a
// StoragePool gets a DeletionTimestamp. StoragePool lifecycle
// is pure registration: deleting the CRD ONLY deregisters the
// in-memory provider and never touches the underlying disk.
// On-disk pool creation is operator-driven via `linstor
// physical-storage create-device-pool`; on-disk teardown is
// an out-of-band operator concern — blockstor refuses to
// `vgremove`/`zpool destroy` to avoid surprising data loss.
func (r *StoragePoolReconciler) handlePoolDelete(ctx context.Context, pool *blockstoriov1alpha1.StoragePool) (ctrl.Result, error) {
	if !slices.Contains(pool.Finalizers, StoragePoolFinalizer) {
		return ctrl.Result{}, nil
	}

	// Deregister the in-memory provider so future ApplyResources
	// for this pool fail fast rather than writing into a pool
	// the operator has unregistered.
	r.Config.Apply.RegisterProvider(pool.Spec.PoolName, nil)

	pool.Finalizers = slices.DeleteFunc(pool.Finalizers,
		func(f string) bool { return f == StoragePoolFinalizer })

	err := r.Update(ctx, pool)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "strip storagepool finalizer")
	}

	return ctrl.Result{}, nil
}
