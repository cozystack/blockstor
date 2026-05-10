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
	"sigs.k8s.io/controller-runtime/pkg/log"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/satellite"
)

// StoragePoolFinalizer guards a StoragePool CRD while it is
// registered on this satellite. Without it `kubectl delete
// storagepool X` would race the satellite: the apiserver would
// remove the CRD before the satellite ran the on-disk teardown
// (`vgremove --force` / `zpool destroy`), leaving orphaned
// VGs/zpools the next discovery pass wouldn't re-publish.
// Phase 10.8.
const StoragePoolFinalizer = "blockstor.io.blockstor.io/satellite-storagepool"

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

		return ctrl.Result{Requeue: true}, nil
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

	return ctrl.Result{}, nil
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

// handlePoolDelete runs the satellite-side teardown when a
// StoragePool gets a DeletionTimestamp. Always deregisters the
// in-memory provider; runs the on-disk destroy chain when
// `Spec.DestroyOnDelete=true`. Phase 10.8.
//
// DestroyOnDelete=false (the default): the LINSTOR-side
// registration is removed but the underlying VG/zpool stays
// intact on disk; operators who want to re-import the data
// later can `vgchange -ay` / `zpool import` manually.
//
// DestroyOnDelete=true: `vgremove --force` (LVM) or `zpool
// destroy -f` (ZFS) — provider's Destroy is idempotent so a
// re-run after a partial teardown finishes cleanly.
func (r *StoragePoolReconciler) handlePoolDelete(ctx context.Context, pool *blockstoriov1alpha1.StoragePool) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("storagepool", pool.Name)

	if !slices.Contains(pool.Finalizers, StoragePoolFinalizer) {
		return ctrl.Result{}, nil
	}

	if pool.Spec.DestroyOnDelete {
		provider, err := satellite.NewProviderFromKind(pool.Spec.ProviderKind, pool.Spec.Props, r.Config.Exec)
		if err != nil {
			// Can't even build a provider — log and keep the
			// finalizer so operator can fix the config and
			// retry. Stripping here would orphan the on-disk
			// VG/zpool.
			logger.Info("NewProviderFromKind failed during teardown", "err", err)

			return ctrl.Result{Requeue: true}, nil
		}

		err = provider.Destroy(ctx)
		if err != nil {
			logger.Info("Destroy failed", "err", err)

			return ctrl.Result{Requeue: true}, nil
		}
	}

	// Deregister the in-memory provider so future ApplyResources
	// for this pool fail fast rather than racing the now-gone VG.
	r.Config.Apply.RegisterProvider(pool.Spec.PoolName, nil)

	pool.Finalizers = slices.DeleteFunc(pool.Finalizers,
		func(f string) bool { return f == StoragePoolFinalizer })

	err := r.Update(ctx, pool)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "strip storagepool finalizer")
	}

	return ctrl.Result{}, nil
}
