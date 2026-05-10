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

	"github.com/cockroachdb/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

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

	// Phase 10.1 follow-up: instantiate via
	// `satellite.NewProviderFromKind(pool.Spec.ProviderKind,
	// pool.Spec.Props, exec)` and call
	// `Config.Apply.RegisterProvider(poolName, provider)`. The
	// per-pool capacity reporting (Status writes) flips to a
	// periodic loop here too — Phase 10.5's stub gets replaced
	// with the c-r-driven real path.
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
