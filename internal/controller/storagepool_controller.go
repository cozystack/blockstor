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

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// StoragePoolReconciler is intentionally minimal. StoragePool CRDs in
// blockstor describe a per-node storage pool; the satellite is the
// authority on capacity, traits, and reachability. On every Apply
// reconcile the satellite re-publishes the pool's status (free /
// total / supportsSnapshots) into the CRD via gRPC + the controller's
// store-side Status writer. Nothing for a controller-runtime
// reconciler to do here — driving capacity from the K8s side would
// just race the satellite's authoritative view.
//
// We keep the reconciler registered (rather than dropping it) so the
// controller-runtime manager owns the watch + cache the REST handlers
// also share — that's how the scheme stays consistent when other
// controllers (RG, RD) need to look up a StoragePool indirectly. The
// Reconcile method is a no-op by design.
type StoragePoolReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=blockstor.io.blockstor.io,resources=storagepools,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=blockstor.io.blockstor.io,resources=storagepools/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=blockstor.io.blockstor.io,resources=storagepools/finalizers,verbs=update

// Reconcile is a deliberate no-op. See StoragePoolReconciler doc for
// the rationale (satellite is authoritative; controller-runtime is
// here for the watch + cache, not for state reconciliation).
func (*StoragePoolReconciler) Reconcile(ctx context.Context, _ ctrl.Request) (ctrl.Result, error) {
	_ = logf.FromContext(ctx)

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *StoragePoolReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&blockstoriov1alpha1.StoragePool{}).
		Named("storagepool").
		Complete(r)
}
