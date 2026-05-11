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

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// SnapshotReconciler is a controller-side stub kept so the
// manager wires a watch on Snapshot CRDs even after Phase 10.6
// retired the gRPC dispatch path. The satellite c-r
// `SnapshotReconciler` (in `pkg/satellite/controllers`) picks
// each Snapshot up via its own watch and runs
// `CreateSnapshot` / `DeleteSnapshot` against the local
// provider — no controller-side fan-out is needed.
type SnapshotReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=blockstor.io.blockstor.io,resources=snapshots,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=blockstor.io.blockstor.io,resources=snapshots/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=blockstor.io.blockstor.io,resources=snapshots/finalizers,verbs=update
// +kubebuilder:rbac:groups=blockstor.io.blockstor.io,resources=resources,verbs=get;list;watch

// Reconcile is a no-op. The satellite's c-r SnapshotReconciler
// is the active driver; the controller keeps the watch alive
// so future controller-side status / housekeeping can land
// here without re-registering the resource.
func (*SnapshotReconciler) Reconcile(_ context.Context, _ ctrl.Request) (ctrl.Result, error) {
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *SnapshotReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&blockstoriov1alpha1.Snapshot{}).
		Named("snapshot").
		Complete(r)
}
