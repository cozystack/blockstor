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

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// MigratingFromProp is the per-Resource Spec.Props key the REST
// migrate-disk handler stamps on the destination replica when
// starting a strict add-before-drop migration. The value is the
// node name of the source replica whose Resource CRD must be
// deleted once the destination's Volumes reach UpToDate.
//
// Mirrors `pkg/rest.MigratingFromProp`; duplicated to keep the
// controller package free of a back-edge import to pkg/rest.
const MigratingFromProp = "BlockstorMigratingFrom"

// migrationRequeueInterval is the requeue cadence the migration
// reconciler uses while waiting for the destination volumes to
// reach UpToDate. Short enough that the operator-visible tail
// (between dst-UpToDate and src-delete) stays under a few seconds;
// long enough to avoid burning workqueue cycles on a still-syncing
// volume. The reconciler also re-fires via watches on Resource
// Status updates (the satellite-observer writes DiskState through
// SSA), so this is just a safety net for missed events.
const migrationRequeueInterval = 5 * time.Second

// ResourceMigrationReconciler implements the strict add-before-drop
// half of `linstor r td --migrate-from <src>` (UG9 §"Migrating a
// resource to another node"). The REST handler stamps
// `Spec.Props["BlockstorMigratingFrom"]=<src-node>` on the
// destination Resource at request time and returns 200 immediately;
// this reconciler:
//
//  1. Watches Resource CRDs that carry the prop.
//  2. Polls the destination's Status.Volumes; once every volume's
//     DiskState is UpToDate, the new copy is durable on the wire
//     and the redundancy invariant has been raised by one.
//  3. Deletes the source Resource (named `<rd>.<src-node>`) and
//     clears the prop on the destination in the same reconcile pass.
//
// While the prop is set, the source replica lives — that's what
// makes the migration strict. A satellite restart, a kube-apiserver
// blip, or any intermediate observed state cannot collapse the
// diskful count below the original because src isn't dropped until
// dst is observed UpToDate.
type ResourceMigrationReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=blockstor.io.blockstor.io,resources=resources,verbs=get;list;watch;update;patch;delete
// +kubebuilder:rbac:groups=blockstor.io.blockstor.io,resources=resources/status,verbs=get

// Reconcile drives one pass over the destination Resource:
// no-op if the migrating-from prop is absent (or stale),
// requeue while dst Volumes are not yet UpToDate, prune src
// and clear the prop once UpToDate is observed.
func (r *ResourceMigrationReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithName("resource-migration")

	var dst blockstoriov1alpha1.Resource

	err := r.Get(ctx, req.NamespacedName, &dst)
	if err != nil {
		if errors.IsNotFound(err) {
			// dst CRD was deleted while we were waiting — operator
			// likely cancelled. Nothing for us to do; src is
			// untouched (Option B guarantee).
			return ctrl.Result{}, nil
		}

		return ctrl.Result{}, err
	}

	srcNode := dst.Spec.Props[MigratingFromProp]
	if srcNode == "" {
		// Not a migration target (or migration already finished).
		return ctrl.Result{}, nil
	}

	if !dst.DeletionTimestamp.IsZero() {
		// dst is going away — src stays diskful, redundancy
		// preserved. The dst deletion clears the trigger
		// implicitly; no work for us.
		return ctrl.Result{}, nil
	}

	if !allVolumesUpToDate(&dst) {
		// dst still syncing. Keep src alive and check back later.
		// SetupWithManager also wires a watch on Resource Status
		// transitions, so this requeue is just a safety net.
		logger.V(1).Info("destination not yet UpToDate; deferring src prune",
			"resource", dst.Name, "src", srcNode)

		return ctrl.Result{RequeueAfter: migrationRequeueInterval}, nil
	}

	srcName := resourceCRDName(dst.Spec.ResourceDefinitionName, srcNode)

	err = r.pruneSrc(ctx, srcName)
	if err != nil {
		return ctrl.Result{}, err
	}

	err = r.clearMigratingFrom(ctx, &dst)
	if err != nil {
		return ctrl.Result{}, err
	}

	logger.Info("migration complete: src pruned, dst UpToDate",
		"resource", dst.Name, "src", srcName)

	return ctrl.Result{}, nil
}

// pruneSrc deletes the source Resource CRD. A NotFound is silently
// swallowed — the operator may have removed it manually, or a
// previous reconcile pass completed the delete but failed to clear
// the prop afterwards. Either way, the reconciler is monotonic.
func (r *ResourceMigrationReconciler) pruneSrc(ctx context.Context, srcName string) error {
	var src blockstoriov1alpha1.Resource

	err := r.Get(ctx, client.ObjectKey{Name: srcName}, &src)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil
		}

		return err
	}

	err = r.Delete(ctx, &src)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	return nil
}

// clearMigratingFrom strips the BlockstorMigratingFrom key from
// the destination's Spec.Props after a successful src prune.
// Leaving the prop set would re-fire the reconciler on every
// Resource event and (harmlessly) re-confirm the same conclusion.
func (r *ResourceMigrationReconciler) clearMigratingFrom(ctx context.Context, dst *blockstoriov1alpha1.Resource) error {
	if dst.Spec.Props == nil {
		return nil
	}

	if _, ok := dst.Spec.Props[MigratingFromProp]; !ok {
		return nil
	}

	delete(dst.Spec.Props, MigratingFromProp)

	if len(dst.Spec.Props) == 0 {
		dst.Spec.Props = nil
	}

	return r.Update(ctx, dst)
}

// allVolumesUpToDate reports whether every volume in the
// destination's Status.Volumes reports DiskState=UpToDate. An
// empty Volumes slice returns false — the satellite-observer
// hasn't written anything yet, so we have no evidence the new
// copy is durable.
func allVolumesUpToDate(dst *blockstoriov1alpha1.Resource) bool {
	if len(dst.Status.Volumes) == 0 {
		return false
	}

	for i := range dst.Status.Volumes {
		if dst.Status.Volumes[i].DiskState != "UpToDate" {
			return false
		}
	}

	return true
}

// SetupWithManager wires Reconcile against Resource events. The
// satellite-observer writes Status.Volumes[].DiskState via SSA on
// every DRBD events2 frame, which the controller-runtime watch
// fires this reconciler on — so the operator-visible tail between
// "dst UpToDate observed" and "src pruned" is bounded by one
// reconcile pass (~ms) plus apiserver propagation.
func (r *ResourceMigrationReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&blockstoriov1alpha1.Resource{}).
		Named("resource-migration").
		Complete(r)
}
