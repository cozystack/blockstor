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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// ResourceStateProjectionFieldOwner is the SSA field-manager identity
// the controller uses when projecting `Status.DrbdState=Unknown` over
// a Resource whose owning satellite is OFFLINE. Distinct from the
// satellite's own `blockstor-satellite` owner so SSA can track the
// two claims separately: while the satellite is offline the
// projection owns the field; on heal, the controller releases its
// claim (apply with no DrbdState set) so the satellite's next
// observer write can re-claim it. Distinct from
// `blockstor-controller` too — the latter owns allocator fields
// (DRBDPort/Minor/NodeID) and cannot be re-used here without
// dropping those claims when we release DrbdState.
const ResourceStateProjectionFieldOwner = "blockstor-controller-resource-projection"

// ResourceStateProjectionReconciler implements scenario 5.5's
// resource-level Unknown projection (state-offline-unknown e2e).
//
// Contract:
//
//   - When a Resource's owning Node has `Status.ConnectionStatus=
//     OFFLINE` (the heartbeat watchdog flips this past
//     NodeMonitorGracePeriod), project `Status.DrbdState=Unknown` on
//     the Resource. The satellite's last `DrbdState=UpToDate` write
//     was made just before the pod died; without this projection the
//     stale value lingers and operators can't tell whether the
//     replica is still alive.
//
//   - The per-volume `Status.Volumes[i].DiskState` is intentionally
//     LEFT UNTOUCHED. That field carries the "last we heard from
//     the satellite" forensic snapshot ("UpToDate / Inconsistent /
//     Failed at the time the satellite went silent"), which
//     operators need for triage. The scenario brief
//     (tests/e2e/state-offline-unknown.sh) requires both shapes at
//     once: Unknown at the resource level + last-known DiskState
//     at the volume level.
//
//   - When the Node returns ONLINE, the controller releases its
//     claim on DrbdState by re-applying with an EMPTY Status — SSA
//     drops fields no longer present in the manifest, freeing the
//     satellite's next observer write to re-claim DrbdState with
//     fresh evidence.
//
// Wire shape: the REST `linstor r l` view reads `state.drbd_state`
// directly from CRD Status; the Python CLI's `r l` State column
// reads `volumes[].state.disk_state`. With this projection in place
// the two columns diverge by design while a satellite is OFFLINE:
// `state.drbd_state` shows Unknown (we can't prove anything), the
// per-volume column shows the last observed disk state.
type ResourceStateProjectionReconciler struct {
	client.Client
}

// +kubebuilder:rbac:groups=blockstor.io.blockstor.io,resources=resources,verbs=get;list;watch
// +kubebuilder:rbac:groups=blockstor.io.blockstor.io,resources=resources/status,verbs=get;patch;update
// +kubebuilder:rbac:groups=blockstor.io.blockstor.io,resources=nodes,verbs=get;list;watch

// Reconcile reads the Resource's owning Node, decides whether to
// project Unknown or release the claim, and applies via SSA. No-op
// when the Resource is being deleted (DeletionTimestamp non-zero)
// or when the owning Node row is absent (treat the resource as
// orphaned; the satellite-resource finalizer will eventually
// clean it).
func (r *ResourceStateProjectionReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var res blockstoriov1alpha1.Resource

	err := r.Get(ctx, req.NamespacedName, &res)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}

		return ctrl.Result{}, errors.Wrap(err, "get Resource")
	}

	if !res.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	if res.Spec.NodeName == "" {
		return ctrl.Result{}, nil
	}

	var node blockstoriov1alpha1.Node

	err = r.Get(ctx, types.NamespacedName{Name: res.Spec.NodeName}, &node)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}

		return ctrl.Result{}, errors.Wrap(err, "get Node")
	}

	offline := node.Status.ConnectionStatus == blockstoriov1alpha1.NodeConnectionStatusOffline

	if offline {
		return ctrl.Result{}, r.projectUnknown(ctx, &res)
	}

	return ctrl.Result{}, r.releaseClaim(ctx, &res)
}

// projectUnknown SSA-applies `Status.DrbdState=Unknown` under the
// projection FieldOwner, taking ownership of the field via
// ForceOwnership (the satellite previously owned it with its last
// `DrbdState=UpToDate` write — without ForceOwnership the apply
// would conflict). Per-volume Status.Volumes is intentionally
// omitted from the apply so the satellite's last-known DiskState
// snapshot is preserved.
//
// Idempotent: SSA recognises a no-op apply (same field value, same
// owner) and skips the write.
func (r *ResourceStateProjectionReconciler) projectUnknown(ctx context.Context, res *blockstoriov1alpha1.Resource) error {
	if res.Status.DrbdState == drbdStateUnknown {
		return nil
	}

	apply := &blockstoriov1alpha1.Resource{
		TypeMeta: metav1.TypeMeta{
			Kind:       resourceProjectionResourceKind,
			APIVersion: blockstoriov1alpha1.GroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{Name: res.Name},
		Status: blockstoriov1alpha1.ResourceStatus{
			DrbdState: drbdStateUnknown,
		},
	}

	err := r.Status().Patch(ctx, apply,
		client.Apply, //nolint:staticcheck // SA1019: applyconfiguration-gen output not yet available
		client.FieldOwner(ResourceStateProjectionFieldOwner),
		client.ForceOwnership)
	if err != nil {
		return errors.Wrapf(err, "project Unknown on %s", res.Name)
	}

	return nil
}

// releaseClaim drops the projection's ownership of DrbdState by
// applying an empty Status under the same FieldOwner. SSA removes
// the manager's previous field claims that aren't repeated in the
// new manifest, freeing the satellite's next observer write to
// re-claim DrbdState with fresh evidence.
//
// Idempotent: if the projection never claimed any field (resource
// never went OFFLINE during its lifetime), the SSA apply is a no-op
// at the apiserver level.
func (r *ResourceStateProjectionReconciler) releaseClaim(ctx context.Context, res *blockstoriov1alpha1.Resource) error {
	// Cheap pre-check: if DrbdState isn't Unknown then either the
	// satellite already overwrote it (we have no claim to release)
	// or we never claimed it. Either way the release-apply is a
	// no-op; skip the apiserver round-trip.
	if res.Status.DrbdState != drbdStateUnknown {
		return nil
	}

	apply := &blockstoriov1alpha1.Resource{
		TypeMeta: metav1.TypeMeta{
			Kind:       resourceProjectionResourceKind,
			APIVersion: blockstoriov1alpha1.GroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{Name: res.Name},
		Status:     blockstoriov1alpha1.ResourceStatus{},
	}

	err := r.Status().Patch(ctx, apply,
		client.Apply, //nolint:staticcheck // SA1019: applyconfiguration-gen output not yet available
		client.FieldOwner(ResourceStateProjectionFieldOwner))
	if err != nil {
		return errors.Wrapf(err, "release projection claim on %s", res.Name)
	}

	return nil
}

// drbdStateUnknown is the resource-level state surfaced when the
// satellite is OFFLINE — operators read this on `linstor r l`'s
// State column when the per-volume disk_state is unavailable. The
// canonical spelling matches upstream LINSTOR's `DrbdConnState
// UNKNOWN`; the REST view echoes Status.DrbdState verbatim so the
// wire payload's `state.drbd_state` field carries this exact
// string.
const drbdStateUnknown = "Unknown"

// resourceProjectionResourceKind mirrors the Resource CRD's Kind
// for the SSA TypeMeta header. Kept distinct from the satellite-
// side `resourceKind` constant (in `pkg/satellite/controllers`) to
// avoid cross-package coupling — both intentionally name the same
// string `"Resource"`.
const resourceProjectionResourceKind = "Resource"

// SetupWithManager wires the reconciler to fan out on:
//
//  1. Resource events (For) — every spec/status change re-evaluates
//     the projection.
//  2. Node events (Watches) — when ConnectionStatus flips, every
//     Resource on that Node must re-reconcile so the projection
//     follows the heartbeat verdict.
func (r *ResourceStateProjectionReconciler) SetupWithManager(mgr ctrl.Manager) error {
	err := ctrl.NewControllerManagedBy(mgr).
		Named("resource-state-projection").
		For(&blockstoriov1alpha1.Resource{}).
		Watches(&blockstoriov1alpha1.Node{},
			handler.EnqueueRequestsFromMapFunc(r.enqueueResourcesForNode),
			builder.WithPredicates(nodeConnectionStatusChanged())).
		Complete(r)
	if err != nil {
		return errors.Wrap(err, "register ResourceStateProjectionReconciler")
	}

	return nil
}

// enqueueResourcesForNode maps a Node event to every Resource whose
// `Spec.NodeName` references that Node. Drives the projection to
// re-evaluate on every ConnectionStatus flip.
func (r *ResourceStateProjectionReconciler) enqueueResourcesForNode(ctx context.Context, obj client.Object) []reconcile.Request {
	node, ok := obj.(*blockstoriov1alpha1.Node)
	if !ok {
		return nil
	}

	var resList blockstoriov1alpha1.ResourceList

	err := r.List(ctx, &resList)
	if err != nil {
		return nil
	}

	out := make([]reconcile.Request, 0, len(resList.Items))

	for i := range resList.Items {
		if resList.Items[i].Spec.NodeName != node.Name {
			continue
		}

		out = append(out, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: resList.Items[i].Name},
		})
	}

	return out
}

// nodeConnectionStatusChanged returns a predicate that fires only
// when a Node's `Status.ConnectionStatus` actually transitions. The
// heartbeat watchdog re-applies the same Status every
// NodeMonitorPeriod (5s) to refresh LastTransitionTime; without
// this filter every refresh would fan out to every Resource on the
// node, burning workqueue cycles for no behaviour change.
func nodeConnectionStatusChanged() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			n, ok := e.Object.(*blockstoriov1alpha1.Node)
			if !ok {
				return false
			}

			// Brand-new Node may already carry ConnectionStatus
			// (controller restart re-reads existing state). Treat as
			// a transition so the projection runs at least once.
			return n.Status.ConnectionStatus != ""
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldNode, ok := e.ObjectOld.(*blockstoriov1alpha1.Node)
			if !ok {
				return false
			}

			newNode, ok := e.ObjectNew.(*blockstoriov1alpha1.Node)
			if !ok {
				return false
			}

			return oldNode.Status.ConnectionStatus != newNode.Status.ConnectionStatus
		},
		DeleteFunc: func(_ event.DeleteEvent) bool { return false },
	}
}
