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
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/satellite"
)

// PhysicalDeviceReconciler runs the attach sequence on
// PhysicalDevice CRDs scoped to this satellite's node. It is
// the load-bearing piece of Phase 10.7 step 2 — translates a
// `Spec.AttachTo` set by `POST /v1/physical-storage/<node>` (or
// `kubectl edit`) into the kind-specific pool-create commands
// via `satellite.Attach`, registers the new provider on the
// shared `Config.Apply` reconciler, and finally deletes the
// CRD as the success signal.
//
// Idempotency is delegated to `satellite.Attach`: every
// provider-specific create command (`pvcreate`, `vgcreate`,
// `zpool create`) short-circuits when the device is already a
// PV / member of the pool, so a crash-restart between
// `Phase=Attaching` and the final Delete safely re-runs.
type PhysicalDeviceReconciler struct {
	client.Client

	Config Config
}

// Reconcile is the per-event driver. Returns:
//   - {}, nil          — nothing to do (no AttachTo, foreign node, already gone).
//   - {Requeue:true}   — Attach failed; controller-runtime back-off retries.
//   - {}, error        — apiserver-level failure (Get/Update/Delete).
func (r *PhysicalDeviceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("physicaldevice", req.Name)

	var dev blockstoriov1alpha1.PhysicalDevice

	err := r.Get(ctx, req.NamespacedName, &dev)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}

		return ctrl.Result{}, errors.Wrap(err, "get PhysicalDevice")
	}

	if dev.Labels[blockstoriov1alpha1.PhysicalDeviceLabelNode] != r.Config.NodeName {
		// Predicate should have filtered this out; defensive check
		// for watch-cache resync windows.
		return ctrl.Result{}, nil
	}

	if dev.Spec.AttachTo == nil {
		// Discovery-side state — nothing for the attach reconciler
		// to do until an operator (or piraeus-operator) flips
		// Spec.AttachTo.
		return ctrl.Result{}, nil
	}

	// Mark in-flight so kubectl get + status views reflect what's
	// happening. Idempotent — repeat writes to the same phase
	// no-op at the apiserver.
	err = r.setPhase(ctx, &dev, blockstoriov1alpha1.PhysicalDevicePhaseAttaching)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "set Phase=Attaching")
	}

	wireDev := crdPhysicalDeviceToWire(&dev)

	result, err := satellite.Attach(ctx, r.Config.Exec, &wireDev)
	if err != nil {
		logger.Info("Attach failed", "err", err)

		// Surface the failure on Status for operator triage; back
		// off via controller-runtime's rate limiter.
		_ = r.setPhase(ctx, &dev, blockstoriov1alpha1.PhysicalDevicePhaseFailed)

		return ctrl.Result{Requeue: true}, nil
	}

	provider, err := satellite.NewProviderFromKind(result.ProviderKind, result.Props, r.Config.Exec)
	if err != nil {
		// NewProviderFromKind only fails on a missing required
		// prop — Attach's contract guarantees it returns a
		// populated Props map, so this is a programming bug, not
		// a transient. Surface and let controller-runtime retry.
		logger.Info("NewProviderFromKind after Attach", "err", err)

		_ = r.setPhase(ctx, &dev, blockstoriov1alpha1.PhysicalDevicePhaseFailed)

		return ctrl.Result{Requeue: true}, nil
	}

	r.Config.Apply.RegisterProvider(result.PoolName, provider)

	// Delete-as-completion: a successful attach removes the CRD
	// so `linstor physical-storage list` stops surfacing the
	// device as free.
	err = r.Delete(ctx, &dev)
	if err != nil && !apierrors.IsNotFound(err) {
		return ctrl.Result{}, errors.Wrap(err, "delete PhysicalDevice")
	}

	return ctrl.Result{}, nil
}

// SetupWithManager wires this reconciler with a node-label
// predicate so only PhysicalDevices labelled with this node's
// name trigger reconciles.
func (r *PhysicalDeviceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	err := ctrl.NewControllerManagedBy(mgr).
		For(&blockstoriov1alpha1.PhysicalDevice{},
			builder.WithPredicates(physicalDeviceNodePredicate(r.Config.NodeName))).
		Named("satellite-physicaldevice").
		Complete(r)
	if err != nil {
		return errors.Wrap(err, "register PhysicalDeviceReconciler")
	}

	return nil
}

// setPhase writes the given Phase value onto Status via a
// straight Update on the status subresource. Status-only writes
// don't conflict with the discovery loop's Status updates
// because controller-runtime's client routes Status() through
// the subresource API.
func (r *PhysicalDeviceReconciler) setPhase(ctx context.Context, dev *blockstoriov1alpha1.PhysicalDevice, phase string) error {
	if dev.Status.Phase == phase {
		return nil
	}

	dev.Status.Phase = phase

	err := r.Status().Update(ctx, dev)
	if err != nil {
		return errors.Wrapf(err, "set Phase=%s", phase)
	}

	return nil
}

// crdPhysicalDeviceToWire maps the CRD shape onto the
// satellite-internal `apiv1.PhysicalDevice` value `satellite.Attach`
// consumes. Inlined rather than imported from
// `pkg/store/k8s` to keep the controllers package free of a
// store dependency (the store imports satellite types — direct
// import would risk a cycle on future refactors).
func crdPhysicalDeviceToWire(crd *blockstoriov1alpha1.PhysicalDevice) apiv1.PhysicalDevice {
	out := apiv1.PhysicalDevice{
		Name:           crd.Name,
		NodeName:       crd.Status.NodeName,
		StableID:       crd.Status.StableID,
		DevicePath:     crd.Status.DevicePath,
		CurrentDevPath: crd.Status.CurrentDevPath,
		SizeBytes:      crd.Status.SizeBytes,
		Model:          crd.Status.Model,
		Serial:         crd.Status.Serial,
		Rotational:     crd.Status.Rotational,
		Transport:      crd.Status.Transport,
		Phase:          crd.Status.Phase,
	}

	if out.NodeName == "" {
		out.NodeName = crd.Labels[blockstoriov1alpha1.PhysicalDeviceLabelNode]
	}

	if crd.Spec.AttachTo != nil {
		out.AttachTo = &apiv1.PhysicalDeviceAttachTo{
			StoragePoolName: crd.Spec.AttachTo.StoragePoolName,
			ProviderKind:    crd.Spec.AttachTo.ProviderKind,
			VGName:          crd.Spec.AttachTo.VGName,
			ThinPoolName:    crd.Spec.AttachTo.ThinPoolName,
			ZPoolName:       crd.Spec.AttachTo.ZPoolName,
			Directory:       crd.Spec.AttachTo.Directory,
			Wipe:            crd.Spec.AttachTo.Wipe,
		}
	}

	return out
}

// physicalDeviceNodePredicate filters PhysicalDevice events to
// those carrying the node label that matches the satellite's
// own name. Mirrors `nodeNamePredicate` (Spec.NodeName-keyed)
// but reads the metadata label since PhysicalDevice scopes its
// node binding there.
func physicalDeviceNodePredicate(nodeName string) predicate.Predicate {
	matches := func(obj client.Object) bool {
		dev, ok := obj.(*blockstoriov1alpha1.PhysicalDevice)
		if !ok {
			return false
		}

		return dev.Labels[blockstoriov1alpha1.PhysicalDeviceLabelNode] == nodeName
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
