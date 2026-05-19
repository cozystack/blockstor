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
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

// poolMissingRequeue is the back-off the reconciler waits for
// the target StoragePool CRD to land when an attach request
// references a pool that doesn't exist yet. Short enough that a
// CDP-creates-pool-and-attach race resolves in seconds; long
// enough that we don't spin if the operator never applies the
// pool. Phase 10.7 race-handling matrix line 4.
const poolMissingRequeue = 10 * time.Second

// poolMissingTimeout caps how long the reconciler keeps
// requeuing for a missing target StoragePool before giving up
// with `Phase=Failed`. 10 minutes is long enough for an
// operator-driven create-pool-and-device GitOps apply to
// land, short enough that an operator who forgot to apply the
// pool sees the failure within a kubectl-get cycle rather than
// after a multi-hour silent wait. Phase 10.7 race-matrix line
// 4 final state.
const poolMissingTimeout = 10 * time.Minute

// physicalDeviceConditionPoolMissing is the
// Status.Conditions[type] the reconciler stamps when it sees
// `Spec.AttachTo.StoragePoolName` referencing a pool that
// doesn't exist yet. Its `LastTransitionTime` is the "first
// observed at" timestamp used to bound the requeue window.
const physicalDeviceConditionPoolMissing = "PoolMissing"

// physicalDeviceConditionDeviceMissing is the
// Status.Conditions[type] the reconciler stamps when the
// PhysicalDevice's discovery-observed device path is empty
// (the operator flipped Spec.AttachTo for a device that
// vanished between discovery and reconcile, or never had a
// device path stamped). Paired with `Phase=Failed`.
const physicalDeviceConditionDeviceMissing = "DeviceMissing"

// PhysicalDeviceAttachFinalizer guards a PhysicalDevice CRD
// while `Spec.AttachTo` is set + the satellite hasn't yet
// completed the attach. Without it, an operator running
// `kubectl delete physicaldevice X` mid-attach would race the
// satellite's Step-6 Delete, potentially removing the CRD before
// the in-progress provider commands complete. The reconciler
// stamps the finalizer on first observation of `Spec.AttachTo`,
// and strips it just before its delete-as-completion call.
// Phase 10.7 step 5 / 10.8 line 4.
const PhysicalDeviceAttachFinalizer = "blockstor.io.blockstor.io/physicaldevice-attach"

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

	// Discovery-side state (no AttachTo) and operator-driven
	// delete both reduce to "strip our finalizer if we stamped
	// one and stop." The provider commands `Attach` ran are
	// idempotent + safe to leave on disk; pool teardown is
	// Phase 10.8's StoragePool CRD concern, not this reconciler's.
	if dev.Spec.AttachTo == nil || !dev.DeletionTimestamp.IsZero() {
		return r.stripAttachFinalizer(ctx, &dev)
	}

	// Stamp our finalizer before touching the device so a racing
	// `kubectl delete` can't whisk the CRD out from under the
	// in-flight Attach commands.
	if !slices.Contains(dev.Finalizers, PhysicalDeviceAttachFinalizer) {
		dev.Finalizers = append(dev.Finalizers, PhysicalDeviceAttachFinalizer)

		err := r.Update(ctx, &dev)
		if err != nil {
			return ctrl.Result{}, errors.Wrap(err, "add attach finalizer")
		}

		return ctrl.Result{RequeueAfter: time.Second}, nil
	}

	// Step 1: target device must still be reachable. A discovery
	// pass that wiped DevicePath while we were waiting on a
	// `Spec.AttachTo` flip means the device is gone — bail out
	// with `Phase=Failed` + an explicit `DeviceMissing` Condition
	// so the operator sees the cause rather than `Attach`
	// returning a generic "no DevicePath" error.
	if deviceMissing(&dev) {
		return r.handleDeviceMissing(ctx, &dev)
	}

	// Step 4 race-handling: an attach request may land before the
	// matching StoragePool CRD is reconciled. Requeue rather than
	// charging into Attach with a dangling pool name; the
	// satellite's StoragePoolReconciler will register the pool on
	// its next pass and our next requeue picks it up.
	poolReady, err := r.targetPoolExists(ctx, dev.Spec.AttachTo.StoragePoolName)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "check StoragePool existence")
	}

	if !poolReady {
		return r.handlePoolMissing(ctx, &dev)
	}

	// Mark in-flight so kubectl get + status views reflect what's
	// happening. Idempotent — repeat writes to the same phase
	// no-op at the apiserver.
	err = r.setPhase(ctx, &dev, blockstoriov1alpha1.PhysicalDevicePhaseAttaching)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "set Phase=Attaching")
	}

	return r.runAttach(ctx, &dev)
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

// runAttach performs Steps 4-6 of the attach flow: invoke
// `satellite.Attach`, register the resulting provider on the
// shared `Config.Apply`, and delete the CRD as the
// delete-as-completion signal. Split out of Reconcile to keep
// the latter under the funlen + gocyclo budgets.
//
// Bug 337: PhysicalDevice reconciler is flat — each device →
// attach. Pool create on the first observed device, `zpool add`
// / `vgextend` on subsequent. No "is this the first?" state
// tracking; the branch is purely a probe of host state inside
// `satellite.Attach`. This keeps the satellite stateless and
// makes `linstor ps cdp` idempotent + online-expansion-friendly:
// re-running ps cdp a week later with a new device just
// extends the existing pool.
//
// See memory:feedback_ps_cdp_incremental for the design rationale.
func (r *PhysicalDeviceReconciler) runAttach(ctx context.Context, dev *blockstoriov1alpha1.PhysicalDevice) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("physicaldevice", dev.Name)

	wireDev := crdPhysicalDeviceToWire(dev)

	result, err := satellite.Attach(ctx, r.Config.Exec, &wireDev)
	if err != nil {
		logger.Info("Attach failed", "err", err)

		// Surface the failure on Status for operator triage; back
		// off via controller-runtime's rate limiter.
		_ = r.setPhase(ctx, dev, blockstoriov1alpha1.PhysicalDevicePhaseFailed)

		return ctrl.Result{RequeueAfter: time.Second}, nil
	}

	provider, err := satellite.NewProviderFromKind(result.ProviderKind, result.Props, r.Config.Exec)
	if err != nil {
		// NewProviderFromKind only fails on a missing required
		// prop — Attach's contract guarantees it returns a
		// populated Props map, so this is a programming bug, not
		// a transient. Surface and let controller-runtime retry.
		logger.Info("NewProviderFromKind after Attach", "err", err)

		_ = r.setPhase(ctx, dev, blockstoriov1alpha1.PhysicalDevicePhaseFailed)

		return ctrl.Result{RequeueAfter: time.Second}, nil
	}

	r.Config.Apply.RegisterProvider(result.PoolName, provider)

	// Record this device's kernel name on the target
	// StoragePool so the satellite's discovery loop knows the
	// device has been consumed and stops re-publishing a Free=False
	// PhysicalDevice CRD for it on every lsblk pass. Best-effort —
	// a transient apiserver failure here doesn't roll back the
	// already-completed pool create; the next reconcile / discovery
	// tick re-runs the stamp. See Bug 91 retro.
	err = r.recordAttachedKName(ctx, dev, result.PoolName)
	if err != nil {
		log.FromContext(ctx).Error(err, "record attached kname on StoragePool",
			"physicaldevice", dev.Name, "storagepool", result.PoolName)
	}

	// Strip our finalizer BEFORE Delete; otherwise the apiserver
	// keeps the CRD around with a DeletionTimestamp until the
	// next reconcile pass, leaving a window where
	// `linstor physical-storage list` still shows the device.
	dev.Finalizers = slices.DeleteFunc(dev.Finalizers,
		func(f string) bool { return f == PhysicalDeviceAttachFinalizer })

	err = r.Update(ctx, dev)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "strip attach finalizer")
	}

	// Delete-as-completion: a successful attach removes the CRD
	// so `linstor physical-storage list` stops surfacing the
	// device as free.
	err = r.Delete(ctx, dev)
	if err != nil && !apierrors.IsNotFound(err) {
		return ctrl.Result{}, errors.Wrap(err, "delete PhysicalDevice")
	}

	return ctrl.Result{}, nil
}

// recordAttachedKName stamps the lsblk kernel name of `dev` onto the
// target StoragePool's `StoragePoolAnnotationAttachedKNames`
// annotation so the discovery loop can skip re-publishing a
// PhysicalDevice CRD for this device after attach (Bug 91). Idempotent
// — re-recording an already-present kname no-ops at the apiserver.
//
// The kname is the leaf of `Status.DevicePath` (e.g. `/dev/sdb` →
// `sdb`); falls back to `Status.CurrentDevPath` since some discovery
// paths leave DevicePath empty until the next tick. Returns nil and
// skips the write for FILE/FILE_THIN pools, which carry no underlying
// kname (directory-backed).
func (r *PhysicalDeviceReconciler) recordAttachedKName(ctx context.Context, dev *blockstoriov1alpha1.PhysicalDevice, poolName string) error {
	kname := extractKName(dev)
	if kname == "" {
		return nil
	}

	var pools blockstoriov1alpha1.StoragePoolList

	err := r.List(ctx, &pools)
	if err != nil {
		return errors.Wrap(err, "list StoragePool")
	}

	var target *blockstoriov1alpha1.StoragePool

	for i := range pools.Items {
		p := &pools.Items[i]
		if p.Spec.PoolName == poolName && p.Spec.NodeName == r.Config.NodeName {
			target = p

			break
		}
	}

	if target == nil {
		return errors.Errorf("target StoragePool %q not found on node %q", poolName, r.Config.NodeName)
	}

	if target.Annotations == nil {
		target.Annotations = map[string]string{}
	}

	existing := target.Annotations[blockstoriov1alpha1.StoragePoolAnnotationAttachedKNames]
	if knameInList(existing, kname) {
		return nil
	}

	if existing == "" {
		target.Annotations[blockstoriov1alpha1.StoragePoolAnnotationAttachedKNames] = kname
	} else {
		target.Annotations[blockstoriov1alpha1.StoragePoolAnnotationAttachedKNames] = existing + "," + kname
	}

	err = r.Update(ctx, target)
	if err != nil {
		return errors.Wrap(err, "annotate StoragePool with attached kname")
	}

	return nil
}

// extractKName returns the lsblk kernel name (e.g. `sdb`) the
// PhysicalDevice was published from. Strips the `/dev/` prefix off
// `Status.DevicePath`, falling back to `Status.CurrentDevPath`. Empty
// when the CRD has no resolved device path (FILE pool / pre-discovery
// race).
func extractKName(dev *blockstoriov1alpha1.PhysicalDevice) string {
	for _, p := range []string{dev.Status.DevicePath, dev.Status.CurrentDevPath} {
		kname := strings.TrimPrefix(p, "/dev/")
		if kname != "" && kname != p {
			// Strip ran — value started with `/dev/`. Keep it.
			return kname
		}
	}

	return ""
}

// knameInList returns true when `kname` is already a comma-separated
// element of `list`. The annotation is the source of truth on which
// devices belong to a pool; idempotency probe before append.
func knameInList(list, kname string) bool {
	if list == "" {
		return false
	}

	for item := range strings.SplitSeq(list, ",") {
		if strings.TrimSpace(item) == kname {
			return true
		}
	}

	return false
}

// stripAttachFinalizer removes our finalizer from the CRD when
// it's present, and no-ops when it isn't. Used by both the
// "AttachTo cleared after a failed attempt" path (operator
// gave up; let the CRD be deletable normally) and the
// "kubectl delete physicaldevice X mid-attach" path (apiserver
// holds Delete pending our consent — drop it and let things
// finalise; pool teardown is Phase 10.8's concern).
func (r *PhysicalDeviceReconciler) stripAttachFinalizer(ctx context.Context, dev *blockstoriov1alpha1.PhysicalDevice) (ctrl.Result, error) {
	if !slices.Contains(dev.Finalizers, PhysicalDeviceAttachFinalizer) {
		return ctrl.Result{}, nil
	}

	dev.Finalizers = slices.DeleteFunc(dev.Finalizers,
		func(f string) bool { return f == PhysicalDeviceAttachFinalizer })

	err := r.Update(ctx, dev)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "strip attach finalizer")
	}

	return ctrl.Result{}, nil
}

// deviceMissing returns true when a non-FILE attach request
// can't resolve to a real block device — discovery hasn't
// stamped DevicePath/CurrentDevPath yet, or the device was
// physically yanked. FILE / FILE_THIN kinds don't need a block
// device, so they short-circuit to false. Phase 10.7 Step 1.
func deviceMissing(dev *blockstoriov1alpha1.PhysicalDevice) bool {
	kind := dev.Spec.AttachTo.ProviderKind
	if kind == satellite.ProviderKindFile || kind == satellite.ProviderKindFileThin {
		return false
	}

	return dev.Status.DevicePath == "" && dev.Status.CurrentDevPath == ""
}

// handleDeviceMissing stamps both `Phase=Failed` and a
// `DeviceMissing` Status Condition when a non-FILE attach
// request can't resolve to a real block device. Pulled out
// of Reconcile to keep the latter under the funlen budget.
func (r *PhysicalDeviceReconciler) handleDeviceMissing(ctx context.Context, dev *blockstoriov1alpha1.PhysicalDevice) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("physicaldevice", dev.Name)

	meta.SetStatusCondition(&dev.Status.Conditions, metav1.Condition{
		Type:               physicalDeviceConditionDeviceMissing,
		Status:             metav1.ConditionTrue,
		Reason:             "DiscoveryDevicePathEmpty",
		Message:            "PhysicalDevice has no Status.DevicePath/CurrentDevPath; device was removed between discovery and attach",
		LastTransitionTime: metav1.Now(),
	})

	dev.Status.Phase = blockstoriov1alpha1.PhysicalDevicePhaseFailed

	err := r.Status().Update(ctx, dev)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "stamp DeviceMissing")
	}

	logger.Info("PhysicalDevice has no DevicePath — DeviceMissing")

	return ctrl.Result{}, nil
}

// handlePoolMissing implements the Phase 10.7 race-matrix
// line 4 final state: short waits during the common
// CDP-creates-pool-and-attach race resolve via
// `RequeueAfter:10s`, but once `poolMissingTimeout` elapses
// without the pool appearing the reconciler stops requeuing
// and stamps `Phase=Failed` so the operator sees the cause.
// First observation lays down a `PoolMissing` Condition whose
// `LastTransitionTime` is the wall-clock anchor for the
// timeout.
func (r *PhysicalDeviceReconciler) handlePoolMissing(ctx context.Context, dev *blockstoriov1alpha1.PhysicalDevice) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("physicaldevice", dev.Name)

	cond := meta.FindStatusCondition(dev.Status.Conditions, physicalDeviceConditionPoolMissing)
	if cond != nil && time.Since(cond.LastTransitionTime.Time) > poolMissingTimeout {
		logger.Info("PoolMissing timeout exceeded; failing attach",
			"pool", dev.Spec.AttachTo.StoragePoolName,
			"since", cond.LastTransitionTime.Time)

		_ = r.setPhase(ctx, dev, blockstoriov1alpha1.PhysicalDevicePhaseFailed)

		return ctrl.Result{}, nil
	}

	if cond == nil {
		meta.SetStatusCondition(&dev.Status.Conditions, metav1.Condition{
			Type:               physicalDeviceConditionPoolMissing,
			Status:             metav1.ConditionTrue,
			Reason:             "TargetStoragePoolNotFound",
			Message:            "Spec.AttachTo.StoragePoolName references a StoragePool that doesn't exist on this node yet",
			LastTransitionTime: metav1.Now(),
		})

		err := r.Status().Update(ctx, dev)
		if err != nil {
			return ctrl.Result{}, errors.Wrap(err, "stamp PoolMissing condition")
		}
	}

	logger.Info("target StoragePool not yet known; requeuing",
		"pool", dev.Spec.AttachTo.StoragePoolName)

	return ctrl.Result{RequeueAfter: poolMissingRequeue}, nil
}

// targetPoolExists returns true when a StoragePool CRD named
// `poolName` and scoped to this satellite's node exists. The
// PhysicalDevice attach request races with the StoragePool CRD
// create (operator applies both in one go); requeue when the
// pool isn't here yet so we don't `vgcreate` for a pool the
// rest of the system hasn't acknowledged. Phase 10.7 race
// matrix line 4.
func (r *PhysicalDeviceReconciler) targetPoolExists(ctx context.Context, poolName string) (bool, error) {
	if poolName == "" {
		// Empty pool name is a malformed AttachTo; let the Attach
		// path surface it as `Phase=Failed` so the operator sees
		// the real cause rather than a noisy requeue loop.
		return true, nil
	}

	var poolList blockstoriov1alpha1.StoragePoolList

	err := r.List(ctx, &poolList)
	if err != nil {
		return false, errors.Wrap(err, "list StoragePool")
	}

	for i := range poolList.Items {
		if poolList.Items[i].Spec.PoolName == poolName &&
			poolList.Items[i].Spec.NodeName == r.Config.NodeName {
			return true, nil
		}
	}

	return false, nil
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
