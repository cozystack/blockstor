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
	"github.com/go-logr/logr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
)

// ToggleDiskRetryCap is the soft upper bound the reconciler surfaces
// via logs/conditions before operators are expected to intervene.
// Past this point the satellite still retries — there is no kernel-
// imposed terminal state — but the high counter makes a permanently-
// failing toggle easy to spot on `linstor r l`.
const ToggleDiskRetryCap int32 = 10

// recordToggleDiskOutcome increments Status.ToggleDiskRetries on a
// failed apply, and resets it to 0 once the Resource reaches the
// post-conversion steady-state. Idempotent: callers may invoke it
// after every reconcile pass without churning the apiserver — the
// helper short-circuits when the desired counter value already
// matches the observed one. Bug 39.
//
// "Mid-conversion" is detected by the absence of the DISKLESS flag
// combined with at least one volume Status entry — i.e. storage has
// been (or is being) carved but the satellite has not yet observed
// the kernel resource reach UpToDate. A diskless replica or one whose
// volumes report `disk_state=UpToDate` is by definition not mid-
// conversion, so we never tick the counter there.
//
// success=true means the latest Apply succeeded for every per-volume
// result; success=false means at least one per-resource apply result
// reported Ok=false.
func (r *ResourceReconciler) recordToggleDiskOutcome(ctx context.Context, res *blockstoriov1alpha1.Resource, success bool) error {
	if !isToggleDiskInFlight(res) {
		// Either DISKLESS (the operator hasn't asked for diskful
		// yet) or fully steady (UpToDate). Resetting any stale
		// counter from a prior conversion run keeps the Resource's
		// observable state crisp.
		if res.Status.ToggleDiskRetries != 0 {
			return r.patchToggleDiskRetries(ctx, res, 0)
		}

		return nil
	}

	if success {
		// Conversion concluded successfully — clear the counter so
		// the next `linstor r l` shows a fresh 0 and a future
		// failed toggle starts counting from scratch.
		if res.Status.ToggleDiskRetries != 0 {
			return r.patchToggleDiskRetries(ctx, res, 0)
		}

		return nil
	}

	// Transient failure mid-conversion → bump the counter by 1.
	return r.patchToggleDiskRetries(ctx, res, res.Status.ToggleDiskRetries+1)
}

// patchToggleDiskRetries writes Status.ToggleDiskRetries via the
// Status subresource. Uses a plain Update against a refreshed object
// (the fake client used in unit tests doesn't support
// Status.Patch(Apply) cleanly, and the real apiserver accepts a
// Status().Update on a status-subresource-enabled CRD without
// stomping Spec).
func (r *ResourceReconciler) patchToggleDiskRetries(ctx context.Context, res *blockstoriov1alpha1.Resource, want int32) error {
	var fresh blockstoriov1alpha1.Resource

	err := r.Get(ctx, client.ObjectKey{Name: res.Name}, &fresh)
	if err != nil {
		return errors.Wrap(err, "get Resource for toggle-disk retry patch")
	}

	if fresh.Status.ToggleDiskRetries == want {
		return nil
	}

	fresh.Status.ToggleDiskRetries = want

	err = r.Status().Update(ctx, &fresh)
	if err != nil {
		return errors.Wrap(err, "update Status.ToggleDiskRetries")
	}

	// Reflect the write back onto the caller's copy so subsequent
	// reads inside the same reconcile see the new value.
	res.Status.ToggleDiskRetries = want

	return nil
}

// isToggleDiskInFlight reports whether the Resource is currently mid
// diskless→diskful conversion. The predicate is intentionally narrow:
// a Resource is mid-conversion iff it's NOT marked DISKLESS in Spec
// AND at least one Status volume has been observed (DevicePath set,
// AllocatedKib > 0, or DiskState non-empty). Once every volume
// reaches UpToDate the conversion is over and we treat the Resource
// as steady-state — the retry counter resets on the next reconcile.
func isToggleDiskInFlight(res *blockstoriov1alpha1.Resource) bool {
	if slices.Contains(res.Spec.Flags, apiv1.ResourceFlagDiskless) {
		return false
	}

	if len(res.Status.Volumes) == 0 {
		// Spec asks for diskful but we have not started carving
		// yet — counted as "in flight" so the very first attempt
		// to provision storage counts as retry 1 on failure.
		return true
	}

	allUpToDate := true

	for i := range res.Status.Volumes {
		v := &res.Status.Volumes[i]
		if v.DiskState != "UpToDate" {
			allUpToDate = false

			break
		}
	}

	return !allUpToDate
}

// handleToggleDiskCancel unwinds an in-flight diskless→diskful
// conversion when the REST shim has set Spec.ToggleDiskCancel. Bug 40.
//
// Steps (idempotent, in order):
//
//  1. Drive the satellite's DeleteResource against the parent RD —
//     same path the finalizer-delete uses, so drbdadm down, drbdmeta
//     wipe, and the storage provider's DeleteVolume all run. The
//     DeleteResource API is a no-op when the kernel resource is
//     already gone.
//  2. Re-stamp the DISKLESS flag on Spec.Flags so subsequent
//     reconciles see the Resource as a connection-mesh-only replica
//     and the rest of the satellite paths leave it alone.
//  3. Clear Spec.ToggleDiskCancel and Status.ToggleDiskRetries so the
//     next operator-issued toggle-disk starts from a clean slate.
//
// Returns a short-backoff Requeue when DeleteResource reports a
// transient failure so the cancel keeps trying until the kernel is
// quiescent. Steady-state Resources (no Status.Volumes, or all
// UpToDate) skip step 1 entirely — there is nothing to unwind, the
// handler still clears the cancel flag so the toggle-disk endpoint
// stays usable.
func (r *ResourceReconciler) handleToggleDiskCancel(ctx context.Context, res *blockstoriov1alpha1.Resource, logger logr.Logger) (ctrl.Result, error) {
	logger.Info("toggle-disk cancel requested; unwinding partial state",
		"resource", res.Name,
		"retries", res.Status.ToggleDiskRetries)

	if isToggleDiskInFlight(res) {
		volumeNumbers, err := r.lookupVolumeNumbers(ctx, res.Spec.ResourceDefinitionName)
		if err != nil {
			return ctrl.Result{}, err
		}

		req := &intent.DeleteResourceRequest{
			Name:          res.Spec.ResourceDefinitionName,
			StoragePool:   resolveDeleteStoragePool(res),
			VolumeNumbers: volumeNumbers,
		}

		resp, err := r.Config.Apply.DeleteResource(ctx, req)
		if err != nil {
			return ctrl.Result{}, errors.Wrap(err, "toggle-disk cancel DeleteResource")
		}

		if !resp.GetOk() {
			logger.Info("toggle-disk cancel DeleteResource pending",
				"message", resp.GetMessage())

			return ctrl.Result{RequeueAfter: applyFailureRequeue}, nil
		}
	}

	// Re-stamp DISKLESS + clear the cancel flag on Spec, atomic
	// against the apiserver. If the operator concurrently mutated
	// Spec we'll surface a conflict and the next reconcile re-runs
	// the cancel idempotently.
	if !slices.Contains(res.Spec.Flags, apiv1.ResourceFlagDiskless) {
		res.Spec.Flags = append(res.Spec.Flags, apiv1.ResourceFlagDiskless)
	}

	res.Spec.ToggleDiskCancel = false

	err := r.Update(ctx, res)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "clear toggle-disk cancel + restore DISKLESS")
	}

	// Status clear is a separate write — the apiserver's spec /
	// status subresources are independent and the spec write above
	// only touches Spec.Flags + Spec.ToggleDiskCancel.
	if res.Status.ToggleDiskRetries != 0 {
		err = r.patchToggleDiskRetries(ctx, res, 0)
		if err != nil {
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}
