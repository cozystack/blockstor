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
	"strconv"

	"github.com/cockroachdb/errors"
	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
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
// Status subresource.
//
// Bug 293 (P0, data-correctness): the previous implementation built
// a typed Resource from a cached Get and called `Status().Update`.
// That issues a wholesale Status subresource REPLACE — every
// Status field the cached copy is missing gets wiped from the
// apiserver, including the controller-side allocator's
// `DRBDNodeID` / `DRBDPort` / `DRBDMinor`. Race window: the
// controller stamps the IDs via SSA, the c-r informer cache trails
// the apiserver by hundreds of milliseconds, the satellite reconcile
// fires on a (legitimate) Apply failure mid-conversion, reads the
// pre-allocation cached copy, bumps the retry counter, and
// `Status().Update` overwrites Status with `DRBDPort=nil` /
// `DRBDMinor=nil` / `DRBDNodeID=nil`. Subsequent reconciles then
// wedge on `waitForControllerAllocation` because the allocator's
// SSA managed-fields entry now points at fields that no longer
// exist on the object; the controller has to re-allocate (or
// declare them already-stamped via its own cache, depending on
// timing) and the satellite logs `nodeID:null port:null minor:null`
// indefinitely. Surface symptom: `recovery-down-reverses.sh` fails
// because the operator-issued `drbdadm down` triggers an Apply
// retry, the retry-counter bump wipes DRBD-IDs, and the revive path
// never gets past the wait gate.
//
// Fix: use a JSON merge-patch scoped to `status.toggleDiskRetries`
// only. The apiserver applies it as a field-surgical mutation —
// every other Status field (DRBDPort, DRBDMinor, DRBDNodeID,
// Conditions, Volumes, DrbdState, etc.) survives untouched.
func (r *ResourceReconciler) patchToggleDiskRetries(ctx context.Context, res *blockstoriov1alpha1.Resource, want int32) error {
	if res.Status.ToggleDiskRetries == want {
		return nil
	}

	// Field-surgical: JSON merge-patch touches ONLY the named
	// field. The apiserver merges this into Status without
	// reading or replacing any other field, so the controller-side
	// allocator's DRBD-ID writes (and any other field owner's
	// claims) survive untouched.
	body := []byte(`{"status":{"toggleDiskRetries":` + strconv.FormatInt(int64(want), 10) + `}}`)

	target := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: res.Name},
	}

	err := r.Status().Patch(ctx, target, client.RawPatch(types.MergePatchType, body))
	if err != nil {
		return errors.Wrap(err, "merge-patch Status.ToggleDiskRetries")
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
		volumeNumbers, err := r.lookupVolumeNumbers(ctx, res)
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
