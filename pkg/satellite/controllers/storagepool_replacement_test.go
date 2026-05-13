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

package controllers_test

import (
	"context"
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/satellite/controllers"
	"github.com/cozystack/blockstor/pkg/storage"
)

// Scenario 6.19 — drive replacement.
//
// Operator runbook: a worker's disk dies, the operator powers the
// node down, swaps the failed drive for a fresh one, recreates the
// LVM VG / ZFS pool with the SAME name (e.g. `blockstor-lvm`), and
// reconnects the satellite. The satellite reconciler MUST notice
// the pool disappeared, surface that to the apiserver so the
// autoplacer/UX can react, then notice the pool reappeared (with
// new capacity because the new drive may be a different size),
// re-register the provider, and let pre-existing DRBD resources on
// this pool re-attach via their own per-resource reconcile.
//
// See tests/scenarios/06-storage-backends.md §6.19.
//
// AUDIT (Phase 10.x, 2026-05-13):
//
//   - Step 2 (initial report — pool present, capacity stamped): WORKS.
//     `StoragePoolReconciler.writeCapacity` populates
//     `Status.FreeCapacity` + `Status.TotalCapacity` on every
//     30s `capacityResyncInterval` requeue.
//
//   - Steps 3+4 (pool missing → controller marks SP Faulted): NOT
//     IMPLEMENTED. `writeCapacity` logs the `PoolStatus` error
//     and returns without touching `Status` — the CRD keeps its
//     last-known FreeCapacity/TotalCapacity values indefinitely
//     and no `Conditions[type=PoolMissing]` is stamped. The
//     autoplacer therefore continues to treat the broken pool as
//     a placement target, which is the bug 6.19's tracking story
//     is meant to surface.
//
//   - Steps 5+6 (pool reappears with NEW capacity → refresh + resync):
//     PARTIALLY WORKS. Capacity will refresh on the next 30s
//     requeue once `lvs`/`zpool` start returning the new pool's
//     numbers again. Resource re-attach is a separate concern
//     handled by the `ResourceReconciler` — out of scope for the
//     StoragePool reconciler.
//
// FEATURE GAPS to address (tracked here so the test can be
// un-skipped once the implementation lands):
//
//  1. `StoragePoolStatus` needs a way to express "pool is currently
//     unreachable on this satellite". Either:
//     (a) a `Phase` field (`Healthy` / `Faulted`); or
//     (b) a well-known `Conditions[type=PoolMissing]` /
//     `Conditions[type=Healthy=False]` entry stamped by
//     `writeCapacity` on `PoolStatus` error and cleared once
//     it succeeds again.
//
//  2. `writeCapacity` (or a peer `writePoolMissing`) must observe
//     the error and Status-patch the CRD accordingly — not just
//     log + return. The patch must be SSA / field-managed so the
//     observer's other status writes don't fight it.
//
//  3. The autoplacer (`pkg/rest/autoplace.go`) and the
//     `view/storage-pools` endpoint must exclude / flag the SP
//     when it is `Faulted` so a pool with a dead drive doesn't
//     get picked for new RDs.
//
//  4. Optional: zero out `FreeCapacity` / `TotalCapacity` on
//     Faulted so the dashboards don't show stale numbers, OR
//     keep them and add a `LastReachableTime` field. Decide as
//     part of the API design.
//
// Until those land, this test is a spec — it documents the
// expected behaviour and skips the assertions that don't pass yet.

// fakeProviderKind / LVM-thin-specific exec key for the FakeExec
// stubs. Centralised so the three reconcile passes in the test
// can rebuild the response map cleanly.
const (
	drvReplacementVG   = "blockstor-lvm"
	drvReplacementThin = "thin"
)

// drvReplacementLvsCmd is the exact command line lvm.Thin.PoolStatus
// runs. Matches the format `pkg/storage/lvm.TestThinPoolStatusParsesVgsLvs`
// uses — kept inline rather than imported because Args() is package-
// private and pulling it in via a helper export would widen the
// public API just for this test.
const drvReplacementLvsCmd = "lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } " +
	"--noheadings --separator | -o lv_size,data_percent --units k --nosuffix " +
	drvReplacementVG + "/" + drvReplacementThin

func newDrvReplacementPool() *blockstoriov1alpha1.StoragePool {
	return &blockstoriov1alpha1.StoragePool{
		ObjectMeta: metav1.ObjectMeta{
			Name:       drvReplacementVG + "-n1",
			Finalizers: []string{controllers.StoragePoolFinalizer},
		},
		Spec: blockstoriov1alpha1.StoragePoolSpec{
			NodeName:     "n1",
			PoolName:     drvReplacementVG,
			ProviderKind: "LVM_THIN",
			Props: map[string]string{
				"StorDriver/LvmVg":    drvReplacementVG,
				"StorDriver/ThinPool": drvReplacementThin,
			},
		},
	}
}

func reconcileDrvReplacement(t *testing.T, cli client.Client, fx *storage.FakeExec) ctrl.Result {
	t.Helper()

	reconciler := &controllers.StoragePoolReconciler{
		Client: cli,
		Config: controllers.Config{
			NodeName: "n1",
			Apply:    newSatelliteReconcilerForTests(),
			Exec:     fx,
		},
	}

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: drvReplacementVG + "-n1"},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	return result
}

// expectLvsOutput stubs the LVM thin pool's `lvs` invocation to
// return a synthetic size + 0% used line. The reconciler's
// `writeCapacity` therefore observes `TotalCapacityKib==sizeKib`
// and `FreeCapacityKib==sizeKib`.
func expectLvsOutput(fx *storage.FakeExec, sizeKib int64) {
	fx.Expect(drvReplacementLvsCmd, storage.FakeResponse{
		Stdout: []byte(fmt.Sprintf("%d.00|0.00\n", sizeKib)),
	})
}

// TestStoragePoolDriveReplacement6_19 is the full lifecycle pin
// for scenario 6.19. Three reconcile passes:
//
//  1. Initial state — pool reports 100 GiB free, Status reflects it.
//  2. Disk fails — pool reports nothing (`lvs` returns empty),
//     reconciler MUST stamp a Faulted condition (currently does
//     not — assertions guarded by t.Skip).
//  3. Disk replaced — pool reports a DIFFERENT size (200 GiB,
//     because the replacement drive is bigger), reconciler MUST
//     update Status.FreeCapacity AND clear the Faulted condition.
//
// Resource re-attach (step 6 in the scenario doc) is the
// `ResourceReconciler`'s job and is asserted by its own integration
// tests (`pkg/satellite/controllers/resource_test.go` family).
func TestStoragePoolDriveReplacement6_19(t *testing.T) {
	t.Parallel()

	const (
		initialFreeKib     int64 = 100 * 1024 * 1024 // 100 GiB
		replacementFreeKib int64 = 200 * 1024 * 1024 // 200 GiB — bigger drive
	)

	scheme := newStoragePoolScheme(t)
	pool := newDrvReplacementPool()

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool).
		WithStatusSubresource(&blockstoriov1alpha1.StoragePool{}).
		Build()

	fx := storage.NewFakeExec()

	// --- Pass 1: initial — pool present, 100 GiB free. ---

	expectLvsOutput(fx, initialFreeKib)
	_ = reconcileDrvReplacement(t, cli, fx)

	var afterInitial blockstoriov1alpha1.StoragePool

	err := cli.Get(context.Background(), client.ObjectKey{Name: drvReplacementVG + "-n1"}, &afterInitial)
	if err != nil {
		t.Fatalf("Get after initial reconcile: %v", err)
	}

	if afterInitial.Status.FreeCapacity != initialFreeKib {
		t.Errorf("Pass 1: Status.FreeCapacity = %d, want %d (initial pool report)",
			afterInitial.Status.FreeCapacity, initialFreeKib)
	}

	if afterInitial.Status.TotalCapacity != initialFreeKib {
		t.Errorf("Pass 1: Status.TotalCapacity = %d, want %d",
			afterInitial.Status.TotalCapacity, initialFreeKib)
	}

	// --- Pass 2: disk fails — `lvs` returns empty stdout, which
	// `lvm.Thin.PoolStatus` surfaces as `thin pool not found`. ---

	fx = storage.NewFakeExec() // empty Responses => empty stdout for `lvs`
	_ = reconcileDrvReplacement(t, cli, fx)

	var afterFailure blockstoriov1alpha1.StoragePool

	err = cli.Get(context.Background(), client.ObjectKey{Name: drvReplacementVG + "-n1"}, &afterFailure)
	if err != nil {
		t.Fatalf("Get after disk-fail reconcile: %v", err)
	}

	t.Run("controller surfaces missing pool as Faulted", func(t *testing.T) {
		t.Parallel()
		t.Skip("scenario 6.19 not implemented — see feature gaps 1+2 in storagepool_replacement_test.go header. " +
			"writeCapacity logs PoolStatus errors but does not patch Status; no Faulted phase / PoolMissing " +
			"condition exists yet.")

		// EXPECTED once implemented: a Conditions entry exists
		// with type=PoolMissing (or Healthy=False) on the SP CRD.
		foundFaulted := false

		for _, cond := range afterFailure.Status.Conditions {
			if cond.Type == "PoolMissing" && cond.Status == metav1.ConditionTrue {
				foundFaulted = true

				break
			}
			// alternative spelling once API decided: Healthy=False
			if cond.Type == "Healthy" && cond.Status == metav1.ConditionFalse {
				foundFaulted = true

				break
			}
		}

		if !foundFaulted {
			t.Errorf("Pass 2: expected Faulted/PoolMissing condition on SP after disk failure, got %+v",
				afterFailure.Status.Conditions)
		}
	})

	t.Run("capacity zeroed or marked stale on missing pool", func(t *testing.T) {
		t.Parallel()

		// writeCapacity zeroes FreeCapacity/TotalCapacity and
		// flips PoolMissing=true on a PoolStatus error. The full
		// PoolMissing-bool lifecycle is pinned by
		// TestStoragePoolReconcilePoolMissingLifecycle; this assertion
		// just confirms the capacity half of the same write landed.
		if afterFailure.Status.FreeCapacity != 0 {
			t.Errorf("Pass 2: Status.FreeCapacity = %d, want 0 (pool is missing)",
				afterFailure.Status.FreeCapacity)
		}

		if !afterFailure.Status.PoolMissing {
			t.Errorf("Pass 2: Status.PoolMissing = false, want true (pool is missing)")
		}
	})

	// --- Pass 3: operator replaced the disk, recreated the VG +
	// thin pool with the same name but a bigger drive. `lvs` now
	// reports 200 GiB free. ---

	fx = storage.NewFakeExec()
	expectLvsOutput(fx, replacementFreeKib)
	_ = reconcileDrvReplacement(t, cli, fx)

	var afterReplacement blockstoriov1alpha1.StoragePool

	err = cli.Get(context.Background(), client.ObjectKey{Name: drvReplacementVG + "-n1"}, &afterReplacement)
	if err != nil {
		t.Fatalf("Get after replacement reconcile: %v", err)
	}

	// Capacity refresh on replacement: this branch is the one
	// that DOES work today (the 30s capacityResyncInterval +
	// writeCapacity's diff-then-Status.Update path). Asserted
	// unconditionally so a regression in the working half of
	// 6.19 is caught.
	if afterReplacement.Status.FreeCapacity != replacementFreeKib {
		t.Errorf("Pass 3: Status.FreeCapacity = %d, want %d (replacement drive bigger than original)",
			afterReplacement.Status.FreeCapacity, replacementFreeKib)
	}

	if afterReplacement.Status.TotalCapacity != replacementFreeKib {
		t.Errorf("Pass 3: Status.TotalCapacity = %d, want %d",
			afterReplacement.Status.TotalCapacity, replacementFreeKib)
	}

	t.Run("Faulted condition cleared on replacement", func(t *testing.T) {
		t.Parallel()
		t.Skip("scenario 6.19 not implemented — once the Faulted condition lands per feature gap 1, " +
			"the replacement pass MUST clear it. Until then there's no condition to clear.")

		for _, cond := range afterReplacement.Status.Conditions {
			if (cond.Type == "PoolMissing" && cond.Status == metav1.ConditionTrue) ||
				(cond.Type == "Healthy" && cond.Status == metav1.ConditionFalse) {
				t.Errorf("Pass 3: Faulted/PoolMissing condition still present after pool reappeared: %+v", cond)
			}
		}
	})
}

// Silence the unused-import check when corev1 ends up not needed
// at compile time (the scheme registration in newStoragePoolScheme
// still wants it referenced via the package-level import path).
var _ = corev1.Pod{}
