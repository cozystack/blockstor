// SPDX-License-Identifier: Apache-2.0

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
	"slices"
	"testing"

	"github.com/cockroachdb/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/satellite/controllers"
	"github.com/cozystack/blockstor/pkg/storage"
)

// TestU32_SnapshotDeleteIdempotentRetryUntilBackingGone pins the
// upstream U32/U317/U440/U437 family: a snapshot delete that fails
// ONCE because the backing object is transiently busy (open fd / zfs
// hold / lvm temporary lock) must NOT latch the Snapshot in DELETING
// forever. The satellite reconciler keeps the finalizer in place and
// requeues, so the next reconcile pass re-attempts the backend tear-down
// — and once the backing object is freed the delete converges and the
// finalizer is stripped, making the snapshot name reusable.
//
// The upstream bug was that the controller marked the snapshot DELETING,
// the first backend lvremove/zfs-destroy failed (busy), and nothing ever
// retried — the snapshot row stuck, orphans accumulated, and the name
// could never be reused. blockstor's handleDelete returns Requeue=true on
// a non-Ok DeleteSnapshot and keeps the finalizer, which is the
// idempotent-retry contract this test guards.
func TestU32_SnapshotDeleteIdempotentRetryUntilBackingGone(t *testing.T) {
	t.Parallel()

	scheme := newSnapshotScheme(t)
	now := metav1.Now()

	snap := &blockstoriov1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "ccu3-u32.snap",
			Finalizers:        []string{controllers.SatelliteSnapshotFinalizer},
			DeletionTimestamp: &now,
		},
		Spec: blockstoriov1alpha1.SnapshotSpec{
			ResourceDefinitionName: "ccu3-u32",
			SnapshotName:           "snap",
			Nodes:                  []string{"n1"},
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(snap).
		Build()

	fx := storage.NewFakeExec()
	// Apply seed: parent volume lvs returns empty so CreateVolume runs.
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/ccu3-u32_00000",
		storage.FakeResponse{Stdout: []byte("")})
	// Snapshot LV exists, so lvremove fires (the busy probe is the
	// existence pre-check; the snapshot is on disk and held busy).
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/ccu3-u32_snap_00000",
		storage.FakeResponse{Stdout: []byte("ccu3-u32_snap_00000\n")})

	// PASS 1: lvremove fails — backing transiently busy (e.g. a consumer
	// still has the snapshot block device open, or an lvm metadata lock).
	lvremoveCmd := "lvremove --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --force vg/ccu3-u32_snap_00000"
	fx.Expect(lvremoveCmd, storage.FakeResponse{
		Err: errors.New("Logical volume vg/ccu3-u32_snap_00000 in use."),
	})

	rec := seedThinResource(t, fx, "ccu3-u32", "thin1")

	reconciler := &controllers.SnapshotReconciler{
		Client: cli,
		Config: controllers.Config{
			NodeName: "n1",
			Apply:    rec,
			Exec:     fx,
		},
	}

	res, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "ccu3-u32.snap"},
	})
	if err != nil {
		t.Fatalf("Reconcile pass 1: %v", err)
	}

	// The busy delete MUST requeue (idempotent retry), NOT give up.
	if !res.Requeue && res.RequeueAfter == 0 {
		t.Errorf("U32: busy snapshot delete did not requeue (would latch DELETING "+
			"forever); result=%+v", res)
	}

	// The finalizer MUST still be present so the CRD stays pinned until
	// the backend tear-down actually succeeds — never reaped with the LV
	// still on disk.
	got := getSnapU32(t, cli, "ccu3-u32.snap")
	if !slices.Contains(got.Finalizers, controllers.SatelliteSnapshotFinalizer) {
		t.Fatalf("U32: finalizer stripped on a FAILED delete — CRD would vanish "+
			"with the backing LV still present; finalizers=%v", got.Finalizers)
	}

	// PASS 2: the backing is freed (consumer closed the fd / lock
	// released). lvremove now succeeds. Re-register the command as a
	// success and reconcile again — the delete must converge.
	fx.Expect(lvremoveCmd, storage.FakeResponse{Stdout: []byte("")})

	_, err = reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "ccu3-u32.snap"},
	})
	if err != nil {
		t.Fatalf("Reconcile pass 2: %v", err)
	}

	// lvremove MUST have fired (at least once on each pass).
	if !slices.Contains(fx.CommandLines(), lvremoveCmd) {
		t.Errorf("U32: lvremove never re-attempted on the retry pass; cmds=%v",
			fx.CommandLines())
	}

	// Finalizer MUST now be stripped — the backend object is gone, so the
	// apiserver can finalise the CRD and the snapshot name is reusable.
	got = getSnapU32(t, cli, "ccu3-u32.snap")
	if got != nil && slices.Contains(got.Finalizers, controllers.SatelliteSnapshotFinalizer) {
		t.Errorf("U32: finalizer still present after the backing was freed — "+
			"name not reusable; finalizers=%v", got.Finalizers)
	}
}

// getSnapU32 returns the Snapshot or nil when it has been fully reaped
// (the fake client garbage-collects an object once its last finalizer is
// stripped under a DeletionTimestamp).
func getSnapU32(t *testing.T, cli client.Client, name string) *blockstoriov1alpha1.Snapshot {
	t.Helper()

	var got blockstoriov1alpha1.Snapshot

	err := cli.Get(context.Background(), client.ObjectKey{Name: name}, &got)
	if err != nil {
		return nil
	}

	return &got
}
