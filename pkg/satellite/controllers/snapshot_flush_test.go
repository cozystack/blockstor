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
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/satellite"
	"github.com/cozystack/blockstor/pkg/satellite/controllers"
	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
	"github.com/cozystack/blockstor/pkg/storage"
	"github.com/cozystack/blockstor/pkg/storage/lvm"
)

// seedThinResourceWithExec mirrors seedThinResource but also wires
// `Exec: fx` onto the ReconcilerConfig so FlushBeforeSuspend's
// `r.cfg.Exec.Run("blockdev", ...)` actually shells out through
// the FakeExec the test asserts against. The plain
// seedThinResource left Exec nil because the pre-flush
// orchestration didn't shell out directly from the satellite
// Reconciler (only the LVM provider's own exec did).
func seedThinResourceWithExec(t *testing.T, fx *storage.FakeExec, resourceName, pool string) *satellite.Reconciler {
	t.Helper()

	thin := lvm.NewThin(lvm.ThinConfig{VolumeGroup: "vg", ThinPool: "tp"}, fx)
	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{pool: thin},
		Exec:      fx,
	})

	_, err := rec.Apply(context.Background(), []*intent.DesiredResource{
		{
			Name: resourceName, NodeName: "n1",
			Volumes: []*intent.DesiredVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024, StoragePool: pool},
			},
		},
	})
	if err != nil {
		t.Fatalf("Apply (seed): %v", err)
	}

	return rec
}

// TestSnapshotReconcileFlushesBackingDeviceBeforeCreate pins the flush
// on a satellite without a DRBD layer: nothing is suspended there, so
// the storage device, which is the top of the stack, is flushed right
// before `provider.CreateSnapshot`. With DRBD the flush moves in front
// of suspend-io and targets the DRBD device instead; see
// TestSnapshotWindowFlushesBeforeSuspendAndNeverWhileSuspended.
func TestSnapshotReconcileFlushesBackingDeviceBeforeCreate(t *testing.T) {
	t.Parallel()

	scheme := newBug351Scheme(t)

	// Phase 1 already acked; orchestrator has stamped Phase 2
	// (TakeSnapshot=true). VolumeDefinitions surfaces volume #0,
	// matching the seedThinResource shape.
	snap := &blockstoriov1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "pvc-1.snap-1",
			Finalizers: []string{controllers.SnapshotNodeFinalizer("n1")},
		},
		Spec: blockstoriov1alpha1.SnapshotSpec{
			ResourceDefinitionName: "pvc-1",
			SnapshotName:           "snap-1",
			Nodes:                  []string{"n1"},
			SuspendIO:              true,
			TakeSnapshot:           true,
			VolumeDefinitions: []blockstoriov1alpha1.SnapshotVolumeRef{
				{VolumeNumber: 0},
			},
		},
		Status: blockstoriov1alpha1.SnapshotStatus{
			NodeStatus: []blockstoriov1alpha1.SnapshotPerNodeStatus{
				{NodeName: "n1", SuspendIOAcked: true},
			},
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Snapshot{}).
		WithObjects(snap).
		Build()

	fx := storage.NewFakeExec()
	// VolumeStatus probe path: lvs with the full --separator
	// shape that volumeStatusViaLVS uses. Stub a real LV row so
	// VolumeStatus returns a non-empty DevicePath and the flush
	// branch fires.
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings --separator | -o lv_path,lv_size --units k --nosuffix vg/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("/dev/vg/pvc-1_00000|1048576\n")})
	// Apply-seed lvs (lv_name-only shape) lands during
	// seedThinResource. Stub empty so CreateVolume fires there.
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("")})
	// Provider.CreateSnapshot path.
	fx.Expect("lvcreate --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --snapshot --name pvc-1_snap-1_00000 vg/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("")})

	rec := seedThinResourceWithExec(t, fx, "pvc-1", "thin1")

	reconciler := &controllers.SnapshotReconciler{
		Client: cli,
		Config: controllers.Config{
			NodeName: "n1",
			Apply:    rec,
			Exec:     fx,
		},
	}

	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pvc-1.snap-1"},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	cmds := fx.CommandLines()

	// blockdev --flushbufs MUST have fired against the backing
	// device path that VolumeStatus returned.
	wantFlush := "blockdev --flushbufs /dev/vg/pvc-1_00000"
	if !slices.Contains(cmds, wantFlush) {
		t.Errorf("missing %q in calls: %v", wantFlush, cmds)
	}

	// And the lvcreate snapshot MUST still fire (flush is in
	// front of CreateSnapshot, not in lieu of).
	wantCreate := "lvcreate --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --snapshot --name pvc-1_snap-1_00000 vg/pvc-1_00000"
	if !slices.Contains(cmds, wantCreate) {
		t.Errorf("missing %q in calls: %v", wantCreate, cmds)
	}

	// Critical ordering: the flush MUST land BEFORE the
	// lvcreate. A regression that flushes AFTER the snapshot
	// would still capture stale bytes — the kernel writeback
	// drain is meaningful only as a pre-snapshot barrier.
	flushIdx := -1
	createIdx := -1

	for i, line := range cmds {
		if line == wantFlush {
			flushIdx = i
		}

		if strings.HasPrefix(line, "lvcreate ") && strings.Contains(line, "--snapshot") {
			createIdx = i
		}
	}

	if flushIdx < 0 || createIdx < 0 {
		t.Fatalf("flush/create indices not both observed: flush=%d, create=%d (cmds=%v)",
			flushIdx, createIdx, cmds)
	}

	if flushIdx > createIdx {
		t.Errorf("flush MUST land before lvcreate --snapshot: flushIdx=%d > createIdx=%d (cmds=%v)",
			flushIdx, createIdx, cmds)
	}
}
