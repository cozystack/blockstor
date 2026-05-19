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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/drbd"
	"github.com/cozystack/blockstor/pkg/satellite"
	"github.com/cozystack/blockstor/pkg/satellite/controllers"
	"github.com/cozystack/blockstor/pkg/storage"
)

// newBug351Scheme is the runtime.Scheme used by the Bug-351
// orchestration tests — corev1 + the blockstor v1alpha1 group.
func newBug351Scheme(t *testing.T) *runtime.Scheme {
	t.Helper()

	s := runtime.NewScheme()
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("corev1: %v", err)
	}

	if err := blockstoriov1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("blockstor: %v", err)
	}

	return s
}

// TestSnapshotReconcileBug351SuspendsAndAcks pins Phase 1: when
// the controller-side orchestrator stamps Spec.SuspendIo=true, the
// satellite reconciler MUST call `drbdsetup suspend-io <rd>`
// against the parent RD and stamp
// Status.NodeStatus[us].SuspendIoAcked=true.
//
// Without this barrier two diskful replicas would snapshot
// independently and capture divergent bytes while the writer's
// traffic was still streaming through DRBD (Bug 351).
func TestSnapshotReconcileBug351SuspendsAndAcks(t *testing.T) {
	t.Parallel()

	scheme := newBug351Scheme(t)

	snap := &blockstoriov1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "pvc-1.snap-1",
			Finalizers: []string{controllers.SatelliteSnapshotFinalizer},
		},
		Spec: blockstoriov1alpha1.SnapshotSpec{
			ResourceDefinitionName: "pvc-1",
			SnapshotName:           "snap-1",
			Nodes:                  []string{"n1"},
			SuspendIo:              true,
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Snapshot{}).
		WithObjects(snap).
		Build()

	fx := storage.NewFakeExec()
	// drbdsetup suspend-io exit 0.
	fx.Expect("drbdsetup suspend-io pvc-1", storage.FakeResponse{Stdout: []byte("")})

	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{},
		Adm:       drbd.NewAdm(fx),
	})

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

	// drbdsetup suspend-io MUST have fired against the parent RD.
	want := "drbdsetup suspend-io pvc-1"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("missing %q in calls: %v", want, fx.CommandLines())
	}

	// MUST NOT dispatch provider.CreateSnapshot in Phase 1 —
	// take-snapshot is Phase 2 territory.
	for _, line := range fx.CommandLines() {
		if line == "lvcreate" {
			t.Errorf("Phase 1 leaked into Phase 2 (provider.CreateSnapshot fired): %s", line)
		}
	}

	var got blockstoriov1alpha1.Snapshot

	err = cli.Get(context.Background(), client.ObjectKey{Name: "pvc-1.snap-1"}, &got)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if len(got.Status.NodeStatus) != 1 {
		t.Fatalf("Status.NodeStatus: got %d entries, want 1", len(got.Status.NodeStatus))
	}

	if !got.Status.NodeStatus[0].SuspendIoAcked {
		t.Errorf("Status.NodeStatus[0].SuspendIoAcked: got false, want true")
	}

	// Ready MUST NOT be set yet — that's Phase 2.
	if got.Status.NodeStatus[0].Ready {
		t.Errorf("Status.NodeStatus[0].Ready: got true, want false (Phase 1 should not have taken the snapshot)")
	}
}

// TestSnapshotReconcileBug351TakeSnapshotAfterAck pins Phase 2:
// once the orchestrator flipped Spec.TakeSnapshot=true (every
// node has stamped SuspendIoAcked), the satellite MUST dispatch
// provider.CreateSnapshot and stamp Status.NodeStatus[us].Ready.
func TestSnapshotReconcileBug351TakeSnapshotAfterAck(t *testing.T) {
	t.Parallel()

	scheme := newBug351Scheme(t)

	// Phase 1 already acked — Status.NodeStatus reflects the
	// satellite's previous Reconcile pass.
	snap := &blockstoriov1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "pvc-1.snap-1",
			Finalizers: []string{controllers.SatelliteSnapshotFinalizer},
		},
		Spec: blockstoriov1alpha1.SnapshotSpec{
			ResourceDefinitionName: "pvc-1",
			SnapshotName:           "snap-1",
			Nodes:                  []string{"n1"},
			SuspendIo:              true,
			TakeSnapshot:           true,
		},
		Status: blockstoriov1alpha1.SnapshotStatus{
			NodeStatus: []blockstoriov1alpha1.SnapshotPerNodeStatus{
				{NodeName: "n1", SuspendIoAcked: true},
			},
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Snapshot{}).
		WithObjects(snap).
		Build()

	fx := storage.NewFakeExec()
	// Provider.CreateSnapshot path: lvs seed (none) + lvcreate.
	fx.Expect("lvs --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --noheadings -o lv_name vg/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("")})
	fx.Expect("lvcreate --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --snapshot --name pvc-1_snap-1_00000 vg/pvc-1_00000",
		storage.FakeResponse{Stdout: []byte("")})

	rec := seedThinResource(t, fx, "pvc-1", "thin1")

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

	wantCmd := "lvcreate --config devices { filter=['r|^/dev/drbd|','r|^/dev/zd|'] } --snapshot --name pvc-1_snap-1_00000 vg/pvc-1_00000"
	if !slices.Contains(fx.CommandLines(), wantCmd) {
		t.Errorf("provider.CreateSnapshot did not fire in Phase 2: %v", fx.CommandLines())
	}

	// Phase 2 MUST NOT re-fire suspend-io (already acked).
	if slices.Contains(fx.CommandLines(), "drbdsetup suspend-io pvc-1") {
		t.Errorf("Phase 2 re-fired suspend-io after Status ack")
	}

	var got blockstoriov1alpha1.Snapshot

	err = cli.Get(context.Background(), client.ObjectKey{Name: "pvc-1.snap-1"}, &got)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if !got.Status.NodeStatus[0].Ready {
		t.Errorf("Status.NodeStatus[0].Ready: got false, want true")
	}

	// SuspendIoAcked MUST still be true — Phase 3 (resume) is
	// gated on the orchestrator flipping Spec.SuspendIo=false.
	if !got.Status.NodeStatus[0].SuspendIoAcked {
		t.Errorf("Phase 2 dropped SuspendIoAcked prematurely: %+v", got.Status.NodeStatus)
	}
}

// TestSnapshotReconcileBug351ResumesWhenSuspendCleared pins
// Phase 3: when the controller-side orchestrator flips
// Spec.SuspendIo=false (success path OR abort path), the
// satellite MUST call `drbdsetup resume-io <rd>` and clear its
// SuspendIoAcked stamp. Without this drain a partial-success or
// abort leaves application I/O hung forever on the still-frozen
// siblings.
func TestSnapshotReconcileBug351ResumesWhenSuspendCleared(t *testing.T) {
	t.Parallel()

	scheme := newBug351Scheme(t)

	// Orchestrator has flipped SuspendIo=false (success: every
	// node already Ready, or abort: some node Failed). Our local
	// state still has SuspendIoAcked=true — we must drain.
	snap := &blockstoriov1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "pvc-1.snap-1",
			Finalizers: []string{controllers.SatelliteSnapshotFinalizer},
		},
		Spec: blockstoriov1alpha1.SnapshotSpec{
			ResourceDefinitionName: "pvc-1",
			SnapshotName:           "snap-1",
			Nodes:                  []string{"n1"},
			SuspendIo:              false,
			TakeSnapshot:           false,
		},
		Status: blockstoriov1alpha1.SnapshotStatus{
			NodeStatus: []blockstoriov1alpha1.SnapshotPerNodeStatus{
				{NodeName: "n1", Ready: true, CreateTimestamp: 1234, SuspendIoAcked: true},
			},
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Snapshot{}).
		WithObjects(snap).
		Build()

	fx := storage.NewFakeExec()
	fx.Expect("drbdsetup resume-io pvc-1", storage.FakeResponse{Stdout: []byte("")})

	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{},
		Adm:       drbd.NewAdm(fx),
	})

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

	want := "drbdsetup resume-io pvc-1"
	if !slices.Contains(fx.CommandLines(), want) {
		t.Errorf("missing %q in calls: %v", want, fx.CommandLines())
	}

	var got blockstoriov1alpha1.Snapshot

	err = cli.Get(context.Background(), client.ObjectKey{Name: "pvc-1.snap-1"}, &got)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.Status.NodeStatus[0].SuspendIoAcked {
		t.Errorf("Status.NodeStatus[0].SuspendIoAcked: got true after resume, want false")
	}

	// Ready+CreateTimestamp from Phase 2 MUST survive the
	// Phase-3 ack-clear — the snapshot is still successful.
	if !got.Status.NodeStatus[0].Ready {
		t.Errorf("Phase 3 dropped Ready: %+v", got.Status.NodeStatus)
	}

	if got.Status.NodeStatus[0].CreateTimestamp == 0 {
		t.Errorf("Phase 3 dropped CreateTimestamp: %+v", got.Status.NodeStatus)
	}
}

// TestSnapshotReconcileBug351AbortStampsFailedOnSuspendError
// pins the abort path: if `drbdsetup suspend-io` fails, the
// satellite MUST stamp Status.Flags=["FAILED"] +
// Status.NodeStatus[us].Failed=true so the orchestrator drains
// the suspended siblings rather than waiting indefinitely for an
// ack that will never come.
func TestSnapshotReconcileBug351AbortStampsFailedOnSuspendError(t *testing.T) {
	t.Parallel()

	scheme := newBug351Scheme(t)

	snap := &blockstoriov1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "pvc-1.snap-1",
			Finalizers: []string{controllers.SatelliteSnapshotFinalizer},
		},
		Spec: blockstoriov1alpha1.SnapshotSpec{
			ResourceDefinitionName: "pvc-1",
			SnapshotName:           "snap-1",
			Nodes:                  []string{"n1"},
			SuspendIo:              true,
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Snapshot{}).
		WithObjects(snap).
		Build()

	fx := storage.NewFakeExec()
	// drbdsetup suspend-io fails — simulates a missing kernel
	// module or a permanently-broken resource.
	fx.Expect("drbdsetup suspend-io pvc-1", storage.FakeResponse{
		Err: errBug351SuspendBroken,
	})

	rec := satellite.NewReconciler(satellite.ReconcilerConfig{
		Providers: map[string]storage.Provider{},
		Adm:       drbd.NewAdm(fx),
	})

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

	var got blockstoriov1alpha1.Snapshot

	err = cli.Get(context.Background(), client.ObjectKey{Name: "pvc-1.snap-1"}, &got)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if !slices.Contains(got.Status.Flags, blockstoriov1alpha1.SnapshotStatusFlagFailed) {
		t.Errorf("Status.Flags missing FAILED after suspend failure: %+v", got.Status.Flags)
	}

	if len(got.Status.NodeStatus) != 1 || !got.Status.NodeStatus[0].Failed {
		t.Errorf("Status.NodeStatus[us].Failed not stamped: %+v", got.Status.NodeStatus)
	}

	// SuspendIoAcked MUST stay false — we never successfully
	// suspended, so we shouldn't pretend we did.
	if len(got.Status.NodeStatus) == 1 && got.Status.NodeStatus[0].SuspendIoAcked {
		t.Errorf("SuspendIoAcked stamped despite suspend failure: %+v", got.Status.NodeStatus)
	}
}

var errBug351SuspendBroken = bug351Error("drbdsetup suspend-io: kernel module not loaded")

// bug351Error is a typed wrapper so the table-driven assertion
// can distinguish "expected failure" from a real storage stack
// regression. Implements error.
type bug351Error string

func (b bug351Error) Error() string { return string(b) }
