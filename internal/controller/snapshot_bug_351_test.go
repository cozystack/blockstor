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

package controller_test

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/internal/controller"
)

// newSnapshotControllerScheme is the runtime.Scheme used by the
// Bug-351 controller-side orchestration tests.
func newSnapshotControllerScheme(t *testing.T) *runtime.Scheme {
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

// TestSnapshotControllerPhase1StampsSuspendIo pins the
// controller-side Phase 1 promotion: a brand-new Snapshot
// (SuspendIo=false, TakeSnapshot=false, empty NodeStatus)
// transitions to SuspendIo=true so the satellites kick off the
// suspend broadcast. In production the apiserver already stamps
// SuspendIo=true at Create time, but the controller must idempotently
// re-stamp it if a hand-crafted CRD ever lands with the flag cleared.
func TestSnapshotControllerPhase1StampsSuspendIo(t *testing.T) {
	t.Parallel()

	scheme := newSnapshotControllerScheme(t)

	snap := &blockstoriov1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.snap-1"},
		Spec: blockstoriov1alpha1.SnapshotSpec{
			ResourceDefinitionName: "pvc-1",
			SnapshotName:           "snap-1",
			Nodes:                  []string{"n1", "n2"},
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(snap).
		Build()

	r := &controller.SnapshotReconciler{Client: cli, Scheme: scheme}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
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

	if !got.Spec.SuspendIo {
		t.Errorf("Spec.SuspendIo: got false, want true (Phase 1)")
	}

	if got.Spec.TakeSnapshot {
		t.Errorf("Spec.TakeSnapshot: got true, want false (still Phase 1)")
	}
}

// TestSnapshotControllerPhase2StampsTakeSnapshot pins the
// Phase-1→Phase-2 promotion: once every targeted node has stamped
// SuspendIoAcked=true, the controller flips Spec.TakeSnapshot=true
// so the satellites dispatch their local provider.CreateSnapshot.
func TestSnapshotControllerPhase2StampsTakeSnapshot(t *testing.T) {
	t.Parallel()

	scheme := newSnapshotControllerScheme(t)

	snap := &blockstoriov1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.snap-1"},
		Spec: blockstoriov1alpha1.SnapshotSpec{
			ResourceDefinitionName: "pvc-1",
			SnapshotName:           "snap-1",
			Nodes:                  []string{"n1", "n2"},
			SuspendIo:              true,
		},
		Status: blockstoriov1alpha1.SnapshotStatus{
			NodeStatus: []blockstoriov1alpha1.SnapshotPerNodeStatus{
				{NodeName: "n1", SuspendIoAcked: true},
				{NodeName: "n2", SuspendIoAcked: true},
			},
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Snapshot{}).
		WithObjects(snap).
		Build()

	r := &controller.SnapshotReconciler{Client: cli, Scheme: scheme}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
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

	if !got.Spec.SuspendIo {
		t.Errorf("Spec.SuspendIo dropped during Phase 2 promotion: %+v", got.Spec)
	}

	if !got.Spec.TakeSnapshot {
		t.Errorf("Spec.TakeSnapshot: got false, want true (Phase 2 promotion did not fire)")
	}
}

// TestSnapshotControllerPhase2WaitsForAllAcks pins the
// every-node-must-ack gate: when only one of two targeted nodes
// has stamped SuspendIoAcked=true, the controller MUST NOT
// promote to Phase 2 — otherwise the un-acked sibling would
// dispatch provider.CreateSnapshot before its DRBD layer was
// frozen, defeating the whole barrier.
func TestSnapshotControllerPhase2WaitsForAllAcks(t *testing.T) {
	t.Parallel()

	scheme := newSnapshotControllerScheme(t)

	snap := &blockstoriov1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.snap-1"},
		Spec: blockstoriov1alpha1.SnapshotSpec{
			ResourceDefinitionName: "pvc-1",
			SnapshotName:           "snap-1",
			Nodes:                  []string{"n1", "n2"},
			SuspendIo:              true,
		},
		Status: blockstoriov1alpha1.SnapshotStatus{
			NodeStatus: []blockstoriov1alpha1.SnapshotPerNodeStatus{
				{NodeName: "n1", SuspendIoAcked: true},
				// n2 hasn't reported yet.
			},
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Snapshot{}).
		WithObjects(snap).
		Build()

	r := &controller.SnapshotReconciler{Client: cli, Scheme: scheme}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
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

	if got.Spec.TakeSnapshot {
		t.Errorf("controller promoted to Phase 2 before every node acked: %+v",
			got.Status.NodeStatus)
	}

	if !got.Spec.SuspendIo {
		t.Errorf("Spec.SuspendIo dropped while still Phase 1: %+v", got.Spec)
	}
}

// TestSnapshotControllerPhase3ClearsSuspendOnSuccess pins the
// happy-path Phase-3 drain: once every targeted node has stamped
// Ready=true, the controller clears Spec.SuspendIo (+ implicit
// TakeSnapshot reset) so the satellites issue `drbdsetup resume-io`.
func TestSnapshotControllerPhase3ClearsSuspendOnSuccess(t *testing.T) {
	t.Parallel()

	scheme := newSnapshotControllerScheme(t)

	snap := &blockstoriov1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.snap-1"},
		Spec: blockstoriov1alpha1.SnapshotSpec{
			ResourceDefinitionName: "pvc-1",
			SnapshotName:           "snap-1",
			Nodes:                  []string{"n1", "n2"},
			SuspendIo:              true,
			TakeSnapshot:           true,
		},
		Status: blockstoriov1alpha1.SnapshotStatus{
			NodeStatus: []blockstoriov1alpha1.SnapshotPerNodeStatus{
				{NodeName: "n1", SuspendIoAcked: true, Ready: true, CreateTimestamp: 1},
				{NodeName: "n2", SuspendIoAcked: true, Ready: true, CreateTimestamp: 2},
			},
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Snapshot{}).
		WithObjects(snap).
		Build()

	r := &controller.SnapshotReconciler{Client: cli, Scheme: scheme}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
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

	if got.Spec.SuspendIo {
		t.Errorf("Spec.SuspendIo: got true after Phase 3 drain, want false")
	}

	if got.Spec.TakeSnapshot {
		t.Errorf("Spec.TakeSnapshot: got true after Phase 3 drain, want false")
	}
}

// TestSnapshotControllerAbortPathClearsSuspendOnFailure pins the
// abort drain: any per-node Failed=true forces the controller
// straight into Phase 3 (clear SuspendIo) regardless of where in
// the suspend/take sequence we currently are. Without this, a
// failure during Phase 1 on one node would leave the acked
// siblings frozen forever.
func TestSnapshotControllerAbortPathClearsSuspendOnFailure(t *testing.T) {
	t.Parallel()

	scheme := newSnapshotControllerScheme(t)

	// Phase 1 in flight — n1 acked, n2 hit a terminal error and
	// stamped Failed=true. The controller MUST abort: clearing
	// SuspendIo so n1 (frozen) drains rather than waiting on n2's
	// never-coming ack.
	snap := &blockstoriov1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.snap-1"},
		Spec: blockstoriov1alpha1.SnapshotSpec{
			ResourceDefinitionName: "pvc-1",
			SnapshotName:           "snap-1",
			Nodes:                  []string{"n1", "n2"},
			SuspendIo:              true,
		},
		Status: blockstoriov1alpha1.SnapshotStatus{
			NodeStatus: []blockstoriov1alpha1.SnapshotPerNodeStatus{
				{NodeName: "n1", SuspendIoAcked: true},
				{NodeName: "n2", Failed: true},
			},
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Snapshot{}).
		WithObjects(snap).
		Build()

	r := &controller.SnapshotReconciler{Client: cli, Scheme: scheme}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
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

	if got.Spec.SuspendIo {
		t.Errorf("abort path did not clear Spec.SuspendIo: %+v", got.Spec)
	}

	if got.Spec.TakeSnapshot {
		t.Errorf("abort path did not clear Spec.TakeSnapshot: %+v", got.Spec)
	}
}

// TestSnapshotControllerTerminalStateNoOp pins the steady-state
// short-circuit: once Phase 3 has drained (SuspendIo=false,
// every node SuspendIoAcked=false), the controller MUST stop
// touching the Spec — otherwise we'd loop the orchestration
// forever, re-firing suspend-io on a long-since-completed
// snapshot.
func TestSnapshotControllerTerminalStateNoOp(t *testing.T) {
	t.Parallel()

	scheme := newSnapshotControllerScheme(t)

	snap := &blockstoriov1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-1.snap-1"},
		Spec: blockstoriov1alpha1.SnapshotSpec{
			ResourceDefinitionName: "pvc-1",
			SnapshotName:           "snap-1",
			Nodes:                  []string{"n1"},
		},
		Status: blockstoriov1alpha1.SnapshotStatus{
			NodeStatus: []blockstoriov1alpha1.SnapshotPerNodeStatus{
				{NodeName: "n1", Ready: true, CreateTimestamp: 1, SuspendIoAcked: false},
			},
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Snapshot{}).
		WithObjects(snap).
		Build()

	r := &controller.SnapshotReconciler{Client: cli, Scheme: scheme}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
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

	if got.Spec.SuspendIo {
		t.Errorf("terminal-state reconcile re-stamped SuspendIo: %+v", got.Spec)
	}

	if got.Spec.TakeSnapshot {
		t.Errorf("terminal-state reconcile re-stamped TakeSnapshot: %+v", got.Spec)
	}
}
