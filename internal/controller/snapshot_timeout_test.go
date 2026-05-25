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
	"slices"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/internal/controller"
)

// suspendedSnapshot builds a Snapshot frozen mid-suspend (SuspendIo=true,
// not all-Ready) with the supplied creation age and per-node status, so
// the timeout tests can pin the deadline behaviour without dragging in
// the b353 group boilerplate.
func suspendedSnapshot(
	name, rd, snap string,
	createdAgo time.Duration,
	nodes []string,
	takeSnapshot bool,
	nodeStatus []blockstoriov1alpha1.SnapshotPerNodeStatus,
) *blockstoriov1alpha1.Snapshot {
	return &blockstoriov1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			CreationTimestamp: metav1.NewTime(time.Now().Add(-createdAgo)),
		},
		Spec: blockstoriov1alpha1.SnapshotSpec{
			ResourceDefinitionName: rd,
			SnapshotName:           snap,
			Nodes:                  nodes,
			SuspendIo:              true,
			TakeSnapshot:           takeSnapshot,
		},
		Status: blockstoriov1alpha1.SnapshotStatus{
			NodeStatus: nodeStatus,
		},
	}
}

// upToDateResource builds a diskful Resource CRD whose single volume
// reports the given DRBD disk state. metadata.name follows the
// <rd>.<node> composite-key convention every Resource CRD uses.
func resourceWithDiskState(rd, node, diskState string) *blockstoriov1alpha1.Resource {
	return &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: rd + "." + node},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: rd,
			NodeName:               node,
		},
		Status: blockstoriov1alpha1.ResourceStatus{
			Volumes: []blockstoriov1alpha1.ResourceVolumeStatus{
				{VolumeNumber: 0, DiskState: diskState},
			},
		},
	}
}

// TestSnapshotSuspendTimeoutAbortsAndResumes pins the PRIMARY outage
// fix: a Snapshot stuck in SuspendIo past snapshotSuspendDeadline with
// not-all-Ready aborts — the controller clears SuspendIo (so satellites
// resume-io) and stamps the FAILED_DISCONNECT terminal reason. Without
// this the volume's I/O stays frozen forever waiting on a take that
// never reports back.
func TestSnapshotSuspendTimeoutAbortsAndResumes(t *testing.T) {
	t.Parallel()

	scheme := newSnapshotControllerScheme(t)

	// Frozen 3 minutes ago (> 2m deadline); n1 acked, n2 silently hung
	// (never acked, never stamped Failed).
	snap := suspendedSnapshot("pvc-1.snap-1", "pvc-1", "snap-1",
		3*time.Minute, []string{"n1", "n2"}, false,
		[]blockstoriov1alpha1.SnapshotPerNodeStatus{
			{NodeName: "n1", SuspendIoAcked: true},
		})

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

	got := getSnap(t, cli, "pvc-1.snap-1")

	if got.Spec.SuspendIo {
		t.Errorf("timeout abort did not clear SuspendIo (I/O still frozen): %+v", got.Spec)
	}

	if got.Spec.TakeSnapshot {
		t.Errorf("timeout abort did not clear TakeSnapshot: %+v", got.Spec)
	}

	if !slices.Contains(got.Status.Flags, blockstoriov1alpha1.SnapshotStatusFlagFailedDisconnect) {
		t.Errorf("timeout abort did not stamp FAILED_DISCONNECT: flags=%v", got.Status.Flags)
	}
}

// TestSnapshotSuspendBeforeDeadlineNotAborted pins the no-premature-
// resume guard: a Snapshot frozen for LESS than the deadline must NOT
// be aborted — SuspendIo stays true and no FAILED flag is stamped so a
// slow-but-healthy take is not killed early.
func TestSnapshotSuspendBeforeDeadlineNotAborted(t *testing.T) {
	t.Parallel()

	scheme := newSnapshotControllerScheme(t)

	// Frozen only 30s ago — well under the 2m deadline.
	snap := suspendedSnapshot("pvc-1.snap-1", "pvc-1", "snap-1",
		30*time.Second, []string{"n1", "n2"}, true,
		[]blockstoriov1alpha1.SnapshotPerNodeStatus{
			{NodeName: "n1", SuspendIoAcked: true},
			{NodeName: "n2", SuspendIoAcked: true},
		})

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

	got := getSnap(t, cli, "pvc-1.snap-1")

	if !got.Spec.SuspendIo {
		t.Errorf("premature abort: SuspendIo cleared before deadline: %+v", got.Spec)
	}

	if slices.Contains(got.Status.Flags, blockstoriov1alpha1.SnapshotStatusFlagFailedDisconnect) {
		t.Errorf("premature FAILED_DISCONNECT stamp before deadline: flags=%v", got.Status.Flags)
	}
}

// TestSnapshotSuspendSetsRequeueBeforeDeadline pins the RequeueAfter
// wiring: while frozen-and-not-done the controller MUST return a
// bounded RequeueAfter so the deadline is actually re-evaluated even
// with no further events (a silently-hung take emits none). Without
// the requeue the timeout abort would never fire.
func TestSnapshotSuspendSetsRequeueBeforeDeadline(t *testing.T) {
	t.Parallel()

	scheme := newSnapshotControllerScheme(t)

	// Phase 1 in flight, one node not yet acked → no Spec flip this
	// pass, so the only way the deadline gets re-checked is a requeue.
	snap := suspendedSnapshot("pvc-1.snap-1", "pvc-1", "snap-1",
		30*time.Second, []string{"n1", "n2"}, false,
		[]blockstoriov1alpha1.SnapshotPerNodeStatus{
			{NodeName: "n1", SuspendIoAcked: true},
		})

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Snapshot{}).
		WithObjects(snap).
		Build()

	r := &controller.SnapshotReconciler{Client: cli, Scheme: scheme}

	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pvc-1.snap-1"},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if res.RequeueAfter <= 0 {
		t.Fatalf("expected a positive RequeueAfter while frozen, got %v", res.RequeueAfter)
	}

	// Bounded by the 15s cap so the deadline is reached promptly.
	if res.RequeueAfter > 15*time.Second {
		t.Errorf("RequeueAfter %v exceeds the 15s cap", res.RequeueAfter)
	}
}

// TestSnapshotPhase2AbortsOnNonUpToDate pins the consistency guard: at
// the Phase 1 -> 2 gate (every node acked the suspend), if any targeted
// diskful replica is NOT UpToDate the controller aborts+resumes rather
// than promote TakeSnapshot=true on a torn device.
func TestSnapshotPhase2AbortsOnNonUpToDate(t *testing.T) {
	t.Parallel()

	scheme := newSnapshotControllerScheme(t)

	// Every node acked the suspend → nextPhase would promote to
	// TakeSnapshot=true. But n2's replica is SyncTarget (not UpToDate).
	snap := suspendedSnapshot("pvc-1.snap-1", "pvc-1", "snap-1",
		20*time.Second, []string{"n1", "n2"}, false,
		[]blockstoriov1alpha1.SnapshotPerNodeStatus{
			{NodeName: "n1", SuspendIoAcked: true},
			{NodeName: "n2", SuspendIoAcked: true},
		})

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Snapshot{}).
		WithObjects(snap,
			resourceWithDiskState("pvc-1", "n1", "UpToDate"),
			resourceWithDiskState("pvc-1", "n2", "SyncTarget")).
		Build()

	r := &controller.SnapshotReconciler{Client: cli, Scheme: scheme}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pvc-1.snap-1"},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got := getSnap(t, cli, "pvc-1.snap-1")

	if got.Spec.TakeSnapshot {
		t.Errorf("Phase 2 promoted with a non-UpToDate replica: %+v", got.Spec)
	}

	if got.Spec.SuspendIo {
		t.Errorf("non-UpToDate abort did not clear SuspendIo (I/O still frozen): %+v", got.Spec)
	}

	if !slices.Contains(got.Status.Flags, blockstoriov1alpha1.SnapshotStatusFlagFailedDisconnect) {
		t.Errorf("non-UpToDate abort did not stamp FAILED_DISCONNECT: flags=%v", got.Status.Flags)
	}
}

// TestSnapshotPhase2PromotesWhenAllUpToDate pins the positive case of
// the consistency gate: when every targeted diskful replica IS UpToDate
// the Phase 1 -> 2 promotion proceeds normally (TakeSnapshot=true), so
// the happy path is not regressed by the new gate.
func TestSnapshotPhase2PromotesWhenAllUpToDate(t *testing.T) {
	t.Parallel()

	scheme := newSnapshotControllerScheme(t)

	snap := suspendedSnapshot("pvc-1.snap-1", "pvc-1", "snap-1",
		20*time.Second, []string{"n1", "n2"}, false,
		[]blockstoriov1alpha1.SnapshotPerNodeStatus{
			{NodeName: "n1", SuspendIoAcked: true},
			{NodeName: "n2", SuspendIoAcked: true},
		})

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Snapshot{}).
		WithObjects(snap,
			resourceWithDiskState("pvc-1", "n1", "UpToDate"),
			resourceWithDiskState("pvc-1", "n2", "UpToDate")).
		Build()

	r := &controller.SnapshotReconciler{Client: cli, Scheme: scheme}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pvc-1.snap-1"},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got := getSnap(t, cli, "pvc-1.snap-1")

	if !got.Spec.TakeSnapshot {
		t.Errorf("Phase 2 not promoted despite every replica UpToDate: %+v", got.Spec)
	}

	if slices.Contains(got.Status.Flags, blockstoriov1alpha1.SnapshotStatusFlagFailedDisconnect) {
		t.Errorf("spurious FAILED_DISCONNECT on a UpToDate snapshot: flags=%v", got.Status.Flags)
	}
}

// TestSnapshotTimeoutCascadesAcrossGroup pins that the timeout abort
// propagates across the whole GroupID batch exactly like the per-node-
// Failed abort cascade: a grouped Snapshot past the deadline drains
// SuspendIo on every sibling, not just the timed-out one — otherwise
// the un-timed-out siblings' frozen peers would never resume.
func TestSnapshotTimeoutCascadesAcrossGroup(t *testing.T) {
	t.Parallel()

	scheme := newSnapshotControllerScheme(t)

	const groupID = "gt1"

	// Whole batch created 3 minutes ago (> 2m deadline) and still
	// mid-suspend. a + b acked, c never reported (silently hung).
	mk := func(rd string, nodes []string, ns []blockstoriov1alpha1.SnapshotPerNodeStatus) *blockstoriov1alpha1.Snapshot {
		s := b353GroupedSnapshot(rd, "snap", groupID, nodes, true, false, ns)
		s.CreationTimestamp = metav1.NewTime(time.Now().Add(-3 * time.Minute))

		return s
	}

	a := mk("pvc-a", []string{"n1"}, []blockstoriov1alpha1.SnapshotPerNodeStatus{
		{NodeName: "n1", SuspendIoAcked: true},
	})
	b := mk("pvc-b", []string{"n2"}, []blockstoriov1alpha1.SnapshotPerNodeStatus{
		{NodeName: "n2", SuspendIoAcked: true},
	})
	c := mk("pvc-c", []string{"n3"}, nil)

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Snapshot{}).
		WithObjects(a, b, c).
		Build()

	r := &controller.SnapshotReconciler{Client: cli, Scheme: scheme}

	// Reconciling ANY sibling must cascade the timeout abort across
	// the whole group.
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pvc-a.snap"},
	})
	if err != nil {
		t.Fatalf("Reconcile pvc-a.snap: %v", err)
	}

	for _, name := range []string{"pvc-a.snap", "pvc-b.snap", "pvc-c.snap"} {
		got := getSnap(t, cli, name)
		if got.Spec.SuspendIo {
			t.Errorf("%s: timeout abort cascade did not clear SuspendIo: %+v", name, got.Spec)
		}

		if !slices.Contains(got.Status.Flags, blockstoriov1alpha1.SnapshotStatusFlagFailedDisconnect) {
			t.Errorf("%s: timeout abort cascade did not stamp FAILED_DISCONNECT: flags=%v",
				name, got.Status.Flags)
		}
	}
}
