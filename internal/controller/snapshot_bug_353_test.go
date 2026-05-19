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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/internal/controller"
)

// snapshotGroupIDLabel duplicates the well-known label key the
// store-side wireToCRDSnapshot stamps onto same-Group siblings.
// Kept here verbatim so the test does not import the production
// constant from pkg/store/k8s (which would drag the whole store
// package into the controller test build for one literal).
const snapshotGroupIDLabel = "blockstor.io/snapshot-group-id"

// b353GroupedSnapshot builds a Snapshot CRD pre-stamped with the
// shared GroupID + the label the controller uses to List
// siblings. Concentrates the boilerplate so each per-phase
// assertion stays focused on the Spec/Status it actually cares
// about.
func b353GroupedSnapshot(
	rdName, snapName, groupID string,
	nodes []string,
	suspendIo, takeSnapshot bool,
	nodeStatus []blockstoriov1alpha1.SnapshotPerNodeStatus,
) *blockstoriov1alpha1.Snapshot {
	return &blockstoriov1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name: rdName + "." + snapName,
			Labels: map[string]string{
				snapshotGroupIDLabel: groupID,
			},
		},
		Spec: blockstoriov1alpha1.SnapshotSpec{
			ResourceDefinitionName: rdName,
			SnapshotName:           snapName,
			Nodes:                  nodes,
			GroupID:                groupID,
			SuspendIo:              suspendIo,
			TakeSnapshot:           takeSnapshot,
		},
		Status: blockstoriov1alpha1.SnapshotStatus{
			NodeStatus: nodeStatus,
		},
	}
}

// reconcileAll drives one Reconcile pass against every CRD in the
// transactional batch — production has independent watch events
// per Snapshot, so a unit test that pins "phase advancement only
// fires once every sibling has acked" must drive every sibling's
// Reconcile to see the gate enforced on each.
func reconcileAll(
	t *testing.T, r *controller.SnapshotReconciler, names []string,
) {
	t.Helper()

	for _, n := range names {
		_, err := r.Reconcile(context.Background(), ctrl.Request{
			NamespacedName: types.NamespacedName{Name: n},
		})
		if err != nil {
			t.Fatalf("Reconcile %s: %v", n, err)
		}
	}
}

// getSnap is a Get/fatal helper so per-assertion blocks stay
// focused on the field they care about.
func getSnap(t *testing.T, cli client.Client, name string) *blockstoriov1alpha1.Snapshot {
	t.Helper()

	var got blockstoriov1alpha1.Snapshot

	err := cli.Get(context.Background(), client.ObjectKey{Name: name}, &got)
	if err != nil {
		t.Fatalf("Get %s: %v", name, err)
	}

	return &got
}

// TestBug353GroupPhase1WaitsForEverySibling pins the cross-RD
// every-sibling-must-ack gate: a 3-RD transactional batch where
// only one sibling has fully acked the suspend MUST NOT promote
// any sibling to Phase 2 (TakeSnapshot=true). Without this gate
// the first sibling's satellite would dispatch CreateSnapshot
// while the other two RDs are still mid-suspend, capturing
// divergent point-in-time bytes for a DB+WAL group snapshot.
func TestBug353GroupPhase1WaitsForEverySibling(t *testing.T) {
	t.Parallel()

	scheme := newSnapshotControllerScheme(t)

	const groupID = "g1"

	// pvc-a fully acked, pvc-b + pvc-c still mid-suspend.
	a := b353GroupedSnapshot("pvc-a", "snap", groupID,
		[]string{"n1", "n2"}, true, false,
		[]blockstoriov1alpha1.SnapshotPerNodeStatus{
			{NodeName: "n1", SuspendIoAcked: true},
			{NodeName: "n2", SuspendIoAcked: true},
		})
	b := b353GroupedSnapshot("pvc-b", "snap", groupID,
		[]string{"n1", "n2"}, true, false,
		[]blockstoriov1alpha1.SnapshotPerNodeStatus{
			{NodeName: "n1", SuspendIoAcked: true},
			// n2 has not reported yet.
		})
	c := b353GroupedSnapshot("pvc-c", "snap", groupID,
		[]string{"n1"}, true, false, nil)

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Snapshot{}).
		WithObjects(a, b, c).
		Build()

	r := &controller.SnapshotReconciler{Client: cli, Scheme: scheme}

	reconcileAll(t, r, []string{"pvc-a.snap", "pvc-b.snap", "pvc-c.snap"})

	for _, name := range []string{"pvc-a.snap", "pvc-b.snap", "pvc-c.snap"} {
		got := getSnap(t, cli, name)

		if got.Spec.TakeSnapshot {
			t.Errorf("%s: Phase 2 fired before group fully acked: %+v", name, got.Spec)
		}

		if !got.Spec.SuspendIo {
			t.Errorf("%s: Phase 1 SuspendIo cleared while still mid-batch: %+v", name, got.Spec)
		}
	}
}

// TestBug353GroupAllSiblingsAckedPromotesEntireGroup pins the
// successful Phase 1 → 2 transition: once every sibling's every
// targeted node has acked, EVERY sibling flips TakeSnapshot=true
// (not just the one whose Reconcile triggered the check). The
// controller drives the flip per-Reconcile, so the test pins both
// the per-sibling result AND that the gate evaluates the union of
// every sibling's node ack state.
func TestBug353GroupAllSiblingsAckedPromotesEntireGroup(t *testing.T) {
	t.Parallel()

	scheme := newSnapshotControllerScheme(t)

	const groupID = "g2"

	a := b353GroupedSnapshot("pvc-a", "snap", groupID,
		[]string{"n1"}, true, false,
		[]blockstoriov1alpha1.SnapshotPerNodeStatus{
			{NodeName: "n1", SuspendIoAcked: true},
		})
	b := b353GroupedSnapshot("pvc-b", "snap", groupID,
		[]string{"n2"}, true, false,
		[]blockstoriov1alpha1.SnapshotPerNodeStatus{
			{NodeName: "n2", SuspendIoAcked: true},
		})
	c := b353GroupedSnapshot("pvc-c", "snap", groupID,
		[]string{"n3"}, true, false,
		[]blockstoriov1alpha1.SnapshotPerNodeStatus{
			{NodeName: "n3", SuspendIoAcked: true},
		})

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Snapshot{}).
		WithObjects(a, b, c).
		Build()

	r := &controller.SnapshotReconciler{Client: cli, Scheme: scheme}

	reconcileAll(t, r, []string{"pvc-a.snap", "pvc-b.snap", "pvc-c.snap"})

	for _, name := range []string{"pvc-a.snap", "pvc-b.snap", "pvc-c.snap"} {
		got := getSnap(t, cli, name)
		if !got.Spec.TakeSnapshot {
			t.Errorf("%s: TakeSnapshot not flipped after every sibling acked: %+v",
				name, got.Spec)
		}

		if !got.Spec.SuspendIo {
			t.Errorf("%s: SuspendIo dropped during Phase 1→2 promotion: %+v",
				name, got.Spec)
		}
	}
}

// TestBug353GroupAllSiblingsReadyDrainsEntireGroup pins the
// successful Phase 2 → 3 transition: once every sibling's every
// targeted node has stamped Ready=true, EVERY sibling drains
// (Spec.SuspendIo=false, Spec.TakeSnapshot=false) so the
// satellites issue resume-io. Without the cross-sibling gate, a
// 3-RD batch where pvc-a completed CreateSnapshot first would
// drain pvc-a's suspend mid-batch — pvc-a's satellite would
// resume I/O while pvc-b/pvc-c were still mid-CreateSnapshot,
// breaking the cross-RD point-in-time consistency that the
// b353 barrier is supposed to guarantee.
func TestBug353GroupAllSiblingsReadyDrainsEntireGroup(t *testing.T) {
	t.Parallel()

	scheme := newSnapshotControllerScheme(t)

	const groupID = "g3"

	a := b353GroupedSnapshot("pvc-a", "snap", groupID,
		[]string{"n1"}, true, true,
		[]blockstoriov1alpha1.SnapshotPerNodeStatus{
			{NodeName: "n1", SuspendIoAcked: true, Ready: true, CreateTimestamp: 1},
		})
	b := b353GroupedSnapshot("pvc-b", "snap", groupID,
		[]string{"n2"}, true, true,
		[]blockstoriov1alpha1.SnapshotPerNodeStatus{
			{NodeName: "n2", SuspendIoAcked: true, Ready: true, CreateTimestamp: 2},
		})
	c := b353GroupedSnapshot("pvc-c", "snap", groupID,
		[]string{"n3"}, true, true,
		[]blockstoriov1alpha1.SnapshotPerNodeStatus{
			{NodeName: "n3", SuspendIoAcked: true, Ready: true, CreateTimestamp: 3},
		})

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Snapshot{}).
		WithObjects(a, b, c).
		Build()

	r := &controller.SnapshotReconciler{Client: cli, Scheme: scheme}

	reconcileAll(t, r, []string{"pvc-a.snap", "pvc-b.snap", "pvc-c.snap"})

	for _, name := range []string{"pvc-a.snap", "pvc-b.snap", "pvc-c.snap"} {
		got := getSnap(t, cli, name)
		if got.Spec.SuspendIo {
			t.Errorf("%s: SuspendIo not cleared after every sibling Ready: %+v",
				name, got.Spec)
		}

		if got.Spec.TakeSnapshot {
			t.Errorf("%s: TakeSnapshot not cleared after every sibling Ready: %+v",
				name, got.Spec)
		}
	}
}

// TestBug353GroupAbortCascadesOnSiblingFailure pins the abort
// cascade: any per-node Failed=true on ANY sibling triggers
// SuspendIo=false on EVERY sibling (not just the failed one).
// Without the cascade, the un-failed siblings would stay in Phase
// 1 waiting for the doomed sibling to ack, and the already-
// suspended satellite peers of the un-failed siblings would never
// resume I/O.
func TestBug353GroupAbortCascadesOnSiblingFailure(t *testing.T) {
	t.Parallel()

	scheme := newSnapshotControllerScheme(t)

	const groupID = "g4"

	// pvc-a fully acked + ready in flight, pvc-b acked, pvc-c
	// hit a terminal failure mid-Phase-1.
	a := b353GroupedSnapshot("pvc-a", "snap", groupID,
		[]string{"n1"}, true, false,
		[]blockstoriov1alpha1.SnapshotPerNodeStatus{
			{NodeName: "n1", SuspendIoAcked: true},
		})
	b := b353GroupedSnapshot("pvc-b", "snap", groupID,
		[]string{"n2"}, true, false,
		[]blockstoriov1alpha1.SnapshotPerNodeStatus{
			{NodeName: "n2", SuspendIoAcked: true},
		})
	c := b353GroupedSnapshot("pvc-c", "snap", groupID,
		[]string{"n3"}, true, false,
		[]blockstoriov1alpha1.SnapshotPerNodeStatus{
			{NodeName: "n3", Failed: true},
		})

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Snapshot{}).
		WithObjects(a, b, c).
		Build()

	r := &controller.SnapshotReconciler{Client: cli, Scheme: scheme}

	// Reconciling ANY sibling must cascade abort across the
	// whole group — driving Reconcile on pvc-a alone is enough
	// because the controller-side reconciler now Lists siblings
	// and clears SuspendIo on every one of them.
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pvc-a.snap"},
	})
	if err != nil {
		t.Fatalf("Reconcile pvc-a.snap: %v", err)
	}

	for _, name := range []string{"pvc-a.snap", "pvc-b.snap", "pvc-c.snap"} {
		got := getSnap(t, cli, name)
		if got.Spec.SuspendIo {
			t.Errorf("%s: abort cascade did not clear SuspendIo: %+v", name, got.Spec)
		}

		if got.Spec.TakeSnapshot {
			t.Errorf("%s: abort cascade did not clear TakeSnapshot: %+v", name, got.Spec)
		}
	}
}

// TestBug353EmptyGroupIDPreservesSingleSnapPath pins backward
// compatibility: a Snapshot with empty Spec.GroupID MUST behave
// exactly like the b351 single-snap orchestrator — the sibling
// fan-out collapses to {self} and the phase gates reduce to the
// self-only walks. Pins the Phase 1 → 2 promotion for a
// single-snap CRD with every node acked.
func TestBug353EmptyGroupIDPreservesSingleSnapPath(t *testing.T) {
	t.Parallel()

	scheme := newSnapshotControllerScheme(t)

	// Empty GroupID, no group label — pure b351 single-snap
	// shape. Every node acked → controller should promote to
	// TakeSnapshot=true exactly like the standalone b351 test.
	snap := &blockstoriov1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-solo.snap"},
		Spec: blockstoriov1alpha1.SnapshotSpec{
			ResourceDefinitionName: "pvc-solo",
			SnapshotName:           "snap",
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
		NamespacedName: types.NamespacedName{Name: "pvc-solo.snap"},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got := getSnap(t, cli, "pvc-solo.snap")
	if got.Spec.GroupID != "" {
		t.Errorf("empty-GroupID reconcile stamped a GroupID: %q", got.Spec.GroupID)
	}

	if !got.Spec.TakeSnapshot {
		t.Errorf("single-snap path: TakeSnapshot not flipped after every node acked: %+v",
			got.Spec)
	}

	if !got.Spec.SuspendIo {
		t.Errorf("single-snap path: SuspendIo dropped during Phase 1→2 promotion: %+v",
			got.Spec)
	}
}
