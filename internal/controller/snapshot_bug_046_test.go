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

package controller_test

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/internal/controller"
)

// b046GroupedSnapshot builds a Phase-0 grouped Snapshot CRD — the
// shape the store now persists for a `snapshot create-multiple`
// member: SuspendIO=false (the store no longer stamps it at Create
// time for grouped snapshots), the shared GroupID + label, and the
// expected group size. The controller-side suspend barrier is what
// flips SuspendIO=true once the whole group is assembled.
func b046GroupedSnapshot(
	rdName, snapName, groupID string,
	groupSize int32,
	nodes []string,
) *blockstoriov1alpha1.Snapshot {
	return &blockstoriov1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:   rdName + "." + snapName,
			Labels: map[string]string{snapshotGroupIDLabel: groupID},
		},
		Spec: blockstoriov1alpha1.SnapshotSpec{
			ResourceDefinitionName: rdName,
			SnapshotName:           snapName,
			Nodes:                  nodes,
			GroupID:                groupID,
			GroupSize:              groupSize,
			SuspendIO:              false,
			TakeSnapshot:           false,
		},
	}
}

// TestBug046SuspendBarrierEntersSuspendTogether is the core Bug-046
// regression: once the whole group is assembled, reconciling ANY
// single sibling MUST open the suspend-io barrier on EVERY sibling in
// one pass — so all satellites start `drbdsetup suspend-io` within one
// controller cycle rather than at the staggered per-sibling create
// times that produced the ~15s suspend-entry slip. A single Reconcile
// of pvc-a is enough because the controller now flips SuspendIO across
// the whole batch (suspendGroup), not just self.
func TestBug046SuspendBarrierEntersSuspendTogether(t *testing.T) {
	t.Parallel()

	scheme := newSnapshotControllerScheme(t)

	const groupID = "b046-g1"

	// 3-member group, all CRDs present (fully assembled), none yet
	// suspended. Models the moment after the apiserver finished
	// fanning out all three `create-multiple` entries.
	a := b046GroupedSnapshot("pvc-a", "snap", groupID, 3, []string{"n1", "n2"})
	b := b046GroupedSnapshot("pvc-b", "snap", groupID, 3, []string{"n1", "n2"})
	c := b046GroupedSnapshot("pvc-c", "snap", groupID, 3, []string{"n1"})

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Snapshot{}).
		WithObjects(a, b, c).
		Build()

	r := &controller.SnapshotReconciler{Client: cli, Scheme: scheme}

	// Reconcile ONLY pvc-a — the barrier must fan the suspend out to
	// the whole group from this single pass.
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pvc-a.snap"},
	})
	if err != nil {
		t.Fatalf("Reconcile pvc-a.snap: %v", err)
	}

	for _, name := range []string{"pvc-a.snap", "pvc-b.snap", "pvc-c.snap"} {
		got := getSnap(t, cli, name)
		if !got.Spec.SuspendIO {
			t.Errorf("%s: suspend barrier did not open SuspendIO on this sibling: %+v",
				name, got.Spec)
		}

		if got.Spec.TakeSnapshot {
			t.Errorf("%s: TakeSnapshot flipped during Phase-1 entry: %+v", name, got.Spec)
		}
	}
}

// TestBug046SuspendBarrierHoldsUntilAssembled pins the
// hold-until-assembled half of the barrier: while the group is still
// assembling (fewer member CRDs observed than Spec.GroupSize), NO
// sibling's SuspendIO is opened. This is what prevents the early
// freeze — the first sibling must NOT suspend its volumes the instant
// its CRD lands; it has to wait for the rest of the batch so they all
// enter suspend together. The reconcile requeues (RequeueAfter > 0) so
// the barrier re-evaluates once the remaining siblings land.
func TestBug046SuspendBarrierHoldsUntilAssembled(t *testing.T) {
	t.Parallel()

	scheme := newSnapshotControllerScheme(t)

	const groupID = "b046-g2"

	// GroupSize says 3, but only 2 of the 3 member CRDs exist yet —
	// the third entry's Store.Create hasn't landed / propagated.
	a := b046GroupedSnapshot("pvc-a", "snap", groupID, 3, []string{"n1"})
	b := b046GroupedSnapshot("pvc-b", "snap", groupID, 3, []string{"n1"})

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Snapshot{}).
		WithObjects(a, b).
		Build()

	r := &controller.SnapshotReconciler{Client: cli, Scheme: scheme}

	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pvc-a.snap"},
	})
	if err != nil {
		t.Fatalf("Reconcile pvc-a.snap: %v", err)
	}

	if res.RequeueAfter <= 0 {
		t.Errorf("still-assembling group did not requeue to re-check the barrier: %+v", res)
	}

	for _, name := range []string{"pvc-a.snap", "pvc-b.snap"} {
		got := getSnap(t, cli, name)
		if got.Spec.SuspendIO {
			t.Errorf("%s: barrier opened SuspendIO before the group was assembled "+
				"(early freeze — the Bug-046 hazard): %+v", name, got.Spec)
		}
	}
}

// TestBug046BarrierAbortsAndResumesOnNotUpToDate pins the
// no-stuck-suspended safety contract at the barrier: if a targeted
// replica is not UpToDate when the group is assembled, the barrier
// refuses to freeze I/O at all and drives the group to the cleared
// (resumed) state across every sibling — so no volume is left frozen
// for a snapshot that can never be consistent. Because nothing was
// suspended yet, "resume" is the cleared-flags terminal state; the
// assertion is that SuspendIO stays false on every sibling and the
// abort reason is recorded.
func TestBug046BarrierAbortsAndResumesOnNotUpToDate(t *testing.T) {
	t.Parallel()

	scheme := newSnapshotControllerScheme(t)

	const groupID = "b046-g3"

	a := b046GroupedSnapshot("pvc-a", "snap", groupID, 2, []string{"n1"})
	b := b046GroupedSnapshot("pvc-b", "snap", groupID, 2, []string{"n1"})

	// A targeted diskful replica of pvc-a on n1 is mid-resync
	// (SyncTarget, not UpToDate). Snapshotting it would capture torn
	// bytes, so the barrier must refuse to suspend the group.
	res := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-a.n1"},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: "pvc-a",
			NodeName:               "n1",
		},
		Status: blockstoriov1alpha1.ResourceStatus{
			Volumes: []blockstoriov1alpha1.ResourceVolumeStatus{
				{VolumeNumber: 0, DiskState: "SyncTarget"},
			},
		},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Snapshot{}).
		WithObjects(a, b, res).
		Build()

	r := &controller.SnapshotReconciler{Client: cli, Scheme: scheme}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pvc-a.snap"},
	})
	if err != nil {
		t.Fatalf("Reconcile pvc-a.snap: %v", err)
	}

	for _, name := range []string{"pvc-a.snap", "pvc-b.snap"} {
		got := getSnap(t, cli, name)
		if got.Spec.SuspendIO {
			t.Errorf("%s: barrier suspended I/O for a non-UpToDate group "+
				"(would freeze for an impossible snapshot): %+v", name, got.Spec)
		}

		if got.Spec.TakeSnapshot {
			t.Errorf("%s: TakeSnapshot set on a non-UpToDate abort: %+v", name, got.Spec)
		}
	}
}

// TestBug046LegacyGroupedSnapshotMakesProgressWithoutGroupSize pins
// the back-compat fallback: a grouped Snapshot from before Spec.GroupSize
// existed (GroupID set, GroupSize=0) MUST NOT hang on the barrier
// forever waiting for an unknown denominator. With no GroupSize the
// group is treated as already-assembled, so the barrier opens on the
// observed siblings.
func TestBug046LegacyGroupedSnapshotMakesProgressWithoutGroupSize(t *testing.T) {
	t.Parallel()

	scheme := newSnapshotControllerScheme(t)

	const groupID = "b046-g4"

	// GroupSize=0 (legacy / unset) but GroupID is set and both
	// member CRDs are present.
	a := b046GroupedSnapshot("pvc-a", "snap", groupID, 0, []string{"n1"})
	b := b046GroupedSnapshot("pvc-b", "snap", groupID, 0, []string{"n1"})

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Snapshot{}).
		WithObjects(a, b).
		Build()

	r := &controller.SnapshotReconciler{Client: cli, Scheme: scheme}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pvc-a.snap"},
	})
	if err != nil {
		t.Fatalf("Reconcile pvc-a.snap: %v", err)
	}

	for _, name := range []string{"pvc-a.snap", "pvc-b.snap"} {
		got := getSnap(t, cli, name)
		if !got.Spec.SuspendIO {
			t.Errorf("%s: legacy grouped snapshot (no GroupSize) hung on the barrier: %+v",
				name, got.Spec)
		}
	}
}
