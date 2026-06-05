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
	"slices"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/internal/controller"
)

// This file pins the campaign-2 upstream-mined snapshot-robustness
// corner cases U138 / U52 / U258 at the controller-orchestration
// layer. Each test is the controller-side half of the
// guaranteed-unwind contract: ANY failure between the suspend-io
// freeze and the resume drain MUST drive the orchestration toward
// resume so the application writer is never left hung on a frozen
// DRBD device.

// TestU138_TakePhaseFailureResumesIO is the precise U138 scenario:
// suspend-io has ALREADY been acked on every targeted node (the freeze
// is live), the orchestration promoted to Phase 2, and then one node's
// provider.CreateSnapshot fails terminally (stamps Failed=true) AFTER
// the freeze. The controller MUST treat this as an abort and clear
// Spec.SuspendIO so the still-frozen peer resumes — upstream LINSTOR's
// bug left the suspended peers frozen forever when the take failed
// post-suspend, hanging the workload indefinitely.
//
// This differs from the existing AbortPathClearsSuspendOnFailure
// (which fails DURING Phase 1, before TakeSnapshot): here TakeSnapshot
// is already true, exercising the Phase-2-in-flight abort branch.
func TestU138_TakePhaseFailureResumesIO(t *testing.T) {
	t.Parallel()

	scheme := newSnapshotControllerScheme(t)

	// Phase 2 in flight: both nodes acked suspend, controller already
	// flipped TakeSnapshot=true, n1 took its snapshot (Ready) but n2
	// hit a terminal backend failure mid-take and stamped Failed=true.
	snap := suspendedSnapshot("ccu3-u138.snap", "ccu3-u138", "snap",
		20*time.Second, []string{"n1", "n2"}, true,
		[]blockstoriov1alpha1.SnapshotPerNodeStatus{
			{NodeName: "n1", SuspendIOAcked: true, Ready: true, CreateTimestamp: 1},
			{NodeName: "n2", SuspendIOAcked: true, Failed: true},
		})

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Snapshot{}).
		WithObjects(snap).
		Build()

	r := &controller.SnapshotReconciler{Client: cli, Scheme: scheme}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "ccu3-u138.snap"},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got := getSnap(t, cli, "ccu3-u138.snap")

	if got.Spec.SuspendIO {
		t.Errorf("U138 regression: take-phase failure left Spec.SuspendIO=true "+
			"(frozen peer never resumes); spec=%+v", got.Spec)
	}

	if got.Spec.TakeSnapshot {
		t.Errorf("U138: take-phase abort did not clear Spec.TakeSnapshot: %+v", got.Spec)
	}
}

// TestU52_SiblingSnapshotIsolation pins U52: a snapshot op failure on
// one resource must stay isolated — an UNRELATED snapshot (different
// RD, no shared GroupID) keeps its own suspend/take state and is NOT
// dragged into the failed one's abort. The b353 abort cascade is
// scoped to same-GroupID siblings only; a single-snap (empty GroupID)
// neighbour must be untouched.
//
// Setup: two independent single-snap Snapshots. The first is mid-Phase-1
// with a Failed node (will abort itself). The second is mid-Phase-1 with
// both nodes acked and healthy. Reconciling the FAILED one must not
// clear the HEALTHY one's SuspendIO — and reconciling the healthy one
// then promotes it normally to Phase 2.
func TestU52_SiblingSnapshotIsolation(t *testing.T) {
	t.Parallel()

	scheme := newSnapshotControllerScheme(t)

	failed := suspendedSnapshot("ccu3-u52a.snap", "ccu3-u52a", "snap",
		20*time.Second, []string{"n1", "n2"}, false,
		[]blockstoriov1alpha1.SnapshotPerNodeStatus{
			{NodeName: "n1", SuspendIOAcked: true},
			{NodeName: "n2", Failed: true},
		})

	healthy := suspendedSnapshot("ccu3-u52b.snap", "ccu3-u52b", "snap",
		20*time.Second, []string{"n1", "n2"}, false,
		[]blockstoriov1alpha1.SnapshotPerNodeStatus{
			{NodeName: "n1", SuspendIOAcked: true},
			{NodeName: "n2", SuspendIOAcked: true},
		})

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Snapshot{}).
		WithObjects(failed, healthy,
			resourceWithDiskState("ccu3-u52b", "n1", "UpToDate"),
			resourceWithDiskState("ccu3-u52b", "n2", "UpToDate")).
		Build()

	r := &controller.SnapshotReconciler{Client: cli, Scheme: scheme}

	// Reconcile the FAILED sibling first — it aborts itself.
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "ccu3-u52a.snap"},
	})
	if err != nil {
		t.Fatalf("Reconcile failed-sibling: %v", err)
	}

	// The healthy neighbour MUST be untouched by the other's abort.
	gotHealthy := getSnap(t, cli, "ccu3-u52b.snap")
	if !gotHealthy.Spec.SuspendIO {
		t.Errorf("U52 isolation breach: unrelated snapshot lost SuspendIO "+
			"due to a sibling RD's failure; spec=%+v", gotHealthy.Spec)
	}

	if slices.Contains(gotHealthy.Status.Flags, blockstoriov1alpha1.SnapshotStatusFlagFailed) ||
		slices.Contains(gotHealthy.Status.Flags, blockstoriov1alpha1.SnapshotStatusFlagFailedDisconnect) {
		t.Errorf("U52 isolation breach: unrelated snapshot stamped a failure flag: %v",
			gotHealthy.Status.Flags)
	}

	// And the failed one DID abort (resume).
	gotFailed := getSnap(t, cli, "ccu3-u52a.snap")
	if gotFailed.Spec.SuspendIO {
		t.Errorf("failed sibling did not resume its own I/O: %+v", gotFailed.Spec)
	}

	// The healthy snapshot still converges on its own: reconciling it
	// promotes to Phase 2 (full IO + progress unaffected by the
	// neighbour's failure).
	_, err = r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "ccu3-u52b.snap"},
	})
	if err != nil {
		t.Fatalf("Reconcile healthy-sibling: %v", err)
	}

	gotHealthy = getSnap(t, cli, "ccu3-u52b.snap")
	if !gotHealthy.Spec.TakeSnapshot {
		t.Errorf("U52: healthy snapshot failed to progress to Phase 2 after a "+
			"neighbour's failure; spec=%+v", gotHealthy.Spec)
	}
}

// TestU258_SyncingReplicaRefusedAndResumes pins the U258 SYNCING
// variant of the non-UpToDate consistency gate. When a targeted
// replica is mid-resync, snapshotting it would capture torn bytes.
// blockstor's behaviour (matching upstream LINSTOR's "Cannot take
// snapshot from non-UpToDate DRBD device" refusal): abort the
// snapshot and resume I/O rather than capture a bad point-in-time.
//
// The existing TestSnapshotPhase2AbortsOnNonUpToDate already pins
// SyncTarget; this table-test adds Inconsistent and Outdated so every
// non-UpToDate DRBD disk-state the resync window can surface is pinned
// to the same abort+resume outcome.
func TestU258_SyncingReplicaRefusedAndResumes(t *testing.T) {
	t.Parallel()

	for _, badState := range []string{"Inconsistent", "Outdated", "SyncTarget"} {
		t.Run(badState, func(t *testing.T) {
			t.Parallel()

			scheme := newSnapshotControllerScheme(t)

			snap := suspendedSnapshot("ccu3-u258.snap", "ccu3-u258", "snap",
				20*time.Second, []string{"n1", "n2"}, false,
				[]blockstoriov1alpha1.SnapshotPerNodeStatus{
					{NodeName: "n1", SuspendIOAcked: true},
					{NodeName: "n2", SuspendIOAcked: true},
				})

			cli := fake.NewClientBuilder().
				WithScheme(scheme).
				WithStatusSubresource(&blockstoriov1alpha1.Snapshot{}).
				WithObjects(snap,
					resourceWithDiskState("ccu3-u258", "n1", "UpToDate"),
					resourceWithDiskState("ccu3-u258", "n2", badState)).
				Build()

			r := &controller.SnapshotReconciler{Client: cli, Scheme: scheme}

			_, err := r.Reconcile(context.Background(), ctrl.Request{
				NamespacedName: types.NamespacedName{Name: "ccu3-u258.snap"},
			})
			if err != nil {
				t.Fatalf("Reconcile: %v", err)
			}

			got := getSnap(t, cli, "ccu3-u258.snap")

			if got.Spec.TakeSnapshot {
				t.Errorf("U258: promoted Phase 2 against a %s replica (torn-data risk): %+v",
					badState, got.Spec)
			}

			if got.Spec.SuspendIO {
				t.Errorf("U258: %s abort left SuspendIO=true (I/O still frozen): %+v",
					badState, got.Spec)
			}
		})
	}
}
