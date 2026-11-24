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
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/internal/controller"
)

// reconcileSnap runs one controller pass on the named Snapshot.
func reconcileSnap(t *testing.T, r *controller.SnapshotReconciler, name string) {
	t.Helper()

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: name},
	})
	if err != nil {
		t.Fatalf("Reconcile %s: %v", name, err)
	}
}

// snapGone reports whether the named Snapshot is gone or being deleted.
func snapGone(t *testing.T, cli client.Client, name string) bool {
	t.Helper()

	var got blockstoriov1alpha1.Snapshot

	err := cli.Get(context.Background(), client.ObjectKey{Name: name}, &got)
	if apierrors.IsNotFound(err) {
		return true
	}

	if err != nil {
		t.Fatalf("Get %s: %v", name, err)
	}

	return !got.DeletionTimestamp.IsZero()
}

// TestNotUpToDateBarrierAbortDeletesGroup pins LINSTOR's handling of a
// take refused on a non-UpToDate replica: every snapshot of the request
// is deleted. Kept, the snapshot outlived the CSI snapshot that asked
// for it and kept its resource definition from being deleted.
func TestNotUpToDateBarrierAbortDeletesGroup(t *testing.T) {
	t.Parallel()

	scheme := newSnapshotControllerScheme(t)

	const groupID = "refused-g1"

	a := b046GroupedSnapshot("pvc-a", "snap", groupID, 2, []string{"n1", "n2"})
	b := b046GroupedSnapshot("pvc-b", "snap", groupID, 2, []string{"n1"})

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Snapshot{}).
		WithObjects(a, b,
			resourceWithDiskState("pvc-a", "n1", "UpToDate"),
			resourceWithDiskState("pvc-a", "n2", "Outdated"),
			resourceWithDiskState("pvc-b", "n1", "UpToDate")).
		Build()

	r := &controller.SnapshotReconciler{Client: cli, Scheme: scheme}

	// The first pass refuses the take and records why; the write it
	// makes brings the group back for the next pass.
	reconcileSnap(t, r, "pvc-a.snap")
	reconcileSnap(t, r, "pvc-b.snap")

	for _, name := range []string{"pvc-a.snap", "pvc-b.snap"} {
		if !snapGone(t, cli, name) {
			t.Errorf("%s: snapshot refused on a non-UpToDate replica was kept", name)
		}
	}
}

// TestNotUpToDateAbortWaitsForResumeBeforeDelete pins the order of the
// delete after a take refused once I/O was already suspended: a
// satellite tears a deleting snapshot down without resuming, so the
// delete must wait until every node reported the resume.
func TestNotUpToDateAbortWaitsForResumeBeforeDelete(t *testing.T) {
	t.Parallel()

	scheme := newSnapshotControllerScheme(t)

	snap := suspendedSnapshot("pvc-1.snap-1", "pvc-1", "snap-1",
		20*time.Second, []string{"n1", "n2"}, false,
		[]blockstoriov1alpha1.SnapshotPerNodeStatus{
			{NodeName: "n1", SuspendIOAcked: true},
			{NodeName: "n2", SuspendIOAcked: true},
		})

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Snapshot{}).
		WithObjects(snap,
			resourceWithDiskState("pvc-1", "n1", "UpToDate"),
			resourceWithDiskState("pvc-1", "n2", "SyncTarget")).
		Build()

	r := &controller.SnapshotReconciler{Client: cli, Scheme: scheme}

	reconcileSnap(t, r, "pvc-1.snap-1")
	reconcileSnap(t, r, "pvc-1.snap-1")

	if snapGone(t, cli, "pvc-1.snap-1") {
		t.Fatal("snapshot deleted while its nodes still hold I/O suspended")
	}

	got := getSnap(t, cli, "pvc-1.snap-1")
	got.Status.NodeStatus = []blockstoriov1alpha1.SnapshotPerNodeStatus{
		{NodeName: "n1"},
		{NodeName: "n2"},
	}

	err := cli.Status().Update(context.Background(), got)
	if err != nil {
		t.Fatalf("report resume: %v", err)
	}

	reconcileSnap(t, r, "pvc-1.snap-1")

	if !snapGone(t, cli, "pvc-1.snap-1") {
		t.Error("snapshot refused on a non-UpToDate replica was kept after every node resumed")
	}
}

// TestNotUpToDateAbortLeftByEarlierVersionIsDeleted pins the cleanup of
// the record an earlier controller left: the refused take with its
// reason and FAILED_DISCONNECT, nothing suspended. Each wording a
// released controller recorded is listed, so a reword that stops
// matching one of them fails here instead of silently disabling the
// sweep on upgrade.
func TestNotUpToDateAbortLeftByEarlierVersionIsDeleted(t *testing.T) {
	t.Parallel()

	for _, tail := range []string{"I/O resumed", "resume of I/O requested", "I/O not suspended"} {
		t.Run(tail, func(t *testing.T) {
			t.Parallel()

			scheme := newSnapshotControllerScheme(t)

			snap := b046GroupedSnapshot("pvc-a", "snap", "refused-g2", 1, []string{"n1", "n2", "n3"})
			snap.Spec.AbortReason = "snapshot aborted: replica on node 'n1' is not UpToDate; " + tail
			snap.Status.Flags = []string{blockstoriov1alpha1.SnapshotStatusFlagFailedDisconnect}

			cli := fake.NewClientBuilder().
				WithScheme(scheme).
				WithStatusSubresource(&blockstoriov1alpha1.Snapshot{}).
				WithObjects(snap).
				Build()

			r := &controller.SnapshotReconciler{Client: cli, Scheme: scheme}

			reconcileSnap(t, r, "pvc-a.snap")

			if !snapGone(t, cli, "pvc-a.snap") {
				t.Error("refused snapshot left by an earlier version was kept")
			}
		})
	}
}

// TestNotUpToDateAbortKeepsSuccessfulGroup pins that a group with a
// sibling recorded SUCCESSFUL is never deleted as a refusal: the
// record contradicts itself, and the successful snapshot may already
// back a VolumeSnapshotContent.
func TestNotUpToDateAbortKeepsSuccessfulGroup(t *testing.T) {
	t.Parallel()

	scheme := newSnapshotControllerScheme(t)

	const groupID = "refused-g3"

	reason := "snapshot aborted: replica on node 'n1' is not UpToDate; I/O not suspended"

	a := b046GroupedSnapshot("pvc-a", "snap", groupID, 2, []string{"n1"})
	a.Spec.AbortReason = reason
	a.Status.Flags = []string{blockstoriov1alpha1.SnapshotStatusFlagFailedDisconnect}

	b := b046GroupedSnapshot("pvc-b", "snap", groupID, 2, []string{"n1"})
	b.Status.Flags = []string{blockstoriov1alpha1.SnapshotStatusFlagSuccessful}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Snapshot{}).
		WithObjects(a, b).
		Build()

	r := &controller.SnapshotReconciler{Client: cli, Scheme: scheme}

	reconcileSnap(t, r, "pvc-a.snap")
	reconcileSnap(t, r, "pvc-b.snap")

	for _, name := range []string{"pvc-a.snap", "pvc-b.snap"} {
		if snapGone(t, cli, name) {
			t.Errorf("%s: group with a SUCCESSFUL sibling was deleted as a refusal", name)
		}
	}
}

// TestNotUpToDateAbortWithFailedNodeIsDeleted pins that a refusal on a
// non-UpToDate replica is deleted even when a node also stamped its
// take failed. That branch handles the group before the recorded
// refusal is looked at; LINSTOR deletes the refusal whatever else
// happened.
func TestNotUpToDateAbortWithFailedNodeIsDeleted(t *testing.T) {
	t.Parallel()

	scheme := newSnapshotControllerScheme(t)

	snap := b046GroupedSnapshot("pvc-a", "snap", "refused-g4", 1, []string{"n1", "n2"})
	snap.Spec.AbortReason = "snapshot aborted: replica on node 'n1' is not UpToDate; I/O resumed"
	snap.Status.Flags = []string{blockstoriov1alpha1.SnapshotStatusFlagFailedDisconnect}
	snap.Status.NodeStatus = []blockstoriov1alpha1.SnapshotPerNodeStatus{
		{NodeName: "n1"},
		{NodeName: "n2", Failed: true},
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Snapshot{}).
		WithObjects(snap).
		Build()

	r := &controller.SnapshotReconciler{Client: cli, Scheme: scheme}

	reconcileSnap(t, r, "pvc-a.snap")

	if !snapGone(t, cli, "pvc-a.snap") {
		t.Error("refusal on a non-UpToDate replica kept because a node also failed")
	}
}

// TestDeadlineAbortKeepsSnapshot pins the scope of the delete: LINSTOR
// keeps a snapshot whose take failed for any other reason, flagged
// FAILED, for linstor-csi to delete when it retries the create.
func TestDeadlineAbortKeepsSnapshot(t *testing.T) {
	t.Parallel()

	scheme := newSnapshotControllerScheme(t)

	snap := suspendedSnapshot("pvc-1.snap-1", "pvc-1", "snap-1",
		3*time.Minute, []string{"n1"}, false, nil)

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Snapshot{}).
		WithObjects(snap).
		Build()

	r := &controller.SnapshotReconciler{Client: cli, Scheme: scheme}

	reconcileSnap(t, r, "pvc-1.snap-1")
	reconcileSnap(t, r, "pvc-1.snap-1")

	if snapGone(t, cli, "pvc-1.snap-1") {
		t.Error("snapshot aborted on its deadline was deleted; LINSTOR keeps it flagged FAILED")
	}
}
