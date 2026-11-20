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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/internal/controller"
)

// successfulFixture builds a single-snapshot reconciler over a fake client
// holding the given snapshots. No Resource exists: the take's outcome is
// decided from the snapshot's own per-node record, never from the source's
// replicas, which may have been deleted by the time it is read.
func successfulFixture(
	t *testing.T, snaps ...*blockstoriov1alpha1.Snapshot,
) (*controller.SnapshotReconciler, client.Client) {
	t.Helper()

	scheme := newSnapshotControllerScheme(t)

	objs := make([]client.Object, 0, len(snaps))
	for _, snap := range snaps {
		objs = append(objs, snap)
	}

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Snapshot{}).
		WithObjects(objs...).
		Build()

	return &controller.SnapshotReconciler{Client: cli, Scheme: scheme}, cli
}

func reconcileSnapshot(t *testing.T, r *controller.SnapshotReconciler, name string) {
	t.Helper()

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: name},
	})
	if err != nil {
		t.Fatalf("Reconcile %s: %v", name, err)
	}
}

func isMarkedSuccessful(t *testing.T, cli client.Client, name string) bool {
	t.Helper()

	return slices.Contains(getSnap(t, cli, name).Status.Flags,
		blockstoriov1alpha1.SnapshotStatusFlagSuccessful)
}

func completedSnapshot(name string, nodes []string, status ...blockstoriov1alpha1.SnapshotPerNodeStatus) *blockstoriov1alpha1.Snapshot {
	return &blockstoriov1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "src." + name},
		Spec: blockstoriov1alpha1.SnapshotSpec{
			ResourceDefinitionName: "src",
			SnapshotName:           name,
			Nodes:                  nodes,
		},
		Status: blockstoriov1alpha1.SnapshotStatus{NodeStatus: status},
	}
}

// LINSTOR sets SUCCESSFUL on the snapshot definition once every node took
// its snapshot and I/O resumed, and the flag stays when the source volume
// is deleted. The controller records it on the Snapshot at the same point,
// with no source replica present, and keeps it on later passes.
func TestSnapshotControllerRecordsSuccessfulOnceTheTakeCompleted(t *testing.T) {
	t.Parallel()

	snap := completedSnapshot("snap-1", []string{"n1", "n2", "n3"},
		blockstoriov1alpha1.SnapshotPerNodeStatus{NodeName: "n1", Ready: true, CreateTimestamp: 1},
		blockstoriov1alpha1.SnapshotPerNodeStatus{NodeName: "n2", Ready: true, CreateTimestamp: 2},
		blockstoriov1alpha1.SnapshotPerNodeStatus{NodeName: "n3", Ready: true, CreateTimestamp: 3},
	)

	r, cli := successfulFixture(t, snap)

	reconcileSnapshot(t, r, snap.Name)

	if !isMarkedSuccessful(t, cli, snap.Name) {
		t.Fatalf("a snapshot taken on every node is not recorded SUCCESSFUL: flags=%v",
			getSnap(t, cli, snap.Name).Status.Flags)
	}

	reconcileSnapshot(t, r, snap.Name)

	got := getSnap(t, cli, snap.Name)
	if n := len(got.Status.Flags); n != 1 {
		t.Errorf("a second pass changed the record: flags=%v", got.Status.Flags)
	}

	if got.Spec.SuspendIO || got.Spec.TakeSnapshot {
		t.Errorf("recording SUCCESSFUL restarted the orchestration: %+v", got.Spec)
	}
}

// Like LINSTOR, which sets SUCCESSFUL after the resume, a take whose nodes
// are all Ready but whose I/O is still suspended is not recorded yet: the
// pass that resumes I/O comes first, the record on the next pass.
func TestSnapshotControllerRecordsSuccessfulAfterTheResume(t *testing.T) {
	t.Parallel()

	snap := completedSnapshot("snap-1", []string{"n1", "n2"},
		blockstoriov1alpha1.SnapshotPerNodeStatus{NodeName: "n1", Ready: true, CreateTimestamp: 1, SuspendIOAcked: true},
		blockstoriov1alpha1.SnapshotPerNodeStatus{NodeName: "n2", Ready: true, CreateTimestamp: 2, SuspendIOAcked: true},
	)
	snap.Spec.SuspendIO = true
	snap.Spec.TakeSnapshot = true

	r, cli := successfulFixture(t, snap)

	reconcileSnapshot(t, r, snap.Name)

	got := getSnap(t, cli, snap.Name)
	if got.Spec.SuspendIO || got.Spec.TakeSnapshot {
		t.Fatalf("the pass did not resume I/O: %+v", got.Spec)
	}

	if isMarkedSuccessful(t, cli, snap.Name) {
		t.Fatalf("SUCCESSFUL recorded while I/O was still suspended")
	}

	clearSuspendAcks(t, cli, snap.Name)
	reconcileSnapshot(t, r, snap.Name)

	if !isMarkedSuccessful(t, cli, snap.Name) {
		t.Errorf("SUCCESSFUL not recorded once I/O resumed")
	}
}

// LINSTOR sets SUCCESSFUL only after every satellite answered the resume.
// The controller clearing Spec.SuspendIO is the order to resume, not the
// resume: a satellite whose resume-io fails keeps its acknowledgment and
// retries, its volume still frozen, and the snapshot is not SUCCESSFUL
// until that node confirms.
func TestSnapshotControllerWaitsForEveryNodeToConfirmTheResume(t *testing.T) {
	t.Parallel()

	snap := completedSnapshot("snap-1", []string{"n1", "n2"},
		blockstoriov1alpha1.SnapshotPerNodeStatus{NodeName: "n1", Ready: true, CreateTimestamp: 1},
		blockstoriov1alpha1.SnapshotPerNodeStatus{NodeName: "n2", Ready: true, CreateTimestamp: 2, SuspendIOAcked: true},
	)

	r, cli := successfulFixture(t, snap)

	reconcileSnapshot(t, r, snap.Name)

	if isMarkedSuccessful(t, cli, snap.Name) {
		t.Fatalf("SUCCESSFUL recorded while n2 had not confirmed the resume")
	}

	got := getSnap(t, cli, snap.Name)
	if got.Spec.SuspendIO || got.Spec.TakeSnapshot {
		t.Fatalf("waiting for the resume restarted the orchestration: %+v", got.Spec)
	}

	clearSuspendAcks(t, cli, snap.Name)
	reconcileSnapshot(t, r, snap.Name)

	if !isMarkedSuccessful(t, cli, snap.Name) {
		t.Errorf("SUCCESSFUL not recorded once every node confirmed the resume")
	}
}

// clearSuspendAcks writes what each satellite writes once its resume-io
// succeeded.
func clearSuspendAcks(t *testing.T, cli client.Client, name string) {
	t.Helper()

	snap := getSnap(t, cli, name)
	for i := range snap.Status.NodeStatus {
		snap.Status.NodeStatus[i].SuspendIOAcked = false
	}

	if err := cli.Status().Update(context.Background(), snap); err != nil {
		t.Fatalf("clear suspend acks on %s: %v", name, err)
	}
}

// A take still in flight on a targeted node, or one that ended in a failure
// marker, is not recorded SUCCESSFUL.
func TestSnapshotControllerDoesNotRecordAnIncompleteOrFailedTake(t *testing.T) {
	t.Parallel()

	partial := completedSnapshot("partial", []string{"n1", "n2"},
		blockstoriov1alpha1.SnapshotPerNodeStatus{NodeName: "n1", Ready: true, CreateTimestamp: 1},
		blockstoriov1alpha1.SnapshotPerNodeStatus{NodeName: "n2"},
	)

	disconnected := completedSnapshot("disconnected", []string{"n1"},
		blockstoriov1alpha1.SnapshotPerNodeStatus{NodeName: "n1", Ready: true, CreateTimestamp: 1},
	)
	disconnected.Status.Flags = []string{blockstoriov1alpha1.SnapshotStatusFlagFailedDisconnect}

	failed := completedSnapshot("failed", []string{"n1", "n2"},
		blockstoriov1alpha1.SnapshotPerNodeStatus{NodeName: "n1", Ready: true, CreateTimestamp: 1},
		blockstoriov1alpha1.SnapshotPerNodeStatus{NodeName: "n2", Ready: true, Failed: true},
	)
	failed.Status.Flags = []string{blockstoriov1alpha1.SnapshotStatusFlagFailed}

	r, cli := successfulFixture(t, partial, disconnected, failed)

	for _, snap := range []*blockstoriov1alpha1.Snapshot{partial, disconnected, failed} {
		reconcileSnapshot(t, r, snap.Name)

		if isMarkedSuccessful(t, cli, snap.Name) {
			t.Errorf("%s: recorded SUCCESSFUL: flags=%v", snap.Name, getSnap(t, cli, snap.Name).Status.Flags)
		}
	}
}

// The record covers the nodes the snapshot was told to take on: one taken
// on a single listed node of a multi-replica definition is SUCCESSFUL as
// soon as that node is, and an entry the satellite wrote under its own node
// spelling counts for the node as the snapshot lists it.
func TestSnapshotControllerRecordsSuccessfulOverTheListedNodes(t *testing.T) {
	t.Parallel()

	scoped := completedSnapshot("scoped", []string{"Node-1"},
		blockstoriov1alpha1.SnapshotPerNodeStatus{NodeName: "node-1", Ready: true, CreateTimestamp: 1},
	)

	// My_Node and my_node fall outside RFC 1123 and are two nodes.
	apart := completedSnapshot("apart", []string{"My_Node", "my_node"},
		blockstoriov1alpha1.SnapshotPerNodeStatus{NodeName: "my_node", Ready: true, CreateTimestamp: 1},
	)

	r, cli := successfulFixture(t, scoped, apart)

	reconcileSnapshot(t, r, scoped.Name)

	if !isMarkedSuccessful(t, cli, scoped.Name) {
		t.Errorf("a snapshot taken on its one listed node, reported as node-1 for Node-1, is not SUCCESSFUL")
	}

	reconcileSnapshot(t, r, apart.Name)

	if isMarkedSuccessful(t, cli, apart.Name) {
		t.Errorf("a snapshot reported on my_node alone is SUCCESSFUL on My_Node too")
	}
}

// A group snapshot is SUCCESSFUL only as a whole, as LINSTOR marks every
// definition of a multi-snapshot request once the request completed: a
// sibling's failed node keeps every sibling unmarked.
func TestSnapshotControllerGroupIsRecordedOnlyAsAWhole(t *testing.T) {
	t.Parallel()

	ok := b353GroupedSnapshot("rd-a", "g", "grp", []string{"n1"}, false, false,
		[]blockstoriov1alpha1.SnapshotPerNodeStatus{{NodeName: "n1", Ready: true, CreateTimestamp: 1}})
	bad := b353GroupedSnapshot("rd-b", "g", "grp", []string{"n1"}, false, false,
		[]blockstoriov1alpha1.SnapshotPerNodeStatus{{NodeName: "n1", Failed: true}})

	r, cli := successfulFixture(t, ok, bad)

	reconcileAll(t, r, []string{ok.Name, bad.Name})

	for _, name := range []string{ok.Name, bad.Name} {
		if isMarkedSuccessful(t, cli, name) {
			t.Errorf("%s: recorded SUCCESSFUL although a sibling failed", name)
		}
	}
}
