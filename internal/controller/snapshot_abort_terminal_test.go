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
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/internal/controller"
)

var errStatusWriteRefused = errors.New("status write refused")

// abortFixture builds a reconciler over a fake client holding objs. While
// refuseStatus is true every Snapshot status write fails, standing in for
// an apiserver that takes the spec write and then rejects the stamp.
func abortFixture(
	t *testing.T, refuseStatus *atomic.Bool, objs ...client.Object,
) (*controller.SnapshotReconciler, client.Client) {
	t.Helper()

	scheme := newSnapshotControllerScheme(t)

	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Snapshot{}).
		WithObjects(objs...).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceUpdate: func(
				ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption,
			) error {
				if _, isSnap := obj.(*blockstoriov1alpha1.Snapshot); isSnap && refuseStatus.Load() {
					return errStatusWriteRefused
				}

				return c.SubResource(sub).Update(ctx, obj, opts...)
			},
		}).
		Build()

	return &controller.SnapshotReconciler{Client: cli, Scheme: scheme}, cli
}

func reconcileIgnoringError(r *controller.SnapshotReconciler, name string) {
	_, _ = r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKey{Name: name}})
}

func hasFlag(snap *blockstoriov1alpha1.Snapshot, flag string) bool {
	return slices.Contains(snap.Status.Flags, flag)
}

// LINSTOR flags a snapshot definition failed when its take times out and
// never takes it again. A take whose deadline passed while one node still
// holds I/O suspended and another never reported is aborted once: no later
// pass orders a new suspend.
func TestSnapshotAbortedTakeIsNeverSuspendedAgain(t *testing.T) {
	t.Parallel()

	snap := suspendedSnapshot("pvc-1.snap-1", "pvc-1", "snap-1", 3*time.Minute,
		[]string{"n1", "n2"}, false,
		[]blockstoriov1alpha1.SnapshotPerNodeStatus{{NodeName: "n1", SuspendIOAcked: true}})

	var refuse atomic.Bool

	r, cli := abortFixture(t, &refuse, snap)

	for pass := range 4 {
		reconcileIgnoringError(r, snap.Name)

		got := getSnap(t, cli, snap.Name)
		if got.Spec.SuspendIO || got.Spec.TakeSnapshot {
			t.Fatalf("pass %d ordered suspendIO=%t takeSnapshot=%t on an aborted take",
				pass, got.Spec.SuspendIO, got.Spec.TakeSnapshot)
		}
	}

	got := getSnap(t, cli, snap.Name)
	if got.Spec.AbortReason == "" {
		t.Errorf("the abort is not on record in the spec")
	}

	if !hasFlag(got, blockstoriov1alpha1.SnapshotStatusFlagFailedDisconnect) {
		t.Errorf("the abort is not on record in the status: flags=%v", got.Status.Flags)
	}
}

// The abort is recorded by the write that orders the resume. When the
// status stamp that follows fails, and the node whose take hung reports
// Ready later, the take still ends failed, never SUCCESSFUL.
func TestSnapshotAbortHoldsWhenTheStatusStampFails(t *testing.T) {
	t.Parallel()

	snap := suspendedSnapshot("pvc-1.snap-1", "pvc-1", "snap-1", 3*time.Minute,
		[]string{"n1", "n2"}, true,
		[]blockstoriov1alpha1.SnapshotPerNodeStatus{
			{NodeName: "n1", SuspendIOAcked: true, Ready: true},
			{NodeName: "n2", SuspendIOAcked: true},
		})

	var refuse atomic.Bool

	refuse.Store(true)

	r, cli := abortFixture(t, &refuse, snap)

	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(snap)})
	if err == nil {
		t.Fatal("Reconcile hid the failed status stamp")
	}

	got := getSnap(t, cli, snap.Name)
	if got.Spec.SuspendIO || got.Spec.TakeSnapshot {
		t.Fatalf("the resume was not ordered before the stamp: %+v", got.Spec)
	}

	if got.Spec.AbortReason == "" {
		t.Fatal("nothing records the abort once the status stamp failed")
	}

	// n2's hung take returns: it reports Ready, and both nodes resume.
	refuse.Store(false)

	got.Status.NodeStatus = []blockstoriov1alpha1.SnapshotPerNodeStatus{
		{NodeName: "n1", Ready: true, CreateTimestamp: 1},
		{NodeName: "n2", Ready: true, CreateTimestamp: 2},
	}
	if err := cli.Status().Update(context.Background(), got); err != nil {
		t.Fatal(err)
	}

	reconcileSnapshot(t, r, snap.Name)
	reconcileSnapshot(t, r, snap.Name)

	got = getSnap(t, cli, snap.Name)
	if hasFlag(got, blockstoriov1alpha1.SnapshotStatusFlagSuccessful) {
		t.Fatalf("an aborted take was recorded SUCCESSFUL: flags=%v", got.Status.Flags)
	}

	if !hasFlag(got, blockstoriov1alpha1.SnapshotStatusFlagFailedDisconnect) {
		t.Errorf("the retried pass did not stamp the abort: flags=%v", got.Status.Flags)
	}
}

// A grouped abort whose write reached one sibling but not the other is
// finished by the next pass over either: the sibling still suspended is
// resumed and its abort recorded, though its own deadline has not passed.
func TestSnapshotGroupAbortReachesTheSiblingItMissed(t *testing.T) {
	t.Parallel()

	aborted := b353GroupedSnapshot("pvc-a", "snap-1", "g1", []string{"n1"}, false, false, nil)
	aborted.Spec.AbortReason = "snapshot aborted: test"

	missed := b353GroupedSnapshot("pvc-b", "snap-1", "g1", []string{"n2"}, true, false,
		[]blockstoriov1alpha1.SnapshotPerNodeStatus{{NodeName: "n2", SuspendIOAcked: true}})

	var refuse atomic.Bool

	r, cli := abortFixture(t, &refuse, aborted, missed)

	reconcileSnapshot(t, r, missed.Name)

	got := getSnap(t, cli, missed.Name)
	if got.Spec.SuspendIO || got.Spec.TakeSnapshot {
		t.Fatalf("the sibling the abort missed stays suspended: %+v", got.Spec)
	}

	if got.Spec.AbortReason != aborted.Spec.AbortReason {
		t.Errorf("abort reason on the missed sibling = %q, want %q", got.Spec.AbortReason, aborted.Spec.AbortReason)
	}
}

// A snapshot an earlier controller aborted carries only the status flag,
// and that controller may already have ordered a new suspend. The flag
// alone ends the take: the suspend is cleared, not carried on.
func TestSnapshotAbortedBeforeTheSpecRecordIsResumed(t *testing.T) {
	t.Parallel()

	snap := suspendedSnapshot("pvc-1.snap-1", "pvc-1", "snap-1", 10*time.Second,
		[]string{"n1", "n2"}, false,
		[]blockstoriov1alpha1.SnapshotPerNodeStatus{{NodeName: "n1", SuspendIOAcked: true}})
	snap.Status.Flags = []string{blockstoriov1alpha1.SnapshotStatusFlagFailedDisconnect}

	var refuse atomic.Bool

	r, cli := abortFixture(t, &refuse, snap)

	reconcileSnapshot(t, r, snap.Name)

	got := getSnap(t, cli, snap.Name)
	if got.Spec.SuspendIO || got.Spec.TakeSnapshot {
		t.Fatalf("a take flagged FAILED_DISCONNECT is still suspended: %+v", got.Spec)
	}

	if got.Spec.AbortReason == "" {
		t.Errorf("the abort is not recorded in the spec")
	}
}

// A SUCCESSFUL take is over too. A node that no longer reports its row
// does not make the snapshot look unstarted: no suspend is ordered for it.
func TestSnapshotSuccessfulTakeIsNeverSuspendedAgain(t *testing.T) {
	t.Parallel()

	snap := completedSnapshot("snap-1", []string{"n1", "n2"},
		blockstoriov1alpha1.SnapshotPerNodeStatus{NodeName: "n1", Ready: true, CreateTimestamp: 1})
	snap.Status.Flags = []string{blockstoriov1alpha1.SnapshotStatusFlagSuccessful}

	r, cli := successfulFixture(t, snap)

	reconcileSnapshot(t, r, snap.Name)

	got := getSnap(t, cli, snap.Name)
	if got.Spec.SuspendIO || got.Spec.TakeSnapshot {
		t.Fatalf("a SUCCESSFUL snapshot was ordered into a new take: %+v", got.Spec)
	}
}

// A pass that works from a listing taken before a sibling's abort landed
// still cannot freeze that sibling again: the write re-reads it, sees the
// abort, and leaves it alone. The pass may still suspend its own snapshot,
// whose abort has not landed yet; the next pass, on a current listing,
// aborts it with the rest of the group. Only the aborted sibling is
// asserted on here.
func TestSnapshotStaleGroupViewDoesNotReSuspendAnAbortedSibling(t *testing.T) {
	t.Parallel()

	fresh := b353GroupedSnapshot("pvc-a", "snap-1", "g1", []string{"n1"}, false, false, nil)

	aborted := b353GroupedSnapshot("pvc-b", "snap-1", "g1", []string{"n2"}, false, false, nil)
	aborted.Spec.AbortReason = "snapshot aborted: test"

	// The listing still shows the sibling as it was before the abort.
	stale := aborted.DeepCopy()
	stale.Spec.AbortReason = ""

	scheme := newSnapshotControllerScheme(t)
	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Snapshot{}).
		WithObjects(fresh, aborted).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				err := c.List(ctx, list, opts...)

				snaps, isSnaps := list.(*blockstoriov1alpha1.SnapshotList)
				if err != nil || !isSnaps {
					return err
				}

				for i := range snaps.Items {
					if snaps.Items[i].Name == aborted.Name {
						rv := snaps.Items[i].ResourceVersion
						snaps.Items[i] = *stale.DeepCopy()
						snaps.Items[i].ResourceVersion = rv
					}
				}

				return nil
			},
		}).
		Build()
	r := &controller.SnapshotReconciler{Client: cli, Scheme: scheme}

	reconcileSnapshot(t, r, fresh.Name)

	got := getSnap(t, cli, aborted.Name)
	if got.Spec.SuspendIO || got.Spec.TakeSnapshot {
		t.Fatalf("a stale view of the group froze the aborted sibling again: %+v", got.Spec)
	}
}

// Whatever left a SUCCESSFUL snapshot with I/O ordered suspended, the
// recorded outcome wins: the next pass orders the resume instead of
// waiting for acks to a take that is already over.
func TestSnapshotSuccessfulTakeLeftSuspendedIsResumed(t *testing.T) {
	t.Parallel()

	snap := suspendedSnapshot("pvc-1.snap-1", "pvc-1", "snap-1", 10*time.Second,
		[]string{"n1"}, false,
		[]blockstoriov1alpha1.SnapshotPerNodeStatus{{NodeName: "n1", Ready: true, CreateTimestamp: 1}})
	snap.Status.Flags = []string{blockstoriov1alpha1.SnapshotStatusFlagSuccessful}

	r, cli := successfulFixture(t, snap)

	reconcileSnapshot(t, r, snap.Name)

	got := getSnap(t, cli, snap.Name)
	if got.Spec.SuspendIO || got.Spec.TakeSnapshot {
		t.Fatalf("a SUCCESSFUL snapshot stays ordered suspended: %+v", got.Spec)
	}
}

// An outcome already on the status is the one that stays: a take a
// satellite flagged FAILED is not re-flagged FAILED_DISCONNECT, nor its
// reason replaced, by the passes that finish the abort.
func TestSnapshotAbortKeepsTheOutcomeAlreadyRecorded(t *testing.T) {
	t.Parallel()

	snap := completedSnapshot("snap-1", []string{"n1"})
	snap.Status.Flags = []string{blockstoriov1alpha1.SnapshotStatusFlagFailed}

	r, cli := successfulFixture(t, snap)

	reconcileSnapshot(t, r, snap.Name)

	got := getSnap(t, cli, snap.Name)
	if !slices.Equal(got.Status.Flags, []string{blockstoriov1alpha1.SnapshotStatusFlagFailed}) {
		t.Errorf("flags after the abort pass = %v, want only FAILED", got.Status.Flags)
	}

	if len(got.Status.Conditions) != 0 {
		t.Errorf("the abort pass rewrote the conditions: %+v", got.Status.Conditions)
	}
}

// A node that reports Failed on a snapshot already SUCCESSFUL does not
// turn it into an aborted take: the success stays the only record.
func TestSnapshotSuccessfulTakeGetsNoAbortRecord(t *testing.T) {
	t.Parallel()

	snap := completedSnapshot("snap-1", []string{"n1"},
		blockstoriov1alpha1.SnapshotPerNodeStatus{NodeName: "n1", Ready: true, Failed: true, CreateTimestamp: 1})
	snap.Status.Flags = []string{blockstoriov1alpha1.SnapshotStatusFlagSuccessful}

	r, cli := successfulFixture(t, snap)

	reconcileSnapshot(t, r, snap.Name)

	got := getSnap(t, cli, snap.Name)
	if got.Spec.AbortReason != "" {
		t.Errorf("a SUCCESSFUL snapshot got an abort record: %q", got.Spec.AbortReason)
	}
}

// A take whose abort record was lost, as to an Update by a satellite
// that predates the field, looks unstarted while a node's suspend ack
// lingers. Past its deadline it is not started again: no suspend is
// ordered, and the abort is recorded.
func TestSnapshotPastItsDeadlineIsNotSuspended(t *testing.T) {
	t.Parallel()

	snap := suspendedSnapshot("pvc-1.snap-1", "pvc-1", "snap-1", 3*time.Minute,
		[]string{"n1", "n2"}, false,
		[]blockstoriov1alpha1.SnapshotPerNodeStatus{{NodeName: "n1", SuspendIOAcked: true}})
	snap.Spec.SuspendIO = false

	r, cli := successfulFixture(t, snap)

	reconcileSnapshot(t, r, snap.Name)

	got := getSnap(t, cli, snap.Name)
	if got.Spec.SuspendIO {
		t.Fatalf("a take past its deadline was ordered to suspend: %+v", got.Spec)
	}

	if got.Spec.AbortReason == "" {
		t.Errorf("the take past its deadline is not recorded aborted")
	}
}

// The success stamp re-reads the snapshot before writing. A pass that
// decided on a view older than the abort now on record writes nothing.
func TestSnapshotSuccessStampYieldsToALaterAbort(t *testing.T) {
	t.Parallel()

	snap := completedSnapshot("snap-1", []string{"n1"},
		blockstoriov1alpha1.SnapshotPerNodeStatus{NodeName: "n1", Ready: true, CreateTimestamp: 1})
	snap.Spec.AbortReason = "snapshot aborted: test"

	stale := snap.DeepCopy()
	stale.Spec.AbortReason = ""

	var served atomic.Bool

	scheme := newSnapshotControllerScheme(t)
	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Snapshot{}).
		WithObjects(snap).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				err := c.Get(ctx, key, obj, opts...)

				got, isSnap := obj.(*blockstoriov1alpha1.Snapshot)
				if err != nil || !isSnap || served.Swap(true) {
					return err
				}

				// The reconcile's first read predates the abort.
				rv := got.ResourceVersion
				*got = *stale.DeepCopy()
				got.ResourceVersion = rv

				return nil
			},
		}).
		Build()
	r := &controller.SnapshotReconciler{Client: cli, Scheme: scheme}

	reconcileIgnoringError(r, snap.Name)

	if hasFlag(getSnap(t, cli, snap.Name), blockstoriov1alpha1.SnapshotStatusFlagSuccessful) {
		t.Fatal("a stale pass recorded SUCCESSFUL over an abort on record")
	}
}
