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
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/internal/controller"
)

func ioSuspendedCond(snap *blockstoriov1alpha1.Snapshot) *metav1.Condition {
	for i := range snap.Status.Conditions {
		if snap.Status.Conditions[i].Type == blockstoriov1alpha1.SnapshotIOSuspendedConditionType {
			return &snap.Status.Conditions[i]
		}
	}

	return nil
}

// An abort only asks the satellites to resume. Until each one's kernel
// confirms, the snapshot says I/O is suspended there and names the
// node; the abort message itself claims no resume.
func TestSnapshotAbortReportsIOSuspendedUntilEveryReplicaResumes(t *testing.T) {
	t.Parallel()

	scheme := newSnapshotControllerScheme(t)

	snap := suspendedSnapshot("pvc-1.snap-1", "pvc-1", "snap-1",
		3*time.Minute, []string{"n1", "n2"}, false,
		[]blockstoriov1alpha1.SnapshotPerNodeStatus{
			{NodeName: "n1", SuspendIOAcked: true},
		})

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Snapshot{}).WithObjects(snap).Build()
	r := &controller.SnapshotReconciler{Client: cli, Scheme: scheme}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "pvc-1.snap-1"}}

	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got := getSnap(t, cli, "pvc-1.snap-1")
	if got.Spec.SuspendIO {
		t.Fatalf("deadline did not abort: %+v", got.Spec)
	}

	for _, cond := range got.Status.Conditions {
		if cond.Type == blockstoriov1alpha1.SnapshotStatusConditionType && strings.Contains(cond.Message, "I/O resumed") {
			t.Errorf("abort claims a resume nobody confirmed: %q", cond.Message)
		}
	}

	cond := ioSuspendedCond(got)
	if cond == nil || cond.Status != metav1.ConditionTrue || !strings.Contains(cond.Message, "n1") {
		t.Fatalf("IOSuspended while n1 still acks the suspend: %+v", cond)
	}

	// n1's satellite confirms the resume.
	got.Status.NodeStatus[0].SuspendIOAcked = false
	if err := cli.Status().Update(context.Background(), got); err != nil {
		t.Fatal(err)
	}

	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	cond = ioSuspendedCond(getSnap(t, cli, "pvc-1.snap-1"))
	if cond == nil || cond.Status != metav1.ConditionFalse {
		t.Errorf("IOSuspended after every replica resumed: %+v", cond)
	}
}

// A snapshot that never suspended gets no IOSuspended condition.
func TestSnapshotNeverSuspendedHasNoIOSuspendedCondition(t *testing.T) {
	t.Parallel()

	scheme := newSnapshotControllerScheme(t)
	snap := suspendedSnapshot("pvc-1.snap-1", "pvc-1", "snap-1", time.Second, []string{"n1"}, false, nil)

	cli := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&blockstoriov1alpha1.Snapshot{}).WithObjects(snap).Build()
	r := &controller.SnapshotReconciler{Client: cli, Scheme: scheme}

	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(snap)}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if cond := ioSuspendedCond(getSnap(t, cli, "pvc-1.snap-1")); cond != nil {
		t.Errorf("IOSuspended on a snapshot no node suspended: %+v", cond)
	}
}

var errStatusRefused = errors.New("status subresource refused the write")

// The IOSuspended stamp runs first in a pass, and a status subresource
// that refuses writes must not keep a take past its deadline frozen:
// the abort's resume is a spec write and still goes out, and the stamp
// error is reported once it has.
func TestSnapshotAbortResumesWhenTheIOSuspendedStampFails(t *testing.T) {
	t.Parallel()

	scheme := newSnapshotControllerScheme(t)

	snap := suspendedSnapshot("pvc-1.snap-1", "pvc-1", "snap-1",
		3*time.Minute, []string{"n1"}, false,
		[]blockstoriov1alpha1.SnapshotPerNodeStatus{
			{NodeName: "n1", SuspendIOAcked: true},
		})

	cli := interceptor.NewClient(
		fake.NewClientBuilder().WithScheme(scheme).
			WithStatusSubresource(&blockstoriov1alpha1.Snapshot{}).WithObjects(snap).Build(),
		interceptor.Funcs{
			SubResourceUpdate: func(context.Context, client.Client, string, client.Object, ...client.SubResourceUpdateOption) error {
				return errStatusRefused
			},
			SubResourcePatch: func(context.Context, client.Client, string, client.Object, client.Patch, ...client.SubResourcePatchOption) error {
				return errStatusRefused
			},
		},
	)
	r := &controller.SnapshotReconciler{Client: cli, Scheme: scheme}

	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(snap)})
	if !errors.Is(err, errStatusRefused) {
		t.Errorf("Reconcile returned %v, want the refused status write reported", err)
	}

	if got := getSnap(t, cli, "pvc-1.snap-1"); got.Spec.SuspendIO {
		t.Fatalf("a refused status write kept I/O suspended past the deadline: %+v", got.Spec)
	}
}
