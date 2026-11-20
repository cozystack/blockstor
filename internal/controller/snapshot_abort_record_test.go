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
	"sync/atomic"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// During a rollout a satellite or API server that predates
// Spec.AbortReason writes the Snapshot back without it, and the status
// stamp of the abort may not have landed. The copy of the record in an
// annotation survives that write: once the nodes report Ready and
// resume, the take still ends failed, never SUCCESSFUL.
func TestSnapshotAbortRecordSurvivesAWriterThatDropsTheField(t *testing.T) {
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

	reconcileIgnoringError(r, snap.Name)

	// An older writer puts the object back without the field.
	got := getSnap(t, cli, snap.Name)
	if got.Spec.AbortReason == "" {
		t.Fatal("the abort was not recorded")
	}

	got.Spec.AbortReason = ""
	if err := cli.Update(context.Background(), got); err != nil {
		t.Fatal(err)
	}

	refuse.Store(false)

	got = getSnap(t, cli, snap.Name)
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
		t.Fatalf("an aborted take whose spec record was dropped was recorded SUCCESSFUL: flags=%v", got.Status.Flags)
	}

	if !hasFlag(got, blockstoriov1alpha1.SnapshotStatusFlagFailedDisconnect) {
		t.Errorf("the abort was not stamped from the surviving record: flags=%v", got.Status.Flags)
	}
}

// The SnapshotComplete=False condition is written by the status stamp of
// an abort and is untouched by any spec write. A snapshot carrying it is
// an aborted take, never SUCCESSFUL, even with both spec copies gone.
func TestSnapshotAbortConditionIsNeverRecordedSuccessful(t *testing.T) {
	t.Parallel()

	snap := completedSnapshot("snap-1", []string{"n1"},
		blockstoriov1alpha1.SnapshotPerNodeStatus{NodeName: "n1", Ready: true, CreateTimestamp: 1})
	snap.Status.Conditions = []metav1.Condition{{
		Type:               blockstoriov1alpha1.SnapshotStatusConditionType,
		Status:             metav1.ConditionFalse,
		Reason:             "Aborted",
		Message:            "snapshot aborted: test",
		LastTransitionTime: metav1.Now(),
	}}

	r, cli := successfulFixture(t, snap)

	reconcileSnapshot(t, r, snap.Name)

	got := getSnap(t, cli, snap.Name)
	if hasFlag(got, blockstoriov1alpha1.SnapshotStatusFlagSuccessful) {
		t.Fatalf("a snapshot carrying the abort condition was recorded SUCCESSFUL: flags=%v", got.Status.Flags)
	}

	if !hasFlag(got, blockstoriov1alpha1.SnapshotStatusFlagFailedDisconnect) {
		t.Errorf("the abort condition did not end the take failed: flags=%v", got.Status.Flags)
	}
}
