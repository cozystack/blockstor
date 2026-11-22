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

package controllers

import (
	"slices"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/storage"
)

const listAdjustable = "drbdadm sh-list-adjustable all"

// driftFixture is a node n1 holding replicas of pvc-a, pvc-b and pvc-c,
// with a replica of pvc-a on n2 as well.
func driftFixture(t *testing.T, exec storage.Exec) (*driftCheck, chan event.GenericEvent) {
	t.Helper()

	replica := func(rd, node string) *blockstoriov1alpha1.Resource {
		return &blockstoriov1alpha1.Resource{
			ObjectMeta: metav1.ObjectMeta{Name: rd + "." + node},
			Spec:       blockstoriov1alpha1.ResourceSpec{NodeName: node, ResourceDefinitionName: rd},
		}
	}

	cli := fake.NewClientBuilder().
		WithScheme(newToggleDiskTestScheme(t)).
		WithObjects(replica("pvc-a", "n1"), replica("pvc-b", "n1"), replica("pvc-c", "n1"), replica("pvc-a", "n2")).
		Build()

	out := make(chan event.GenericEvent, 16)

	return &driftCheck{reader: cli, exec: exec, node: "n1", out: out}, out
}

func drained(out chan event.GenericEvent) []string {
	var names []string

	for {
		select {
		case ev := <-out:
			names = append(names, ev.Object.GetName())
		default:
			slices.Sort(names)

			return names
		}
	}
}

// TestDriftCheckPassesWhatDrbdadmLists: a converged replica gets no pass
// of its own, so the only way kernel drift reaches it is the drift
// check. It passes exactly the local replicas drbdadm lists as
// adjustable, whatever the spelling drbdadm prints, and nothing when
// drbdadm lists nothing.
func TestDriftCheckPassesWhatDrbdadmLists(t *testing.T) {
	exec := storage.NewFakeExec()
	exec.ExpectSequence(listAdjustable,
		storage.FakeResponse{Stdout: []byte("PVC-A\npvc-c\nnot-ours\n")},
		storage.FakeResponse{Stdout: []byte("")},
	)

	check, out := driftFixture(t, exec)

	check.check(t.Context(), logr.Discard())

	if got, want := drained(out), []string{"pvc-a.n1", "pvc-c.n1"}; !slices.Equal(got, want) {
		t.Errorf("passes for %v, want %v", got, want)
	}

	check.check(t.Context(), logr.Discard())

	if got := drained(out); len(got) != 0 {
		t.Errorf("passes for %v with nothing adjustable", got)
	}
}

// TestDriftCheckPassesEveryReplicaOnceWhenDrbdadmFails: drbdadm fails
// on a .res it cannot parse, and LINSTOR then adjusts every resource of
// the run. The check passes every local replica when the failure
// starts, and not again every interval while it lasts: a sweep of every
// replica every 30 s is the periodic full pass the check replaced.
func TestDriftCheckPassesEveryReplicaOnceWhenDrbdadmFails(t *testing.T) {
	parseErr := storage.FakeResponse{
		Err: errors.Wrap(storage.FakeExitError{Code: 10}, "drbdadm sh-list-adjustable all: pvc-x.res:3: parse error"),
	}

	exec := storage.NewFakeExec()
	exec.ExpectSequence(listAdjustable,
		parseErr, parseErr,
		storage.FakeResponse{Stdout: []byte("")},
		parseErr,
	)

	check, out := driftFixture(t, exec)
	every := []string{"pvc-a.n1", "pvc-b.n1", "pvc-c.n1"}

	check.check(t.Context(), logr.Discard())

	if got := drained(out); !slices.Equal(got, every) {
		t.Errorf("passes for %v when the failure starts, want every local replica %v", got, every)
	}

	check.check(t.Context(), logr.Discard())

	if got := drained(out); len(got) != 0 {
		t.Errorf("passes for %v while the failure lasts, want none", got)
	}

	check.check(t.Context(), logr.Discard())
	check.check(t.Context(), logr.Discard())

	if got := drained(out); !slices.Equal(got, every) {
		t.Errorf("passes for %v when a new failure starts, want every local replica %v", got, every)
	}
}

// TestDriftCheckTurnsOffWithoutListAdjustable: Debian's drbd-utils
// 9.22, which the satellite image on main installs, has no
// sh-list-adjustable. LINSTOR does not ask such a drbdadm and runs
// nothing periodic, so the check sends nothing and stops, where it used
// to pass every replica on the node every 30 s.
func TestDriftCheckTurnsOffWithoutListAdjustable(t *testing.T) {
	exec := storage.NewFakeExec()
	exec.Expect(listAdjustable, storage.FakeResponse{
		Err: errors.Wrap(storage.FakeExitError{Code: 1},
			"drbdadm sh-list-adjustable all: drbdadm: Unknown command 'sh-list-adjustable'"),
	})

	check, out := driftFixture(t, exec)
	check.interval = time.Millisecond

	done := make(chan error, 1)

	go func() { done <- check.run(logr.Discard())(t.Context()) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("the drift check ended with %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the drift check kept running on a drbdadm without sh-list-adjustable")
	}

	if got := drained(out); len(got) != 0 {
		t.Errorf("passes for %v on a drbdadm without sh-list-adjustable, want none", got)
	}

	if calls := len(exec.CallsSnapshot()); calls != 1 {
		t.Errorf("drbdadm asked %d times, want once", calls)
	}
}
