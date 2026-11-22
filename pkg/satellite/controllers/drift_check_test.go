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

// TestDriftCheckPassesEveryReplicaWhenDrbdadmCannotTell: drbd-utils
// before 9.32 has no sh-list-adjustable, and drbdadm fails on a .res it
// cannot parse. LINSTOR then adjusts every resource; so does the check.
func TestDriftCheckPassesEveryReplicaWhenDrbdadmCannotTell(t *testing.T) {
	exec := storage.NewFakeExec()
	exec.Expect(listAdjustable, storage.FakeResponse{Err: storage.FakeExitError{Code: 20}})

	check, out := driftFixture(t, exec)

	check.check(t.Context(), logr.Discard())

	if got, want := drained(out), []string{"pvc-a.n1", "pvc-b.n1", "pvc-c.n1"}; !slices.Equal(got, want) {
		t.Errorf("passes for %v, want every local replica %v", got, want)
	}
}
