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

package k8s

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	crdv1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
)

// TestCrdToWireSnapshotStatusNodeStatus pins the per-node status
// flatten: each Status.NodeStatus row surfaces as one
// apiv1.SnapshotPerNode entry on the wire so /v1/view/snapshots
// shows linstor-csi which nodes have completed the snapshot.
//
// Internal test (package k8s) so we can construct a CRD with the
// Status subresource populated directly — there's no public Set
// path for snapshot status today (the Snapshot reconciler writes
// it via the ctrl-runtime Status() client).
func TestCrdToWireSnapshotStatusNodeStatus(t *testing.T) {
	t.Parallel()

	crd := &crdv1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pvc-1.snap-1",
		},
		Spec: crdv1alpha1.SnapshotSpec{
			ResourceDefinitionName: "pvc-1",
			SnapshotName:           "snap-1",
		},
		Status: crdv1alpha1.SnapshotStatus{
			NodeStatus: []crdv1alpha1.SnapshotPerNodeStatus{
				{NodeName: "n1", CreateTimestamp: 1714000000, Ready: true},
				{NodeName: "n2", CreateTimestamp: 1714000050, Ready: true},
			},
		},
	}

	got := crdToWireSnapshot(crd, nil)

	if len(got.Snapshots) != 2 {
		t.Fatalf("Snapshots: got %d, want 2", len(got.Snapshots))
	}

	for i, want := range []struct {
		node string
		ts   int64
	}{
		{"n1", 1714000000},
		{"n2", 1714000050},
	} {
		if got.Snapshots[i].NodeName != want.node {
			t.Errorf("[%d] NodeName: got %q, want %q",
				i, got.Snapshots[i].NodeName, want.node)
		}

		if got.Snapshots[i].CreateTimestamp != want.ts {
			t.Errorf("[%d] CreateTimestamp: got %d, want %d",
				i, got.Snapshots[i].CreateTimestamp, want.ts)
		}

		// Every per-node row must carry the parent SnapshotName so
		// linstor-csi's CreateSnapshot poll loop can correlate.
		if got.Snapshots[i].SnapshotName != "snap-1" {
			t.Errorf("[%d] SnapshotName: got %q, want snap-1",
				i, got.Snapshots[i].SnapshotName)
		}
	}
}

// TestWireToCRDSnapshotSuspendIOByGroup pins the Bug-046 / Bug-353
// suspend-deferral contract at the store boundary:
//
//   - A SINGLE snapshot (empty GroupID) keeps the Bug-351 behaviour —
//     SuspendIO=true stamped at Create time so its lone satellite begins
//     the suspend immediately.
//   - A GROUPED snapshot (non-empty GroupID) must NOT be stamped
//     SuspendIO=true at Create time. The controller-side suspend barrier
//     owns that flip and only opens it once the whole group is assembled,
//     so the siblings enter suspend together rather than ~15s apart. The
//     GroupSize denominator the barrier gates on must carry through.
func TestWireToCRDSnapshotSuspendIOByGroup(t *testing.T) {
	t.Parallel()

	single := wireToCRDSnapshot(&apiv1.Snapshot{
		ResourceName: "pvc-a",
		Name:         "snap-1",
		Nodes:        []string{"n1"},
	})
	if !single.Spec.SuspendIO {
		t.Errorf("single snapshot: SuspendIO=false at Create, want true (Bug-351 path)")
	}

	if single.Spec.GroupID != "" {
		t.Errorf("single snapshot: GroupID=%q, want empty", single.Spec.GroupID)
	}

	grouped := wireToCRDSnapshot(&apiv1.Snapshot{
		ResourceName: "pvc-a",
		Name:         "snap-1",
		Nodes:        []string{"n1"},
		GroupID:      "g-batch-1",
		GroupSize:    3,
	})
	if grouped.Spec.SuspendIO {
		t.Errorf("grouped snapshot: SuspendIO=true at Create — early freeze, " +
			"the Bug-046 hazard; the controller barrier must own the flip")
	}

	if grouped.Spec.GroupID != "g-batch-1" {
		t.Errorf("grouped snapshot: GroupID=%q, want g-batch-1", grouped.Spec.GroupID)
	}

	if grouped.Spec.GroupSize != 3 {
		t.Errorf("grouped snapshot: GroupSize=%d, want 3", grouped.Spec.GroupSize)
	}

	// The GroupID label must be mirrored so the controller can List
	// siblings by selector.
	if grouped.Labels[LabelSnapshotGroupID] != "g-batch-1" {
		t.Errorf("grouped snapshot: group-id label=%q, want g-batch-1",
			grouped.Labels[LabelSnapshotGroupID])
	}
}
