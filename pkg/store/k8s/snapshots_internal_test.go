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

	got := crdToWireSnapshot(crd)

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
