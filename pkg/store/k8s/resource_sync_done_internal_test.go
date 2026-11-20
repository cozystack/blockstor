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
)

// TestCrdToWireResourceCarriesDonePerPeerAndVolume pins where the
// resync progress comes from on the wire: each peer's
// `replication_states[peer].done_percentage` is that peer's `done`
// for that volume, the field LINSTOR's controller fills from the
// satellite (Json.apiToVolumeState) and its client prints. A peer with
// no running resync has none, and an unparsable token reports none
// rather than a made-up figure.
func TestCrdToWireResourceCarriesDonePerPeerAndVolume(t *testing.T) {
	t.Parallel()

	crd := &crdv1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-x.node-c"},
		Spec:       crdv1alpha1.ResourceSpec{ResourceDefinitionName: "pvc-x", NodeName: "node-c"},
		Status: crdv1alpha1.ResourceStatus{
			Volumes: []crdv1alpha1.ResourceVolumeStatus{
				{VolumeNumber: 0, DiskState: "Inconsistent", OutOfSyncKib: 1258291},
				{VolumeNumber: 1, DiskState: "Inconsistent"},
			},
			Connections: []crdv1alpha1.ResourceConnectionStatus{
				{
					PeerNodeName:     "node-a",
					Connected:        true,
					ReplicationState: "SyncTarget",
					PeerVolumes: []crdv1alpha1.PeerVolumeStatus{
						{VolumeNumber: 0, PeerDiskState: "UpToDate", Done: "50.31"},
						{VolumeNumber: 1, PeerDiskState: "UpToDate", Done: "7.5"},
					},
				},
				{
					PeerNodeName:     "node-b",
					Connected:        true,
					ReplicationState: "Established",
					PeerVolumes: []crdv1alpha1.PeerVolumeStatus{
						{VolumeNumber: 0, PeerDiskState: "UpToDate"},
						{VolumeNumber: 1, PeerDiskState: "UpToDate", Done: "garbage"},
					},
				},
			},
		},
	}

	wire := crdToWireResource(crd)
	if len(wire.Volumes) != 2 {
		t.Fatalf("volumes: got %d, want 2", len(wire.Volumes))
	}

	for _, want := range []struct {
		volume int
		peer   string
		done   *float64
	}{
		{0, "node-a", ptrFloat64(50.31)},
		{1, "node-a", ptrFloat64(7.5)},
		{0, "node-b", nil},
		{1, "node-b", nil},
	} {
		got, ok := wire.Volumes[want.volume].State.ReplicationStates[want.peer]
		if !ok {
			t.Fatalf("volume %d: no replication state for %s", want.volume, want.peer)
		}

		switch {
		case want.done == nil && got.DonePercentage != nil:
			t.Errorf("volume %d peer %s: done_percentage = %v, want none", want.volume, want.peer, *got.DonePercentage)
		case want.done != nil && (got.DonePercentage == nil || *got.DonePercentage != *want.done):
			t.Errorf("volume %d peer %s: done_percentage = %v, want %v", want.volume, want.peer, got.DonePercentage, *want.done)
		}
	}
}

func ptrFloat64(v float64) *float64 {
	return &v
}

// TestCrdToWireResourceReadsReplicationPerVolume pins that each
// volume's replication state with a peer is that volume's own. DRBD
// and LINSTOR keep it per peer and per volume; the connection-level
// field is whichever volume's peer-device frame came last. A
// two-volume resource whose volume 0 resyncs while volume 1's frame
// said Established last must still report volume 0 in SyncTarget with
// its done, and volume 1 must not borrow volume 0's SyncTarget. A
// volume with no per-volume entry falls back to the connection.
func TestCrdToWireResourceReadsReplicationPerVolume(t *testing.T) {
	t.Parallel()

	crd := &crdv1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-x.node-c"},
		Spec:       crdv1alpha1.ResourceSpec{ResourceDefinitionName: "pvc-x", NodeName: "node-c"},
		Status: crdv1alpha1.ResourceStatus{
			Volumes: []crdv1alpha1.ResourceVolumeStatus{
				{VolumeNumber: 0, DiskState: "Inconsistent"},
				{VolumeNumber: 1, DiskState: "Inconsistent"},
				{VolumeNumber: 2, DiskState: "UpToDate"},
			},
			Connections: []crdv1alpha1.ResourceConnectionStatus{
				{
					PeerNodeName:     "node-a",
					Connected:        true,
					ReplicationState: "PausedSyncT",
					PeerVolumes: []crdv1alpha1.PeerVolumeStatus{
						{VolumeNumber: 0, PeerDiskState: "UpToDate", ReplicationState: "SyncTarget", Done: "50.31"},
						{VolumeNumber: 1, PeerDiskState: "UpToDate", ReplicationState: "PausedSyncT", Done: "3.00"},
					},
				},
			},
		},
	}

	wire := crdToWireResource(crd)
	if len(wire.Volumes) != 3 {
		t.Fatalf("volumes: got %d, want 3", len(wire.Volumes))
	}

	for _, want := range []struct {
		volume int
		repl   string
	}{
		{0, "SyncTarget"},
		{1, "PausedSyncT"},
		{2, "PausedSyncT"}, // no per-volume entry: the connection's
	} {
		got := wire.Volumes[want.volume].State.ReplicationStates["node-a"]
		if got.ReplicationState != want.repl {
			t.Errorf("volume %d: replication_state = %q, want %q", want.volume, got.ReplicationState, want.repl)
		}
	}

	if done, ok := wire.Volumes[0].State.SyncProgress("SyncTarget"); !ok || done != 50.31 {
		t.Errorf("volume 0: SyncTarget progress = %v, %v; want 50.31, true", done, ok)
	}

	if done, ok := wire.Volumes[1].State.SyncProgress("SyncTarget"); ok {
		t.Errorf("volume 1: SyncTarget progress = %v, want none (it is paused)", done)
	}
}
