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

package dispatcher_test

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
	"github.com/cozystack/blockstor/pkg/dispatcher"
)

// TestBuildDesiredRendersTargetNodeIDZero is the Bug 342 C3
// manifestation-2 regression pin. A target legitimately allocated
// node-id 0 (LowestFreeNodeID hands out 0 for the first replica) MUST
// render `node-id 0` in its own drbd_options — NOT be dropped by an
// absent-key zero-collision. Before C3 this happened to work for the
// target only because nodeIDOf>=0 kept it in idOf; the regression
// pin guards against a refactor reintroducing the int32 map.
func TestBuildDesiredRendersTargetNodeIDZero(t *testing.T) {
	rdName := "pvc-c3"
	targetID := int32(0)
	peerID := int32(1)

	rd := &blockstoriov1alpha1.ResourceDefinition{
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024},
			},
		},
	}

	target := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: rdName + "-n1"},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: rdName,
			NodeName:               "n1",
			StoragePool:            "pool",
		},
		Status: blockstoriov1alpha1.ResourceStatus{DRBDNodeID: &targetID},
	}

	peer := blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: rdName + "-n2"},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: rdName,
			NodeName:               "n2",
			StoragePool:            "pool",
		},
		Status: blockstoriov1alpha1.ResourceStatus{DRBDNodeID: &peerID},
	}

	got := dispatcher.BuildDesired(target, []blockstoriov1alpha1.Resource{peer}, nil, nil, rd, nil)
	if got == nil {
		t.Fatalf("BuildDesired returned nil")
	}

	if got.DrbdOptions["node-id"] != "0" {
		t.Errorf("target node-id = %q, want \"0\" (allocated id 0 must render, not collapse to absent)",
			got.DrbdOptions["node-id"])
	}

	if got.DrbdOptions["peer.n2.node-id"] != "1" {
		t.Errorf("peer.n2.node-id = %q, want \"1\"", got.DrbdOptions["peer.n2.node-id"])
	}
}

// TestBuildDesiredOmitsUnresolvedPeer pins the Bug 342 C3
// manifestation-2 skip contract: a peer whose Status.DRBDNodeID is
// still nil (controller-side allocator hasn't stamped) must be OMITTED
// from the rendered config — no `peer.<name>.node-id` key, no
// DesiredPeer entry — rather than rendered with a Go-zero-default
// node-id 0 that would collide with the local node (the
// `peer node id cannot be my own node id` / new-peer ... 0 wedge).
func TestBuildDesiredOmitsUnresolvedPeer(t *testing.T) {
	rdName := "pvc-c3-unres"
	targetID := int32(0)

	rd := &blockstoriov1alpha1.ResourceDefinition{
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024},
			},
		},
	}

	target := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: rdName + "-n1"},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: rdName,
			NodeName:               "n1",
			StoragePool:            "pool",
		},
		Status: blockstoriov1alpha1.ResourceStatus{DRBDNodeID: &targetID},
	}

	// Peer with NO allocated DRBDNodeID (nil Status) — the
	// allocation-window state behind manifestation 2.
	peer := blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: rdName + "-n2"},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: rdName,
			NodeName:               "n2",
			StoragePool:            "pool",
		},
	}

	got := dispatcher.BuildDesired(target, []blockstoriov1alpha1.Resource{peer}, nil, nil, rd, nil)
	if got == nil {
		t.Fatalf("BuildDesired returned nil")
	}

	if _, ok := got.DrbdOptions["peer.n2.node-id"]; ok {
		t.Errorf("unresolved peer n2 must be omitted, but peer.n2.node-id=%q was rendered",
			got.DrbdOptions["peer.n2.node-id"])
	}

	for _, p := range got.Peers {
		if p.Name == "n2" {
			t.Errorf("unresolved peer n2 must not appear in DesiredPeers, got %+v", p)
		}
	}

	// The target's own node-id still renders normally.
	if got.DrbdOptions["node-id"] != "0" {
		t.Errorf("target node-id = %q, want \"0\"", got.DrbdOptions["node-id"])
	}
}

// TestBuildDesiredPeerNodeIDZeroPropagates pins that a peer
// legitimately on node-id 0 (e.g. after the local node relocated to a
// higher id) is rendered with peer.<name>.node-id 0 AND carries a
// non-nil DesiredPeer.NodeID so the satellite's EvictPeersByUIDMismatch
// can act on it instead of deferring forever.
func TestBuildDesiredPeerNodeIDZeroPropagates(t *testing.T) {
	rdName := "pvc-c3-peer0"
	targetID := int32(1)
	peerID := int32(0)

	rd := &blockstoriov1alpha1.ResourceDefinition{
		Spec: blockstoriov1alpha1.ResourceDefinitionSpec{
			VolumeDefinitions: []blockstoriov1alpha1.ResourceDefinitionVolume{
				{VolumeNumber: 0, SizeKib: 1024 * 1024},
			},
		},
	}

	target := &blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: rdName + "-n1"},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: rdName,
			NodeName:               "n1",
			StoragePool:            "pool",
		},
		Status: blockstoriov1alpha1.ResourceStatus{DRBDNodeID: &targetID},
	}

	peer := blockstoriov1alpha1.Resource{
		ObjectMeta: metav1.ObjectMeta{Name: rdName + "-n2", UID: "peer-uid"},
		Spec: blockstoriov1alpha1.ResourceSpec{
			ResourceDefinitionName: rdName,
			NodeName:               "n2",
			StoragePool:            "pool",
		},
		Status: blockstoriov1alpha1.ResourceStatus{DRBDNodeID: &peerID},
	}

	got := dispatcher.BuildDesired(target, []blockstoriov1alpha1.Resource{peer}, nil, nil, rd, nil)
	if got == nil {
		t.Fatalf("BuildDesired returned nil")
	}

	if got.DrbdOptions["peer.n2.node-id"] != "0" {
		t.Errorf("peer.n2.node-id = %q, want \"0\"", got.DrbdOptions["peer.n2.node-id"])
	}

	var found bool

	for _, p := range got.Peers {
		if p.Name != "n2" {
			continue
		}

		found = true

		if p.NodeID == nil {
			t.Fatalf("peer n2 DesiredPeer.NodeID is nil, want non-nil pointer to 0")
		}

		if *p.NodeID != 0 {
			t.Errorf("peer n2 DesiredPeer.NodeID = %d, want 0", *p.NodeID)
		}
	}

	if !found {
		t.Fatalf("peer n2 missing from DesiredPeers")
	}
}
