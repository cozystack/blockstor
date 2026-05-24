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
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	blockstoriov1alpha1 "github.com/cozystack/blockstor/api/v1alpha1"
)

// TestDesiredPeersFromCRDsZeroVsNil is the Bug 342 C3 round-trip pin:
// a peer with Status.DRBDNodeID = &0 must produce a DesiredPeer whose
// NodeID is a non-nil pointer to 0 (a real allocated id), while a peer
// with nil Status.DRBDNodeID must produce a nil NodeID (unallocated).
// The pre-C3 int32 field collapsed both to 0, which is the collision
// behind both manifestations.
func TestDesiredPeersFromCRDsZeroVsNil(t *testing.T) {
	zero := int32(0)

	peers := []blockstoriov1alpha1.Resource{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "rd-w-allocated0", UID: "uid-alloc0"},
			Spec:       blockstoriov1alpha1.ResourceSpec{NodeName: "w-allocated0"},
			Status:     blockstoriov1alpha1.ResourceStatus{DRBDNodeID: &zero},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "rd-w-unallocated", UID: "uid-unalloc"},
			Spec:       blockstoriov1alpha1.ResourceSpec{NodeName: "w-unallocated"},
			// Status.DRBDNodeID intentionally nil.
		},
	}

	got := desiredPeersFromCRDs(peers)
	if len(got) != 2 {
		t.Fatalf("got %d DesiredPeers, want 2: %+v", len(got), got)
	}

	byName := map[string]int{}
	for i := range got {
		byName[got[i].Name] = i
	}

	alloc := got[byName["w-allocated0"]]
	if alloc.NodeID == nil {
		t.Errorf("allocated-id-0 peer: NodeID is nil, want non-nil *0")
	} else if *alloc.NodeID != 0 {
		t.Errorf("allocated-id-0 peer: NodeID = %d, want 0", *alloc.NodeID)
	}

	if alloc.ResourceUID != "uid-alloc0" {
		t.Errorf("allocated peer UID = %q, want uid-alloc0", alloc.ResourceUID)
	}

	unalloc := got[byName["w-unallocated"]]
	if unalloc.NodeID != nil {
		t.Errorf("unallocated peer: NodeID = *%d, want nil", *unalloc.NodeID)
	}
}
